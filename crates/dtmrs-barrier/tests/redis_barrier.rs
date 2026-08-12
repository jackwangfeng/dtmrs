//! Redis 屏障：跟 SQL 版**同名同义**的三条不变量，外加 msg 回查。
//!
//! 用例名刻意跟 `src/lib.rs` 里 SQL 版的测试对齐 —— 两种介质的判定语义
//! 必须一致，名字对不上就说明有一边跑偏了。
//!
//! 没配 `DTMRS_TEST_REDIS` 就跳过。**跳过不等于通过**。
#![cfg(feature = "redis")]

use dtmrs_barrier::{RedisBarrier, RedisOutcome};

/// 必须串行：每个用例开头按前缀清键，并行跑会把别人的清掉
static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn conn(
    hint: &str,
) -> Option<(
    tokio::sync::MutexGuard<'static, ()>,
    redis::aio::MultiplexedConnection,
)> {
    let guard = LOCK.lock().await;
    let Ok(url) = std::env::var("DTMRS_TEST_REDIS") else {
        require_real_db("DTMRS_TEST_REDIS");
        eprintln!(
            "\n⚠ 跳过 Redis 屏障测试（{hint}）：DTMRS_TEST_REDIS 没配。\n  \
             这不等于 Redis 屏障通过 —— 它只有对着真 Redis 才能验。\n"
        );
        return None;
    };
    let c = redis::Client::open(url).expect("连不上 Redis");
    let mut c = c.get_multiplexed_async_connection().await.expect("建连接");
    // 清掉屏障键和测试用的业务键
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("dtmrs:bar:*")
        .query_async(&mut c)
        .await
        .unwrap();
    let biz: Vec<String> = redis::cmd("KEYS")
        .arg("t:stock:*")
        .query_async(&mut c)
        .await
        .unwrap();
    for k in keys.into_iter().chain(biz) {
        let _: i64 = redis::cmd("DEL").arg(k).query_async(&mut c).await.unwrap();
    }
    Some((guard, c))
}

async fn stock(c: &mut redis::aio::MultiplexedConnection, key: &str) -> i64 {
    redis::cmd("GET")
        .arg(key)
        .query_async::<Option<i64>>(c)
        .await
        .unwrap()
        .unwrap_or(-1)
}

async fn set_stock(c: &mut redis::aio::MultiplexedConnection, key: &str, v: i64) {
    let _: () = redis::cmd("SET")
        .arg(key)
        .arg(v)
        .query_async(c)
        .await
        .unwrap();
}

#[tokio::test]
async fn redis_重复调用同一分支只执行一次() {
    let Some((_g, mut c)) = conn("幂等").await else {
        return;
    };
    set_stock(&mut c, "t:stock:1", 100).await;

    for expect in [RedisOutcome::Executed, RedisOutcome::Duplicated] {
        // 每次都新建 barrier —— 模拟 TC 重试时是两个独立的请求
        let mut b = RedisBarrier::new("saga", "g-idem", "01", "action").unwrap();
        let r = b
            .check_adjust_amount(&mut c, "t:stock:1", -10)
            .await
            .unwrap();
        assert_eq!(r, expect);
    }
    assert_eq!(stock(&mut c, "t:stock:1").await, 90, "只该扣一次");
}

#[tokio::test]
async fn redis_正向没跑过时补偿要空转() {
    let Some((_g, mut c)) = conn("空回滚").await else {
        return;
    };
    set_stock(&mut c, "t:stock:2", 100).await;

    // 正向分支丢包了，补偿直接来
    let mut b = RedisBarrier::new("saga", "g-null", "01", "compensate").unwrap();
    let r = b
        .check_adjust_amount(&mut c, "t:stock:2", 10)
        .await
        .unwrap();
    assert_eq!(r, RedisOutcome::NullCompensation);
    assert_eq!(stock(&mut c, "t:stock:2").await, 100, "空回滚不能动数据");
}

#[tokio::test]
async fn redis_补偿先到时晚到的正向必须被丢弃() {
    let Some((_g, mut c)) = conn("悬挂").await else {
        return;
    };
    set_stock(&mut c, "t:stock:3", 100).await;

    // 补偿先到 —— 空回滚，同时把正向的位置占了
    let mut b = RedisBarrier::new("saga", "g-hang", "01", "compensate").unwrap();
    assert_eq!(
        b.check_adjust_amount(&mut c, "t:stock:3", 10)
            .await
            .unwrap(),
        RedisOutcome::NullCompensation
    );

    // 迟到的正向必须被丢弃，否则扣了款没人补
    let mut b = RedisBarrier::new("saga", "g-hang", "01", "action").unwrap();
    assert_eq!(
        b.check_adjust_amount(&mut c, "t:stock:3", -10)
            .await
            .unwrap(),
        RedisOutcome::Duplicated
    );
    assert_eq!(stock(&mut c, "t:stock:3").await, 100, "悬挂的正向不能生效");
}

#[tokio::test]
async fn redis_库存不足要明确失败而不是未知() {
    let Some((_g, mut c)) = conn("余额检查").await else {
        return;
    };
    set_stock(&mut c, "t:stock:4", 5).await;

    let mut b = RedisBarrier::new("saga", "g-low", "01", "action").unwrap();
    let r = b
        .check_adjust_amount(&mut c, "t:stock:4", -10)
        .await
        .unwrap();
    assert_eq!(r, RedisOutcome::Failure, "扣完会变负数，必须明确失败");
    assert_eq!(stock(&mut c, "t:stock:4").await, 5, "失败了就不能动数据");
}

#[tokio::test]
async fn redis_键不存在不能凭空创建库存() {
    let Some((_g, mut c)) = conn("键不存在").await else {
        return;
    };
    let mut b = RedisBarrier::new("saga", "g-nokey", "01", "action").unwrap();
    let r = b
        .check_adjust_amount(&mut c, "t:stock:404", -1)
        .await
        .unwrap();
    assert_eq!(r, RedisOutcome::Failure);
    assert_eq!(
        stock(&mut c, "t:stock:404").await,
        -1,
        "INCRBY 会把不存在的键当 0，必须先挡住"
    );
}

#[tokio::test]
async fn redis_业务lua可以自定义() {
    let Some((_g, mut c)) = conn("通用 call").await else {
        return;
    };
    set_stock(&mut c, "t:stock:5", 3).await;

    // 不是加减：这里演示「只在等于某值时才改」
    let mut b = RedisBarrier::new("saga", "g-cas", "01", "action").unwrap();
    let r = b
        .call(
            &mut c,
            r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 'FAILURE' end
            redis.call('SET', KEYS[1], ARGV[2])
            "#,
            &["t:stock:5".into()],
            &["3".into(), "7".into()],
        )
        .await
        .unwrap();
    assert_eq!(r, RedisOutcome::Executed);
    assert_eq!(stock(&mut c, "t:stock:5").await, 7);
}

#[tokio::test]
async fn redis_回查没见过的单子要固化成回滚() {
    let Some((_g, mut c)) = conn("msg 回查").await else {
        return;
    };

    // 业务侧从没跑过这一单 —— 回查必须答「没提交」
    let mut b = RedisBarrier::new("msg", "g-query", "01", "action").unwrap();
    assert_eq!(
        b.query_prepared(&mut c).await.unwrap(),
        RedisOutcome::Failure
    );

    // ⚠ 关键：回查完之后，晚到的正向分支必须被挡住。
    // 否则 TC 已经按「没提交」回滚了，业务这边却又执行了一次
    let mut b = RedisBarrier::new("msg", "g-query", "01", "action").unwrap();
    set_stock(&mut c, "t:stock:6", 100).await;
    let r = b
        .check_adjust_amount(&mut c, "t:stock:6", -10)
        .await
        .unwrap();
    assert_eq!(
        r,
        RedisOutcome::Duplicated,
        "回查已经把这单判成回滚了，正向不能再执行"
    );
    assert_eq!(stock(&mut c, "t:stock:6").await, 100);
}

/// 「跳过 ≠ 通过」的闸门。没配环境变量时测试直接返回、**仍显示为 passed**，
/// 所以 CI 里容器没起来或变量名打错，job 会安安静静全绿。
/// CI 的真库 job 设 `DTMRS_TEST_REQUIRE_REAL_DB=1`，把「悄悄没测」变成「响亮地失败」。
fn require_real_db(缺的变量: &str) {
    if std::env::var("DTMRS_TEST_REQUIRE_REAL_DB").is_ok() {
        panic!(
            "设了 DTMRS_TEST_REQUIRE_REAL_DB，却没有 {缺的变量} —— \
             这是 CI 配置坏了（容器没起来？变量名打错？），不是可以跳过的情况"
        );
    }
}
