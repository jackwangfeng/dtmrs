//! Redis 后端端到端。
//!
//! ⚠ 整个文件挂在 `redis` feature 下 —— 集成测试文件**不管 feature 开没开都会
//! 被编译**，不加 cfg 的话，关掉 feature 的构建会因为找不到 `redis_store`
//! 而直接编译失败（CI 上撞过）。
//!
//! ```bash
//! docker run -d --rm -p 16379:6379 redis:7-alpine
//! export DTMRS_TEST_REDIS='redis://127.0.0.1:16379/0'
//! cargo test -p dtmrs-server --features redis --test redis
//! ```
//!
//! 没配环境变量就跳过 —— **跳过不等于通过**，会打印醒目提示。
//!
//! 重点验两件 SQL 后端那边已经验过、但 Redis 得重新证一遍的事：
//!
//! 1. **多实例不重复推进**。这是 Redis 后端存在的意义所在 —— 秒杀场景要靠
//!    多个 TC 实例扛并发，推重了就是重复扣款。SQL 那边靠行锁 + 条件 UPDATE，
//!    Redis 这边靠 Lua 脚本的单线程原子性，是完全不同的机制，必须各证各的。
//! 2. **状态机行为跟 SQL 后端一致** —— 正向、补偿、超时不回滚。
//!
//! 外加一条 Redis 独有的：终态事务会挂 TTL（不然秒杀几千万笔会撑爆内存）。

#![cfg(feature = "redis")]

use dtmrs_core::{BranchResult, GlobalStatus, SagaStep, TransType};
use dtmrs_server::driver::Driver;
use dtmrs_server::saga_rows;
use dtmrs_store::Store;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// **这些测试必须串行。**
///
/// 两个原因，缺一不可：
/// 1. `lock_one_due` 是**全局扫描**，不按 gid 过滤 —— 并行跑会互相抢到
///    对方的事务，断言就没意义了（跟 store 里 PG_LOCK 是同一个道理）
/// 2. 每个测试开头 `flush_prefix` 会把整个前缀清空，并行时会把别人跑到
///    一半的数据冲掉 —— 并发那个测试会因此空转到超时
static REDIS_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// 取 Redis 连接 + 串行锁。锁要持到测试结束，所以由调用方接着。
async fn store(prefix_hint: &str) -> Option<(tokio::sync::MutexGuard<'static, ()>, Store)> {
    let guard = REDIS_LOCK.lock().await;
    let Ok(url) = std::env::var("DTMRS_TEST_REDIS") else {
        require_real_db("DTMRS_TEST_REDIS");
        eprintln!(
            "\n⚠ 跳过 Redis 测试（{prefix_hint}）：DTMRS_TEST_REDIS 没配。\n  \
             这不等于 Redis 后端通过 —— 它只有对着真 Redis 才能验。\n"
        );
        return None;
    };
    let s = Store::open(&url).await.expect("连不上 Redis");
    // 每个测试开头清干净
    s.as_redis().unwrap().flush_prefix().await.unwrap();
    Some((guard, s))
}

/// 假业务服务：记录每个 (gid, branch, op) 被调了几次
#[derive(Default)]
struct Busi {
    calls: Mutex<HashMap<String, usize>>,
    fail_on: Mutex<Option<String>>,
}

impl Busi {
    fn hits(&self, key: &str) -> usize {
        *self.calls.lock().unwrap().get(key).unwrap_or(&0)
    }
    fn total(&self) -> usize {
        self.calls.lock().unwrap().values().sum()
    }
    /// 有没有任何一个分支被调了不止一次
    fn duplicates(&self) -> Vec<(String, usize)> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, &n)| n > 1)
            .map(|(k, &n)| (k.clone(), n))
            .collect()
    }
}

/// 把假业务挂成进程内分支
fn registry(busi: Arc<Busi>, names: &[&str]) -> dtmrs_server::registry::Registry {
    let mut r = dtmrs_server::registry::Registry::new();
    for name in names {
        let b = busi.clone();
        let n = name.to_string();
        r.register(name, move |ctx| {
            let (b, n) = (b.clone(), n.clone());
            async move {
                let key = format!("{}|{}|{}", ctx.gid, ctx.branch_id, ctx.op.as_str());
                *b.calls.lock().unwrap().entry(key).or_insert(0) += 1;
                // 业务端故意慢一点，把并发窗口撑开
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                let fail = b.fail_on.lock().unwrap().clone();
                if fail.as_deref() == Some(n.as_str()) {
                    BranchResult::Failure
                } else {
                    BranchResult::Success
                }
            }
        });
    }
    r
}

#[tokio::test]
async fn redis_正向提交与逆序补偿() {
    let Some((_guard, st)) = store("正向与补偿").await else {
        return;
    };
    let busi = Arc::new(Busi::default());
    let d = Driver::new(st.clone(), "tc-1".into()).with_registry(Arc::new(registry(
        busi.clone(),
        &["扣款", "退款", "发货", "退货"],
    )));

    // ① 正常提交
    let steps = vec![
        SagaStep::new("local://扣款", "local://退款"),
        SagaStep::new("local://发货", "local://退货"),
    ];
    let (g, br) = saga_rows("r-ok", &steps);
    st.create_global(&g, &br).await.unwrap();
    d.process(&g).await.unwrap();
    assert_eq!(
        st.get_global("r-ok").await.unwrap().unwrap().status,
        GlobalStatus::Succeed
    );
    assert_eq!(busi.hits("r-ok|01|action"), 1);
    assert_eq!(busi.hits("r-ok|02|action"), 1);
    assert_eq!(busi.hits("r-ok|01|compensate"), 0, "成功的事务不该有补偿");

    // ② 第二步失败 → 逆序补偿
    *busi.fail_on.lock().unwrap() = Some("发货".into());
    let (g2, br2) = saga_rows("r-fail", &steps);
    st.create_global(&g2, &br2).await.unwrap();
    d.process(&g2).await.unwrap();
    assert_eq!(
        st.get_global("r-fail").await.unwrap().unwrap().status,
        GlobalStatus::Failed
    );
    assert_eq!(busi.hits("r-fail|01|compensate"), 1, "扣款要被补偿");
    assert_eq!(busi.hits("r-fail|02|compensate"), 1, "失败分支也要补偿");
}

#[tokio::test]
async fn redis_超时不能触发回滚() {
    let Some((_guard, st)) = store("超时不回滚").await else {
        return;
    };
    let mut reg = dtmrs_server::registry::Registry::new();
    reg.register("超时", |_| async { BranchResult::Unknown });
    reg.register("补偿", |_| async { BranchResult::Success });
    let d = Driver::new(st.clone(), "tc-1".into()).with_registry(Arc::new(reg));

    let steps = vec![SagaStep::new("local://超时", "local://补偿")];
    let (g, br) = saga_rows("r-timeout", &steps);
    st.create_global(&g, &br).await.unwrap();
    d.process(&g).await.unwrap();

    // 换后端不换语义：结果未知只重试，绝不回滚
    assert_eq!(
        st.get_global("r-timeout").await.unwrap().unwrap().status,
        GlobalStatus::Submitted
    );
}

#[tokio::test]
async fn redis_多实例并发不重复推进() {
    // **这是 Redis 后端存在的理由所在。**
    // 秒杀场景靠多个 TC 实例扛并发，推重了就是重复扣款。
    // SQL 那边靠行锁 + 条件 UPDATE；Redis 这边靠 Lua 脚本的单线程原子性 ——
    // 完全不同的机制，必须单独证。
    let Some((_guard, st)) = store("多实例并发").await else {
        return;
    };
    const N: usize = 20;
    const INSTANCES: usize = 3;

    let busi = Arc::new(Busi::default());
    let names = ["扣款", "退款"];

    // 先塞 N 笔待办
    let steps = vec![SagaStep::new("local://扣款", "local://退款")];
    for i in 0..N {
        let (g, br) = saga_rows(&format!("r-race-{i:02}"), &steps);
        st.create_global(&g, &br).await.unwrap();
    }

    // 三个实例同时抢
    let mut handles = Vec::new();
    for inst in 0..INSTANCES {
        let st = st.clone();
        let reg = Arc::new(registry(busi.clone(), &names));
        handles.push(tokio::spawn(async move {
            let d = Driver::new(st.clone(), format!("tc-{inst}")).with_registry(reg);
            let mut done = 0;
            // 一直抢到没活为止
            for _ in 0..200 {
                match d.store.lock_one_due(&d.owner, 30).await {
                    Ok(Some(g)) => {
                        let _ = d.process(&g).await;
                        done += 1;
                    }
                    Ok(None) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
                    Err(e) => panic!("抢活失败: {e}"),
                }
            }
            (inst, done)
        }));
    }

    let mut per_instance = Vec::new();
    for h in handles {
        per_instance.push(h.await.unwrap());
    }

    // 每笔都该终结
    let mut succeeded = 0;
    for i in 0..N {
        let g = st
            .get_global(&format!("r-race-{i:02}"))
            .await
            .unwrap()
            .unwrap();
        if g.status == GlobalStatus::Succeed {
            succeeded += 1;
        }
    }
    assert_eq!(succeeded, N, "{N} 笔都该成功");

    // **核心断言**：每个分支正好被调一次
    let dups = busi.duplicates();
    assert!(
        dups.is_empty(),
        "有分支被重复推进了（这就是重复扣款）: {dups:?}"
    );
    assert_eq!(busi.total(), N, "{N} 笔 × 1 步 = {N} 次调用");

    // 活确实分散在多个实例上（否则这个测试等于没测并发）
    let working: Vec<_> = per_instance.iter().filter(|(_, n)| *n > 0).collect();
    println!("各实例处理数: {per_instance:?}");
    assert!(
        working.len() >= 2,
        "至少两个实例该抢到活，否则没真正并发: {per_instance:?}"
    );
}

#[tokio::test]
async fn redis_终态会挂ttl() {
    // Redis 独有的语义：终态事务会过期。秒杀几千万笔之后不回收内存会撑爆。
    // SQL 后端不会这样 —— 这是两者的实打实差异，写在文档里也钉在测试里。
    let Some((_guard, st)) = store("终态 TTL").await else {
        return;
    };
    let mut reg = dtmrs_server::registry::Registry::new();
    reg.register("好", |_| async { BranchResult::Success });
    let d = Driver::new(st.clone(), "tc-1".into()).with_registry(Arc::new(reg));

    let steps = vec![SagaStep::new("local://好", "local://好")];
    let (g, br) = saga_rows("r-ttl", &steps);
    st.create_global(&g, &br).await.unwrap();

    // 还没终结：不该有 TTL
    let url = std::env::var("DTMRS_TEST_REDIS").unwrap();
    let client = redis::Client::open(url).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let ttl: i64 = redis::cmd("TTL")
        .arg("dtmrs:g:r-ttl")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(ttl, -1, "没终结的事务不该有 TTL，-1 表示永不过期");

    d.process(&g).await.unwrap();
    assert_eq!(
        st.get_global("r-ttl").await.unwrap().unwrap().status,
        GlobalStatus::Succeed
    );

    let ttl: i64 = redis::cmd("TTL")
        .arg("dtmrs:g:r-ttl")
        .query_async(&mut c)
        .await
        .unwrap();
    assert!(ttl > 0, "终结之后该挂上 TTL，实际 {ttl}");
    assert!(
        ttl <= dtmrs_store::redis_store::DEFAULT_FINAL_TTL,
        "TTL 不该超过默认值"
    );
}

#[tokio::test]
async fn redis_终态不再被调度() {
    let Some((_guard, st)) = store("终态不调度").await else {
        return;
    };
    let steps = vec![SagaStep::new("local://x", "local://y")];
    let (g, br) = saga_rows("r-final", &steps);
    st.create_global(&g, &br).await.unwrap();
    st.set_global_status("r-final", GlobalStatus::Succeed, TransType::Saga, "")
        .await
        .unwrap();

    // 索引里应该已经没有它了
    let got = st.lock_one_due("tc-1", 30).await.unwrap();
    assert!(got.is_none(), "终态事务不该再被捞起来: {got:?}");
}

#[tokio::test]
async fn redis_租约到期后别的实例能接手() {
    // 持租约的实例崩了，租约到期后必须有人接手 —— 这是崩溃恢复的基础
    let Some((_guard, st)) = store("租约接手").await else {
        return;
    };
    let steps = vec![SagaStep::new("local://x", "local://y")];
    let (g, br) = saga_rows("r-lease", &steps);
    st.create_global(&g, &br).await.unwrap();

    // tc-1 抢到，租约 1 秒
    let first = st.lock_one_due("tc-1", 1).await.unwrap();
    assert_eq!(first.unwrap().gid, "r-lease");
    // 租约没到期，别人抢不到
    assert!(
        st.lock_one_due("tc-2", 30).await.unwrap().is_none(),
        "租约期内不该被抢走"
    );
    // 等租约过期
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let second = st.lock_one_due("tc-2", 30).await.unwrap();
    assert_eq!(
        second.expect("租约到期后该能接手").owner,
        "tc-2",
        "接手方要写上自己的 owner"
    );
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

/// 重号判定：Redis 侧靠 `hset_nx` 的返回值 + 回读比对，SQL 侧靠冲突忽略后回读。
/// **机制不同，结论必须逐条一致** —— 跟 `两个分支用同一个分支号必须拒绝`
/// / `同一个分支重复登记仍然要幂等`（SQL 侧，tcc_msg.rs）一一对应。
#[tokio::test]
async fn redis_重号登记要拒绝而同号重试要幂等() {
    let Some((_g, s)) = store("dupbid").await else {
        return;
    };
    use dtmrs_core::BranchOp;
    use dtmrs_store::RegisterOutcome;

    let gid = "dupbid-1";
    let 库存 = [
        (BranchOp::Confirm, "http://库存/confirm".to_string()),
        (BranchOp::Cancel, "http://库存/cancel".to_string()),
    ];
    let 订单 = [
        (BranchOp::Confirm, "http://订单/confirm".to_string()),
        (BranchOp::Cancel, "http://订单/cancel".to_string()),
    ];

    assert_eq!(
        s.register_branch(gid, "01", &库存).await.unwrap(),
        RegisterOutcome::Registered
    );
    // 同号同 URL = 客户端重试 → 幂等放行
    assert_eq!(
        s.register_branch(gid, "01", &库存).await.unwrap(),
        RegisterOutcome::Registered,
        "URL 一致的重复登记是客户端重试，必须放行"
    );
    // 同号不同 URL = 两个分支撞号 → 必须报冲突
    assert!(
        matches!(
            s.register_branch(gid, "01", &订单).await.unwrap(),
            RegisterOutcome::Conflict { .. }
        ),
        "重号必须报冲突：放行的话订单的 URL 根本写不进去，那份资源永久泄漏"
    );
    // 各用各的号则互不影响
    assert_eq!(
        s.register_branch(gid, "02", &订单).await.unwrap(),
        RegisterOutcome::Registered
    );

    let rows = s.list_branches(gid).await.unwrap();
    assert_eq!(rows.len(), 4, "两个分支各两个 op");
    for r in &rows {
        let 期望 = if r.branch_id == "01" { "库存" } else { "订单" };
        assert!(
            r.url.contains(期望),
            "分支 {} 的地址串味了：{}",
            r.branch_id,
            r.url
        );
    }
}
