//! 集成测试共用：让同一个用例**在每种存储后端上各跑一遍**。
//!
//! sqlite 总是跑。Redis 要同时满足「开了 `redis` feature」和「配了
//! `DTMRS_TEST_REDIS`」才跑 —— **没跑就是没测**，会打印醒目提示，
//! 设了 `DTMRS_TEST_REQUIRE_REAL_DB` 时直接 panic（CI 配置坏了不能当通过）。
//!
//! # Redis 那一遍为什么要串行
//!
//! 跟 `tests/redis.rs` 的 `REDIS_LOCK` 同一个理由：`lock_one_due` 是**全局扫描**，
//! 并行的两个用例会抢到对方的事务（而对方的 handler 记在另一份日志里）；
//! 开头的 `flush_prefix` 也会冲掉别人跑到一半的数据。sqlite 每个用例一个库文件，
//! 互不相干，照旧并行。
//!
//! 上一个用例的 `Embedded` 析构时推进器会被一并 abort（`run_forever` 用的是
//! `JoinSet`），所以锁放掉之后不会有僵尸 worker 来抢下一个用例的事务。

#![allow(dead_code)] // 每个测试文件只用到其中一部分

use std::future::Future;

pub struct Backend {
    /// `"sqlite"` / `"redis"`，断言信息里带上它，失败时一眼看出是哪个后端
    pub label: &'static str,
    pub url: String,
}

#[cfg(feature = "redis")]
static REDIS_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// 在每种可用的后端上跑一遍 `f`。`name` 用来给 sqlite 库文件起名，每个用例要不同。
pub async fn for_each_backend<F, Fut>(name: &str, f: F)
where
    F: Fn(Backend) -> Fut,
    Fut: Future<Output = ()>,
{
    let p = std::env::temp_dir().join(format!("dtmrs_be_{}_{}.db", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    eprintln!("--- [{name}] 后端: sqlite");
    f(Backend {
        label: "sqlite",
        url: format!("sqlite:{}", p.display()),
    })
    .await;
    let _ = std::fs::remove_file(&p);

    redis(name, &f).await;
}

#[cfg(feature = "redis")]
async fn redis<F, Fut>(name: &str, f: &F)
where
    F: Fn(Backend) -> Fut,
    Fut: Future<Output = ()>,
{
    let Ok(url) = std::env::var("DTMRS_TEST_REDIS") else {
        skipped(name, "DTMRS_TEST_REDIS 没配");
        return;
    };
    let _g = REDIS_LOCK.lock().await;
    flush(&url).await;
    eprintln!("--- [{name}] 后端: redis");
    f(Backend {
        label: "redis",
        url: url.clone(),
    })
    .await;
    // 跑完也清一次，别给共用的测试 Redis 留垃圾
    flush(&url).await;
}

#[cfg(feature = "redis")]
async fn flush(url: &str) {
    let s = dtmrs_store::Store::open(url).await.expect("连不上 Redis");
    s.as_redis().unwrap().flush_prefix().await.unwrap();
}

#[cfg(not(feature = "redis"))]
async fn redis<F, Fut>(name: &str, _f: &F)
where
    F: Fn(Backend) -> Fut,
    Fut: Future<Output = ()>,
{
    if std::env::var("DTMRS_TEST_REDIS").is_ok() {
        // 配了变量却没开 feature：多半是忘了带 --features，这种最容易被当成「测过了」
        skipped(name, "配了 DTMRS_TEST_REDIS 但这次构建没开 redis feature");
    } else {
        skipped(name, "没开 redis feature");
    }
}

fn skipped(name: &str, why: &str) {
    if std::env::var("DTMRS_TEST_REQUIRE_REAL_DB").is_ok() {
        panic!("设了 DTMRS_TEST_REQUIRE_REAL_DB，Redis 却没跑（{why}）—— CI 配置坏了，不能跳过");
    }
    eprintln!("\n⚠ [{name}] 跳过 Redis 那一遍：{why}。这不等于 Redis 后端通过。\n");
}
