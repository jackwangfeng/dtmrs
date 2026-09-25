//! 集成测试共用：让同一个用例**在每种存储后端上各跑一遍**。
//!
//! sqlite 总是跑。Redis 要同时满足「开了 `redis` feature」和「配了
//! `DTMRS_TEST_REDIS`」才跑 —— **没跑就是没测**，会打印醒目提示，
//! 设了 `DTMRS_TEST_REQUIRE_REAL_DB` 时直接 panic（CI 配置坏了不能当通过）。
//!
//! # Redis 那一遍每个用例一个 key 前缀
//!
//! `lock_one_due` 在一个前缀内是全局扫描：两个用例共用前缀的话会抢到对方的事务
//! （而对方的 handler 记在另一份日志里），开头的 `flush_prefix` 也会冲掉别人跑到
//! 一半的数据。所以每个用例用 `?key_prefix=dtmrs-t-<用例名>:` 各占一块 ——
//! 互相看不见，照样能并行，也不会去清共用 Redis 上默认的 `dtmrs:` 前缀。

#![allow(dead_code)] // 每个测试文件只用到其中一部分

use std::future::Future;

pub struct Backend {
    /// `"sqlite"` / `"redis"`，断言信息里带上它，失败时一眼看出是哪个后端
    pub label: &'static str,
    pub url: String,
}

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
    let sep = if url.contains('?') { '&' } else { '?' };
    let url = format!("{url}{sep}key_prefix=dtmrs-t-{name}:");
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
