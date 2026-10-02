//! TCC / XA 停在 prepared 的超时回滚（0.13）。
//!
//! 场景：发起方 begin → register → try（或 XA PREPARE）之后、submit / abort 之前崩了。
//! 0.13 之前没人收尾：TCC 的资源永久冻结，XA 的 prepared 事务永久持锁。
//!
//! 每条钉一个具体的失效模式：
//! - 到时限 TC 自己转回滚、调 cancel / rollback，原因写明是超时
//! - 时限之前 submit 了：照常 confirm，超时不掺和
//! - **已 submit 的 confirm 一直失败，过了时限也绝不转 cancel**（铁律不能被超时绕过）
//! - 时限之前被提前捞起来（老数据 / 立刻重试）：不回滚，排回时限
//! - 超时和 submit 撞在同一刻：以先落库的为准，不能两边都做

mod common;

use common::for_each_backend;
use dtmrs_core::{BranchResult, GlobalStatus};
use dtmrs_server::api::{Api, PrepareOpts, RegisterBranch};
use dtmrs_server::driver::Driver;
use dtmrs_server::embedded::Embedded;
use dtmrs_store::Store;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct Hits {
    confirm: AtomicUsize,
    cancel: AtomicUsize,
    confirm_fails: AtomicBool,
}

async fn spawn(h: Arc<Hits>) -> String {
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::post;
    let app = axum::Router::new()
        .route(
            "/confirm",
            post(|State(h): State<Arc<Hits>>| async move {
                h.confirm.fetch_add(1, Ordering::SeqCst);
                if h.confirm_fails.load(Ordering::SeqCst) {
                    (StatusCode::CONFLICT, "FAILURE")
                } else {
                    (StatusCode::OK, "SUCCESS")
                }
            }),
        )
        .route(
            "/cancel",
            post(|State(h): State<Arc<Hits>>| async move {
                h.cancel.fetch_add(1, Ordering::SeqCst);
                (StatusCode::OK, "SUCCESS")
            }),
        )
        .with_state(h);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    format!("http://{addr}")
}

/// begin（带时限）+ 登记一个分支 —— 然后发起方「崩了」，什么都不再调
async fn begin(api: &Api, gid: &str, tt: &str, base: &str, timeout: i64) {
    api.prepare_with(
        gid,
        tt,
        &[],
        "",
        None,
        &PrepareOpts {
            timeout_to_fail: timeout,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (fwd, bwd) = (format!("{base}/confirm"), format!("{base}/cancel"));
    let mut r = RegisterBranch {
        gid: gid.into(),
        branch_id: "01".into(),
        ..Default::default()
    };
    if tt == "xa" {
        (r.commit, r.rollback) = (fwd, bwd);
    } else {
        (r.confirm, r.cancel) = (fwd, bwd);
    }
    api.register_branch(&r).await.unwrap();
}

/// 模拟推进器跑一轮：能抢到就推
async fn tick(s: &Store, d: &Driver) -> Option<String> {
    let g = s.lock_one_due("tc", 30).await.unwrap()?;
    d.process(&g).await.unwrap();
    Some(g.gid)
}

async fn status(s: &Store, gid: &str) -> GlobalStatus {
    s.get_global(gid).await.unwrap().unwrap().status
}

#[tokio::test]
async fn 发起方崩了_到时限tc自己回滚并调cancel() {
    for_each_backend("timeout_tcc", |be| async move {
        let h = Arc::new(Hits::default());
        let base = spawn(h.clone()).await;
        let s = Store::open(&be.url).await.unwrap();
        let api = Api::new(s.clone());
        let d = Driver::new(s.clone(), "tc".into());
        begin(&api, "to-1", "tcc", &base, 1).await;

        assert_eq!(tick(&s, &d).await, None, "{}: 时限之前不能碰", be.label);
        tokio::time::sleep(Duration::from_millis(2100)).await;
        assert_eq!(tick(&s, &d).await.as_deref(), Some("to-1"), "{}", be.label);

        let g = s.get_global("to-1").await.unwrap().unwrap();
        assert_eq!(g.status, GlobalStatus::Failed, "{}", be.label);
        assert_eq!(g.rollback_reason, "Timeout after 1 seconds", "{}", be.label);
        assert_eq!(h.cancel.load(Ordering::SeqCst), 1, "{}", be.label);
        assert_eq!(h.confirm.load(Ordering::SeqCst), 0, "{}", be.label);
    })
    .await;
}

#[tokio::test]
async fn xa发起方崩了_到时限tc自己rollback() {
    // XA 比 TCC 更要紧：没人 rollback 的话，各库里 PREPARE 过的事务永久持锁
    let h = Arc::new(Hits::default());
    let base = spawn(h.clone()).await;
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    let d = Driver::new(s.clone(), "tc".into());
    begin(&api, "to-xa", "xa", &base, 1).await;
    tokio::time::sleep(Duration::from_millis(2100)).await;
    tick(&s, &d).await;
    assert_eq!(status(&s, "to-xa").await, GlobalStatus::Failed);
    assert_eq!(h.cancel.load(Ordering::SeqCst), 1, "rollback 分支要被调到");
    assert_eq!(h.confirm.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn 时限之前submit了就照常confirm_超时不掺和() {
    let h = Arc::new(Hits::default());
    let base = spawn(h.clone()).await;
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    let d = Driver::new(s.clone(), "tc".into());
    begin(&api, "to-2", "tcc", &base, 1).await;
    api.submit("to-2", "tcc", &[]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(2100)).await;
    tick(&s, &d).await;
    assert_eq!(status(&s, "to-2").await, GlobalStatus::Succeed);
    assert_eq!(h.confirm.load(Ordering::SeqCst), 1);
    assert_eq!(h.cancel.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn 已submit的confirm一直失败_过了时限也绝不转cancel() {
    // 「confirm 失败绝不能转 cancel」是铁律。超时只管 prepared，不能变成绕过它的后门
    let h = Arc::new(Hits::default());
    h.confirm_fails.store(true, Ordering::SeqCst);
    let base = spawn(h.clone()).await;
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    let d = Driver::new(s.clone(), "tc".into());
    begin(&api, "to-3", "tcc", &base, 1).await;
    api.submit("to-3", "tcc", &[]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(2100)).await;
    for _ in 0..3 {
        let g = s.get_global("to-3").await.unwrap().unwrap();
        d.process(&g).await.unwrap();
    }
    assert_eq!(status(&s, "to-3").await, GlobalStatus::Submitted);
    assert_eq!(h.cancel.load(Ordering::SeqCst), 0);
    assert!(h.confirm.load(Ordering::SeqCst) >= 3);
}

#[tokio::test]
async fn 时限之前被提前捞起来_不回滚只排回时限() {
    // 0.13 之前建的 prepared tcc 排在建立那一刻；管理台「立刻重试」也会把它提前
    let h = Arc::new(Hits::default());
    let base = spawn(h.clone()).await;
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    let d = Driver::new(s.clone(), "tc".into());
    begin(&api, "to-4", "tcc", &base, 60).await;
    api.retry("to-4").await.unwrap();
    assert_eq!(
        tick(&s, &d).await.as_deref(),
        Some("to-4"),
        "立刻重试之后能捞到"
    );

    let g = s.get_global("to-4").await.unwrap().unwrap();
    assert_eq!(g.status, GlobalStatus::Prepared);
    assert_eq!(h.cancel.load(Ordering::SeqCst), 0);
    assert!(
        (g.next_cron_time - (g.create_time + 60)).abs() <= 1,
        "要排回时限那一刻：next={} create={}",
        g.next_cron_time,
        g.create_time
    );
    assert_eq!(tick(&s, &d).await, None, "排回去之后不会再被捞");
}

#[tokio::test]
async fn 超时和submit撞在同一刻_以先落库的为准() {
    // 推进器拿着「还是 prepared」的旧快照，发起方恰好在这时 submit 成功了
    let h = Arc::new(Hits::default());
    let base = spawn(h.clone()).await;
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    let d = Driver::new(s.clone(), "tc".into());
    begin(&api, "to-5", "tcc", &base, 1).await;
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let stale = s.lock_one_due("tc", 30).await.unwrap().unwrap();
    assert_eq!(stale.status, GlobalStatus::Prepared);
    api.submit("to-5", "tcc", &[]).await.unwrap();
    d.process(&stale).await.unwrap();

    assert_eq!(
        h.cancel.load(Ordering::SeqCst),
        0,
        "submit 先落库了，不能再回滚"
    );
    assert_ne!(status(&s, "to-5").await, GlobalStatus::Failed);
}

#[tokio::test]
async fn 负数时限要拒绝() {
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s);
    let e = api
        .prepare_with(
            "to-neg",
            "tcc",
            &[],
            "",
            None,
            &PrepareOpts {
                timeout_to_fail: -1,
                ..Default::default()
            },
        )
        .await;
    assert!(e.is_err());
}

#[tokio::test]
async fn 嵌入式_发起方没submit就走了_tc到点调cancel() {
    let cancelled = Arc::new(AtomicUsize::new(0));
    let c2 = cancelled.clone();
    let tc = Embedded::builder("sqlite::memory:")
        .handler("confirm", |_| async { BranchResult::Success })
        .handler("cancel", move |_| {
            let c = c2.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                BranchResult::Success
            }
        })
        .tick(Duration::from_millis(50))
        .start()
        .await
        .unwrap();
    {
        let mut t = tc.tcc_with_timeout("emb-to", 1).await.unwrap();
        let r = t
            .try_branch("local://confirm", "local://cancel", |_bid| async {
                BranchResult::Success
            })
            .await
            .unwrap();
        assert_eq!(r, BranchResult::Success);
        // 发起方到这里「崩了」：既不 submit 也不 abort
    }
    assert_eq!(
        tc.wait_final("emb-to", Duration::from_secs(8))
            .await
            .unwrap(),
        GlobalStatus::Failed
    );
    assert_eq!(cancelled.load(Ordering::SeqCst), 1);
    tc.shutdown().await;
}
