//! 卡住的事务怎么被人发现（0.13）：重试计数、按「卡住」查询、告警。
//!
//! 失效模式：confirm 失败只能无限重试、等人介入 —— 语义对，但**没人知道它卡着**。

mod common;

use common::for_each_backend;
use dtmrs_core::{BranchOp, GlobalStatus};
use dtmrs_server::alert::{Alert, AlertConfig, AlertSink};
use dtmrs_server::api::{Api, RegisterBranch};
use dtmrs_server::driver::Driver;
use dtmrs_store::Store;
use std::sync::{Arc, Mutex};

/// confirm 永远 409，cancel 永远成功
async fn spawn() -> String {
    use axum::http::StatusCode;
    use axum::routing::post;
    let app = axum::Router::new()
        .route(
            "/confirm",
            post(|| async { (StatusCode::CONFLICT, "FAILURE") }),
        )
        .route("/cancel", post(|| async { "SUCCESS" }))
        .route("/ok", post(|| async { "SUCCESS" }));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    format!("http://{addr}")
}

/// 一笔已 submit、confirm 一直失败的 TCC
async fn stuck_tcc(api: &Api, gid: &str, base: &str) {
    api.prepare(gid, "tcc", &[], "", None).await.unwrap();
    api.register_branch(&RegisterBranch {
        gid: gid.into(),
        branch_id: "01".into(),
        confirm: format!("{base}/confirm"),
        cancel: format!("{base}/cancel"),
        ..Default::default()
    })
    .await
    .unwrap();
    api.submit(gid, "tcc", &[]).await.unwrap();
}

async fn round(s: &Store, d: &Driver, gid: &str) {
    let g = s.get_global(gid).await.unwrap().unwrap();
    d.process(&g).await.unwrap();
}

fn collector() -> (AlertConfig, Arc<Mutex<Vec<Alert>>>) {
    let got = Arc::new(Mutex::new(Vec::new()));
    let g2 = got.clone();
    (
        AlertConfig {
            sink: AlertSink::Callback(Arc::new(move |a| g2.lock().unwrap().push(a))),
            retry_limit: 3,
        },
        got,
    )
}

#[tokio::test]
async fn confirm一直失败_重试计数累加_到上限开始告警且带上卡住的分支() {
    for_each_backend("stuck_alert", |be| async move {
        let base = spawn().await;
        let s = Store::open(&be.url).await.unwrap();
        let api = Api::new(s.clone());
        let (cfg, got) = collector();
        let d = Driver::new(s.clone(), "tc".into()).with_alert(Some(cfg));
        stuck_tcc(&api, "st-1", &base).await;

        for _ in 0..2 {
            round(&s, &d, "st-1").await;
        }
        assert_eq!(
            api.query("st-1").await.unwrap().retry_count,
            2,
            "{}",
            be.label
        );
        assert!(
            got.lock().unwrap().is_empty(),
            "{}: 没到上限不能报",
            be.label
        );

        round(&s, &d, "st-1").await;
        round(&s, &d, "st-1").await;
        let alerts = got.lock().unwrap().clone();
        assert_eq!(
            alerts.iter().map(|a| a.retry_count).collect::<Vec<_>>(),
            [3, 4],
            "{}: 到上限之后每轮都报",
            be.label
        );
        let a = &alerts[0];
        assert_eq!(
            (a.gid.as_str(), a.status.as_str()),
            ("st-1", "submitted"),
            "{}",
            be.label
        );
        // 卡住的是 confirm，cancel 没跑过也算「没成功」—— 都列出来，运维看全貌
        assert!(
            a.pending
                .iter()
                .any(|p| p.op == BranchOp::Confirm.as_str() && p.url.ends_with("/confirm")),
            "{}: {:?}",
            be.label,
            a.pending
        );
        // 铁律没被破坏：还在 submitted，cancel 一次没调
        assert_eq!(
            s.get_global("st-1").await.unwrap().unwrap().status,
            GlobalStatus::Submitted,
            "{}",
            be.label
        );
    })
    .await;
}

#[tokio::test]
async fn 按卡住查询_只列没终结且重试够多的_最老的在前带分支() {
    for_each_backend("stuck_list", |be| async move {
        let base = spawn().await;
        let s = Store::open(&be.url).await.unwrap();
        let api = Api::new(s.clone());
        let d = Driver::new(s.clone(), "tc".into());
        stuck_tcc(&api, "st-old", &base).await;
        stuck_tcc(&api, "st-new", &base).await;
        // 一笔正常完成的，重试过也不该出现
        api.prepare(
            "st-done",
            "msg",
            &[format!("{base}/ok")],
            &format!("{base}/ok"),
            None,
        )
        .await
        .unwrap();
        api.submit("st-done", "msg", &[]).await.unwrap();
        round(&s, &d, "st-done").await;

        for _ in 0..3 {
            round(&s, &d, "st-old").await;
        }
        round(&s, &d, "st-new").await;

        let v = api.list_stuck(3, 100).await.unwrap();
        assert_eq!(
            v.iter().map(|t| t.gid.as_str()).collect::<Vec<_>>(),
            ["st-old"],
            "{}",
            be.label
        );
        assert_eq!(v[0].retry_count, 3, "{}", be.label);
        assert!(!v[0].branches.is_empty(), "{}: 要带分支明细", be.label);

        let all = api.list_stuck(1, 100).await.unwrap();
        let mut ids: Vec<_> = all.iter().map(|t| t.gid.clone()).collect();
        ids.sort();
        assert_eq!(ids, ["st-new", "st-old"], "{}", be.label);
    })
    .await;
}

#[tokio::test]
async fn 状态被外面改了的立刻重排不算一轮重试() {
    // abort 和推进器撞上时推进器会 release_lease(…, 0) 立刻重排，那不是「重试」
    let s = Store::open("sqlite::memory:").await.unwrap();
    let mut g = dtmrs_server::tcc_rows("st-x");
    g.status = GlobalStatus::Submitted;
    s.create_global(&g, &[]).await.unwrap();
    let g = s.lock_one_due("tc", 30).await.unwrap().unwrap();
    s.release_lease("st-x", g.lease_until, 0).await.unwrap();
    assert_eq!(s.get_global("st-x").await.unwrap().unwrap().retry_count, 0);
    let g = s.lock_one_due("tc", 30).await.unwrap().unwrap();
    s.release_lease("st-x", g.lease_until, 10).await.unwrap();
    assert_eq!(s.get_global("st-x").await.unwrap().unwrap().retry_count, 1);
}

#[tokio::test]
async fn 告警webhook收到的json形状() {
    use axum::routing::post;
    let got = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let g2 = got.clone();
    let app = axum::Router::new().route(
        "/alert",
        post(move |axum::Json(v): axum::Json<serde_json::Value>| {
            let g = g2.clone();
            async move {
                g.lock().unwrap().push(v);
                "ok"
            }
        }),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hook = format!("http://{}/alert", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });

    let base = spawn().await;
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    let d = Driver::new(s.clone(), "tc".into()).with_alert(Some(AlertConfig {
        sink: AlertSink::Webhook(hook),
        retry_limit: 1,
    }));
    stuck_tcc(&api, "st-hook", &base).await;
    round(&s, &d, "st-hook").await;
    for _ in 0..50 {
        if !got.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let v = got.lock().unwrap()[0].clone();
    assert_eq!(v["gid"], "st-hook");
    assert_eq!(v["status"], "submitted");
    assert_eq!(v["retry_count"], 1);
    assert!(v["pending"].as_array().is_some_and(|a| !a.is_empty()));
}

#[tokio::test]
async fn 告警webhook挂了也不影响推进() {
    let base = spawn().await;
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    let d = Driver::new(s.clone(), "tc".into()).with_alert(Some(AlertConfig {
        sink: AlertSink::Webhook("http://127.0.0.1:9/nobody".into()),
        retry_limit: 1,
    }));
    stuck_tcc(&api, "st-dead", &base).await;
    for _ in 0..3 {
        round(&s, &d, "st-dead").await;
    }
    assert_eq!(
        s.get_global("st-dead").await.unwrap().unwrap().retry_count,
        3
    );
}

#[tokio::test]
async fn http_stuck接口能查到卡住的事务() {
    let base = spawn().await;
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    let d = Driver::new(s.clone(), "tc".into());
    stuck_tcc(&api, "st-http", &base).await;
    for _ in 0..3 {
        round(&s, &d, "st-http").await;
    }
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let app = dtmrs_server::http::router(dtmrs_server::http::App::new(api));
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    let v: serde_json::Value = reqwest::get(format!("http://{addr}/api/dtmsvr/stuck"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v[0]["gid"], "st-http");
    assert_eq!(v[0]["retry_count"], 3);
    // 门槛调高就查不到
    let v: serde_json::Value =
        reqwest::get(format!("http://{addr}/api/dtmsvr/stuck?min_retries=10"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(v.as_array().unwrap().len(), 0);
}
