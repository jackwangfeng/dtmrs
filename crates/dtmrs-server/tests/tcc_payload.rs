//! TCC 分支登记带业务数据（0.13，同 DTM registerBranch 的 `data`）。
//!
//! 失效模式：confirm / cancel 拿不到 try 冻结了什么，业务方只能自建 try 记录表回查。
//! 现在登记时带上，二阶段收到的请求体就是它。

mod common;

use common::for_each_backend;
use dtmrs_core::{BranchOp, BranchResult, GlobalStatus};
use dtmrs_server::api::{Api, RegisterBranch};
use dtmrs_server::driver::Driver;
use dtmrs_server::embedded::Embedded;
use dtmrs_store::Store;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const FROZEN: &str = r#"{"account":"A-7","amount":100}"#;

/// 记下 confirm / cancel 收到的 (路径, body)
type Log = Arc<Mutex<Vec<(String, String)>>>;

async fn spawn(log: Log) -> String {
    use axum::extract::{Path, State};
    use axum::routing::post;
    let app = axum::Router::new()
        .route(
            "/{op}",
            post(
                |State(l): State<Log>, Path(op): Path<String>, body: String| async move {
                    l.lock().unwrap().push((op, body));
                    "SUCCESS"
                },
            ),
        )
        .with_state(log);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    format!("http://{addr}")
}

fn reg(gid: &str, base: &str, data: &str) -> RegisterBranch {
    RegisterBranch {
        gid: gid.into(),
        branch_id: "01".into(),
        confirm: format!("{base}/confirm"),
        cancel: format!("{base}/cancel"),
        data: data.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn 登记时带的数据_confirm和cancel都原样收到() {
    for_each_backend("tcc_payload", |be| async move {
        let log = Arc::new(Mutex::new(Vec::new()));
        let base = spawn(log.clone()).await;
        let s = Store::open(&be.url).await.unwrap();
        let api = Api::new(s.clone());
        let d = Driver::new(s.clone(), "tc".into());

        // 一笔提交（走 confirm），一笔中止（走 cancel）
        for (gid, commit) in [("pl-ok", true), ("pl-no", false)] {
            api.prepare(gid, "tcc", &[], "", None).await.unwrap();
            api.register_branch(&reg(gid, &base, FROZEN)).await.unwrap();
            if commit {
                api.submit(gid, "tcc", &[]).await.unwrap();
            } else {
                api.abort(gid).await.unwrap();
            }
            let g = s.get_global(gid).await.unwrap().unwrap();
            d.process(&g).await.unwrap();
        }
        assert_eq!(
            *log.lock().unwrap(),
            [
                ("confirm".to_string(), FROZEN.to_string()),
                ("cancel".to_string(), FROZEN.to_string())
            ],
            "{}",
            be.label
        );
    })
    .await;
}

#[tokio::test]
async fn 不带数据时跟以前一样收到空对象() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let base = spawn(log.clone()).await;
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    let d = Driver::new(s.clone(), "tc".into());
    api.prepare("pl-empty", "tcc", &[], "", None).await.unwrap();
    api.register_branch(&reg("pl-empty", &base, ""))
        .await
        .unwrap();
    api.submit("pl-empty", "tcc", &[]).await.unwrap();
    d.process(&s.get_global("pl-empty").await.unwrap().unwrap())
        .await
        .unwrap();
    assert_eq!(log.lock().unwrap()[0].1, "{}");
}

#[tokio::test]
async fn 重复登记以第一次的数据为准_数据不同不算撞号() {
    for_each_backend("tcc_payload_dup", |be| async move {
        let s = Store::open(&be.url).await.unwrap();
        let api = Api::new(s.clone());
        api.prepare("pl-dup", "tcc", &[], "", None).await.unwrap();
        api.register_branch(&reg("pl-dup", "http://x", "first"))
            .await
            .unwrap();
        // 客户端重试：地址相同、数据变了（比如带了时间戳）—— 放行，但不覆盖
        api.register_branch(&reg("pl-dup", "http://x", "second"))
            .await
            .unwrap();
        let rows = s.list_branches("pl-dup").await.unwrap();
        assert_eq!(rows.len(), 2, "{}", be.label);
        assert!(
            rows.iter().all(|r| r.payload == "first"),
            "{}: {:?}",
            be.label,
            rows.iter().map(|r| &r.payload).collect::<Vec<_>>()
        );
        assert!(
            rows.iter()
                .any(|r| r.op == BranchOp::Confirm && r.url == "http://x/confirm"),
            "{}",
            be.label
        );
    })
    .await;
}

#[tokio::test]
async fn 嵌入式_try_branch_with的数据二阶段能拿到() {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let (s1, s2) = (seen.clone(), seen.clone());
    let tc = Embedded::builder("sqlite::memory:")
        .handler("confirm", move |c| {
            let s = s1.clone();
            async move {
                s.lock().unwrap().push(format!("confirm:{}", c.payload));
                BranchResult::Success
            }
        })
        .handler("cancel", move |c| {
            let s = s2.clone();
            async move {
                s.lock().unwrap().push(format!("cancel:{}", c.payload));
                BranchResult::Success
            }
        })
        .tick(Duration::from_millis(20))
        .start()
        .await
        .unwrap();
    let mut t = tc.tcc("emb-pl").await.unwrap();
    let r = t
        .try_branch_with("local://confirm", "local://cancel", FROZEN, |_bid| async {
            BranchResult::Success
        })
        .await
        .unwrap();
    assert_eq!(r, BranchResult::Success);
    t.submit().await.unwrap();
    assert_eq!(
        tc.wait_final("emb-pl", Duration::from_secs(5))
            .await
            .unwrap(),
        GlobalStatus::Succeed
    );
    assert_eq!(*seen.lock().unwrap(), [format!("confirm:{FROZEN}")]);
    tc.shutdown().await;
}
