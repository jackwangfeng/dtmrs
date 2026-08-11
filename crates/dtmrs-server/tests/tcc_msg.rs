//! TCC 与二阶段消息的端到端测试。
//!
//! 两个模式各有一条最容易写错的规则，各配一个专门的测试：
//! - TCC：**confirm 失败绝不能触发 cancel**（try 已成功、方向已定）
//! - msg：**进程崩在 prepare 和 submit 之间，靠回查决断**（取代 MQ 事务消息）

use dtmrs_core::{BranchOp, GlobalStatus, TransType};
use dtmrs_server::driver::Driver;
use dtmrs_server::{msg_rows, tcc_rows};
use dtmrs_store::Store;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// 假业务服务。`mode` 决定各路径的返回，计数器记录被调次数。
#[derive(Default)]
struct Hits {
    confirm1: AtomicUsize,
    confirm2: AtomicUsize,
    cancel1: AtomicUsize,
    cancel2: AtomicUsize,
    action1: AtomicUsize,
    query: AtomicUsize,
}

async fn spawn(hits: Arc<Hits>, confirm2_ok: bool, query_answer: &'static str) -> String {
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::{get, post};
    use axum::Router;

    macro_rules! counted {
        ($field:ident) => {
            post(|State(h): State<Arc<Hits>>| async move {
                h.$field.fetch_add(1, Ordering::SeqCst);
                (StatusCode::OK, "SUCCESS")
            })
        };
    }

    let app = Router::new()
        .route("/confirm1", counted!(confirm1))
        .route("/cancel1", counted!(cancel1))
        .route("/cancel2", counted!(cancel2))
        .route("/action1", counted!(action1))
        .route(
            "/confirm2",
            post(move |State(h): State<Arc<Hits>>| async move {
                h.confirm2.fetch_add(1, Ordering::SeqCst);
                if confirm2_ok {
                    (StatusCode::OK, "SUCCESS")
                } else {
                    // 业务返回明确失败 —— TCC 里这**不能**变成 cancel
                    (StatusCode::CONFLICT, "FAILURE")
                }
            }),
        )
        .route(
            "/query",
            // 回查地址：TC 会 POST，但用 get+post 都挂上更宽容
            post(move |State(h): State<Arc<Hits>>| async move {
                h.query.fetch_add(1, Ordering::SeqCst);
                match query_answer {
                    "committed" => (StatusCode::OK, "SUCCESS"),
                    "not_committed" => (StatusCode::CONFLICT, "FAILURE"),
                    _ => (StatusCode::TOO_EARLY, "ONGOING"),
                }
            })
            .merge(get(|| async { "SUCCESS" })),
        )
        .with_state(hits);

    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    format!("http://{addr}")
}

async fn store() -> Store {
    Store::open("sqlite::memory:").await.unwrap()
}

// ======================= TCC =======================

/// 模拟客户端的 try 阶段：先登记分支，再（假装）调 try
async fn client_try(s: &Store, gid: &str, base: &str, n: usize) {
    for i in 0..n {
        let bid = format!("{:02}", i + 1);
        s.register_branch(
            gid,
            &bid,
            &[
                (BranchOp::Confirm, format!("{base}/confirm{}", i + 1)),
                (BranchOp::Cancel, format!("{base}/cancel{}", i + 1)),
            ],
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn tcc_全部try成功后confirm两个分支() {
    let h = Arc::new(Hits::default());
    let base = spawn(h.clone(), true, "committed").await;
    let s = store().await;
    let d = Driver::new(s.clone(), "tc".into());

    s.create_global(&tcc_rows("tcc-1"), &[]).await.unwrap();
    client_try(&s, "tcc-1", &base, 2).await;

    // prepared 阶段 TC 不该动它 —— try 是客户端驱动的
    let g = s.get_global("tcc-1").await.unwrap().unwrap();
    d.process(&g).await.unwrap();
    assert_eq!(
        s.get_global("tcc-1").await.unwrap().unwrap().status,
        GlobalStatus::Prepared,
        "prepared 阶段 TC 不能插手"
    );
    assert_eq!(h.confirm1.load(Ordering::SeqCst), 0);

    // 客户端 try 都成功了 → submit
    s.set_global_status("tcc-1", GlobalStatus::Submitted, TransType::Tcc, "")
        .await
        .unwrap();
    let g = s.get_global("tcc-1").await.unwrap().unwrap();
    d.process(&g).await.unwrap();

    assert_eq!(
        s.get_global("tcc-1").await.unwrap().unwrap().status,
        GlobalStatus::Succeed
    );
    assert_eq!(h.confirm1.load(Ordering::SeqCst), 1);
    assert_eq!(h.confirm2.load(Ordering::SeqCst), 1);
    assert_eq!(h.cancel1.load(Ordering::SeqCst), 0, "成功路径不该 cancel");
}

#[tokio::test]
async fn tcc_confirm失败只重试绝不转cancel() {
    // TCC 最危险的失效模式：try 已成功、资源已预留、全局已决定提交，
    // 这时候 confirm 失败如果去 cancel，就把已确认的事务撤销了。
    let h = Arc::new(Hits::default());
    let base = spawn(h.clone(), false, "committed").await; // confirm2 返回 409
    let s = store().await;
    let d = Driver::new(s.clone(), "tc".into());

    s.create_global(&tcc_rows("tcc-2"), &[]).await.unwrap();
    client_try(&s, "tcc-2", &base, 2).await;
    s.set_global_status("tcc-2", GlobalStatus::Submitted, TransType::Tcc, "")
        .await
        .unwrap();

    // 推三轮，confirm2 一直失败
    for _ in 0..3 {
        let g = s.get_global("tcc-2").await.unwrap().unwrap();
        d.process(&g).await.unwrap();
    }

    let g = s.get_global("tcc-2").await.unwrap().unwrap();
    assert_eq!(
        g.status,
        GlobalStatus::Submitted,
        "必须保持 submitted，绝不能转 aborting/failed"
    );
    assert_eq!(
        h.cancel1.load(Ordering::SeqCst),
        0,
        "绝不能 cancel 已确认的分支"
    );
    assert_eq!(h.cancel2.load(Ordering::SeqCst), 0);
    assert!(h.confirm2.load(Ordering::SeqCst) >= 2, "confirm 要持续重试");
    assert!(g.next_cron_interval > 0, "要有退避间隔");
}

#[tokio::test]
async fn tcc_try失败则逆序cancel() {
    let h = Arc::new(Hits::default());
    let base = spawn(h.clone(), true, "committed").await;
    let s = store().await;
    let d = Driver::new(s.clone(), "tc".into());

    s.create_global(&tcc_rows("tcc-3"), &[]).await.unwrap();
    client_try(&s, "tcc-3", &base, 2).await;
    // 客户端发现某个 try 失败了 → abort
    s.set_global_status(
        "tcc-3",
        GlobalStatus::Aborting,
        TransType::Tcc,
        "第 2 步 try 失败",
    )
    .await
    .unwrap();

    let g = s.get_global("tcc-3").await.unwrap().unwrap();
    d.process(&g).await.unwrap();

    assert_eq!(
        s.get_global("tcc-3").await.unwrap().unwrap().status,
        GlobalStatus::Failed
    );
    assert_eq!(h.cancel1.load(Ordering::SeqCst), 1);
    assert_eq!(h.cancel2.load(Ordering::SeqCst), 1, "两个分支都要 cancel");
    assert_eq!(h.confirm1.load(Ordering::SeqCst), 0, "回滚路径不该 confirm");
}

#[tokio::test]
async fn tcc_没登记分支的空事务直接落终态() {
    let h = Arc::new(Hits::default());
    let _ = spawn(h.clone(), true, "committed").await;
    let s = store().await;
    let d = Driver::new(s.clone(), "tc".into());
    s.create_global(&tcc_rows("tcc-4"), &[]).await.unwrap();
    s.set_global_status("tcc-4", GlobalStatus::Submitted, TransType::Tcc, "")
        .await
        .unwrap();
    let g = s.get_global("tcc-4").await.unwrap().unwrap();
    d.process(&g).await.unwrap();
    assert_eq!(
        s.get_global("tcc-4").await.unwrap().unwrap().status,
        GlobalStatus::Succeed
    );
}

// ======================= 二阶段消息 =======================

#[tokio::test]
async fn msg_正常提交后推进正向分支() {
    let h = Arc::new(Hits::default());
    let base = spawn(h.clone(), true, "committed").await;
    let s = store().await;
    let d = Driver::new(s.clone(), "tc".into());

    let (g, br) = msg_rows(
        "msg-1",
        &[format!("{base}/action1")],
        &format!("{base}/query"),
        0,
    );
    s.create_global(&g, &br).await.unwrap();

    // 客户端本地事务提交成功 → submit
    s.set_global_status("msg-1", GlobalStatus::Submitted, TransType::Msg, "")
        .await
        .unwrap();
    let g = s.get_global("msg-1").await.unwrap().unwrap();
    d.process(&g).await.unwrap();

    assert_eq!(
        s.get_global("msg-1").await.unwrap().unwrap().status,
        GlobalStatus::Succeed
    );
    assert_eq!(h.action1.load(Ordering::SeqCst), 1);
    assert_eq!(h.query.load(Ordering::SeqCst), 0, "正常路径不需要回查");
}

#[tokio::test]
async fn msg_客户端崩在中间_回查说已提交则继续推() {
    // 这是二阶段消息的核心价值：不需要 MQ 也能保证"本地事务成功 ⇒ 消息必达"
    let h = Arc::new(Hits::default());
    let base = spawn(h.clone(), true, "committed").await;
    let s = store().await;
    let d = Driver::new(s.clone(), "tc".into());

    let (g, br) = msg_rows(
        "msg-2",
        &[format!("{base}/action1")],
        &format!("{base}/query"),
        0,
    );
    s.create_global(&g, &br).await.unwrap();
    // 客户端 prepare 之后就崩了，从来没调 submit —— 状态卡在 prepared
    assert_eq!(
        s.get_global("msg-2").await.unwrap().unwrap().status,
        GlobalStatus::Prepared
    );

    let g = s.get_global("msg-2").await.unwrap().unwrap();
    d.process(&g).await.unwrap();

    assert_eq!(h.query.load(Ordering::SeqCst), 1, "必须回查");
    assert_eq!(
        s.get_global("msg-2").await.unwrap().unwrap().status,
        GlobalStatus::Succeed,
        "回查说已提交 → 自动推完，不需要客户端再来"
    );
    assert_eq!(h.action1.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn msg_回查说没提交则整单作废() {
    let h = Arc::new(Hits::default());
    let base = spawn(h.clone(), true, "not_committed").await;
    let s = store().await;
    let d = Driver::new(s.clone(), "tc".into());

    let (g, br) = msg_rows(
        "msg-3",
        &[format!("{base}/action1")],
        &format!("{base}/query"),
        0,
    );
    s.create_global(&g, &br).await.unwrap();
    let g = s.get_global("msg-3").await.unwrap().unwrap();
    d.process(&g).await.unwrap();

    let got = s.get_global("msg-3").await.unwrap().unwrap();
    assert_eq!(got.status, GlobalStatus::Failed);
    assert!(
        got.rollback_reason.contains("未提交"),
        "要记下原因: {}",
        got.rollback_reason
    );
    assert_eq!(
        h.action1.load(Ordering::SeqCst),
        0,
        "作废了就不能发正向分支"
    );
}

#[tokio::test]
async fn msg_回查本身失败时不能当成没提交() {
    // 回查超时 ≠ 本地事务没提交。当成没提交就把一笔已成功的业务丢了。
    let h = Arc::new(Hits::default());
    let base = spawn(h.clone(), true, "ongoing").await; // 回查返回 425
    let s = store().await;
    let d = Driver::new(s.clone(), "tc".into());

    let (g, br) = msg_rows(
        "msg-4",
        &[format!("{base}/action1")],
        &format!("{base}/query"),
        0,
    );
    s.create_global(&g, &br).await.unwrap();
    let g = s.get_global("msg-4").await.unwrap().unwrap();
    d.process(&g).await.unwrap();

    let got = s.get_global("msg-4").await.unwrap().unwrap();
    assert_eq!(
        got.status,
        GlobalStatus::Prepared,
        "回查没结论就保持 prepared 重试"
    );
    assert!(got.next_cron_interval > 0);
    assert_eq!(h.action1.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn msg_prepared状态会被cron捞起来() {
    // 其它类型的 prepared 不该被捞（TCC 的 try 由客户端驱动），
    // 但 msg 的 prepared 必须被捞 —— 否则客户端崩了就永远没人管
    let s = store().await;

    let (g, br) = msg_rows("msg-5", &["http://x/a".into()], "http://x/q", 0);
    s.create_global(&g, &br).await.unwrap();
    let got = s.lock_one_due("tc", 30).await.unwrap();
    assert!(got.is_some(), "prepared 的 msg 必须能被 cron 捞到");
    assert_eq!(got.unwrap().gid, "msg-5");

    // 对照：prepared 的 tcc 不该被捞
    let s2 = store().await;
    s2.create_global(&tcc_rows("tcc-x"), &[]).await.unwrap();
    assert!(
        s2.lock_one_due("tc", 30).await.unwrap().is_none(),
        "prepared 的 tcc 不能被 cron 碰，那是客户端的 try 阶段"
    );
}

#[tokio::test]
async fn msg_没给回查地址时不瞎猜() {
    let s = store().await;
    let d = Driver::new(s.clone(), "tc".into());
    let (mut g, br) = msg_rows("msg-6", &["http://127.0.0.1:1/a".into()], "", 0);
    g.query_prepared = String::new();
    s.create_global(&g, &br).await.unwrap();
    let g = s.get_global("msg-6").await.unwrap().unwrap();
    d.process(&g).await.unwrap();
    // 猜"已提交"会重复扣款，猜"没提交"会丢单 —— 只能挂着等人
    assert_eq!(
        s.get_global("msg-6").await.unwrap().unwrap().status,
        GlobalStatus::Prepared
    );
}

#[tokio::test]
async fn xa_空事务直接落终态() {
    // XA 已经实现了（见 tests/xa.rs 的真 Postgres 端到端）。
    // 这里只覆盖不需要数据库的边界：一个分支都没登记就 submit。
    let s = store().await;
    let d = Driver::new(s.clone(), "tc".into());
    let mut g = tcc_rows("xa-empty");
    g.trans_type = dtmrs_core::TransType::Xa;
    g.status = GlobalStatus::Submitted;
    s.create_global(&g, &[]).await.unwrap();
    let g = s.get_global("xa-empty").await.unwrap().unwrap();
    d.process(&g).await.unwrap();
    assert_eq!(
        s.get_global("xa-empty").await.unwrap().unwrap().status,
        GlobalStatus::Succeed,
        "没有分支要提交，空事务直接成功"
    );
}

#[tokio::test]
async fn xa_分支不可达时只重试不改方向() {
    // 二阶段调不通是"结果未知"。XA 里方向一旦定了就不能改 ——
    // 别的分支可能已经 COMMIT PREPARED 了。
    let s = store().await;
    let d = Driver::new(s.clone(), "tc".into());
    let mut g = tcc_rows("xa-unreach");
    g.trans_type = dtmrs_core::TransType::Xa;
    s.create_global(&g, &[]).await.unwrap();
    s.register_branch(
        "xa-unreach",
        "01",
        &[
            (BranchOp::Commit, "http://127.0.0.1:1/commit".to_string()),
            (
                BranchOp::Rollback,
                "http://127.0.0.1:1/rollback".to_string(),
            ),
        ],
    )
    .await
    .unwrap();
    s.set_global_status("xa-unreach", GlobalStatus::Submitted, TransType::Xa, "")
        .await
        .unwrap();

    let g = s.get_global("xa-unreach").await.unwrap().unwrap();
    d.process(&g).await.unwrap();

    let got = s.get_global("xa-unreach").await.unwrap().unwrap();
    assert_eq!(
        got.status,
        GlobalStatus::Submitted,
        "调不通只能重试，不能转回滚"
    );
    assert!(got.next_cron_interval > 0, "要设置退避间隔");
}
