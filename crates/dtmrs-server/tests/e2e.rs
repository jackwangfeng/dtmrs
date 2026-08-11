//! 端到端：起一个假的业务服务，让真实推进器去调它，验证 SAGA 的正向、
//! 补偿、以及「超时不回滚」这条命门。
//!
//! 每个断言都盯着一个具体的失效模式，不是"跑通了就行"。

use dtmrs_core::{GlobalStatus, SagaStep};
use dtmrs_server::driver::Driver;
use dtmrs_server::saga_rows;
use dtmrs_store::Store;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// 假业务服务：记录每个路径被调了多少次，并按预设剧本返回
#[derive(Default)]
struct Busi {
    a1: AtomicUsize,
    c1: AtomicUsize,
    a2: AtomicUsize,
    c2: AtomicUsize,
    /// /a2 前几次返回 500（模拟超时/不可用）
    a2_fail_times: AtomicUsize,
}

impl Busi {
    fn counts(&self) -> (usize, usize, usize, usize) {
        (
            self.a1.load(Ordering::SeqCst),
            self.c1.load(Ordering::SeqCst),
            self.a2.load(Ordering::SeqCst),
            self.c2.load(Ordering::SeqCst),
        )
    }
}

/// 起服务，返回 base url。`a2_mode`: "ok" | "fail409" | "flaky"
async fn spawn_busi(busi: Arc<Busi>, a2_mode: &'static str) -> String {
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::Router;

    let app = Router::new()
        .route(
            "/a1",
            post(|State(b): State<Arc<Busi>>| async move {
                b.a1.fetch_add(1, Ordering::SeqCst);
                (StatusCode::OK, "SUCCESS")
            }),
        )
        .route(
            "/c1",
            post(|State(b): State<Arc<Busi>>| async move {
                b.c1.fetch_add(1, Ordering::SeqCst);
                (StatusCode::OK, "SUCCESS")
            }),
        )
        .route(
            "/c2",
            post(|State(b): State<Arc<Busi>>| async move {
                b.c2.fetch_add(1, Ordering::SeqCst);
                (StatusCode::OK, "SUCCESS")
            }),
        )
        .route(
            "/a2",
            post(move |State(b): State<Arc<Busi>>| async move {
                b.a2.fetch_add(1, Ordering::SeqCst);
                match a2_mode {
                    // 业务明确要求回滚
                    "fail409" => (StatusCode::CONFLICT, "FAILURE"),
                    // 前两次挂掉（结果未知），第三次成功
                    "flaky" => {
                        let n = b.a2_fail_times.fetch_add(1, Ordering::SeqCst);
                        if n < 2 {
                            (StatusCode::INTERNAL_SERVER_ERROR, "boom")
                        } else {
                            (StatusCode::OK, "SUCCESS")
                        }
                    }
                    _ => (StatusCode::OK, "SUCCESS"),
                }
            }),
        )
        .with_state(busi);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn steps(base: &str) -> Vec<SagaStep> {
    vec![
        SagaStep::new(&format!("{base}/a1"), &format!("{base}/c1")),
        SagaStep::new(&format!("{base}/a2"), &format!("{base}/c2")),
    ]
}

async fn setup(mode: &'static str) -> (Store, Driver, Arc<Busi>, Vec<SagaStep>) {
    let busi = Arc::new(Busi::default());
    let base = spawn_busi(busi.clone(), mode).await;
    let store = Store::open("sqlite::memory:").await.unwrap();
    let driver = Driver::new(store.clone(), "test-tc".into());
    let st = steps(&base);
    (store, driver, busi, st)
}

#[tokio::test]
async fn 全部成功则事务成功且每个分支只调一次() {
    let (store, driver, busi, st) = setup("ok").await;
    let (g, br) = saga_rows("happy", &st);
    store.create_global(&g, &br).await.unwrap();

    let g = store.get_global("happy").await.unwrap().unwrap();
    driver.process(&g).await.unwrap();

    let got = store.get_global("happy").await.unwrap().unwrap();
    assert_eq!(got.status, GlobalStatus::Succeed);
    let (a1, c1, a2, c2) = busi.counts();
    assert_eq!((a1, a2), (1, 1), "正向分支各调一次");
    assert_eq!((c1, c2), (0, 0), "成功路径绝不能调补偿");
    assert!(got.finish_time.is_some());
}

#[tokio::test]
async fn 分支明确失败则逆序补偿并落failed() {
    let (store, driver, busi, st) = setup("fail409").await;
    let (g, br) = saga_rows("rollback", &st);
    store.create_global(&g, &br).await.unwrap();

    let g = store.get_global("rollback").await.unwrap().unwrap();
    driver.process(&g).await.unwrap();

    let got = store.get_global("rollback").await.unwrap().unwrap();
    assert_eq!(got.status, GlobalStatus::Failed);
    assert!(
        got.rollback_reason.contains("02"),
        "要记下是哪个分支要求回滚，排障全靠它: {}",
        got.rollback_reason
    );
    let (a1, c1, a2, c2) = busi.counts();
    assert_eq!((a1, a2), (1, 1));
    // 两步都补：第 2 步的 action 虽然失败了，也可能有副作用；
    // 多余的补偿由客户端屏障空转掉，这是安全的一侧
    assert_eq!((c1, c2), (1, 1), "两步都要补偿");
}

#[tokio::test]
async fn 超时不能触发回滚而要重试() {
    // 这是分布式事务最容易写错的地方：500/超时代表**结果未知**，
    // 对方可能已经成功了。此时回滚会造成不一致，必须重试。
    let (store, driver, busi, st) = setup("flaky").await;
    let (g, br) = saga_rows("flaky", &st);
    store.create_global(&g, &br).await.unwrap();

    // 第一轮：a1 成功，a2 返回 500 → 应该退避重试，而不是回滚
    let g = store.get_global("flaky").await.unwrap().unwrap();
    driver.process(&g).await.unwrap();
    let got = store.get_global("flaky").await.unwrap().unwrap();
    assert_eq!(
        got.status,
        GlobalStatus::Submitted,
        "500 不能让事务转 aborting"
    );
    assert_eq!(busi.counts().1, 0, "结果未知时绝不能调补偿");
    assert!(got.next_cron_interval > 0, "要设置退避间隔");

    // 再推两轮，第三次 a2 会成功
    for _ in 0..2 {
        let g = store.get_global("flaky").await.unwrap().unwrap();
        driver.process(&g).await.unwrap();
    }
    let got = store.get_global("flaky").await.unwrap().unwrap();
    assert_eq!(got.status, GlobalStatus::Succeed, "重试到成功");
    let (_, c1, a2, c2) = busi.counts();
    assert_eq!(a2, 3, "a2 被重试了 3 次");
    assert_eq!((c1, c2), (0, 0), "最终成功，补偿一次都不该发");
}

#[tokio::test]
async fn 崩溃恢复_未终结事务会被重新捞起推完() {
    // 模拟 TC 在事务推进中途崩溃：DB 里留着 submitted 状态。
    // 重启后 cron 应该把它捞起来继续推 —— 不需要客户端重新提交。
    let (store, driver, busi, st) = setup("ok").await;
    let (g, br) = saga_rows("crashed", &st);
    store.create_global(&g, &br).await.unwrap();

    // 这里模拟"新起来的实例"通过 lock_one_due 抢到活
    let locked = store.lock_one_due("restarted-tc", 30).await.unwrap();
    let locked = locked.expect("未终结事务必须能被新实例捞到");
    assert_eq!(locked.gid, "crashed");
    assert_eq!(locked.owner, "restarted-tc", "租约要归新实例");

    driver.process(&locked).await.unwrap();
    let got = store.get_global("crashed").await.unwrap().unwrap();
    assert_eq!(got.status, GlobalStatus::Succeed);
    assert_eq!(busi.counts().0, 1);
}

#[tokio::test]
async fn 重复推进不会重复调用已成功的分支() {
    // TC 崩溃恢复会导致重复推进。已成功的分支不该被再调 ——
    // 这是 TC 侧的第一道防线（第二道是客户端屏障）。
    let (store, driver, busi, st) = setup("ok").await;
    let (g, br) = saga_rows("idem", &st);
    store.create_global(&g, &br).await.unwrap();

    let g = store.get_global("idem").await.unwrap().unwrap();
    driver.process(&g).await.unwrap();
    // 再推一遍
    let g2 = store.get_global("idem").await.unwrap().unwrap();
    driver.process(&g2).await.unwrap();

    let (a1, _, a2, _) = busi.counts();
    assert_eq!((a1, a2), (1, 1), "终态事务重复推进不该再调分支");
}

#[tokio::test]
async fn 主动中止会触发补偿() {
    let (store, driver, busi, st) = setup("ok").await;
    let (g, br) = saga_rows("aborted", &st);
    store.create_global(&g, &br).await.unwrap();
    // 调用方改主意了
    store
        .set_global_status("aborted", GlobalStatus::Aborting, "调用方主动中止")
        .await
        .unwrap();

    let g = store.get_global("aborted").await.unwrap().unwrap();
    driver.process(&g).await.unwrap();

    let got = store.get_global("aborted").await.unwrap().unwrap();
    assert_eq!(got.status, GlobalStatus::Failed);
    let (a1, c1, a2, c2) = busi.counts();
    assert_eq!((a1, a2), (0, 0), "还没跑正向就中止了");
    // 正向没跑过也要发补偿：可能正在飞行中。空转由屏障负责
    assert_eq!((c1, c2), (1, 1));
}

/// 记录「哪个路径收到了什么请求体」。抽成别名是 clippy 要求的
type SeenBodies = Arc<std::sync::Mutex<Vec<(String, String)>>>;
/// 只记请求体
type SeenOne = Arc<std::sync::Mutex<Vec<String>>>;

/// 每步的 payload 必须**各自独立**地送到对应分支。
///
/// 这条以前是坏的：`branch_payload()` 硬编码返回 `{}`，所有分支收到的请求体
/// 一模一样，真实业务（扣款要金额、发货要地址）根本用不了。
#[tokio::test]
async fn 每步的payload各自独立送达() {
    // 记录每个路径收到的请求体
    let seen: SeenBodies = Arc::new(std::sync::Mutex::new(Vec::new()));

    let s = seen.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        use axum::extract::State;
        use axum::routing::post;
        let app = axum::Router::new()
            .route(
                "/a1",
                post(|State(s): State<SeenBodies>, body: String| async move {
                    s.lock().unwrap().push(("a1".into(), body));
                    "ok"
                }),
            )
            .route(
                "/a2",
                post(|State(s): State<SeenBodies>, body: String| async move {
                    s.lock().unwrap().push(("a2".into(), body));
                    "ok"
                }),
            )
            .with_state(s);
        axum::serve(listener, app).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let store = Store::open("sqlite::memory:").await.unwrap();
    let d = Driver::new(store.clone(), "tc-1".into());
    let steps = vec![
        SagaStep::with_payload(
            &format!("http://{addr}/a1"),
            &format!("http://{addr}/a1"),
            r#"{"amount":100}"#,
        ),
        SagaStep::with_payload(
            &format!("http://{addr}/a2"),
            &format!("http://{addr}/a2"),
            r#"{"address":"北京"}"#,
        ),
    ];
    let (g, br) = saga_rows("payload-1", &steps);
    store.create_global(&g, &br).await.unwrap();
    d.process(&g).await.unwrap();

    let got = seen.lock().unwrap().clone();
    assert_eq!(got.len(), 2, "两步各调一次");
    assert_eq!(got[0], ("a1".to_string(), r#"{"amount":100}"#.to_string()));
    assert_eq!(
        got[1],
        ("a2".to_string(), r#"{"address":"北京"}"#.to_string()),
        "第二步必须收到自己的 payload，不能是第一步的、也不能是 {{}}"
    );
}

/// 没写 payload 的步骤仍然发 `{}` —— 保持跟 0.2 一致，不破坏现有分支
#[tokio::test]
async fn 没写payload的步骤发空对象() {
    let seen: SeenOne = Arc::new(std::sync::Mutex::new(Vec::new()));
    let s = seen.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        use axum::extract::State;
        use axum::routing::post;
        let app = axum::Router::new()
            .route(
                "/a",
                post(|State(s): State<SeenOne>, body: String| async move {
                    s.lock().unwrap().push(body);
                    "ok"
                }),
            )
            .with_state(s);
        axum::serve(listener, app).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let store = Store::open("sqlite::memory:").await.unwrap();
    let d = Driver::new(store.clone(), "tc-1".into());
    let steps = vec![SagaStep::new(
        &format!("http://{addr}/a"),
        &format!("http://{addr}/a"),
    )];
    let (g, br) = saga_rows("payload-empty", &steps);
    store.create_global(&g, &br).await.unwrap();
    d.process(&g).await.unwrap();

    assert_eq!(seen.lock().unwrap().clone(), vec!["{}".to_string()]);
}

/// 管理台的「立刻重试」：把事务排到调度队首，但不跳过任何安全检查
#[tokio::test]
async fn 立刻重试把事务排到队首() {
    use dtmrs_server::api::{Api, ApiError};

    let store = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(store.clone());

    let steps = vec![SagaStep::new("http://x/a", "http://x/c")];
    let (g, br) = saga_rows("retry-1", &steps);
    store.create_global(&g, &br).await.unwrap();
    // 先让它退避到很久以后，模拟「重试了几次正在等」
    store.schedule_retry("retry-1", 300).await.unwrap();
    let before = store.get_global("retry-1").await.unwrap().unwrap();
    assert!(
        before.next_cron_time > dtmrs_store::now() + 100,
        "应该被推到很久以后"
    );

    api.retry("retry-1").await.expect("未终结的事务可以重试");
    let after = store.get_global("retry-1").await.unwrap().unwrap();
    assert!(
        after.next_cron_time <= dtmrs_store::now() + 1,
        "重试后应该立刻可被调度"
    );
    assert_eq!(
        after.next_cron_interval, 0,
        "退避累积要清零，否则下次又等 300 秒"
    );

    // 终态不能重试 —— 那会让已完结的事务重新变成活跃事务
    store
        .set_global_status("retry-1", GlobalStatus::Succeed, "")
        .await
        .unwrap();
    assert!(
        matches!(api.retry("retry-1").await, Err(ApiError::Conflict(_))),
        "终态事务重试必须被拒"
    );
    assert!(
        matches!(api.retry("没这个").await, Err(ApiError::NotFound(_))),
        "不存在的 gid 应该 404"
    );
}
