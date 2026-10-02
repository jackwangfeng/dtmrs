//! 二阶段消息按主题投递（`topic://`）。
//!
//! 每条测试钉一个**具体的失效模式**：
//! - 扇出：每个订阅者都收到、收到的是同一份 payload、分支号跟 DTM 一致
//! - 一个订阅者一直失败，**不能挡住**别的订阅者；成功了的不重发
//! - 订阅变更下一次 prepare 立刻生效，已经 prepare 的消息**不受影响**（不补发、不撤回）
//! - 崩溃（推进器换一个、存储重开）后接着投，已成功的不重发
//! - 没有订阅者：默认报 `topic not found`；`allow_empty_topic` 放行、直接完成、计数

mod common;

use common::for_each_backend;
use dtmrs_core::{BranchResult, BranchStatus, GlobalStatus};
use dtmrs_server::api::{Api, ApiError, PrepareOpts};
use dtmrs_server::driver::Driver;
use dtmrs_server::embedded::Embedded;
use dtmrs_store::Store;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// keel 库存「可售数跨 0」通知的形状：只带键，订阅方自己回库存读最新水位
const PAYLOAD: &str = r#"{"store_id":3,"sku_ids":[11,12]}"#;

/// 假订阅方。每个路径记被调次数、收到的 body 和 branch_id；`fail` 里的路径返回 409
#[derive(Default)]
struct Subs {
    hits: Mutex<HashMap<String, usize>>,
    bodies: Mutex<Vec<(String, String, String)>>,
    fail: Mutex<Vec<String>>,
}

impl Subs {
    fn hits(&self, path: &str) -> usize {
        *self.hits.lock().unwrap().get(path).unwrap_or(&0)
    }
    fn set_fail(&self, path: &str, on: bool) {
        let mut f = self.fail.lock().unwrap();
        f.retain(|p| p != path);
        if on {
            f.push(path.to_string());
        }
    }
}

async fn spawn(subs: Arc<Subs>) -> String {
    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::Router;

    let app = Router::new()
        .route(
            "/{name}",
            post(
                |State(s): State<Arc<Subs>>,
                 Path(name): Path<String>,
                 Query(q): Query<HashMap<String, String>>,
                 body: String| async move {
                    let path = format!("/{name}");
                    *s.hits.lock().unwrap().entry(path.clone()).or_default() += 1;
                    s.bodies.lock().unwrap().push((
                        path.clone(),
                        q.get("branch_id").cloned().unwrap_or_default(),
                        body,
                    ));
                    if s.fail.lock().unwrap().contains(&path) {
                        (StatusCode::CONFLICT, "FAILURE")
                    } else {
                        (StatusCode::OK, "SUCCESS")
                    }
                },
            ),
        )
        .with_state(subs);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    format!("http://{addr}")
}

fn opts(payload: &str, allow_empty: bool) -> PrepareOpts {
    PrepareOpts {
        payloads: vec![payload.to_string()],
        allow_empty_topic: allow_empty,
    }
}

/// prepare（发到主题）+ submit。回查地址随便给，正常路径用不上
async fn send(api: &Api, gid: &str, topic: &str, o: &PrepareOpts) -> Result<(), ApiError> {
    api.prepare_with(
        gid,
        "msg",
        &[format!("topic://{topic}")],
        "http://127.0.0.1:9/query",
        None,
        o,
    )
    .await?;
    api.submit(gid, "msg", &[]).await
}

/// 推一轮（相当于推进器抢到它一次）
async fn round(s: &Store, d: &Driver, gid: &str) -> GlobalStatus {
    let g = s.get_global(gid).await.unwrap().unwrap();
    d.process(&g).await.unwrap();
    s.get_global(gid).await.unwrap().unwrap().status
}

async fn branch_ids(s: &Store, gid: &str) -> Vec<String> {
    let mut v: Vec<String> = s
        .list_branches(gid)
        .await
        .unwrap()
        .into_iter()
        .map(|b| b.branch_id)
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn 扇出_每个订阅者都收到同一份payload_分支号同dtm() {
    for_each_backend("topic_fanout", |be| async move {
        let subs = Arc::new(Subs::default());
        let base = spawn(subs.clone()).await;
        let s = Store::open(&be.url).await.unwrap();
        let api = Api::new(s.clone());
        let d = Driver::new(s.clone(), "tc".into());
        for p in ["/a", "/b", "/c"] {
            api.subscribe("stock.zero_crossing", &format!("{base}{p}"), "")
                .await
                .unwrap();
        }

        send(&api, "fan-1", "stock.zero_crossing", &opts(PAYLOAD, false))
            .await
            .unwrap();
        assert_eq!(
            branch_ids(&s, "fan-1").await,
            ["01-01", "01-02", "01-03"],
            "{}",
            be.label
        );
        assert_eq!(
            round(&s, &d, "fan-1").await,
            GlobalStatus::Succeed,
            "{}",
            be.label
        );

        for p in ["/a", "/b", "/c"] {
            assert_eq!(subs.hits(p), 1, "{}: {p}", be.label);
        }
        let bodies = subs.bodies.lock().unwrap().clone();
        assert!(
            bodies.iter().all(|(_, _, b)| b == PAYLOAD),
            "{}: {bodies:?}",
            be.label
        );
        let mut bids: Vec<_> = bodies.iter().map(|(_, b, _)| b.clone()).collect();
        bids.sort();
        // 分支号各不相同 —— 订阅方的屏障按 gid+branch_id 去重，撞号就会把别人的当重复
        assert_eq!(bids, ["01-01", "01-02", "01-03"], "{}", be.label);
    })
    .await;
}

#[tokio::test]
async fn 只有一个订阅者时分支号是01() {
    let subs = Arc::new(Subs::default());
    let base = spawn(subs.clone()).await;
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    api.subscribe("t", &format!("{base}/a"), "").await.unwrap();
    send(&api, "one-1", "t", &opts(PAYLOAD, false))
        .await
        .unwrap();
    assert_eq!(branch_ids(&s, "one-1").await, ["01"]);
}

#[tokio::test]
async fn 一个订阅者一直失败不能挡住别的_成功的不重发() {
    for_each_backend("topic_isolate", |be| async move {
        let subs = Arc::new(Subs::default());
        let base = spawn(subs.clone()).await;
        let s = Store::open(&be.url).await.unwrap();
        let api = Api::new(s.clone());
        let d = Driver::new(s.clone(), "tc".into());
        // 坏的那个排第一个：串行实现会在它这里停住，后面的永远发不出去
        for p in ["/bad", "/ok1", "/ok2"] {
            api.subscribe("t", &format!("{base}{p}"), "").await.unwrap();
        }
        subs.set_fail("/bad", true);
        send(&api, "iso-1", "t", &opts(PAYLOAD, false))
            .await
            .unwrap();

        assert_eq!(
            round(&s, &d, "iso-1").await,
            GlobalStatus::Submitted,
            "{}",
            be.label
        );
        assert_eq!(
            subs.hits("/ok1"),
            1,
            "{}: 好的订阅者第一轮就要送到",
            be.label
        );
        assert_eq!(subs.hits("/ok2"), 1, "{}", be.label);
        let st: HashMap<String, BranchStatus> = s
            .list_branches("iso-1")
            .await
            .unwrap()
            .into_iter()
            .map(|b| (b.branch_id, b.status))
            .collect();
        assert_eq!(st["01-02"], BranchStatus::Succeed, "{}", be.label);
        assert_eq!(st["01-03"], BranchStatus::Succeed, "{}", be.label);
        assert_eq!(st["01-01"], BranchStatus::Prepared, "{}", be.label);

        // 再推两轮：只重试坏的那个，好的不重发
        round(&s, &d, "iso-1").await;
        round(&s, &d, "iso-1").await;
        assert_eq!(subs.hits("/bad"), 3, "{}", be.label);
        assert_eq!(subs.hits("/ok1"), 1, "{}: 成功过的不能重发", be.label);

        // FAILURE 也只是重试（msg 没有补偿）。修好之后送达，整单完成
        subs.set_fail("/bad", false);
        assert_eq!(
            round(&s, &d, "iso-1").await,
            GlobalStatus::Succeed,
            "{}",
            be.label
        );
        assert_eq!(subs.hits("/bad"), 4, "{}", be.label);
        assert_eq!(subs.hits("/ok2"), 1, "{}", be.label);
    })
    .await;
}

#[tokio::test]
async fn 订阅者挂住时别的订阅者不用等它() {
    // 并发而不是串行：一个订阅者卡到超时，别的订阅者在同一轮里照样送达。
    // ⚠ 慢的那个必须排在前面（同一秒登记的按 url 排序）：排后面的话串行实现也能过
    use axum::routing::post;
    let slow_done = Arc::new(AtomicBool::new(false));
    let fast_at = Arc::new(Mutex::new(None::<bool>));
    let (sd, fa) = (slow_done.clone(), fast_at.clone());
    let app = axum::Router::new()
        .route(
            "/a_slow",
            post(move || {
                let sd = sd.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(800)).await;
                    sd.store(true, Ordering::SeqCst);
                    "SUCCESS"
                }
            }),
        )
        .route(
            "/z_fast",
            post(move || {
                let (sd, fa) = (slow_done.clone(), fa.clone());
                async move {
                    *fa.lock().unwrap() = Some(sd.load(Ordering::SeqCst));
                    "SUCCESS"
                }
            }),
        );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });

    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    let d = Driver::new(s.clone(), "tc".into());
    api.subscribe("t", &format!("{base}/a_slow"), "")
        .await
        .unwrap();
    api.subscribe("t", &format!("{base}/z_fast"), "")
        .await
        .unwrap();
    send(&api, "slow-1", "t", &opts("", false)).await.unwrap();
    assert_eq!(round(&s, &d, "slow-1").await, GlobalStatus::Succeed);
    assert_eq!(
        *fast_at.lock().unwrap(),
        Some(false),
        "快的订阅者应该在慢的返回之前就被调到"
    );
}

#[tokio::test]
async fn 订阅变更下一次prepare立刻生效_已prepare的消息不受影响() {
    for_each_backend("topic_change", |be| async move {
        let subs = Arc::new(Subs::default());
        let base = spawn(subs.clone()).await;
        let s = Store::open(&be.url).await.unwrap();
        let api = Api::new(s.clone());
        let d = Driver::new(s.clone(), "tc".into());
        let (a, b) = (format!("{base}/a"), format!("{base}/b"));

        api.subscribe("t", &a, "").await.unwrap();
        send(&api, "chg-1", "t", &opts("1", false)).await.unwrap();
        // 1 已经 prepare：之后订阅的 b 收不到它（不补发），之后退订的 a 照样收到它（不撤回）
        api.subscribe("t", &b, "").await.unwrap();
        send(&api, "chg-2", "t", &opts("2", false)).await.unwrap();
        api.unsubscribe("t", &a).await.unwrap();
        send(&api, "chg-3", "t", &opts("3", false)).await.unwrap();

        for gid in ["chg-1", "chg-2", "chg-3"] {
            assert_eq!(
                round(&s, &d, gid).await,
                GlobalStatus::Succeed,
                "{}: {gid}",
                be.label
            );
        }
        let mut got: Vec<(String, String)> = subs
            .bodies
            .lock()
            .unwrap()
            .iter()
            .map(|(p, _, body)| (p.clone(), body.clone()))
            .collect();
        got.sort();
        let want: Vec<(String, String)> = [("/a", "1"), ("/a", "2"), ("/b", "2"), ("/b", "3")]
            .iter()
            .map(|(p, x)| (p.to_string(), x.to_string()))
            .collect();
        assert_eq!(got, want, "{}", be.label);
    })
    .await;
}

#[tokio::test]
async fn 崩溃后换一个推进器接着投_已成功的不重发() {
    for_each_backend("topic_crash", |be| async move {
        let subs = Arc::new(Subs::default());
        let base = spawn(subs.clone()).await;
        {
            let s = Store::open(&be.url).await.unwrap();
            let api = Api::new(s.clone());
            let d = Driver::new(s.clone(), "tc-old".into());
            api.subscribe("t", &format!("{base}/a"), "").await.unwrap();
            api.subscribe("t", &format!("{base}/b"), "").await.unwrap();
            subs.set_fail("/b", true);
            send(&api, "crash-1", "t", &opts(PAYLOAD, false))
                .await
                .unwrap();
            round(&s, &d, "crash-1").await;
            // 「进程」到这里没了：推进器、Api、存储句柄全丢
            s.close().await;
        }
        subs.set_fail("/b", false);
        let s = Store::open(&be.url).await.unwrap();
        let d = Driver::new(s.clone(), "tc-new".into());
        assert_eq!(
            round(&s, &d, "crash-1").await,
            GlobalStatus::Succeed,
            "{}",
            be.label
        );
        assert_eq!(subs.hits("/a"), 1, "{}: 崩溃前已送达的不重发", be.label);
        assert_eq!(subs.hits("/b"), 2, "{}", be.label);
        // 重启后不靠内存里的订阅表：分支在 prepare 时就落库了
        let bodies = subs.bodies.lock().unwrap().clone();
        assert!(bodies.iter().all(|(_, _, b)| b == PAYLOAD), "{}", be.label);
    })
    .await;
}

#[tokio::test]
async fn 主题没有订阅者默认报错_不能悄悄丢消息() {
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    let e = send(&api, "none-1", "没人订", &opts(PAYLOAD, false))
        .await
        .unwrap_err();
    assert_eq!(e.message(), "topic not found");
    assert!(
        s.get_global("none-1").await.unwrap().is_none(),
        "报错就不能落库"
    );
    assert_eq!(api.empty_topic_count(), 0);
}

#[tokio::test]
async fn 允许为空时没订阅者照常受理_提交后直接完成并计数() {
    let subs = Arc::new(Subs::default());
    let base = spawn(subs.clone()).await;
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    let d = Driver::new(s.clone(), "tc".into());

    send(&api, "empty-1", "没人订", &opts(PAYLOAD, true))
        .await
        .unwrap();
    assert_eq!(api.empty_topic_count(), 1);
    assert!(branch_ids(&s, "empty-1").await.is_empty());
    assert_eq!(round(&s, &d, "empty-1").await, GlobalStatus::Succeed);

    // 一条消息里空主题 + 普通地址：普通的照发，分支号仍按 step 编（02）
    api.prepare_with(
        "empty-2",
        "msg",
        &["topic://没人订".to_string(), format!("{base}/a")],
        "http://127.0.0.1:9/query",
        None,
        &PrepareOpts {
            payloads: vec!["x".into(), "y".into()],
            allow_empty_topic: true,
        },
    )
    .await
    .unwrap();
    api.submit("empty-2", "msg", &[]).await.unwrap();
    assert_eq!(branch_ids(&s, "empty-2").await, ["02"]);
    assert_eq!(round(&s, &d, "empty-2").await, GlobalStatus::Succeed);
    assert_eq!(subs.hits("/a"), 1);
    assert_eq!(api.empty_topic_count(), 2);
}

#[tokio::test]
async fn 订阅管理的错误文案同dtm() {
    let s = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(s.clone());
    let msg = |r: Result<(), ApiError>| r.unwrap_err().message().to_string();
    assert_eq!(msg(api.subscribe("", "http://a", "").await), "empty topic");
    assert_eq!(msg(api.subscribe("t", "", "").await), "empty url");
    api.subscribe("t", "http://a", "").await.unwrap();
    assert_eq!(
        msg(api.subscribe("t", "http://a", "").await),
        "this url exists"
    );
    assert_eq!(
        msg(api.unsubscribe("没有", "http://a").await),
        "no such a topic"
    );
    assert_eq!(
        msg(api.unsubscribe("t", "http://b").await),
        "no such an url "
    );
    assert_eq!(msg(api.delete_topic("没有").await), "storage: NotFound");
    api.delete_topic("t").await.unwrap();
    // 主题套主题会展开成打不通的分支
    assert!(api.subscribe("t", "topic://别的", "").await.is_err());
}

// ======================= 嵌入式 =======================

#[tokio::test]
async fn 嵌入式_静态订阅和存储订阅取并集_local订阅者收到payload() {
    let got = Arc::new(Mutex::new(Vec::<String>::new()));
    let subs = Arc::new(Subs::default());
    let base = spawn(subs.clone()).await;
    let g2 = got.clone();
    let tc = Embedded::builder("sqlite::memory:")
        .handler("刷新有货", move |c| {
            let g = g2.clone();
            async move {
                g.lock().unwrap().push(c.payload.clone());
                BranchResult::Success
            }
        })
        .handler("回查", |_| async { BranchResult::Success })
        .subscribe("stock.zero_crossing", "local://刷新有货")
        .tick(Duration::from_millis(20))
        .start()
        .await
        .unwrap();
    tc.subscribe("stock.zero_crossing", &format!("{base}/a"), "core")
        .await
        .unwrap();
    // 静态的不能被接口退订
    assert!(tc
        .unsubscribe("stock.zero_crossing", "local://刷新有货")
        .await
        .is_err());
    let list = tc.subscriptions(None).await.unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].remark, "(static)", "静态的排前面");

    tc.msg("emb-1")
        .topic("stock.zero_crossing", PAYLOAD)
        .query_prepared("local://回查")
        .do_and_submit(|| async { BranchResult::Success })
        .await
        .unwrap();
    assert_eq!(
        tc.wait_final("emb-1", Duration::from_secs(5))
            .await
            .unwrap(),
        GlobalStatus::Succeed
    );
    assert_eq!(*got.lock().unwrap(), [PAYLOAD]);
    assert_eq!(subs.hits("/a"), 1);
    tc.shutdown().await;
}

#[tokio::test]
async fn 嵌入式_静态订阅的local没注册要在启动时报错() {
    let r = Embedded::builder("sqlite::memory:")
        .subscribe("t", "local://没注册")
        .start()
        .await;
    assert!(r.is_err());
}

#[tokio::test]
async fn 嵌入式_允许为空时不挡住业务() {
    // keel 的场景：库存在扣减的本地事务里发通知，订阅方还没登记 ——
    // 通知晚到可以接受，扣库存本身不能失败
    let tc = Embedded::builder("sqlite::memory:")
        .handler("回查", |_| async { BranchResult::Success })
        .tick(Duration::from_millis(20))
        .start()
        .await
        .unwrap();
    let ran = Arc::new(AtomicUsize::new(0));
    let r2 = ran.clone();
    let r = tc
        .msg("emb-empty")
        .topic("stock.zero_crossing", PAYLOAD)
        .allow_empty_topic()
        .query_prepared("local://回查")
        .do_and_submit(|| async move {
            r2.fetch_add(1, Ordering::SeqCst);
            BranchResult::Success
        })
        .await
        .unwrap();
    assert_eq!(r, BranchResult::Success);
    assert_eq!(ran.load(Ordering::SeqCst), 1, "本地事务要照常跑");
    assert_eq!(
        tc.wait_final("emb-empty", Duration::from_secs(5))
            .await
            .unwrap(),
        GlobalStatus::Succeed
    );
    assert_eq!(tc.empty_topic_count(), 1);

    // 不开关的话 prepare 失败、本地事务不跑（跟 DTM 一致）
    let r3 = ran.clone();
    let e = tc
        .msg("emb-empty-2")
        .topic("stock.zero_crossing", PAYLOAD)
        .query_prepared("local://回查")
        .do_and_submit(|| async move {
            r3.fetch_add(1, Ordering::SeqCst);
            BranchResult::Success
        })
        .await;
    assert!(e.is_err());
    assert_eq!(ran.load(Ordering::SeqCst), 1);
    tc.shutdown().await;
}

#[tokio::test]
async fn 嵌入式_订阅接口要鉴权_订阅方自己登记后就能收到() {
    let subs = Arc::new(Subs::default());
    let base = spawn(subs.clone()).await;
    let tc = Embedded::builder("sqlite::memory:")
        .handler("回查", |_| async { BranchResult::Success })
        .tick(Duration::from_millis(20))
        .start()
        .await
        .unwrap();
    assert!(
        tc.serve_topic_api("127.0.0.1:0", "").await.is_err(),
        "不带密钥不能开"
    );
    let addr = tc.serve_topic_api("127.0.0.1:0", "s3cret").await.unwrap();
    let api = format!("http://{addr}");
    let c = reqwest::Client::new();
    let url = format!("{base}/core");
    let sub = |tok: Option<&'static str>| {
        let mut r = c.get(format!("{api}/api/dtmsvr/subscribe")).query(&[
            ("topic", "stock.zero_crossing"),
            ("url", url.as_str()),
            ("remark", "core"),
        ]);
        if let Some(t) = tok {
            r = r.bearer_auth(t);
        }
        r.send()
    };

    assert_eq!(sub(None).await.unwrap().status(), 401);
    assert_eq!(sub(Some("wrong")).await.unwrap().status(), 401);
    assert!(
        tc.subscriptions(None).await.unwrap().is_empty(),
        "没鉴权不能登记上"
    );
    assert_eq!(sub(Some("s3cret")).await.unwrap().status(), 200);
    // 重复登记（订阅方重启后再登一次）：报 this url exists，订阅方按「已经在了」处理
    let dup = sub(Some("s3cret")).await.unwrap();
    assert_ne!(dup.status(), 200);
    assert!(dup.text().await.unwrap().contains("this url exists"));

    // /health 不鉴权；prepare 这类接口根本不开放
    assert_eq!(
        c.get(format!("{api}/health"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let p = c
        .post(format!("{api}/api/dtmsvr/prepare"))
        .bearer_auth("s3cret")
        .send()
        .await
        .unwrap();
    assert!(p.status() == 404 || p.status() == 405, "{}", p.status());

    // queryKV 的形状同 DTM：v 是 JSON 字符串
    let kv: serde_json::Value = c
        .get(format!(
            "{api}/api/dtmsvr/queryKV?cat=topics&key=stock.zero_crossing"
        ))
        .bearer_auth("s3cret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let item = &kv["kv"][0];
    assert_eq!(item["cat"], "topics");
    assert_eq!(item["k"], "stock.zero_crossing");
    let v: serde_json::Value = serde_json::from_str(item["v"].as_str().unwrap()).unwrap();
    assert_eq!(v[0]["url"], url.as_str());
    assert_eq!(v[0]["remark"], "core");

    tc.msg("api-1")
        .topic("stock.zero_crossing", PAYLOAD)
        .query_prepared("local://回查")
        .do_and_submit(|| async { BranchResult::Success })
        .await
        .unwrap();
    assert_eq!(
        tc.wait_final("api-1", Duration::from_secs(5))
            .await
            .unwrap(),
        GlobalStatus::Succeed
    );
    assert_eq!(subs.hits("/core"), 1);

    tc.shutdown().await;
    // 关掉之后端口不再受理
    assert!(c.get(format!("{api}/health")).send().await.is_err());
}
