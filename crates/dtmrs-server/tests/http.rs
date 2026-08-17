//! HTTP 协议层的测试，外加**与 gRPC 的等价性**测试。
//!
//! 为什么要有这个文件：HTTP 层原先写在二进制 crate 的 main.rs 里，tests/ 够不着，
//! 覆盖率是 **0%**，而 gRPC 层有 86%。也就是说「两个协议层不许漂移」这条结构约束
//! 只有一半受测试保护 —— 而漂移的后果恰恰是最难查的那种：
//! **同一个请求走 HTTP 被拒、走 gRPC 却受理了**。
//!
//! ⚠ 等价性**不是**比状态码。两个协议表达「拒绝」的方式天然不同：
//!
//! | | 合法但当前状态做不了（ApiError::Conflict） |
//! |---|---|
//! | HTTP | 200 + body `{"dtm_result":"FAILURE"}`（DTM 协议靠 body 表达结果） |
//! | gRPC | `FAILED_PRECONDITION`(9) |
//!
//! 所以下面比的是**语义结论**：这个请求到底被受理了还是被拒了。

use dtmrs_core::{GlobalStatus, SagaStep, TransType};
use dtmrs_server::api::Api;
use dtmrs_server::http::{router, App};
use dtmrs_server::tcc_rows;
use dtmrs_store::Store;

async fn store() -> Store {
    Store::open("sqlite::memory:").await.unwrap()
}

/// 起一个真的 HTTP 服务，返回 base url。
/// 刻意走真 TCP 而不是 tower 的 oneshot —— 序列化/反序列化也要一起测到。
async fn spawn_http(api: Api) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(App::new(api))).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    format!("http://{addr}")
}

/// 一次 POST，返回 (HTTP 状态码, body 文本)
async fn post(base: &str, path: &str, body: &str) -> (u16, String) {
    let r = reqwest::Client::new()
        .post(format!("{base}{path}"))
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let code = r.status().as_u16();
    (code, r.text().await.unwrap())
}

/// 把 HTTP 应答归约成「受理了吗」这一个布尔 —— 这才是能跟 gRPC 比的东西。
///
/// DTM 协议里 TC 的应答**靠 body 里的 dtm_result 表达结果**，
/// 光看 HTTP 状态码会把 `200 + FAILURE` 误读成成功。
fn accepted(code: u16, body: &str) -> bool {
    (200..300).contains(&code) && !body.contains("FAILURE")
}

// ==================== 路由本身 ====================

#[tokio::test]
async fn 所有路由都挂上了() {
    let base = spawn_http(Api::new(store().await)).await;
    let cli = reqwest::Client::new();

    // 这些是 GET
    for (path, 期望非空) in [("/health", true), ("/api/dtmsvr/newGid", true), ("/console", true)] {
        let r = cli.get(format!("{base}{path}")).send().await.unwrap();
        assert!(r.status().is_success(), "{path} 应该 200，实际 {}", r.status());
        if 期望非空 {
            assert!(!r.text().await.unwrap().is_empty(), "{path} 不该返回空");
        }
    }
    // 管理台是内嵌 HTML，不该是空壳
    let html = cli.get(format!("{base}/")).send().await.unwrap().text().await.unwrap();
    assert!(html.contains("<"), "管理台应该返回 HTML");
}

#[tokio::test]
async fn new_gid每次都不一样() {
    let base = spawn_http(Api::new(store().await)).await;
    let cli = reqwest::Client::new();
    let mut seen = std::collections::HashSet::new();
    for _ in 0..5 {
        let t = cli
            .get(format!("{base}/api/dtmsvr/newGid"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(seen.insert(t.clone()), "gid 重复了: {t}");
    }
}

#[tokio::test]
async fn 提交saga并能查回来() {
    let st = store().await;
    let base = spawn_http(Api::new(st.clone())).await;

    let (code, body) = post(
        &base,
        "/api/dtmsvr/submit",
        r#"{"gid":"h1","steps":[{"action":"http://x/a","compensate":"http://x/c"}]}"#,
    )
    .await;
    assert!(accepted(code, &body), "提交应该被受理，得到 {code} {body}");

    let q = reqwest::get(format!("{base}/api/dtmsvr/query?gid=h1"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(q.contains("\"gid\":\"h1\""), "查询应该能查到: {q}");

    let all = reqwest::get(format!("{base}/api/dtmsvr/all"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(all.contains("h1"), "列表里应该有这一笔: {all}");
}

#[tokio::test]
async fn 重复提交同一个gid必须幂等成功() {
    // 客户端重试时会重复提交，报错会让它以为没受理（见「绝不能破坏的语义」第 4 条）
    let base = spawn_http(Api::new(store().await)).await;
    let b = r#"{"gid":"h-dup","steps":[{"action":"http://x/a","compensate":"http://x/c"}]}"#;
    for i in 1..=3 {
        let (code, body) = post(&base, "/api/dtmsvr/submit", b).await;
        assert!(accepted(code, &body), "第 {i} 次提交应该成功，得到 {code} {body}");
    }
}

#[tokio::test]
async fn 报文不合法要被拒而不是panic() {
    let base = spawn_http(Api::new(store().await)).await;
    for (path, body) in [
        ("/api/dtmsvr/submit", "{}"),                       // 缺 gid
        ("/api/dtmsvr/submit", "不是json"),                 // 根本不是 JSON
        ("/api/dtmsvr/registerBranch", r#"{"gid":"x"}"#),   // 缺 branch_id
        ("/api/dtmsvr/abort", r#"{"gid":"不存在"}"#),        // gid 不存在
    ] {
        let (code, _) = post(&base, path, body).await;
        assert!(
            code < 500,
            "{path} 的烂报文应该是 4xx 客户端错误，不该是 5xx（得到 {code}）"
        );
        // 服务还活着
        let h = reqwest::get(format!("{base}/health")).await.unwrap();
        assert!(h.status().is_success(), "{path} 之后服务应该还活着");
    }
}

// ==================== 与 gRPC 的等价性 ====================

#[cfg(feature = "grpc")]
mod 等价 {
    use super::*;
    use dtmrs_server::grpc::pb;
    use dtmrs_server::grpc::server::TcService;
    use tokio_stream::wrappers::TcpListenerStream;

    async fn spawn_grpc(api: Api) -> String {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TcService::new(api).into_server())
                .serve_with_incoming(TcpListenerStream::new(l))
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        format!("http://{addr}")
    }

    /// 造一笔处于指定状态的 tcc 事务，然后**两个协议各自**试着登记分支，
    /// 返回 (http 受理了吗, grpc 受理了吗)。
    async fn 两边登记分支(状态: GlobalStatus) -> (bool, bool) {
        let st = store().await;
        let http_base = spawn_http(Api::new(st.clone())).await;
        let grpc_base = spawn_grpc(Api::new(st.clone())).await;

        for gid in ["eq-http", "eq-grpc"] {
            st.create_global(&tcc_rows(gid), &[]).await.unwrap();
            st.set_global_status(gid, 状态, TransType::Tcc, "")
                .await
                .unwrap();
        }

        let (c, b) = post(
            &http_base,
            "/api/dtmsvr/registerBranch",
            r#"{"gid":"eq-http","branch_id":"01","confirm":"http://x/c","cancel":"http://x/n"}"#,
        )
        .await;
        let http_ok = accepted(c, &b);

        let mut cli = pb::tc_client::TcClient::connect(grpc_base).await.unwrap();
        let grpc_ok = cli
            .register_branch(pb::RegisterBranchRequest {
                gid: "eq-grpc".into(),
                branch_id: "01".into(),
                confirm: "http://x/c".into(),
                cancel: "http://x/n".into(),
                r#try: String::new(),
                commit: String::new(),
                rollback: String::new(),
            })
            .await
            .is_ok();

        (http_ok, grpc_ok)
    }

    #[tokio::test]
    async fn 登记分支的受理结论两边必须一致() {
        // 这就是那个最难查的漂移：同一个请求走 HTTP 被拒、走 gRPC 却受理了。
        for 状态 in [
            GlobalStatus::Prepared,
            GlobalStatus::Submitted,
            GlobalStatus::Aborting,
            GlobalStatus::Succeed,
            GlobalStatus::Failed,
        ] {
            let (http_ok, grpc_ok) = 两边登记分支(状态).await;
            assert_eq!(
                http_ok,
                grpc_ok,
                "{} 状态下两个协议结论不一致：HTTP {} / gRPC {}",
                状态.as_str(),
                if http_ok { "受理" } else { "拒绝" },
                if grpc_ok { "受理" } else { "拒绝" },
            );
        }
    }

    #[tokio::test]
    async fn 提交的受理结论两边必须一致() {
        let st = store().await;
        let http_base = spawn_http(Api::new(st.clone())).await;
        let grpc_base = spawn_grpc(Api::new(st.clone())).await;
        let mut cli = pb::tc_client::TcClient::connect(grpc_base).await.unwrap();

        // 空步骤：两边都该受理（幂等成功），这是历史上真踩过的一次漂移 ——
        // 曾经把 steps.is_empty() 的检查挪到存在性检查之前，
        // 结果「重复提交必须幂等成功」在 gRPC 侧被破坏了
        let (c, b) = post(&http_base, "/api/dtmsvr/submit", r#"{"gid":"eq-s1"}"#).await;
        let http_ok = accepted(c, &b);
        let grpc_ok = cli
            .submit(pb::SubmitRequest {
                gid: "eq-s2".into(),
                trans_type: String::new(),
                steps: vec![],
            })
            .await
            .is_ok();
        assert_eq!(http_ok, grpc_ok, "空步骤提交：HTTP {http_ok} / gRPC {grpc_ok}");

        // gid 为空：两边都该拒
        let (c, b) = post(&http_base, "/api/dtmsvr/submit", r#"{"gid":""}"#).await;
        let http_ok = accepted(c, &b);
        let grpc_ok = cli
            .submit(pb::SubmitRequest {
                gid: String::new(),
                trans_type: String::new(),
                steps: vec![],
            })
            .await
            .is_ok();
        assert_eq!(http_ok, grpc_ok, "空 gid：HTTP {http_ok} / gRPC {grpc_ok}");
    }

    #[tokio::test]
    async fn 中止的受理结论两边必须一致() {
        for 状态 in [GlobalStatus::Submitted, GlobalStatus::Succeed] {
            let st = store().await;
            let http_base = spawn_http(Api::new(st.clone())).await;
            let grpc_base = spawn_grpc(Api::new(st.clone())).await;
            for gid in ["ab-http", "ab-grpc"] {
                let steps = vec![SagaStep::new("http://x/a", "http://x/c")];
                let (g, br) = dtmrs_server::saga_rows(gid, &steps);
                st.create_global(&g, &br).await.unwrap();
                st.set_global_status(gid, 状态, TransType::Saga, "")
                    .await
                    .unwrap();
            }

            let (c, b) = post(&http_base, "/api/dtmsvr/abort", r#"{"gid":"ab-http"}"#).await;
            let http_ok = accepted(c, &b);
            let mut cli = pb::tc_client::TcClient::connect(grpc_base).await.unwrap();
            let grpc_ok = cli
                .abort(pb::AbortRequest {
                    gid: "ab-grpc".into(),
                })
                .await
                .is_ok();
            assert_eq!(
                http_ok,
                grpc_ok,
                "{} 状态下中止：HTTP {http_ok} / gRPC {grpc_ok}",
                状态.as_str()
            );
        }
    }
}

// ==================== 认证 ====================
//
// ⚠ 认证做在中间件/拦截器层，**在 api.rs 之外**，所以上面那组等价性测试
// 覆盖不到它。这正是它出过事的地方：0.5.0 给 HTTP 加了全局认证却漏了 gRPC，
// 于是「同一个请求走 HTTP 被拒、走 gRPC 却受理了」—— 恰好是
// 「绝对不能破坏的语义」第 2 条防的那种漂移。

#[cfg(feature = "grpc")]
mod 认证 {
    use super::*;
    use dtmrs_server::auth::Auth;
    use dtmrs_server::grpc::pb;
    use dtmrs_server::grpc::server::TcService;
    use std::sync::Arc;
    use tokio_stream::wrappers::TcpListenerStream;

    const TOKEN: &str = "test-token-9f3a";

    fn auth() -> Arc<Auth> {
        // from_env 读进程环境，测试里并行跑会互相污染，所以直接构造
        std::env::set_var("DTMRS_AUTH_TOKEN", TOKEN);
        std::env::set_var("DTMRS_ADMIN_PASSWORD", "pw-for-test");
        let a = Auth::from_env().expect("配了 token 应该启用");
        std::env::remove_var("DTMRS_AUTH_TOKEN");
        std::env::remove_var("DTMRS_ADMIN_PASSWORD");
        a
    }

    async fn spawn_http_auth(api: Api, a: Arc<Auth>, st: Store) -> String {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(l, dtmrs_server::http::router_with_auth(App::new(api), a, st)).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        format!("http://{addr}")
    }

    async fn spawn_grpc_auth(api: Api, a: Arc<Auth>) -> String {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TcService::new(api).into_server_with_auth(a))
                .serve_with_incoming(TcpListenerStream::new(l))
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        format!("http://{addr}")
    }

    /// 用给定的 token 分别打两个协议，返回 (http 通过吗, grpc 通过吗)
    async fn 两边带token(token: Option<&str>) -> (bool, bool) {
        let st = store().await;
        let a = auth();
        let h = spawn_http_auth(Api::new(st.clone()), a.clone(), st.clone()).await;
        let g = spawn_grpc_auth(Api::new(st.clone()), a.clone()).await;

        let mut rb = reqwest::Client::new().get(format!("{h}/api/dtmsvr/newGid"));
        if let Some(t) = token {
            rb = rb.header("authorization", format!("Bearer {t}"));
        }
        let http_ok = rb.send().await.unwrap().status().is_success();

        let mut cli = pb::tc_client::TcClient::connect(g).await.unwrap();
        let mut req = tonic::Request::new(pb::NewGidRequest {});
        if let Some(t) = token {
            req.metadata_mut()
                .insert("authorization", format!("Bearer {t}").parse().unwrap());
        }
        let grpc_ok = cli.new_gid(req).await.is_ok();
        (http_ok, grpc_ok)
    }

    #[tokio::test]
    async fn 带对的token两个协议都放行() {
        let (h, g) = 两边带token(Some(TOKEN)).await;
        assert!(h && g, "HTTP {h} / gRPC {g}，都该放行");
    }

    #[tokio::test]
    async fn 不带token两个协议都拒绝() {
        // 这条如果失败，多半是某个协议层漏挂了认证 —— 0.5.0 的 gRPC 就是这样
        let (h, g) = 两边带token(None).await;
        assert!(!h && !g, "HTTP {h} / gRPC {g}，都该拒绝");
    }

    #[tokio::test]
    async fn 带错的token两个协议都拒绝() {
        let (h, g) = 两边带token(Some("wrong-token")).await;
        assert!(!h && !g, "HTTP {h} / gRPC {g}，都该拒绝");
    }
}
