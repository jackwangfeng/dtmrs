//! gRPC 端到端：起真的 gRPC 服务，让真实推进器去调它。
//!
//! 两个方向都验：
//!
//! - TC **去调** gRPC 分支（`grpc://` 地址）—— 含「超时不回滚」这条命门
//! - TC **对外提供** gRPC API —— 与 HTTP 走同一份逻辑，不能有语义差异
//!
//! 每个断言都盯着一个具体的失效模式，不是「跑通了就行」。

use dtmrs_core::{GlobalStatus, SagaStep};
use dtmrs_server::api::Api;
use dtmrs_server::driver::Driver;
use dtmrs_server::grpc::busi_pb::busi_server::{Busi, BusiServer};
use dtmrs_server::grpc::busi_pb::Empty;
use dtmrs_server::grpc::pb;
use dtmrs_server::grpc::server::TcService;
use dtmrs_server::grpc::{MD_BRANCH_ID, MD_GID, MD_OP, MD_TRANS_TYPE};
use dtmrs_server::saga_rows;
use dtmrs_store::Store;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

/// 假业务服务：记录每个方法被调了几次、以及收到的 metadata，
/// 并按预设剧本返回不同的 gRPC 状态码
#[derive(Default)]
struct FakeBusi {
    deduct: AtomicUsize,
    deduct_undo: AtomicUsize,
    shipment: AtomicUsize,
    shipment_undo: AtomicUsize,
    /// shipment 的剧本："ok" | "aborted" | "unavailable" | "ongoing"
    shipment_mode: Mutex<String>,
    /// 收到的 metadata，用来验证分支身份透传对不对
    seen: Mutex<Vec<HashMap<String, String>>>,
}

impl FakeBusi {
    fn counts(&self) -> (usize, usize, usize, usize) {
        (
            self.deduct.load(Ordering::SeqCst),
            self.deduct_undo.load(Ordering::SeqCst),
            self.shipment.load(Ordering::SeqCst),
            self.shipment_undo.load(Ordering::SeqCst),
        )
    }

    fn record(&self, req: &Request<Empty>) {
        let mut m = HashMap::new();
        for k in [MD_GID, MD_TRANS_TYPE, MD_BRANCH_ID, MD_OP] {
            if let Some(v) = req.metadata().get(k).and_then(|v| v.to_str().ok()) {
                m.insert(k.to_string(), v.to_string());
            }
        }
        self.seen.lock().unwrap().push(m);
    }
}

/// 孤儿规则不让给 `Arc<FakeBusi>` 直接实现外部 trait，包一层
struct BusiSvc(Arc<FakeBusi>);

impl std::ops::Deref for BusiSvc {
    type Target = FakeBusi;
    fn deref(&self) -> &FakeBusi {
        &self.0
    }
}

#[tonic::async_trait]
impl Busi for BusiSvc {
    async fn deduct(&self, req: Request<Empty>) -> Result<Response<Empty>, Status> {
        self.record(&req);
        self.deduct.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(Empty {}))
    }

    async fn deduct_undo(&self, req: Request<Empty>) -> Result<Response<Empty>, Status> {
        self.record(&req);
        self.deduct_undo.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(Empty {}))
    }

    async fn shipment(&self, req: Request<Empty>) -> Result<Response<Empty>, Status> {
        self.record(&req);
        self.shipment.fetch_add(1, Ordering::SeqCst);
        let mode = self.shipment_mode.lock().unwrap().clone();
        match mode.as_str() {
            // 业务**明确**要求回滚 —— gRPC 侧唯一能触发补偿的码
            "aborted" => Err(Status::aborted("库存不足")),
            // 服务不可用：结果未知，绝不能触发回滚
            "unavailable" => Err(Status::unavailable("下游挂了")),
            "ongoing" => Err(Status::failed_precondition("还在处理")),
            _ => Ok(Response::new(Empty {})),
        }
    }

    async fn shipment_undo(&self, req: Request<Empty>) -> Result<Response<Empty>, Status> {
        self.record(&req);
        self.shipment_undo.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(Empty {}))
    }
}

/// 起假业务服务，返回它的 `host:port`
async fn spawn_busi(busi: Arc<FakeBusi>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(BusiServer::new(BusiSvc(busi)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    // 等它真的开始听
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    addr.to_string()
}

/// 起 TC 的 gRPC API，返回 endpoint
async fn spawn_tc(api: Api) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TcService::new(api).into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    format!("http://{addr}")
}

async fn store() -> Store {
    Store::open("sqlite::memory:").await.unwrap()
}

fn steps(host: &str) -> Vec<SagaStep> {
    vec![
        SagaStep::new(
            &format!("grpc://{host}/busi.v1.Busi/Deduct"),
            &format!("grpc://{host}/busi.v1.Busi/DeductUndo"),
        ),
        SagaStep::new(
            &format!("grpc://{host}/busi.v1.Busi/Shipment"),
            &format!("grpc://{host}/busi.v1.Busi/ShipmentUndo"),
        ),
    ]
}

#[tokio::test]
async fn grpc分支正向提交() {
    let busi = Arc::new(FakeBusi::default());
    *busi.shipment_mode.lock().unwrap() = "ok".into();
    let host = spawn_busi(busi.clone()).await;

    let st = store().await;
    let d = Driver::new(st.clone(), "tc-1".into());
    let (g, br) = saga_rows("grpc-ok", &steps(&host));
    st.create_global(&g, &br).await.unwrap();
    d.process(&g).await.unwrap();

    let got = st.get_global("grpc-ok").await.unwrap().unwrap();
    assert_eq!(got.status, GlobalStatus::Succeed);
    assert_eq!(
        busi.counts(),
        (1, 0, 1, 0),
        "两个正向各调一次，补偿不该被调"
    );
}

#[tokio::test]
async fn grpc的aborted触发逆序补偿() {
    let busi = Arc::new(FakeBusi::default());
    // ABORTED = 业务明确要求回滚
    *busi.shipment_mode.lock().unwrap() = "aborted".into();
    let host = spawn_busi(busi.clone()).await;

    let st = store().await;
    let d = Driver::new(st.clone(), "tc-1".into());
    let (g, br) = saga_rows("grpc-abort", &steps(&host));
    st.create_global(&g, &br).await.unwrap();
    d.process(&g).await.unwrap();

    let got = st.get_global("grpc-abort").await.unwrap().unwrap();
    assert_eq!(got.status, GlobalStatus::Failed);
    let (a1, c1, a2, c2) = busi.counts();
    assert_eq!(a1, 1);
    assert_eq!(a2, 1);
    // 两个分支的补偿都要发 —— 包括自己就失败了的那个（它可能有副作用）
    assert_eq!(c1, 1, "扣款必须被补偿");
    assert_eq!(c2, 1, "失败分支也要补偿，多余的由屏障空转");
}

#[tokio::test]
async fn grpc超时和不可达不能触发回滚() {
    // **这是整个 gRPC 支持里最要命的一条。**
    // UNAVAILABLE / DEADLINE_EXCEEDED 恰恰是网络抖动产生的码，
    // 而超时的时候对方可能已经成功了 —— 判失败去补偿就是数据不一致。
    let busi = Arc::new(FakeBusi::default());
    *busi.shipment_mode.lock().unwrap() = "unavailable".into();
    let host = spawn_busi(busi.clone()).await;

    let st = store().await;
    let d = Driver::new(st.clone(), "tc-1".into());
    let (g, br) = saga_rows("grpc-unavail", &steps(&host));
    st.create_global(&g, &br).await.unwrap();
    d.process(&g).await.unwrap();

    let got = st.get_global("grpc-unavail").await.unwrap().unwrap();
    assert_eq!(
        got.status,
        GlobalStatus::Submitted,
        "UNAVAILABLE 必须保持 submitted 等重试，绝不能转 aborting"
    );
    let (_, c1, _, c2) = busi.counts();
    assert_eq!((c1, c2), (0, 0), "一个补偿都不该发出去");
}

#[tokio::test]
async fn grpc的ongoing也只是等待() {
    let busi = Arc::new(FakeBusi::default());
    *busi.shipment_mode.lock().unwrap() = "ongoing".into();
    let host = spawn_busi(busi.clone()).await;

    let st = store().await;
    let d = Driver::new(st.clone(), "tc-1".into());
    let (g, br) = saga_rows("grpc-ongoing", &steps(&host));
    st.create_global(&g, &br).await.unwrap();
    d.process(&g).await.unwrap();

    let got = st.get_global("grpc-ongoing").await.unwrap().unwrap();
    assert_eq!(got.status, GlobalStatus::Submitted, "ONGOING 不是失败");
    assert_eq!(busi.counts().1, 0, "不该有补偿");
}

#[tokio::test]
async fn 分支身份通过metadata透传() {
    // 业务方要靠这四个值做幂等（子事务屏障）。传丢了屏障就失效，
    // 重试会变成重复扣款
    let busi = Arc::new(FakeBusi::default());
    *busi.shipment_mode.lock().unwrap() = "ok".into();
    let host = spawn_busi(busi.clone()).await;

    let st = store().await;
    let d = Driver::new(st.clone(), "tc-1".into());
    let (g, br) = saga_rows("grpc-md", &steps(&host));
    st.create_global(&g, &br).await.unwrap();
    d.process(&g).await.unwrap();

    let seen = busi.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    for (i, m) in seen.iter().enumerate() {
        assert_eq!(m.get(MD_GID).map(String::as_str), Some("grpc-md"));
        assert_eq!(m.get(MD_TRANS_TYPE).map(String::as_str), Some("saga"));
        assert_eq!(m.get(MD_OP).map(String::as_str), Some("action"));
        // 分支号必须跟步序对上，错位会导致补偿补错对象
        let want = format!("{:02}", i + 1);
        assert_eq!(m.get(MD_BRANCH_ID), Some(&want));
    }
}

#[tokio::test]
async fn grpc与http分支可以混用() {
    // 同一笔事务里一步走 gRPC、一步走 HTTP。真实迁移场景就是这样：
    // 老服务还是 HTTP，新服务已经是 gRPC
    let busi = Arc::new(FakeBusi::default());
    *busi.shipment_mode.lock().unwrap() = "ok".into();
    let host = spawn_busi(busi.clone()).await;

    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/act",
            axum::routing::post(move || {
                let h = h.clone();
                async move {
                    h.fetch_add(1, Ordering::SeqCst);
                    "ok"
                }
            }),
        );
        axum::serve(listener, app).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let st = store().await;
    let d = Driver::new(st.clone(), "tc-1".into());
    let mixed = vec![
        SagaStep::new(
            &format!("grpc://{host}/busi.v1.Busi/Deduct"),
            &format!("grpc://{host}/busi.v1.Busi/DeductUndo"),
        ),
        SagaStep::new(
            &format!("http://{http_addr}/act"),
            &format!("http://{http_addr}/act"),
        ),
    ];
    let (g, br) = saga_rows("grpc-mixed", &mixed);
    st.create_global(&g, &br).await.unwrap();
    d.process(&g).await.unwrap();

    assert_eq!(
        st.get_global("grpc-mixed").await.unwrap().unwrap().status,
        GlobalStatus::Succeed
    );
    assert_eq!(busi.counts().0, 1, "gRPC 那一步跑了");
    assert_eq!(hits.load(Ordering::SeqCst), 1, "HTTP 那一步也跑了");
}

// ---------------- TC 自己的 gRPC API ----------------

#[tokio::test]
async fn tc的grpc_api与http同源() {
    // 两套接口共用 api.rs，这里验 gRPC 这条路能完整走通：
    // 提交 → 查询 → 中止
    let st = store().await;
    let endpoint = spawn_tc(Api::new(st.clone())).await;
    let mut cli = pb::tc_client::TcClient::connect(endpoint).await.unwrap();

    let gid = cli
        .new_gid(pb::NewGidRequest {})
        .await
        .unwrap()
        .into_inner()
        .gid;
    assert!(!gid.is_empty());

    cli.submit(pb::SubmitRequest {
        gid: gid.clone(),
        trans_type: String::new(), // 留空按 saga
        // pb::SagaStep 是 proto 生成的类型，没有 new()
        steps: vec![pb::SagaStep {
            action: "http://x/a".into(),
            compensate: "http://x/c".into(),
            payload: String::new(),
        }],
    })
    .await
    .expect("提交应该成功");

    let v = cli
        .query(pb::QueryRequest { gid: gid.clone() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(v.gid, gid);
    assert_eq!(v.trans_type, "saga");
    assert_eq!(v.status, "submitted");
    assert_eq!(v.branches.len(), 2, "一步展开成 action + compensate 两行");
    assert_eq!(v.finish_time, None, "还没终结就不该有完成时间");

    // 同一个 gid 再提交一次必须成功 —— 客户端重试不该失败
    cli.submit(pb::SubmitRequest {
        gid: gid.clone(),
        trans_type: "saga".into(),
        steps: vec![],
    })
    .await
    .expect("重复提交必须幂等成功");

    cli.abort(pb::AbortRequest { gid: gid.clone() })
        .await
        .expect("中止应该成功");
    let v = cli
        .query(pb::QueryRequest { gid })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(v.status, "aborting");
    assert_eq!(v.rollback_reason, "调用方主动中止");
}

#[tokio::test]
async fn grpc的错误映射到正确的状态码() {
    let st = store().await;
    let endpoint = spawn_tc(Api::new(st.clone())).await;
    let mut cli = pb::tc_client::TcClient::connect(endpoint).await.unwrap();

    // 查不存在的 gid → NOT_FOUND
    let e = cli
        .query(pb::QueryRequest {
            gid: "没这个".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound);

    // gid 为空 → INVALID_ARGUMENT
    let e = cli
        .submit(pb::SubmitRequest {
            gid: String::new(),
            trans_type: "saga".into(),
            steps: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument);

    // msg 不给 query_prepared 必须被拒 —— 没有回查地址就没人能决断这单
    let e = cli
        .prepare(pb::PrepareRequest {
            gid: "msg-1".into(),
            trans_type: "msg".into(),
            actions: vec!["http://x/a".into()],
            query_prepared: String::new(),
            grace_secs: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    assert!(e.message().contains("query_prepared"));

    // 已终结的事务再 abort → FAILED_PRECONDITION（HTTP 那边是 200 + FAILURE 体）
    let (g, br) = saga_rows("done-1", &[]);
    st.create_global(&g, &br).await.unwrap();
    st.set_global_status("done-1", GlobalStatus::Succeed, "")
        .await
        .unwrap();
    let e = cli
        .abort(pb::AbortRequest {
            gid: "done-1".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition);
}
