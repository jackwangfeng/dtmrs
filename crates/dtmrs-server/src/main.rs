//! dtmrs TC（事务协调器）—— 单个静态二进制，状态全在 DB，可多实例。
//!
//! 同时提供两套等价的接口：
//!
//! - **HTTP**（`DTMRS_ADDR`，默认 36789）：路径刻意跟 DTM 对齐（`/api/dtmsvr/...`），
//!   现有 DTM 客户端改个地址就能试
//! - **gRPC**（`DTMRS_GRPC_ADDR`，默认 36790）：`dtmrs.v1.Tc` 服务
//!
//! 两套共用同一个 [`Api`]，所以语义完全一致，也能混着用 ——
//! gRPC 提交的事务可以用 HTTP 查，反之亦然。

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use dtmrs_core::SagaStep;
use dtmrs_server::api::{Api, ApiError, RegisterBranch, TransView};
use dtmrs_server::driver::Driver;
use dtmrs_store::Store;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

#[derive(Clone)]
struct App {
    api: Api,
}

#[derive(Deserialize)]
struct SubmitReq {
    gid: String,
    #[serde(default = "default_trans_type")]
    trans_type: String,
    /// saga 一次性给全部步骤；tcc/msg 走 prepare + submit，这里可以不带
    #[serde(default)]
    steps: Vec<SagaStep>,
}

/// 二阶段消息 / TCC 的第一阶段
#[derive(Deserialize)]
struct PrepareReq {
    gid: String,
    trans_type: String,
    /// msg 用：正向分支列表（没有补偿）
    #[serde(default)]
    actions: Vec<String>,
    /// msg 用：回查地址。进程在 prepare 和 submit 之间崩了，TC 靠它决断
    #[serde(default)]
    query_prepared: String,
    /// msg 用：回查前的宽限秒数，默认 10
    #[serde(default)]
    grace_secs: Option<i64>,
}

/// 分支登记。TCC 用 confirm/cancel，XA 用 commit/rollback。
#[derive(Deserialize)]
struct RegisterBranchReq {
    gid: String,
    branch_id: String,
    #[serde(default)]
    confirm: String,
    #[serde(default)]
    cancel: String,
    /// TCC 的 try，可选，只为可观测性存一份
    #[serde(default)]
    r#try: String,
    #[serde(default)]
    commit: String,
    #[serde(default)]
    rollback: String,
}

fn default_trans_type() -> String {
    "saga".into()
}

#[derive(Serialize)]
struct Reply {
    dtm_result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

impl Reply {
    fn ok() -> Json<Self> {
        Json(Self {
            dtm_result: "SUCCESS",
            message: None,
        })
    }
    fn err(m: impl Into<String>) -> Json<Self> {
        Json(Self {
            dtm_result: "FAILURE",
            message: Some(m.into()),
        })
    }
}

/// [`ApiError`] → HTTP。
///
/// `Conflict` 返回 **200 + FAILURE 体**是刻意保留的历史行为（已终结的事务
/// 再调 abort），换成 4xx 会打破现有客户端。
fn http_err(e: ApiError) -> (StatusCode, Json<Reply>) {
    let code = match &e {
        ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
        ApiError::NotFound(_) => StatusCode::NOT_FOUND,
        ApiError::Conflict(_) => StatusCode::OK,
        ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, Reply::err(e.message().to_string()))
}

fn http_result(r: Result<(), ApiError>) -> (StatusCode, Json<Reply>) {
    match r {
        Ok(()) => (StatusCode::OK, Reply::ok()),
        Err(e) => http_err(e),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let db = std::env::var("DTMRS_DB").unwrap_or_else(|_| "sqlite:dtmrs.db".into());
    let addr = std::env::var("DTMRS_ADDR").unwrap_or_else(|_| "0.0.0.0:36789".into());
    let grpc_addr = std::env::var("DTMRS_GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:36790".into());
    let owner =
        std::env::var("DTMRS_OWNER").unwrap_or_else(|_| format!("tc-{}", std::process::id()));

    let store = Store::open(&db).await?;
    let driver = Driver::new(store.clone(), owner.clone());
    info!(db = %db, http = %addr, grpc = %grpc_addr, owner = %owner, "dtmrs 启动");

    // 常驻推进器。崩溃恢复就靠它：重启后未终结的事务会被重新捞起
    tokio::spawn(driver.clone().run_forever(Duration::from_secs(1)));

    let api = Api::new(store);

    // gRPC 和 HTTP 各占一个端口，任一个挂了就整体退出 —— 不能出现
    // 「HTTP 还活着但 gRPC 已经死了」这种半可用状态，那会让客户端困惑
    let grpc = serve_grpc(api.clone(), grpc_addr);
    let http = serve_http(api, addr);
    tokio::select! {
        r = grpc => r?,
        r = http => r?,
    }
    Ok(())
}

async fn serve_http(api: Api, addr: String) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, router(App { api })).await?;
    Ok(())
}

#[cfg(feature = "grpc")]
async fn serve_grpc(api: Api, addr: String) -> anyhow::Result<()> {
    use dtmrs_server::grpc::server::TcService;
    let sock = addr.parse()?;
    tonic::transport::Server::builder()
        .add_service(TcService::new(api).into_server())
        .serve(sock)
        .await?;
    Ok(())
}

#[cfg(not(feature = "grpc"))]
async fn serve_grpc(_api: Api, _addr: String) -> anyhow::Result<()> {
    // 关掉 grpc feature 的构建里，这个 future 永远挂着，让 select! 只跑 HTTP
    std::future::pending::<()>().await;
    Ok(())
}

fn router(app: App) -> Router {
    Router::new()
        .route("/api/dtmsvr/newGid", get(new_gid))
        .route("/api/dtmsvr/prepare", post(prepare))
        .route("/api/dtmsvr/registerBranch", post(register_branch))
        .route("/api/dtmsvr/submit", post(submit))
        .route("/api/dtmsvr/abort", post(abort))
        .route("/api/dtmsvr/query", get(query))
        .route("/api/dtmsvr/all", get(all))
        .route("/health", get(|| async { "ok" }))
        .with_state(app)
}

async fn new_gid(State(app): State<App>) -> Json<HashMap<&'static str, String>> {
    Json(HashMap::from([("gid", app.api.new_gid())]))
}

async fn submit(State(app): State<App>, Json(req): Json<SubmitReq>) -> (StatusCode, Json<Reply>) {
    http_result(app.api.submit(&req.gid, &req.trans_type, &req.steps).await)
}

async fn prepare(State(app): State<App>, Json(req): Json<PrepareReq>) -> (StatusCode, Json<Reply>) {
    http_result(
        app.api
            .prepare(
                &req.gid,
                &req.trans_type,
                &req.actions,
                &req.query_prepared,
                req.grace_secs,
            )
            .await,
    )
}

async fn register_branch(
    State(app): State<App>,
    Json(req): Json<RegisterBranchReq>,
) -> (StatusCode, Json<Reply>) {
    http_result(
        app.api
            .register_branch(&RegisterBranch {
                gid: req.gid,
                branch_id: req.branch_id,
                confirm: req.confirm,
                cancel: req.cancel,
                r#try: req.r#try,
                commit: req.commit,
                rollback: req.rollback,
            })
            .await,
    )
}

#[derive(Deserialize)]
struct GidQuery {
    gid: String,
}

async fn abort(State(app): State<App>, Json(q): Json<GidQuery>) -> (StatusCode, Json<Reply>) {
    http_result(app.api.abort(&q.gid).await)
}

async fn query(
    State(app): State<App>,
    Query(q): Query<GidQuery>,
) -> Result<Json<TransView>, (StatusCode, Json<Reply>)> {
    app.api.query(&q.gid).await.map(Json).map_err(http_err)
}

async fn all(State(app): State<App>) -> Json<Vec<TransView>> {
    Json(app.api.list_recent(100).await)
}
