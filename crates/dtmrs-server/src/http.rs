//! HTTP 协议层：把 HTTP 请求翻译成 `Api` 调用，再把结果翻译成 DTM 的应答格式。
//!
//! ⚠ **这一层只做协议转换，不做任何业务判断** —— 判断全在 `api.rs`。
//! gRPC 层（`grpc/server.rs`）是它的对偶，两边必须对同一个请求给出同样的受理/
//! 拒绝结论。否则会出现「同一个请求走 HTTP 被拒、走 gRPC 却受理了」。
//!
//! 这个模块**刻意放在 dtmrs-server 而不是二进制 crate 里**：早先它写在
//! `main.rs`，测试够不着，覆盖率是 0%，而 gRPC 层有 86% —— 防漂移的约束
//! 只有一半受测试保护。搬过来之后 `router()` 可导出，两边就能用同一组
//! 用例做等价性测试（见 tests/http.rs 的「HTTP 与 gRPC 等价」那几个）。

use crate::api::{Api, ApiError, RegisterBranch, TransView};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use dtmrs_core::SagaStep;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// axum 的 `State` 要求 Clone；`Api` 内部是 Arc，克隆很便宜。
#[derive(Clone)]
pub struct App {
    api: Api,
}

impl App {
    pub fn new(api: Api) -> Self {
        Self { api }
    }
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


/// 不带认证的 router（本地/内网用）。要保护请用 [`router_with_auth`]
pub fn router(app: App) -> Router {
    routes(app)
}

/// 带登录保护的 router。
///
/// ⚠ 中间件是**全局**的，不是只挡管理台页面 —— 真正危险的是它调的那些接口
/// （abort 能中止在途事务、retry 能改调度、submit 能凭空造事务）。
/// 白名单只有 `/health`（反代健康检查）和 `/login` `/logout`。
pub fn router_with_auth(
    app: App,
    auth: std::sync::Arc<crate::auth::Auth>,
    store: dtmrs_store::Store,
) -> Router {
    use axum::middleware;
    // 三组路由三种状态：业务路由用 App、登录用 Arc<Auth>、令牌管理两个都要。
    // 各自 with_state 收敛成 Router<()> 之后再 merge，最后统一挂中间件
    let auth_routes = Router::new()
        .route(
            "/login",
            get(crate::auth::login_page).post(crate::auth::login_submit),
        )
        .route("/logout", post(crate::auth::logout))
        .with_state(auth.clone());
    let token_routes = Router::new()
        .route("/api/admin/tokens", get(crate::auth::tokens_list))
        .route("/api/admin/tokens/create", post(crate::auth::tokens_create))
        .route("/api/admin/tokens/revoke", post(crate::auth::tokens_revoke))
        .route("/api/admin/tokens/reveal", post(crate::auth::tokens_reveal))
        .with_state((auth.clone(), store));
    routes(app)
        .merge(auth_routes)
        .merge(token_routes)
        .layer(middleware::from_fn_with_state(auth, crate::auth::guard))
}

fn routes(app: App) -> Router {
    Router::new()
        .route("/api/dtmsvr/newGid", get(new_gid))
        .route("/api/dtmsvr/prepare", post(prepare))
        .route("/api/dtmsvr/registerBranch", post(register_branch))
        .route("/api/dtmsvr/submit", post(submit))
        .route("/api/dtmsvr/abort", post(abort))
        .route("/api/dtmsvr/retry", post(retry))
        .route("/api/dtmsvr/query", get(query))
        .route("/api/dtmsvr/all", get(all))
        .route("/health", get(|| async { "ok" }))
        // 管理台。单文件内嵌，没有构建步骤也没有外部依赖 ——
        // 内网和离线环境都能直接用
        .route("/", get(console))
        .route("/console", get(console))
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

/// 立刻重试：把事务排到调度队首。管理台用，也可以直接调
async fn retry(State(app): State<App>, Json(q): Json<GidQuery>) -> (StatusCode, Json<Reply>) {
    http_result(app.api.retry(&q.gid).await)
}

/// 管理台页面。`include_str!` 编进二进制，部署时不用带额外文件
async fn console() -> axum::response::Html<&'static str> {
    axum::response::Html(include_str!("console.html"))
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
