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

use crate::api::{Api, ApiError, PrepareOpts, RegisterBranch, TransView};
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
    /// msg 用：每个 action 的请求体（字段名同 DTM）。空表示都不带
    #[serde(default)]
    payloads: Vec<String>,
    /// msg 用：`topic://` 没有订阅者时放行而不是报错（dtmrs 扩展）
    #[serde(default)]
    allow_empty_topic: bool,
}

/// subscribe / unsubscribe 的参数。**走 query string**，跟 DTM 一样（GET 请求）
#[derive(Deserialize)]
struct TopicReq {
    #[serde(default)]
    topic: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    remark: String,
}

/// queryKV / scanKV 的参数（DTM 的通用 KV 接口，这里只有 `cat=topics` 一类）
#[derive(Deserialize)]
struct KvReq {
    #[serde(default)]
    cat: String,
    #[serde(default)]
    key: String,
    /// scanKV 的游标：上一页最后一个主题名
    #[serde(default)]
    position: String,
    #[serde(default)]
    limit: Option<usize>,
}

/// DTM 的 KV 条目。`v` 是 JSON **字符串**（`[{"url":..,"remark":..}]`），不是对象 ——
/// DTM 原样存原样吐，客户端是再解一次的
#[derive(Serialize)]
struct KvItem {
    id: usize,
    cat: &'static str,
    k: String,
    v: String,
    version: i64,
    create_time: i64,
    update_time: i64,
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

/// 主题订阅的那几个接口（DTM 的路径和方法）。完整 router 和
/// [`topic_router`] 共用这一份，免得两处漂移
fn topic_routes() -> Router<App> {
    Router::new()
        .route("/api/dtmsvr/subscribe", get(subscribe))
        .route("/api/dtmsvr/unsubscribe", get(unsubscribe))
        .route(
            "/api/dtmsvr/topic/{topic}",
            axum::routing::delete(delete_topic),
        )
        .route("/api/dtmsvr/queryKV", get(query_kv))
        .route("/api/dtmsvr/scanKV", get(scan_kv))
}

/// **只有**主题订阅接口的 router，带共享密钥认证。
///
/// 给嵌入式用：发布方进程里嵌着协调器，订阅方要在启动时自己登记上来 ——
/// 订阅关系由订阅方维护，发布方的代码和配置里都不出现下游地址。
/// 只开放订阅相关的接口：prepare / submit / abort 这些不该被别的服务随手调。
///
/// 请求必须带 `Authorization: Bearer <token>`（定长比较）。`/health` 不需要。
/// `token` 为空会 panic —— 不带认证的订阅入口等于谁都能把消息改发到任意地址，
/// 调用方（[`crate::embedded::Embedded::serve_topic_api`]）会先挡掉空值。
pub fn topic_router(app: App, token: String) -> Router {
    assert!(!token.is_empty(), "topic_router 必须带共享密钥");
    let token = std::sync::Arc::new(token);
    topic_routes()
        .with_state(app)
        .layer(axum::middleware::from_fn(
            move |req: axum::extract::Request, next: axum::middleware::Next| {
                let token = token.clone();
                async move {
                    use axum::response::IntoResponse;
                    let ok = req
                        .headers()
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|v| v.to_str().ok())
                        .and_then(crate::auth::Auth::bearer)
                        .is_some_and(|p| crate::auth::ct_eq(p, &token));
                    if ok {
                        next.run(req).await
                    } else {
                        (StatusCode::UNAUTHORIZED, Reply::err("unauthorized")).into_response()
                    }
                }
            },
        ))
        // 健康检查在认证层外面
        .route("/health", get(|| async { "ok" }))
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
        .merge(topic_routes())
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
            .prepare_with(
                &req.gid,
                &req.trans_type,
                &req.actions,
                &req.query_prepared,
                req.grace_secs,
                &PrepareOpts {
                    payloads: req.payloads,
                    allow_empty_topic: req.allow_empty_topic,
                },
            )
            .await,
    )
}

async fn subscribe(State(app): State<App>, Query(q): Query<TopicReq>) -> (StatusCode, Json<Reply>) {
    http_result(app.api.subscribe(&q.topic, &q.url, &q.remark).await)
}

async fn unsubscribe(
    State(app): State<App>,
    Query(q): Query<TopicReq>,
) -> (StatusCode, Json<Reply>) {
    http_result(app.api.unsubscribe(&q.topic, &q.url).await)
}

async fn delete_topic(
    State(app): State<App>,
    axum::extract::Path(topic): axum::extract::Path<String>,
) -> (StatusCode, Json<Reply>) {
    http_result(app.api.delete_topic(&topic).await)
}

/// 把订阅按主题聚成 DTM 的 KV 条目
async fn topic_kvs(app: &App, key: &str) -> Result<Vec<KvItem>, ApiError> {
    if !key.is_empty() && key.len() > dtmrs_store::TOPIC_LEN {
        return Ok(Vec::new());
    }
    let subs = app
        .api
        .list_topic_subs((!key.is_empty()).then_some(key))
        .await?;
    let mut out: Vec<KvItem> = Vec::new();
    for s in subs {
        let entry = serde_json::json!({"url": s.url, "remark": s.remark});
        match out.last_mut() {
            Some(last) if last.k == s.topic => {
                let mut v: Vec<serde_json::Value> =
                    serde_json::from_str(&last.v).unwrap_or_default();
                v.push(entry);
                last.v = serde_json::Value::from(v).to_string();
                last.create_time = last.create_time.min(s.create_time);
                last.update_time = last.update_time.max(s.create_time);
            }
            _ => out.push(KvItem {
                id: out.len() + 1,
                cat: "topics",
                k: s.topic,
                v: serde_json::Value::from(vec![entry]).to_string(),
                version: 1,
                create_time: s.create_time,
                update_time: s.create_time,
            }),
        }
    }
    Ok(out)
}

fn kv_cat_ok(cat: &str) -> Result<(), ApiError> {
    if cat == "topics" {
        Ok(())
    } else {
        Err(ApiError::BadRequest(format!(
            "不支持的 cat：{cat}（只有 topics）"
        )))
    }
}

async fn query_kv(State(app): State<App>, Query(q): Query<KvReq>) -> axum::response::Response {
    use axum::response::IntoResponse;
    let r = async {
        kv_cat_ok(&q.cat)?;
        topic_kvs(&app, &q.key).await
    }
    .await;
    match r {
        Ok(kv) => Json(serde_json::json!({ "kv": kv })).into_response(),
        Err(e) => http_err(e).into_response(),
    }
}

/// 按主题名翻页。`next_position` 为空表示到底了（同 DTM）
async fn scan_kv(State(app): State<App>, Query(q): Query<KvReq>) -> axum::response::Response {
    use axum::response::IntoResponse;
    let r = async {
        kv_cat_ok(&q.cat)?;
        topic_kvs(&app, "").await
    }
    .await;
    match r {
        Ok(all) => {
            let limit = q.limit.unwrap_or(100).max(1);
            let page: Vec<KvItem> = all
                .into_iter()
                .filter(|kv| q.position.is_empty() || kv.k > q.position)
                .take(limit + 1)
                .collect();
            let more = page.len() > limit;
            let mut page = page;
            page.truncate(limit);
            let next = if more {
                page.last().map(|kv| kv.k.clone()).unwrap_or_default()
            } else {
                String::new()
            };
            Json(serde_json::json!({ "kv": page, "next_position": next })).into_response()
        }
        Err(e) => http_err(e).into_response(),
    }
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
