//! dtmrs TC（事务协调器）—— 单个静态二进制，状态全在 DB，可多实例。
//!
//! 路由路径刻意跟 DTM 对齐（`/api/dtmsvr/...`），这样现有 DTM 客户端改个地址
//! 就能试，迁移成本最低。


use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use dtmrs_server::driver::Driver;
use dtmrs_server::{msg_rows, saga_rows, tcc_rows};
use dtmrs_core::{BranchOp, GlobalStatus, SagaStep, TransType};
use dtmrs_store::Store;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

#[derive(Clone)]
struct App {
    store: Store,
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
///
/// 两种模式都是**先登记再做一阶段**：反过来的话一阶段成功了但 TC 不知道
/// 有这个分支，回滚时漏掉它 —— TCC 是预留资源泄漏，XA 更糟，
/// 会留下一个永久持锁的 prepared 事务。
#[derive(Deserialize)]
struct RegisterBranchReq {
    gid: String,
    branch_id: String,
    /// TCC
    #[serde(default)]
    confirm: String,
    #[serde(default)]
    cancel: String,
    /// TCC 的 try，可选，只为可观测性存一份
    #[serde(default)]
    r#try: String,
    /// XA
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
        Json(Self { dtm_result: "SUCCESS", message: None })
    }
    fn err(m: impl Into<String>) -> Json<Self> {
        Json(Self { dtm_result: "FAILURE", message: Some(m.into()) })
    }
}

#[derive(Serialize)]
struct TransView {
    gid: String,
    trans_type: String,
    status: String,
    rollback_reason: String,
    create_time: i64,
    finish_time: Option<i64>,
    branches: Vec<BranchView>,
}

#[derive(Serialize)]
struct BranchView {
    branch_id: String,
    op: String,
    url: String,
    status: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let db = std::env::var("DTMRS_DB").unwrap_or_else(|_| "sqlite:dtmrs.db".into());
    let addr = std::env::var("DTMRS_ADDR").unwrap_or_else(|_| "0.0.0.0:36789".into());
    let owner = std::env::var("DTMRS_OWNER").unwrap_or_else(|_| {
        format!("tc-{}", std::process::id())
    });

    let store = Store::open(&db).await?;
    let driver = Driver::new(store.clone(), owner.clone());
    info!(db = %db, addr = %addr, owner = %owner, "dtmrs 启动");

    // 常驻推进器。崩溃恢复就靠它：重启后未终结的事务会被重新捞起
    tokio::spawn(driver.clone().run_forever(Duration::from_secs(1)));

    let app = router(App { store });
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
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

async fn new_gid() -> Json<HashMap<&'static str, String>> {
    // 时间戳 + 进程内计数，够用；生产建议客户端用业务单号当 gid
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let gid = format!("{}-{}", dtmrs_store::now(), n);
    Json(HashMap::from([("gid", gid)]))
}

async fn submit(
    State(app): State<App>,
    Json(req): Json<SubmitReq>,
) -> (StatusCode, Json<Reply>) {
    if req.gid.is_empty() {
        return (StatusCode::BAD_REQUEST, Reply::err("gid 不能为空"));
    }
    let Some(tt) = TransType::parse(&req.trans_type) else {
        return (StatusCode::BAD_REQUEST, Reply::err("未知 trans_type"));
    };

    // tcc / msg：prepare 已经建过事务了，submit 只是把它推成 submitted
    if let Ok(Some(g)) = app.store.get_global(&req.gid).await {
        if g.status == GlobalStatus::Prepared {
            let _ = app
                .store
                .set_global_status(&req.gid, GlobalStatus::Submitted, "")
                .await;
            let _ = app.store.schedule_now(&req.gid).await;
            return (StatusCode::OK, Reply::ok());
        }
        // 已经提交过 —— 幂等，返回成功而不是报错
        return (StatusCode::OK, Reply::ok());
    }

    match tt {
        TransType::Saga => {
            if req.steps.is_empty() {
                return (StatusCode::BAD_REQUEST, Reply::err("saga 的 steps 不能为空"));
            }
            let (g, branches) = saga_rows(&req.gid, &req.steps);
            match app.store.create_global(&g, &branches).await {
                // 重复提交同一个 gid 返回成功 —— 幂等，客户端重试不该失败
                Ok(_) => (StatusCode::OK, Reply::ok()),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Reply::err(e.to_string())),
            }
        }
        TransType::Tcc | TransType::Msg | TransType::Xa => (
            StatusCode::BAD_REQUEST,
            Reply::err("tcc/xa/msg 要先调 /api/dtmsvr/prepare"),
        ),
    }
}

/// 第一阶段。msg 建 prepared 事务 + 正向分支；tcc 只建空事务（分支后面登记）。
async fn prepare(
    State(app): State<App>,
    Json(req): Json<PrepareReq>,
) -> (StatusCode, Json<Reply>) {
    if req.gid.is_empty() {
        return (StatusCode::BAD_REQUEST, Reply::err("gid 不能为空"));
    }
    match TransType::parse(&req.trans_type) {
        Some(TransType::Msg) => {
            if req.actions.is_empty() {
                return (StatusCode::BAD_REQUEST, Reply::err("msg 的 actions 不能为空"));
            }
            if req.query_prepared.is_empty() {
                // 没有回查地址，客户端崩在中间就没人能决断这单了 —— 直接拒绝
                return (
                    StatusCode::BAD_REQUEST,
                    Reply::err("msg 必须提供 query_prepared，否则崩溃后无法决断"),
                );
            }
            let (g, br) = msg_rows(
                &req.gid,
                &req.actions,
                &req.query_prepared,
                req.grace_secs.unwrap_or(10),
            );
            match app.store.create_global(&g, &br).await {
                Ok(_) => (StatusCode::OK, Reply::ok()),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Reply::err(e.to_string())),
            }
        }
        Some(tt @ (TransType::Tcc | TransType::Xa)) => {
            // 两者都是"先建空的 prepared 事务，分支随后登记"
            let mut g = tcc_rows(&req.gid);
            g.trans_type = tt;
            match app.store.create_global(&g, &[]).await {
                Ok(_) => (StatusCode::OK, Reply::ok()),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Reply::err(e.to_string())),
            }
        }
        _ => (
            StatusCode::BAD_REQUEST,
            Reply::err("prepare 支持 tcc / xa / msg；saga 直接 submit"),
        ),
    }
}

/// TCC 的分支登记。**必须先登记再调 try**：
/// 反过来的话 try 成功但登记失败，TC 就不知道有这个分支，
/// 回滚时不会 cancel 它 —— 预留的资源永久泄漏。
async fn register_branch(
    State(app): State<App>,
    Json(req): Json<RegisterBranchReq>,
) -> (StatusCode, Json<Reply>) {
    if req.gid.is_empty() || req.branch_id.is_empty() {
        return (StatusCode::BAD_REQUEST, Reply::err("gid / branch_id 不能为空"));
    }
    let tt = match app.store.get_global(&req.gid).await {
        Ok(Some(g)) => g.trans_type,
        Ok(None) => return (StatusCode::NOT_FOUND, Reply::err("gid 不存在，先 prepare")),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Reply::err(e.to_string())),
    };

    let mut ops = Vec::new();
    match tt {
        TransType::Tcc => {
            if req.confirm.is_empty() || req.cancel.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    Reply::err("tcc 分支必须提供 confirm 和 cancel"),
                );
            }
            ops.push((BranchOp::Confirm, req.confirm.clone()));
            ops.push((BranchOp::Cancel, req.cancel.clone()));
            if !req.r#try.is_empty() {
                ops.push((BranchOp::Try, req.r#try.clone()));
            }
        }
        TransType::Xa => {
            if req.commit.is_empty() || req.rollback.is_empty() {
                // XA 缺了任一个都可能留下永久持锁的 prepared 事务，必须拒绝
                return (
                    StatusCode::BAD_REQUEST,
                    Reply::err("xa 分支必须提供 commit 和 rollback"),
                );
            }
            ops.push((BranchOp::Commit, req.commit.clone()));
            ops.push((BranchOp::Rollback, req.rollback.clone()));
        }
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Reply::err("只有 tcc 和 xa 需要登记分支"),
            )
        }
    }

    match app.store.register_branch(&req.gid, &req.branch_id, &ops).await {
        Ok(()) => (StatusCode::OK, Reply::ok()),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Reply::err(e.to_string())),
    }
}

#[derive(Deserialize)]
struct GidQuery {
    gid: String,
}

async fn abort(
    State(app): State<App>,
    Json(q): Json<GidQuery>,
) -> (StatusCode, Json<Reply>) {
    match app.store.get_global(&q.gid).await {
        Ok(Some(g)) if !g.status.is_final() => {
            let _ = app
                .store
                .set_global_status(&q.gid, GlobalStatus::Aborting, "调用方主动中止")
                .await;
            let _ = app.store.schedule_now(&q.gid).await;
            (StatusCode::OK, Reply::ok())
        }
        Ok(Some(_)) => (StatusCode::OK, Reply::err("事务已终结，无法中止")),
        Ok(None) => (StatusCode::NOT_FOUND, Reply::err("gid 不存在")),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Reply::err(e.to_string())),
    }
}

async fn query(
    State(app): State<App>,
    Query(q): Query<GidQuery>,
) -> Result<Json<TransView>, (StatusCode, Json<Reply>)> {
    let g = app
        .store
        .get_global(&q.gid)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Reply::err(e.to_string())))?
        .ok_or((StatusCode::NOT_FOUND, Reply::err("gid 不存在")))?;
    let branches = app
        .store
        .list_branches(&q.gid)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Reply::err(e.to_string())))?;
    Ok(Json(TransView {
        gid: g.gid,
        trans_type: g.trans_type.to_string(),
        status: g.status.as_str().into(),
        rollback_reason: g.rollback_reason,
        create_time: g.create_time,
        finish_time: g.finish_time,
        branches: branches
            .into_iter()
            .map(|b| BranchView {
                branch_id: b.branch_id,
                op: b.op.as_str().into(),
                url: b.url,
                status: b.status.as_str().into(),
            })
            .collect(),
    }))
}

async fn all(State(app): State<App>) -> Json<Vec<HashMap<String, String>>> {
    let rows = app.store.list_recent(100).await.unwrap_or_default();
    Json(
        rows.into_iter()
            .map(|g| {
                HashMap::from([
                    ("gid".to_string(), g.gid),
                    ("trans_type".to_string(), g.trans_type.to_string()),
                    ("status".to_string(), g.status.as_str().to_string()),
                    ("rollback_reason".to_string(), g.rollback_reason),
                ])
            })
            .collect(),
    )
}
