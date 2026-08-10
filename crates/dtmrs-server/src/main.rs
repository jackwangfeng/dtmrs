//! dtmrs TC（事务协调器）—— 单个静态二进制，状态全在 DB，可多实例。
//!
//! 路由路径刻意跟 DTM 对齐（`/api/dtmsvr/...`），这样现有 DTM 客户端改个地址
//! 就能试，迁移成本最低。


use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use dtmrs_server::driver::Driver;
use dtmrs_server::saga_rows;
use dtmrs_core::{GlobalStatus, SagaStep, TransType};
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
    steps: Vec<SagaStep>,
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
    if req.steps.is_empty() {
        return (StatusCode::BAD_REQUEST, Reply::err("steps 不能为空"));
    }
    let Some(tt) = TransType::parse(&req.trans_type) else {
        return (StatusCode::BAD_REQUEST, Reply::err("未知 trans_type"));
    };
    if tt != TransType::Saga {
        // 老实说做不到，别假装支持
        return (
            StatusCode::BAD_REQUEST,
            Reply::err("当前版本只实现了 saga，tcc/msg/xa 在路线图上"),
        );
    }

    let (g, branches) = saga_rows(&req.gid, &req.steps);
    match app.store.create_global(&g, &branches).await {
        // 重复提交同一个 gid 返回成功 —— 幂等，客户端重试不该失败
        Ok(_) => (StatusCode::OK, Reply::ok()),
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
