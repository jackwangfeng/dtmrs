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

use dtmrs::server::api::Api;
use dtmrs::server::driver::Driver;
use dtmrs::server::http::{router, App};
use dtmrs::Store;
use std::time::Duration;
use tracing::info;

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

    // 推进器空闲时的轮询间隔。**直接决定单笔事务的调度延迟下限** ——
    // 有积压时不受影响（driver 找到活会立刻接着推，不睡）
    let tick_ms: u64 = std::env::var("DTMRS_TICK_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1000);

    let store = Store::open(&db).await?;
    // 按环境变量配超时/租约/退避，非法值退回默认
    let driver = Driver::from_env(store.clone(), owner.clone());
    info!(db = %db, http = %addr, grpc = %grpc_addr, owner = %owner,
          branch_timeout = driver.http_timeout_secs(), lease = driver.lease,
          retry_initial = driver.retry.initial, retry_max = driver.retry.max, tick_ms,
          "dtmrs 启动");

    // 常驻推进器。崩溃恢复就靠它：重启后未终结的事务会被重新捞起
    tokio::spawn(driver.clone().run_forever(Duration::from_millis(tick_ms)));

    // 提交后直接开推：省掉每笔事务一次抢占往返。默认开，
    // `DTMRS_INLINE_SUBMIT=0` 关掉（见 `Api::with_inline_driver`）
    let inline = !matches!(
        std::env::var("DTMRS_INLINE_SUBMIT").as_deref(),
        Ok("0") | Ok("false")
    );
    let api = if inline {
        Api::new(store).with_inline_driver(driver.clone())
    } else {
        Api::new(store)
    };
    info!(inline_submit = inline, "提交后是否直接开推");

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
    axum::serve(listener, router(App::new(api))).await?;
    Ok(())
}

#[cfg(feature = "grpc")]
async fn serve_grpc(api: Api, addr: String) -> anyhow::Result<()> {
    use dtmrs::server::grpc::server::TcService;
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

