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
    // ⚠ 两个协议层**必须共用同一个 Auth** —— 各构造一个就会漂移
    let auth = dtmrs::server::auth::Auth::from_env();
    if let Some(a) = &auth {
        info!(
            登录页 = a.has_login(),
            "认证已开启（业务端用 Authorization: Bearer <DTMRS_AUTH_TOKEN>）"
        );
    }
    let grpc = serve_grpc(api.clone(), grpc_addr, auth.clone());
    let http = serve_http(api, addr, auth);
    tokio::select! {
        r = grpc => r?,
        r = http => r?,
    }
    Ok(())
}

async fn serve_http(
    api: Api,
    addr: String,
    auth: Option<std::sync::Arc<dtmrs::server::auth::Auth>>,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let app = App::new(api);

    // 配了 DTMRS_ADMIN_PASSWORD 才开登录保护；没配保持原样（本地开发用）。
    // ⚠ 但监听地址不是回环还不设密码，等于把 abort / retry / submit 敞给外面，
    //   这种情况必须吼一嗓子 —— 静默放行是最糟的默认值。
    let router = match auth {
        Some(auth) => dtmrs::server::http::router_with_auth(app, auth),
        None => {
            let public = !addr.starts_with("127.") && !addr.starts_with("localhost");
            if public {
                tracing::warn!(
                    %addr,
                    "⚠ 监听在非回环地址但既没设 DTMRS_AUTH_TOKEN 也没设 \
                     DTMRS_ADMIN_PASSWORD —— 管理台和全部接口（含 abort/retry/submit）\
                     对任何能连上的人开放"
                );
            }
            router(app)
        }
    };
    axum::serve(listener, router).await?;
    Ok(())
}

#[cfg(feature = "grpc")]
async fn serve_grpc(
    api: Api,
    addr: String,
    auth: Option<std::sync::Arc<dtmrs::server::auth::Auth>>,
) -> anyhow::Result<()> {
    use dtmrs::server::grpc::server::TcService;
    let sock = addr.parse()?;
    let svc = TcService::new(api);
    // gRPC 没有 cookie/登录页，只认 Bearer token。**必须跟 HTTP 用同一个 Auth**
    match auth {
        Some(a) => {
            tonic::transport::Server::builder()
                .add_service(svc.into_server_with_auth(a))
                .serve(sock)
                .await?
        }
        None => {
            tonic::transport::Server::builder()
                .add_service(svc.into_server())
                .serve(sock)
                .await?
        }
    }
    Ok(())
}

#[cfg(not(feature = "grpc"))]
async fn serve_grpc(
    _api: Api,
    _addr: String,
    _auth: Option<std::sync::Arc<dtmrs::server::auth::Auth>>,
) -> anyhow::Result<()> {
    // 关掉 grpc feature 的构建里，这个 future 永远挂着，让 select! 只跑 HTTP
    std::future::pending::<()>().await;
    Ok(())
}

