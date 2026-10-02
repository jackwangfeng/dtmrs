//! TC 对外的 gRPC API。
//!
//! 这一层**只做协议转换**，所有判断都在 [`crate::api`] 里 —— HTTP 和 gRPC
//! 共用同一份逻辑，不会出现「同一个请求走 HTTP 被拒、走 gRPC 却受理了」。
//!
//! 错误映射见 [`crate::api::ApiError`] 的表。

use tonic::{Request, Response, Status};

use super::pb;
use crate::api::{Api, ApiError, RegisterBranch};
use dtmrs_core::SagaStep;

impl From<ApiError> for Status {
    fn from(e: ApiError) -> Self {
        match &e {
            ApiError::BadRequest(m) => Status::invalid_argument(m.clone()),
            ApiError::NotFound(m) => Status::not_found(m.clone()),
            ApiError::Conflict(m) => Status::failed_precondition(m.clone()),
            ApiError::Internal(m) => Status::internal(m.clone()),
        }
    }
}

pub struct TcService {
    api: Api,
}

impl TcService {
    pub fn new(api: Api) -> Self {
        Self { api }
    }

    /// 包成 tonic 的 server，调用方直接挂到 `Server::builder().add_service(..)`
    pub fn into_server(self) -> pb::tc_server::TcServer<Self> {
        pb::tc_server::TcServer::new(self)
    }

    /// 带认证的版本。**必须和 HTTP 侧用同一个 `Auth`**，否则就出现
    /// 「同一个请求走 HTTP 被拒、走 gRPC 却受理了」—— 这正是
    /// 「绝对不能破坏的语义」里防的那种漂移。
    ///
    /// gRPC 没有 cookie 和登录页的概念，所以这里**只认 Bearer token**
    /// （metadata 的 `authorization` 键）。管理台的会话 cookie 只在 HTTP 侧有意义。
    pub fn into_server_with_auth(self, auth: std::sync::Arc<crate::auth::Auth>) -> AuthedTc {
        AuthedTc {
            inner: pb::tc_server::TcServer::new(self),
            auth,
        }
    }
}

/// 带认证的 gRPC 服务。
///
/// # 为什么不用 tonic 的 interceptor
///
/// interceptor 是**同步**的，而托管令牌（管理台签发的那种）要查缓存、过期了还得
/// 异步刷新一次存储。早先这里用 interceptor，只能比 env 里的静态令牌 ——
/// 结果同一个托管令牌**走 HTTP 放行、走 gRPC 被拒**，正是两个协议层不许出现的漂移
/// （`管理台签发的托管令牌两个协议都放行` 钉着）。
///
/// 现在包一层异步的 tower Service，判定跟 HTTP 的 `auth::guard` 走同一套：
/// 先比静态令牌，再查托管令牌。
#[derive(Clone)]
pub struct AuthedTc {
    inner: pb::tc_server::TcServer<TcService>,
    auth: std::sync::Arc<crate::auth::Auth>,
}

impl tonic::server::NamedService for AuthedTc {
    const NAME: &'static str =
        <pb::tc_server::TcServer<TcService> as tonic::server::NamedService>::NAME;
}

impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for AuthedTc
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
    pb::tc_server::TcServer<TcService>: tonic::codegen::Service<
        tonic::codegen::http::Request<B>,
        Response = tonic::codegen::http::Response<tonic::body::Body>,
        Error = std::convert::Infallible,
    >,
    <pb::tc_server::TcServer<TcService> as tonic::codegen::Service<
        tonic::codegen::http::Request<B>,
    >>::Future: Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        tonic::codegen::Service::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, req: tonic::codegen::http::Request<B>) -> Self::Future {
        // 按 tower 的惯例：用 poll_ready 过的那个实例处理这次请求，留一个新克隆给下次
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let auth = self.auth.clone();
        Box::pin(async move {
            let presented = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(crate::auth::Auth::bearer)
                .map(str::to_string);
            let ip = req
                .extensions()
                .get::<tonic::transport::server::TcpConnectInfo>()
                .and_then(|c| c.remote_addr())
                .map(|a| a.ip().to_string())
                .unwrap_or_default();
            let ok = match &presented {
                Some(t) => auth.token_ok(t) || auth.managed_ok(t, &ip).await,
                None => false,
            };
            if ok {
                inner.call(req).await
            } else {
                Ok(tonic::Status::unauthenticated("需要 Bearer token").into_http())
            }
        })
    }
}

#[tonic::async_trait]
impl pb::tc_server::Tc for TcService {
    async fn new_gid(
        &self,
        _req: Request<pb::NewGidRequest>,
    ) -> Result<Response<pb::NewGidReply>, Status> {
        Ok(Response::new(pb::NewGidReply {
            gid: self.api.new_gid(),
        }))
    }

    async fn prepare(
        &self,
        req: Request<pb::PrepareRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let r = req.into_inner();
        // proto3 的 int64 没法区分「没传」和「传了 0」，所以用 0 表示走默认值。
        // 宽限期本来也不该是 0 —— 那等于 prepare 完立刻回查，白问一次
        let grace = if r.grace_secs > 0 {
            Some(r.grace_secs)
        } else {
            None
        };
        self.api
            .prepare_with(
                &r.gid,
                &r.trans_type,
                &r.actions,
                &r.query_prepared,
                grace,
                &crate::api::PrepareOpts {
                    payloads: r.payloads,
                    allow_empty_topic: r.allow_empty_topic,
                },
            )
            .await?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn subscribe(
        &self,
        req: Request<pb::TopicRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let r = req.into_inner();
        self.api.subscribe(&r.topic, &r.url, &r.remark).await?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn unsubscribe(
        &self,
        req: Request<pb::TopicRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let r = req.into_inner();
        self.api.unsubscribe(&r.topic, &r.url).await?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn delete_topic(
        &self,
        req: Request<pb::TopicRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let r = req.into_inner();
        self.api.delete_topic(&r.topic).await?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn register_branch(
        &self,
        req: Request<pb::RegisterBranchRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        let r = req.into_inner();
        self.api
            .register_branch(&RegisterBranch {
                gid: r.gid,
                branch_id: r.branch_id,
                confirm: r.confirm,
                cancel: r.cancel,
                r#try: r.r#try,
                commit: r.commit,
                rollback: r.rollback,
            })
            .await?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn submit(&self, req: Request<pb::SubmitRequest>) -> Result<Response<pb::Empty>, Status> {
        let r = req.into_inner();
        let tt = if r.trans_type.is_empty() {
            "saga"
        } else {
            &r.trans_type
        };
        let steps: Vec<SagaStep> = r
            .steps
            .into_iter()
            .map(|s| SagaStep {
                action: s.action,
                compensate: s.compensate,
                payload: s.payload,
            })
            .collect();
        self.api.submit(&r.gid, tt, &steps).await?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn abort(&self, req: Request<pb::AbortRequest>) -> Result<Response<pb::Empty>, Status> {
        self.api.abort(&req.into_inner().gid).await?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn retry(&self, req: Request<pb::RetryRequest>) -> Result<Response<pb::Empty>, Status> {
        self.api.retry(&req.into_inner().gid).await?;
        Ok(Response::new(pb::Empty {}))
    }

    async fn query(
        &self,
        req: Request<pb::QueryRequest>,
    ) -> Result<Response<pb::TransView>, Status> {
        let v = self.api.query(&req.into_inner().gid).await?;
        Ok(Response::new(pb::TransView {
            gid: v.gid,
            trans_type: v.trans_type,
            status: v.status,
            rollback_reason: v.rollback_reason,
            create_time: v.create_time,
            finish_time: v.finish_time,
            branches: v
                .branches
                .into_iter()
                .map(|b| pb::BranchView {
                    branch_id: b.branch_id,
                    op: b.op,
                    url: b.url,
                    status: b.status,
                })
                .collect(),
        }))
    }
}
