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
    pub fn into_server_with_auth(
        self,
        auth: std::sync::Arc<crate::auth::Auth>,
    ) -> tonic::service::interceptor::InterceptedService<
        pb::tc_server::TcServer<Self>,
        impl tonic::service::Interceptor + Clone,
    > {
        pb::tc_server::TcServer::with_interceptor(self, move |req: tonic::Request<()>| {
            let ok = req
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(crate::auth::Auth::bearer)
                .is_some_and(|t| auth.token_ok(t));
            if ok {
                Ok(req)
            } else {
                Err(tonic::Status::unauthenticated("需要 Bearer token"))
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
            .prepare(&r.gid, &r.trans_type, &r.actions, &r.query_prepared, grace)
            .await?;
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
