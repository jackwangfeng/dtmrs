//! TC 的对外操作，**与协议无关**。
//!
//! HTTP 和 gRPC 两套接口都只做「协议 ↔ 这一层」的转换，业务判断全在这里。
//! 分成两处写迟早会漂移 —— 而这一层漂移的后果是「同一个请求走 HTTP 被拒、
//! 走 gRPC 却受理了」，这种不一致在事务系统里是要命的。
//!
//! 错误用 [`ApiError`] 表达，由各协议层翻译成自己的表示：
//!
//! | ApiError | HTTP | gRPC |
//! |---|---|---|
//! | `BadRequest` | 400 | `INVALID_ARGUMENT` |
//! | `NotFound` | 404 | `NOT_FOUND` |
//! | `Conflict` | 200 + `dtm_result=FAILURE` | `FAILED_PRECONDITION` |
//! | `Internal` | 500 | `INTERNAL` |
//!
//! `Conflict` 在 HTTP 上返回 200 是**刻意保留的历史行为**（已终结的事务再调
//! abort），换成 4xx 会打破现有客户端。

use crate::{msg_rows, saga_rows, tcc_rows};
use dtmrs_core::{BranchOp, GlobalStatus, SagaStep, TransType};
use dtmrs_store::Store;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    BadRequest(String),
    NotFound(String),
    /// 请求本身合法，但当前状态下做不了
    Conflict(String),
    Internal(String),
}

impl ApiError {
    pub fn message(&self) -> &str {
        match self {
            Self::BadRequest(m) | Self::NotFound(m) | Self::Conflict(m) | Self::Internal(m) => m,
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

pub type Result<T> = std::result::Result<T, ApiError>;

fn internal(e: impl std::fmt::Display) -> ApiError {
    ApiError::Internal(e.to_string())
}

#[derive(Debug, Clone, Serialize)]
pub struct BranchView {
    pub branch_id: String,
    pub op: String,
    pub url: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransView {
    pub gid: String,
    pub trans_type: String,
    pub status: String,
    pub rollback_reason: String,
    pub create_time: i64,
    pub finish_time: Option<i64>,
    pub branches: Vec<BranchView>,
}

/// 分支登记请求。TCC 用 confirm/cancel，XA 用 commit/rollback。
#[derive(Debug, Clone, Default)]
pub struct RegisterBranch {
    pub gid: String,
    pub branch_id: String,
    pub confirm: String,
    pub cancel: String,
    pub r#try: String,
    pub commit: String,
    pub rollback: String,
}

#[derive(Clone)]
pub struct Api {
    pub store: Store,
}

impl Api {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    /// 时间戳 + 进程内计数。生产建议客户端直接用业务单号当 gid ——
    /// 那样天然幂等，重试不会变成两笔
    pub fn new_gid(&self) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        format!("{}-{}", dtmrs_store::now(), n)
    }

    /// 提交。
    ///
    /// **重复提交同一个 gid 必须成功而不是报错** —— 客户端网络抖动重试时
    /// 返回错误会让它以为没受理，然后换个 gid 再来一次，就成了两笔。
    pub async fn submit(&self, gid: &str, trans_type: &str, steps: &[SagaStep]) -> Result<()> {
        if gid.is_empty() {
            return Err(ApiError::BadRequest("gid 不能为空".into()));
        }
        let Some(tt) = TransType::parse(trans_type) else {
            return Err(ApiError::BadRequest("未知 trans_type".into()));
        };

        // tcc / msg / xa：prepare 已经建过事务，submit 只是把它推成 submitted
        if let Ok(Some(g)) = self.store.get_global(gid).await {
            if g.status == GlobalStatus::Prepared {
                self.store
                    .set_global_status(gid, GlobalStatus::Submitted, "")
                    .await
                    .map_err(internal)?;
                let _ = self.store.schedule_now(gid).await;
            }
            // 已经提交过 —— 幂等返回成功
            return Ok(());
        }

        match tt {
            TransType::Saga => {
                if steps.is_empty() {
                    return Err(ApiError::BadRequest("saga 的 steps 不能为空".into()));
                }
                let (g, branches) = saga_rows(gid, steps);
                self.store
                    .create_global(&g, &branches)
                    .await
                    .map_err(internal)?;
                Ok(())
            }
            TransType::Tcc | TransType::Msg | TransType::Xa => {
                Err(ApiError::BadRequest("tcc/xa/msg 要先调 prepare".into()))
            }
            // workflow 的「步骤」是**代码**，没法表示成 URL 存进库里，
            // 所以只能在嵌入式形态下提交（Embedded::workflow + submit_workflow）。
            // 这不是暂未实现，是这个模式的本质决定的
            TransType::Workflow => Err(ApiError::BadRequest(
                "workflow 模式只能在嵌入式形态下提交（步骤是进程内的函数，不是 URL）".into(),
            )),
        }
    }

    /// 第一阶段。msg 建 prepared 事务 + 正向分支；tcc / xa 只建空事务。
    pub async fn prepare(
        &self,
        gid: &str,
        trans_type: &str,
        actions: &[String],
        query_prepared: &str,
        grace_secs: Option<i64>,
    ) -> Result<()> {
        if gid.is_empty() {
            return Err(ApiError::BadRequest("gid 不能为空".into()));
        }
        match TransType::parse(trans_type) {
            Some(TransType::Msg) => {
                if actions.is_empty() {
                    return Err(ApiError::BadRequest("msg 的 actions 不能为空".into()));
                }
                if query_prepared.is_empty() {
                    // 没有回查地址，客户端崩在 prepare 和 submit 之间就没人能
                    // 决断这单了。猜「已提交」会重复扣款，猜「没提交」会丢单
                    return Err(ApiError::BadRequest(
                        "msg 必须提供 query_prepared，否则崩溃后无法决断".into(),
                    ));
                }
                let (g, br) = msg_rows(gid, actions, query_prepared, grace_secs.unwrap_or(10));
                self.store.create_global(&g, &br).await.map_err(internal)?;
                Ok(())
            }
            Some(tt @ (TransType::Tcc | TransType::Xa)) => {
                let mut g = tcc_rows(gid);
                g.trans_type = tt;
                self.store.create_global(&g, &[]).await.map_err(internal)?;
                Ok(())
            }
            _ => Err(ApiError::BadRequest(
                "prepare 支持 tcc / xa / msg；saga 直接 submit".into(),
            )),
        }
    }

    /// 分支登记。**必须先登记再做一阶段**：反过来的话一阶段成功但登记失败，
    /// TC 就不知道有这个分支，回滚时不会处理它 —— TCC 是预留资源永久泄漏，
    /// XA 更糟，会留下一个永久持锁的 prepared 事务。
    pub async fn register_branch(&self, r: &RegisterBranch) -> Result<()> {
        if r.gid.is_empty() || r.branch_id.is_empty() {
            return Err(ApiError::BadRequest("gid / branch_id 不能为空".into()));
        }
        let tt = match self.store.get_global(&r.gid).await {
            Ok(Some(g)) => g.trans_type,
            Ok(None) => return Err(ApiError::NotFound("gid 不存在，先 prepare".into())),
            Err(e) => return Err(internal(e)),
        };

        let mut ops = Vec::new();
        match tt {
            TransType::Tcc => {
                if r.confirm.is_empty() || r.cancel.is_empty() {
                    return Err(ApiError::BadRequest(
                        "tcc 分支必须提供 confirm 和 cancel".into(),
                    ));
                }
                ops.push((BranchOp::Confirm, r.confirm.clone()));
                ops.push((BranchOp::Cancel, r.cancel.clone()));
                if !r.r#try.is_empty() {
                    ops.push((BranchOp::Try, r.r#try.clone()));
                }
            }
            TransType::Xa => {
                if r.commit.is_empty() || r.rollback.is_empty() {
                    // 缺任一个都可能留下永久持锁的 prepared 事务
                    return Err(ApiError::BadRequest(
                        "xa 分支必须提供 commit 和 rollback".into(),
                    ));
                }
                ops.push((BranchOp::Commit, r.commit.clone()));
                ops.push((BranchOp::Rollback, r.rollback.clone()));
            }
            _ => return Err(ApiError::BadRequest("只有 tcc 和 xa 需要登记分支".into())),
        }

        self.store
            .register_branch(&r.gid, &r.branch_id, &ops)
            .await
            .map_err(internal)
    }

    /// 主动中止，触发逆序补偿
    pub async fn abort(&self, gid: &str) -> Result<()> {
        match self.store.get_global(gid).await {
            Ok(Some(g)) if !g.status.is_final() => {
                self.store
                    .set_global_status(gid, GlobalStatus::Aborting, "调用方主动中止")
                    .await
                    .map_err(internal)?;
                let _ = self.store.schedule_now(gid).await;
                Ok(())
            }
            Ok(Some(_)) => Err(ApiError::Conflict("事务已终结，无法中止".into())),
            Ok(None) => Err(ApiError::NotFound("gid 不存在".into())),
            Err(e) => Err(internal(e)),
        }
    }

    pub async fn query(&self, gid: &str) -> Result<TransView> {
        let g = self
            .store
            .get_global(gid)
            .await
            .map_err(internal)?
            .ok_or_else(|| ApiError::NotFound("gid 不存在".into()))?;
        let branches = self.store.list_branches(gid).await.map_err(internal)?;
        Ok(TransView {
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
        })
    }

    pub async fn list_recent(&self, limit: i64) -> Vec<TransView> {
        self.store
            .list_recent(limit)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|g| TransView {
                gid: g.gid,
                trans_type: g.trans_type.to_string(),
                status: g.status.as_str().into(),
                rollback_reason: g.rollback_reason,
                create_time: g.create_time,
                finish_time: g.finish_time,
                branches: Vec::new(),
            })
            .collect()
    }
}
