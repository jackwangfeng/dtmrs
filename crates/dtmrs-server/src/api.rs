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

use crate::driver;
use crate::{msg_rows, saga_rows, tcc_rows};
use dtmrs_core::{BranchOp, GlobalStatus, SagaStep, TransType};
use dtmrs_store::{Store, SubmitOutcome};
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
    /// 提交后**直接开推**用的推进器。`None` 就是老行为：写完就返回，
    /// 等推进器自己抢到再推。见 [`Api::with_inline_driver`]
    inline: Option<crate::driver::Driver>,
}

impl Api {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            inline: None,
        }
    }

    /// 开启「提交后直接开推」。
    ///
    /// # 省掉的是那次抢占往返
    ///
    /// 老流程：提交方写完事务就返回，推进器再 `lock_one_due` 抢一次才能推。
    /// 那次抢占**每笔事务都要付**，在 Redis 上是一次 Lua 往返 —— 实测它就是
    /// saga 落后 DTM 的主要原因（saga 只有一次客户端请求，摊不薄）。
    ///
    /// 新流程：建事务的那条写入里**顺便把租约占在自己手上**
    /// （`owner=自己`、`next_cron_time=现在+租约`），写成功就等于抢到了，
    /// 直接推。零额外往返。
    ///
    /// # 跟 DTM 的差别：我们不阻塞提交
    ///
    /// DTM 是在 submit 请求里同步把事务推完，客户端要一直等。这里是
    /// **spawn 出去推，提交立刻返回** —— 省掉往返的同时保住了提交延迟。
    ///
    /// # 代价
    ///
    /// 租约一占就是 `lease` 秒。如果进程在「写完」和「推完」之间挂了，
    /// 这笔要等租约到期才会被别的实例接手，而不是下一个 tick。
    /// 这跟「推进器抢到之后崩了」是同一种情形，不是新引入的风险。
    pub fn with_inline_driver(mut self, d: crate::driver::Driver) -> Self {
        self.inline = Some(d);
        self
    }

    /// 建事务前把租约字段填上。返回是否真的占了 —— 没开内联就不占。
    fn claim_for_inline(&self, g: &mut dtmrs_store::GlobalRow) -> bool {
        let Some(d) = &self.inline else { return false };
        g.owner = d.owner.clone();
        g.next_cron_time = dtmrs_store::now() + d.lease;
        true
    }

    /// 把 prepared 推成 submitted，开了内联就**顺便占下租约**。
    ///
    /// 返回 `Advanced` 时事务体一起带回来了，调用方可以直接 [`Self::drive_detached`]，
    /// 不用再读一次（Redis 是脚本尾巴上的 HGETALL，SQL 是本来就要发的那条 SELECT）
    async fn claim_and_submit(&self, gid: &str) -> Result<SubmitOutcome> {
        let (owner, nct) = match &self.inline {
            Some(d) => (d.owner.clone(), dtmrs_store::now() + d.lease),
            None => (String::new(), dtmrs_store::now()),
        };
        self.store
            .submit_prepared(gid, &owner, nct)
            .await
            .map_err(internal)
    }

    /// 把已经拿到租约的事务扔出去推。**不等它跑完** —— 提交要立刻返回。
    fn drive_detached(&self, g: dtmrs_store::GlobalRow) {
        let Some(d) = self.inline.clone() else { return };
        tokio::spawn(async move {
            if let Err(e) = d.process(&g).await {
                // 推失败不影响提交的结果，租约到期后会被重新捞起来
                tracing::warn!(gid = %g.gid, error = %e, "提交后直接推进出错，等租约到期重试");
            }
        });
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

        match tt {
            TransType::Saga => {
                if steps.is_empty() {
                    // ⚠ 不带步骤的重复提交**必须幂等成功**，不能因为 steps 为空
                    // 就报错 —— 客户端重试时经常只带 gid。只有事务压根不存在，
                    // 才是真的参数错误。（`tc的grpc_api与http同源` 钉着这条）
                    return match self.claim_and_submit(gid).await? {
                        SubmitOutcome::Advanced(g) => {
                            self.drive_detached(*g);
                            Ok(())
                        }
                        SubmitOutcome::Already => Ok(()),
                        SubmitOutcome::Missing => {
                            Err(ApiError::BadRequest("saga 的 steps 不能为空".into()))
                        }
                    };
                }
                let (mut g, branches) = saga_rows(gid, steps);
                // 开了内联推进的话，这条写入顺便把租约占下来，写成功就直接推，
                // 不用再走一次抢占（见 `with_inline_driver`）
                let claimed = self.claim_for_inline(&mut g);
                // **先建，不先查。** `create_global` 本身就是幂等的（已存在返回
                // false 且不覆盖），所以正常路径一次往返就够 —— 这是 saga 提交的
                // 热路径，先查一次等于白付一次往返，而且那还是个 Lua 脚本调用，
                // 比普通命令贵得多（实测多这一次让 saga 吞吐掉了 16%）
                if self
                    .store
                    .create_global(&g, &branches)
                    .await
                    .map_err(internal)?
                {
                    if claimed {
                        self.drive_detached(g);
                    }
                    return Ok(());
                }
                // 已存在。可能是重复提交（幂等返回成功就行），也可能是这个 gid
                // 其实是 prepare 过的 tcc/msg/xa —— 客户端没传 trans_type 时
                // 会被当成 saga。后一种要真的把它推成 submitted，交给下面决断
                if let SubmitOutcome::Advanced(g) = self.claim_and_submit(gid).await? {
                    self.drive_detached(*g);
                }
                Ok(())
            }
            TransType::Tcc | TransType::Msg | TransType::Xa => {
                // prepare 已经建过事务，submit 只是把它推成 submitted。
                //
                // **一次存储调用做完**（原来是 get_global + set_global_status
                // + schedule_now 三次）。Redis 上这三次是 11 条命令，现在 3 条
                match self.claim_and_submit(gid).await? {
                    // 推成 submitted 了。开了内联就直接推 —— 跟 saga 一样，
                    // 省掉那次抢占往返。事务体是 submit_prepared 顺带返回的，
                    // 没有多付一次读
                    SubmitOutcome::Advanced(g) => {
                        self.drive_detached(*g);
                        Ok(())
                    }
                    // 已经提交过 —— 幂等返回成功
                    SubmitOutcome::Already => Ok(()),
                    SubmitOutcome::Missing => {
                        Err(ApiError::BadRequest("tcc/xa/msg 要先调 prepare".into()))
                    }
                }
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
        // ⚠ 分支号的格式必须在**入口**挡住，放进去之后就来不及了。
        //
        // 这里原先只校验非空，于是客户端随手写个 branch_id="inventory" 就能
        // 触发两个都很难查的故障（都实测过，不是推演）：
        //
        //   · 解析不出下标 → 推进器把整笔事务当成「空事务」直接判 succeed，
        //     confirm 一次都不会调，那份已经 try 冻结的资源永久泄漏；
        //   · 写个 "2000000000" → 推进时按下标开数组，一次 submit 把 RSS
        //     从 38 MB 顶到 3.4 GB，而且这行留在库里，每轮轮询再来一遍。
        //
        // 判据见 `is_canonical_branch_id`：driver 推进时是拿下标**重新生成**
        // 分支号去反查行的，所以还原不出原样的写法一律不收 —— 包括 "1"、"001"
        // 这种「看起来对但少了/多了补零」的，它们会让状态更新静默落空。
        if !driver::is_canonical_branch_id(&r.branch_id) {
            return Err(ApiError::BadRequest(format!(
                "branch_id \"{}\" 格式不对：必须是从 01 开始、至少两位补零的十进制序号\
                 （01、02 …… 99、100），且不超过 {}",
                r.branch_id,
                driver::MAX_BRANCH_INDEX + 1
            )));
        }
        let tt = match self.store.get_global(&r.gid).await {
            // ⚠ 必须挡住「事务已经不可能再推进新分支」的状态。
            //
            // TCC / XA 的正确顺序是**先登记分支再做一阶段**（见 CLAUDE.md
            // 「绝对不能破坏的语义」第 5 条）。如果这里放行，客户端拿到 SUCCESS
            // 之后就会去执行 try / XA PREPARE —— 而 TC 这边事务已经终结或正在
            // 回滚，那份资源**永远不会有人 confirm 或 cancel**：
            //   TCC 是资源永久泄漏，XA 更糟 —— 留下永久持锁的 prepared 事务。
            //
            // 真实触发路径不需要客户端有 bug：多分支 TCC 登记完分支 1、做完 try、
            // 正要登记分支 2 时，这笔事务**超时了**，TC 已经回滚并落终态。
            //
            // 只放行 Prepared（正常流程）和 Submitted（容忍重试 ——
            // register_branch 本身是幂等的，见 `分支登记是幂等的`）。
            Ok(Some(g)) if matches!(g.status, GlobalStatus::Aborting) || g.status.is_final() => {
                return Err(ApiError::Conflict(format!(
                    "事务处于 {} 状态，不能再登记分支（登记后的一阶段将无人收尾）",
                    g.status.as_str()
                )));
            }
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
                    .set_global_status(gid, GlobalStatus::Aborting, g.trans_type, "调用方主动中止")
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

    /// 立刻重试：把下次调度时间提到现在，并清掉退避累积。
    ///
    /// 只是「排到队首」，不跳过任何安全检查 —— 分支该幂等还是要幂等。
    /// 终态事务不能重试（没意义，而且会让它重新变成活跃事务）。
    pub async fn retry(&self, gid: &str) -> Result<()> {
        match self.store.get_global(gid).await {
            Ok(Some(g)) if !g.status.is_final() => {
                self.store.schedule_now(gid).await.map_err(internal)?;
                Ok(())
            }
            Ok(Some(_)) => Err(ApiError::Conflict("事务已终结，无需重试".into())),
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
