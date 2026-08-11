//! 事务推进器：把状态机的决策落成真实的 HTTP 调用和状态更新。
//!
//! 这里是**唯一**会修改全局事务状态的地方，决策全部来自 `dtmrs_core::saga_advance`，
//! 本文件只负责 I/O。这样状态迁移的正确性可以在 core 里纯单测覆盖。

use crate::registry::{parse_target, BranchCtx, Registry, Target};
use dtmrs_core::{
    msg_advance, saga_advance, tcc_advance, xa_advance, Advance, BranchOp, BranchResult,
    BranchStatus, GlobalStatus, SagaStep, TransType,
};
use dtmrs_store::{GlobalRow, Store};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

#[derive(Clone)]
pub struct Driver {
    pub store: Store,
    pub http: reqwest::Client,
    pub owner: String,
    /// 租约时长（秒）。持租约的实例崩了，这么久之后别的实例接手
    pub lease: i64,
    /// 重试退避策略。默认 10s 起、300s 封顶，可用环境变量改
    pub retry: dtmrs_core::RetryPolicy,
    /// 分支调用超时（秒），只为可观测性保留一份
    branch_timeout_secs: u64,
    /// 并行推进的 worker 数
    pub workers: usize,
    /// 进程内分支注册表。嵌入式模式用，纯 HTTP 部署时是空表
    pub registry: Arc<Registry>,
    /// gRPC 分支调用器（带 channel 缓存）
    #[cfg(feature = "grpc")]
    pub grpc: crate::grpc::client::GrpcCaller,
    /// workflow 函数注册表。跟 registry 一样，纯 HTTP 部署时是空表
    pub workflows: Arc<crate::workflow::WorkflowRegistry>,
}

impl Driver {
    /// 默认配置的推进器。分支超时 10s、租约 30s、退避 10s→300s。
    /// 想按环境变量配就用 [`Driver::from_env`]
    pub fn new(store: Store, owner: String) -> Self {
        Self::with_config(store, owner, DriverConfig::default())
    }

    /// 按环境变量配置：
    ///
    /// | 变量 | 默认 | 说明 |
    /// |---|---|---|
    /// | `DTMRS_BRANCH_TIMEOUT` | 10 | 调一个分支最多等几秒 |
    /// | `DTMRS_LEASE` | 30 | 租约时长（秒） |
    /// | `DTMRS_RETRY_INTERVAL` | 10 | 首次重试间隔（秒） |
    /// | `DTMRS_RETRY_MAX_INTERVAL` | 300 | 退避上限（秒） |
    /// | `DTMRS_WORKERS` | 16 | 并行推进的 worker 数 |
    ///
    /// 存储连接池另有 `DTMRS_DB_POOL`（默认 32），跟 worker 数**要一起调** ——
    /// 池子小于 worker 数时，多出来的 worker 只会排队等连接
    ///
    /// **非法值一律退回默认**，绝不因为配置写错就让推进器起不来
    pub fn from_env(store: Store, owner: String) -> Self {
        Self::with_config(store, owner, DriverConfig::from_env())
    }

    pub fn with_config(store: Store, owner: String, cfg: DriverConfig) -> Self {
        Self {
            store,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(cfg.branch_timeout_secs.max(1) as u64))
                .build()
                .expect("build http client"),
            owner,
            lease: cfg.lease_secs,
            retry: cfg.retry,
            branch_timeout_secs: cfg.branch_timeout_secs.max(1) as u64,
            workers: cfg.workers.max(1),
            registry: Arc::new(Registry::new()),
            #[cfg(feature = "grpc")]
            grpc: crate::grpc::client::GrpcCaller::new(Duration::from_secs(
                cfg.branch_timeout_secs.max(1) as u64,
            )),
            workflows: Arc::new(crate::workflow::WorkflowRegistry::new()),
        }
    }

    /// 当前的分支调用超时（秒），启动日志里打出来方便确认配置生效
    pub fn http_timeout_secs(&self) -> u64 {
        self.branch_timeout_secs
    }

    /// 挂上进程内分支注册表 —— 嵌入式模式的入口
    pub fn with_registry(mut self, r: Arc<Registry>) -> Self {
        self.registry = r;
        self
    }

    /// 挂上 workflow 函数注册表
    pub fn with_workflows(mut self, w: Arc<crate::workflow::WorkflowRegistry>) -> Self {
        self.workflows = w;
        self
    }

    /// 常驻推进器。起 `workers` 个并行的抢占循环。
    ///
    /// # 为什么可以直接并行，不需要新的并发控制
    ///
    /// 每个 worker 都走 `lock_one_due` —— 那是一次**原子抢占**（SQL 靠带条件的
    /// UPDATE，Redis 靠 Lua 脚本），抢到才推。所以进程内 N 个 worker
    /// 跟部署 N 个实例是**完全相同的情形**，而后者的正确性已经有测试钉死了
    /// （`两个实例并发不会重复推进` / `redis_多实例并发不重复推进`）。
    ///
    /// 换句话说：这里没有引入新的竞态，只是把「多实例才能用上的并行」
    /// 在单进程内也用上。
    ///
    /// # 为什么不是把 process() 内部并行
    ///
    /// 一笔事务内部的分支**必须按序**（SAGA 就是顺序语义），并行只能跨事务。
    ///
    /// # ⚠ 为什么必须用 JoinSet 而不是 Vec<JoinHandle>
    ///
    /// 调用方是 `tokio::spawn(driver.run_forever(..))`，靠 **abort 这个外层
    /// 任务**来停推进器（`Embedded` 的 Drop 就是这么干的）。
    /// `tokio::spawn` 出来的子任务是**游离的**：外层被 abort 掉，它们照跑不误。
    ///
    /// 这个坑实测撞出来过：`跨进程重启_事务不丢且已完成的步骤不重做` 里
    /// 第一个「进程」析构后，它那些僵尸 worker 还在抢同一笔事务，
    /// 而它们的 handler 永远返回 Unknown —— 于是第二个「进程」怎么等都推不完。
    ///
    /// `JoinSet` 被 drop 时会把里面所有任务一并 abort，正好是我们要的语义。
    pub async fn run_forever(self, tick: Duration) {
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..self.workers.max(1) {
            let d = self.clone();
            set.spawn(async move { d.worker_loop(tick).await });
        }
        // 任一 worker 意外退出就整体结束 —— 静默少几个 worker 比直接挂更难查
        set.join_next().await;
    }

    /// 单个 worker：抢一个到期事务推一下，没活就睡
    async fn worker_loop(&self, tick: Duration) {
        loop {
            match self.store.lock_one_due(&self.owner, self.lease).await {
                Ok(Some(g)) => {
                    if let Err(e) = self.process(&g).await {
                        warn!(gid = %g.gid, error = %e, "推进出错，等下轮重试");
                    }
                }
                Ok(None) => tokio::time::sleep(tick).await,
                Err(e) => {
                    warn!(error = %e, "取待办失败");
                    tokio::time::sleep(tick).await;
                }
            }
        }
    }

    /// 推进一个全局事务，直到它落终态或需要等待。
    ///
    /// 可以被重复调用（崩溃恢复就靠这个）—— 分支的幂等由客户端屏障保证。
    pub async fn process(&self, g: &GlobalRow) -> anyhow::Result<()> {
        match g.trans_type {
            TransType::Saga => self.process_saga(g).await,
            TransType::Tcc => self.process_tcc(g).await,
            TransType::Msg => self.process_msg(g).await,
            TransType::Xa => self.process_xa(g).await,
            TransType::Workflow => self.process_workflow(g).await,
        }
    }

    // ---------------- workflow ----------------

    /// 跑用户的 workflow 函数，失败则逆序补偿它**已经登记过**的分支。
    ///
    /// 跟另外四种模式的差别见 [`dtmrs_core::workflow_advance`]：正向走向由
    /// 用户函数决定，状态机只管「什么时候跑、什么时候补、按什么顺序补」。
    async fn process_workflow(&self, g: &GlobalRow) -> anyhow::Result<()> {
        let (name, input) = crate::workflow::decode_payload(&g.payload);
        let mut status = g.status;

        loop {
            let rows = self.store.list_branches(&g.gid).await?;
            let compensates = compensate_states(&rows);

            match dtmrs_core::workflow_advance(status, &compensates) {
                Advance::Finish(s) => {
                    info!(gid = %g.gid, status = s.as_str(), "workflow 事务终结");
                    self.store
                        .set_global_status(&g.gid, s, g.trans_type, "")
                        .await?;
                    return Ok(());
                }
                Advance::Wait => return Ok(()),

                Advance::RunWorkflow => {
                    let Some(f) = self.workflows.get(&name) else {
                        // 漏注册（新版本删了 workflow / 换了名字）。
                        // **按结果未知处理** —— 这是部署问题，改回来重试就好，
                        // 判失败会白白触发回滚
                        warn!(gid = %g.gid, workflow = %name,
                              "workflow 未注册，按结果未知处理（会重试，不回滚）");
                        self.retry_later(g).await?;
                        return Ok(());
                    };
                    let ctx =
                        crate::workflow::WorkflowCtx::new(&g.gid, &input, self.store.clone(), rows);
                    match f(ctx).await {
                        Ok(()) => {
                            info!(gid = %g.gid, workflow = %name, "workflow 跑完");
                            self.store
                                .set_global_status(&g.gid, GlobalStatus::Succeed, g.trans_type, "")
                                .await?;
                            return Ok(());
                        }
                        Err(crate::workflow::WorkflowError::Rollback(reason)) => {
                            info!(gid = %g.gid, workflow = %name, %reason, "workflow 要求回滚");
                            self.store
                                .set_global_status(
                                    &g.gid,
                                    GlobalStatus::Aborting,
                                    g.trans_type,
                                    &reason,
                                )
                                .await?;
                            status = GlobalStatus::Aborting;
                            continue;
                        }
                        Err(crate::workflow::WorkflowError::Diverged {
                            branch_id: bid,
                            recorded,
                            got,
                        }) => {
                            // **绝不能继续**：按位置记忆化会张冠李戴，回滚也会补错对象。
                            // 也不回滚 —— 我们已经不知道真实进度了，硬回滚更危险。
                            // 停在这里等人：改回确定性的代码，重启就能接着推。
                            warn!(gid = %g.gid, workflow = %name, branch = %bid,
                                  %recorded, %got,
                                  "workflow 重放走岔了，已停止推进，需要人工介入");
                            self.retry_later(g).await?;
                            return Ok(());
                        }
                        Err(e) => {
                            // Retry / Internal：只重试，绝不回滚
                            warn!(gid = %g.gid, workflow = %name, error = %e, "workflow 需要重试");
                            self.retry_later(g).await?;
                            return Ok(());
                        }
                    }
                }

                Advance::Call { index, op } => {
                    let bid = branch_id(index);
                    let Some(url) = url_of(&rows, &bid, op) else {
                        // 补偿行不见了，不该发生
                        warn!(gid = %g.gid, branch = %bid, "workflow 补偿地址缺失");
                        self.retry_later(g).await?;
                        return Ok(());
                    };
                    let bp = payload_of(&rows, &bid, op);
                    match self.call_branch(g, &bid, op, &url, &bp).await {
                        BranchResult::Success => {
                            self.store
                                .set_branch_status(&g.gid, &bid, op, BranchStatus::Succeed)
                                .await?;
                        }
                        // 补偿失败只能不停重试 —— 漏掉就是真的漏了副作用
                        _ => {
                            warn!(gid = %g.gid, branch = %bid, "workflow 补偿未成功，会重试");
                            self.retry_later(g).await?;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    // ---------------- SAGA ----------------

    async fn process_saga(&self, g: &GlobalRow) -> anyhow::Result<()> {
        let steps: Vec<SagaStep> = serde_json::from_str(&g.payload).unwrap_or_default();
        if steps.is_empty() {
            self.store
                .set_global_status(&g.gid, GlobalStatus::Succeed, g.trans_type, "")
                .await?;
            return Ok(());
        }
        let mut status = g.status;

        loop {
            let (actions, compensates) = self.branch_states(&g.gid, steps.len()).await?;
            match saga_advance(status, &actions, &compensates) {
                Advance::Finish(s) => {
                    if s == GlobalStatus::Aborting {
                        // 防御性分支：状态机发现有 failed 分支但全局还没转 aborting
                        status = s;
                        self.store
                            .set_global_status(&g.gid, s, g.trans_type, "分支已判失败")
                            .await?;
                        continue;
                    }
                    info!(gid = %g.gid, status = s.as_str(), "事务终结");
                    self.store
                        .set_global_status(&g.gid, s, g.trans_type, "")
                        .await?;
                    return Ok(());
                }
                Advance::Wait => return Ok(()),
                // 只有 workflow 模式会出现，别的模式走到这里说明状态机接错了。
                // **不 panic** —— 推进器是常驻的，崩了整个 TC 就停了
                Advance::RunWorkflow => {
                    warn!(gid = %g.gid, "非 workflow 事务收到 RunWorkflow 决策，跳过");
                    return Ok(());
                }
                Advance::Call { index, op } => {
                    let branch_id = branch_id(index);
                    let url = match op {
                        BranchOp::Action => &steps[index].action,
                        _ => &steps[index].compensate,
                    };
                    match self
                        .call_branch(g, &branch_id, op, url, &steps[index].payload)
                        .await
                    {
                        BranchResult::Success => {
                            self.store
                                .set_branch_status(&g.gid, &branch_id, op, BranchStatus::Succeed)
                                .await?;
                        }
                        BranchResult::Failure => {
                            self.store
                                .set_branch_status(&g.gid, &branch_id, op, BranchStatus::Failed)
                                .await?;
                            if op == BranchOp::Action {
                                // 只有业务**明确**说失败才回滚
                                info!(gid = %g.gid, branch = %branch_id, "分支要求回滚");
                                status = GlobalStatus::Aborting;
                                self.store
                                    .set_global_status(
                                        &g.gid,
                                        GlobalStatus::Aborting,
                                        g.trans_type,
                                        &format!("分支 {branch_id} 返回 FAILURE"),
                                    )
                                    .await?;
                            } else {
                                // 补偿都失败了，只能不停重试 —— 这时候需要人介入
                                warn!(gid = %g.gid, branch = %branch_id, "补偿失败，需要人工介入");
                                self.retry_later(g).await?;
                                return Ok(());
                            }
                        }
                        BranchResult::Ongoing | BranchResult::Unknown => {
                            // **绝不能当成失败**：对方可能已经成功了。退避重试。
                            self.retry_later(g).await?;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    // ---------------- TCC ----------------

    /// TCC 的 try 阶段是**客户端驱动**的（客户端先 registerBranch 再调 try），
    /// TC 只负责 confirm / cancel。所以分支的 URL 来自 `trans_branch_op` 表，
    /// 不是全局 payload。
    async fn process_tcc(&self, g: &GlobalRow) -> anyhow::Result<()> {
        self.drive_two_phase(g, BranchOp::Confirm, BranchOp::Cancel, "TCC")
            .await
    }

    // ---------------- XA ----------------

    /// XA 的一阶段（业务 SQL + `PREPARE TRANSACTION`）由**客户端**做，
    /// TC 只负责统一决定 `COMMIT PREPARED` 还是 `ROLLBACK PREPARED`。
    ///
    /// 形状跟 TCC 一样，只是 op 换成 commit/rollback。语义上那条铁律也一样：
    /// **commit 失败绝不能转 rollback** —— 别的分支可能已经提交了。
    ///
    /// XA 独有的严重性：没解决的 prepared 事务会**永久持锁**，在 Postgres 里
    /// 还阻塞 VACUUM。所以这里的重试比 SAGA 的补偿重试要紧得多。
    async fn process_xa(&self, g: &GlobalRow) -> anyhow::Result<()> {
        self.drive_two_phase(g, BranchOp::Commit, BranchOp::Rollback, "XA")
            .await
    }

    /// TCC 和 XA 共用的二阶段推进：正向 op 全做完就成功，反向 op 全做完就失败，
    /// **任一方向的失败都只重试，绝不改变方向**。
    async fn drive_two_phase(
        &self,
        g: &GlobalRow,
        fwd: BranchOp,
        bwd: BranchOp,
        label: &str,
    ) -> anyhow::Result<()> {
        let rows = self.store.list_branches(&g.gid).await?;
        let n = rows
            .iter()
            .filter_map(|r| index_of(&r.branch_id))
            .max()
            .map(|m| m + 1)
            .unwrap_or(0);
        if n == 0 {
            // 一个分支都没登记就 submit/abort 了 —— 空事务，直接落终态
            let s = if g.status == GlobalStatus::Aborting {
                GlobalStatus::Failed
            } else {
                GlobalStatus::Succeed
            };
            self.store
                .set_global_status(&g.gid, s, g.trans_type, "")
                .await?;
            return Ok(());
        }

        let status = g.status;
        loop {
            let rows = self.store.list_branches(&g.gid).await?;
            let (f, b) = split_by_op(&rows, n, fwd, bwd);
            let adv = if fwd == BranchOp::Commit {
                xa_advance(status, &f, &b)
            } else {
                tcc_advance(status, &f, &b)
            };
            match adv {
                Advance::Finish(s) => {
                    info!(gid = %g.gid, status = s.as_str(), mode = label, "事务终结");
                    self.store
                        .set_global_status(&g.gid, s, g.trans_type, "")
                        .await?;
                    return Ok(());
                }
                Advance::Wait => return Ok(()),
                // 只有 workflow 模式会出现，别的模式走到这里说明状态机接错了。
                // **不 panic** —— 推进器是常驻的，崩了整个 TC 就停了
                Advance::RunWorkflow => {
                    warn!(gid = %g.gid, "非 workflow 事务收到 RunWorkflow 决策，跳过");
                    return Ok(());
                }
                Advance::Call { index, op } => {
                    let bid = branch_id(index);
                    let Some(url) = url_of(&rows, &bid, op) else {
                        warn!(gid = %g.gid, branch = %bid, op = op.as_str(), mode = label,
                              "分支没登记这个操作的 URL，无法调用");
                        self.retry_later(g).await?;
                        return Ok(());
                    };
                    let bp = payload_of(&rows, &bid, op);
                    match self.call_branch(g, &bid, op, &url, &bp).await {
                        BranchResult::Success => {
                            self.store
                                .set_branch_status(&g.gid, &bid, op, BranchStatus::Succeed)
                                .await?;
                        }
                        // 二阶段失败**绝不改变全局方向**：一阶段已经成功、
                        // 方向已经定了，反向操作会造成一半提交一半回滚。
                        // 唯一正确处理是无限重试 + 报警。
                        BranchResult::Failure => {
                            self.store
                                .set_branch_status(&g.gid, &bid, op, BranchStatus::Failed)
                                .await?;
                            warn!(gid = %g.gid, branch = %bid, op = op.as_str(), mode = label,
                                  "二阶段失败，会持续重试，需要人工介入");
                            self.retry_later(g).await?;
                            return Ok(());
                        }
                        BranchResult::Ongoing | BranchResult::Unknown => {
                            self.retry_later(g).await?;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    // ---------------- 二阶段消息 ----------------

    /// 流程：`prepare` 落库 → 业务提交本地事务 → `submit`。
    ///
    /// 如果进程在这两步之间崩了，事务会一直停在 prepared。这时 TC 靠回查
    /// `query_prepared` 问业务方"你那个本地事务到底提交了没有"，据此决定
    /// 是往前推还是整单作废。**这是取代 MQ 事务消息的关键一环。**
    async fn process_msg(&self, g: &GlobalRow) -> anyhow::Result<()> {
        let mut status = g.status;

        if status == GlobalStatus::Prepared {
            if g.query_prepared.is_empty() {
                // 没给回查地址就没法自动决断。不能瞎猜 —— 猜错要么丢单要么重复扣款
                warn!(gid = %g.gid, "msg 事务没提供 query_prepared，无法回查，等人处理");
                self.retry_later(g).await?;
                return Ok(());
            }
            // 借用分支调用的通道做回查，branch_id 用 "00" 跟真实分支区分开
            match self
                .call_branch(g, "00", BranchOp::Action, &g.query_prepared, "")
                .await
            {
                BranchResult::Success => {
                    info!(gid = %g.gid, "回查：本地事务已提交 → 继续推进");
                    self.store
                        .set_global_status(&g.gid, GlobalStatus::Submitted, g.trans_type, "")
                        .await?;
                    status = GlobalStatus::Submitted;
                }
                BranchResult::Failure => {
                    // 业务方明确说"这单没提交" → 整单作废。msg 没有补偿分支，
                    // 但也不需要：正向分支压根还没跑过
                    info!(gid = %g.gid, "回查：本地事务未提交 → 整单作废");
                    self.store
                        .set_global_status(
                            &g.gid,
                            GlobalStatus::Failed,
                            g.trans_type,
                            "回查得到 FAILURE：本地事务未提交",
                        )
                        .await?;
                    return Ok(());
                }
                BranchResult::Ongoing | BranchResult::Unknown => {
                    // 回查本身失败了，不能当作"没提交"。退避重试。
                    self.retry_later(g).await?;
                    return Ok(());
                }
            }
        }

        let steps: Vec<SagaStep> = serde_json::from_str(&g.payload).unwrap_or_default();
        if steps.is_empty() {
            self.store
                .set_global_status(&g.gid, GlobalStatus::Succeed, g.trans_type, "")
                .await?;
            return Ok(());
        }
        loop {
            let (actions, _) = self.branch_states(&g.gid, steps.len()).await?;
            match msg_advance(status, &actions) {
                Advance::Finish(s) => {
                    info!(gid = %g.gid, status = s.as_str(), "消息事务终结");
                    self.store
                        .set_global_status(&g.gid, s, g.trans_type, "")
                        .await?;
                    return Ok(());
                }
                Advance::Wait => return Ok(()),
                // 只有 workflow 模式会出现，别的模式走到这里说明状态机接错了。
                // **不 panic** —— 推进器是常驻的，崩了整个 TC 就停了
                Advance::RunWorkflow => {
                    warn!(gid = %g.gid, "非 workflow 事务收到 RunWorkflow 决策，跳过");
                    return Ok(());
                }
                Advance::Call { index, op } => {
                    let bid = branch_id(index);
                    match self
                        .call_branch(g, &bid, op, &steps[index].action, &steps[index].payload)
                        .await
                    {
                        BranchResult::Success => {
                            self.store
                                .set_branch_status(&g.gid, &bid, op, BranchStatus::Succeed)
                                .await?;
                        }
                        // msg 保证"最终一定送达"，没有补偿一说。失败只能重试。
                        BranchResult::Failure | BranchResult::Ongoing | BranchResult::Unknown => {
                            self.retry_later(g).await?;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn retry_later(&self, g: &GlobalRow) -> anyhow::Result<()> {
        let iv = dtmrs_core::next_interval_with(g.next_cron_interval, self.retry);
        self.store.schedule_retry(&g.gid, iv).await?;
        Ok(())
    }

    /// 取每一步的 action / compensate 分支当前状态，按步序对齐
    async fn branch_states(
        &self,
        gid: &str,
        n: usize,
    ) -> anyhow::Result<(Vec<BranchStatus>, Vec<BranchStatus>)> {
        let rows = self.store.list_branches(gid).await?;
        let mut actions = vec![BranchStatus::Prepared; n];
        let mut compensates = vec![BranchStatus::Prepared; n];
        for r in rows {
            let Some(i) = index_of(&r.branch_id) else {
                continue;
            };
            if i >= n {
                continue;
            }
            match r.op {
                BranchOp::Action | BranchOp::Try => actions[i] = r.status,
                BranchOp::Compensate | BranchOp::Cancel | BranchOp::Rollback => {
                    compensates[i] = r.status
                }
                _ => {}
            }
        }
        Ok((actions, compensates))
    }

    /// 调一个分支。`local://名字` 走进程内函数，`grpc://` 走 gRPC，其它走 HTTP。
    async fn call_branch(
        &self,
        g: &GlobalRow,
        branch_id: &str,
        op: BranchOp,
        url: &str,
        payload: &str,
    ) -> BranchResult {
        match parse_target(url) {
            Target::Local(name) => self.call_local(g, branch_id, op, &name).await,
            Target::Http(u) => self.call_http(g, branch_id, op, &u, payload).await,
            #[cfg(feature = "grpc")]
            Target::Grpc(t) => {
                self.grpc
                    .call(
                        &t,
                        &g.gid,
                        &g.trans_type.to_string(),
                        branch_id,
                        op.as_str(),
                    )
                    .await
            }
            // 编译时关掉了 grpc feature，却遇到 grpc:// 分支。
            // 按「结果未知」处理（重试，不回滚）—— 这是构建配置问题，不是业务失败
            #[cfg(not(feature = "grpc"))]
            Target::Grpc(t) => {
                warn!(gid = %g.gid, branch = %branch_id, endpoint = %t.endpoint,
                      "遇到 grpc:// 分支但本次构建关掉了 grpc feature，按结果未知处理");
                BranchResult::Unknown
            }
        }
    }

    /// 进程内调用：没有网络、没有序列化，一次函数调用。
    async fn call_local(
        &self,
        g: &GlobalRow,
        branch_id: &str,
        op: BranchOp,
        name: &str,
    ) -> BranchResult {
        let Some(h) = self.registry.get(name) else {
            // 漏注册（比如新版本删了 handler）。**必须当 Unknown 而不是 Failure**：
            // 判失败会触发回滚，而这其实是部署问题，改回来重试才对。
            warn!(gid = %g.gid, branch = %branch_id, handler = name,
                  "本地分支未注册，按结果未知处理（会重试，不回滚）");
            return BranchResult::Unknown;
        };
        let ctx = BranchCtx {
            gid: g.gid.clone(),
            branch_id: branch_id.to_string(),
            op,
            trans_type: g.trans_type.to_string(),
        };
        let r = h(ctx).await;
        info!(gid = %g.gid, branch = %branch_id, op = op.as_str(),
              handler = name, result = ?r, "本地分支返回");
        r
    }

    /// 远端调用。查询参数跟 DTM 保持一致，客户端屏障库可以直接复用。
    async fn call_http(
        &self,
        g: &GlobalRow,
        branch_id: &str,
        op: BranchOp,
        url: &str,
        payload: &str,
    ) -> BranchResult {
        let req = self
            .http
            .post(url)
            .query(&[
                ("gid", g.gid.as_str()),
                ("trans_type", &g.trans_type.to_string()),
                ("branch_id", branch_id),
                ("op", op.as_str()),
            ])
            .header("content-type", "application/json")
            .body(branch_payload(payload));
        match req.send().await {
            Ok(resp) => {
                let code = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();
                let r = BranchResult::from_http(code, &body);
                info!(gid = %g.gid, branch = %branch_id, op = op.as_str(), code, result = ?r, "分支返回");
                r
            }
            Err(e) => {
                // 超时/连不上 —— 结果未知，必须重试而不是回滚
                warn!(gid = %g.gid, branch = %branch_id, error = %e, "分支不可达，结果未知");
                BranchResult::Unknown
            }
        }
    }
}

/// 分支号：第 0 步是 "01"，跟 DTM 一致
pub fn branch_id(index: usize) -> String {
    format!("{:02}", index + 1)
}

fn index_of(branch_id: &str) -> Option<usize> {
    branch_id
        .parse::<usize>()
        .ok()
        .and_then(|v| v.checked_sub(1))
}

/// 按 op 把分支行拆成两列（正向 / 反向），按步序对齐
fn split_by_op(
    rows: &[dtmrs_store::BranchRow],
    n: usize,
    fwd: BranchOp,
    bwd: BranchOp,
) -> (Vec<BranchStatus>, Vec<BranchStatus>) {
    let mut a = vec![BranchStatus::Prepared; n];
    let mut b = vec![BranchStatus::Prepared; n];
    for r in rows {
        let Some(i) = index_of(&r.branch_id) else {
            continue;
        };
        if i >= n {
            continue;
        }
        if r.op == fwd {
            a[i] = r.status;
        } else if r.op == bwd {
            b[i] = r.status;
        }
    }
    (a, b)
}

/// 按分支序取出各 compensate 行的状态，用于 workflow 的逆序补偿。
///
/// 跟 `split_by_op` 的区别：workflow 的分支数是**运行时长出来的**，
/// 提交时并不知道有几个，所以长度得从已登记的行里推出来。
fn compensate_states(rows: &[dtmrs_store::BranchRow]) -> Vec<BranchStatus> {
    let n = rows
        .iter()
        .filter(|r| r.op == BranchOp::Compensate)
        .filter_map(|r| index_of(&r.branch_id))
        .max()
        .map(|m| m + 1)
        .unwrap_or(0);
    // 没登记补偿的分支（比如纯查询步骤）留成 Succeed，逆序扫描时会跳过它 ——
    // 本来就没有副作用要收拾
    let mut v = vec![BranchStatus::Succeed; n];
    for r in rows.iter().filter(|r| r.op == BranchOp::Compensate) {
        if let Some(i) = index_of(&r.branch_id) {
            if i < n {
                v[i] = r.status;
            }
        }
    }
    v
}

/// 取某个分支行上存的 payload（TCC / XA / workflow 的分支是动态登记的，
/// 业务数据跟着行走，不在全局 payload 里）
fn payload_of(rows: &[dtmrs_store::BranchRow], branch_id: &str, op: BranchOp) -> String {
    rows.iter()
        .find(|r| r.branch_id == branch_id && r.op == op)
        .map(|r| r.payload.clone())
        .unwrap_or_default()
}

fn url_of(rows: &[dtmrs_store::BranchRow], branch_id: &str, op: BranchOp) -> Option<String> {
    rows.iter()
        .find(|r| r.branch_id == branch_id && r.op == op)
        .map(|r| r.url.clone())
}

/// 这一步要发给分支的请求体。
///
/// 每步各自独立 —— 扣款那步要金额、发货那步要地址，本来就不该收到同一份数据。
/// 步骤没写 payload 就发 `{}`（很多分支只靠 gid/branch_id/op 做幂等，不需要请求体）。
fn branch_payload(step_payload: &str) -> String {
    if step_payload.trim().is_empty() {
        "{}".to_string()
    } else {
        step_payload.to_string()
    }
}

/// 推进器的可配置项
#[derive(Debug, Clone, Copy)]
pub struct DriverConfig {
    /// 调一个分支最多等几秒
    pub branch_timeout_secs: i64,
    /// 租约时长（秒）
    pub lease_secs: i64,
    pub retry: dtmrs_core::RetryPolicy,
    /// 并行推进的 worker 数。**一笔事务内部仍然按序**，并行只发生在事务之间
    pub workers: usize,
}

impl Default for DriverConfig {
    fn default() -> Self {
        // 跟 0.2 的写死值一致，不配置的人行为不变
        Self {
            branch_timeout_secs: 10,
            lease_secs: 30,
            retry: dtmrs_core::RetryPolicy::default(),
            // 推一笔事务的时间几乎全花在等 I/O（存储往返 + 分支调用）上，
            // 所以 worker 数可以明显高于核数。
            //
            // 16 基本就是这台机器上的天花板了：实测（20 核，空库，
            //   三次取中位数，bench/）
            //   Postgres  1→267 笔/秒  16→3424  64→3184（一样，没收益）
            //   Redis     1→965        16→4695  32→4974
            //   sqlite    1→435        16→682 —— 写是全库串行的，并行收益有限
            //   MySQL     1→19         16→129   32→202
            // 再往上加 worker 不涨，连接池也不是瓶颈。要继续提升得**减少
            // 每笔事务的存储往返次数**，不是加并发。
            // 另外 worker 开多少就要占多少条数据库连接，而 TC 常常和业务
            // 共用一个库，所以默认值往保守取
            workers: 16,
        }
    }
}

impl DriverConfig {
    pub fn from_env() -> Self {
        let d = Self::default();
        let get = |k: &str, fallback: i64| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(fallback)
        };
        Self {
            branch_timeout_secs: get("DTMRS_BRANCH_TIMEOUT", d.branch_timeout_secs),
            lease_secs: get("DTMRS_LEASE", d.lease_secs),
            retry: dtmrs_core::RetryPolicy::from_env(),
            workers: get("DTMRS_WORKERS", d.workers as i64) as usize,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 分支号与下标互转() {
        assert_eq!(branch_id(0), "01");
        assert_eq!(branch_id(9), "10");
        assert_eq!(index_of("01"), Some(0));
        assert_eq!(index_of("10"), Some(9));
        assert_eq!(index_of("00"), None);
        assert_eq!(index_of("xx"), None);
    }
}
