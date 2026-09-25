//! 嵌入式 TC —— 把事务协调器当库链进你自己的进程，**不需要单独部署一个服务**。
//!
//! 这是 dtmrs 相对 DTM 的结构性差异。DTM 是 Go，`c-shared` 会把整个运行时拖进去，
//! 实际没法当库用，所以必须独立部署：
//!
//! ```text
//! DTM:    你的服务 ──HTTP──► 独立部署的 TC 进程 ──► DB
//!                            （要运维、要高可用、要监控）
//! dtmrs:  你的服务（TC 就在进程里）──► DB
//! ```
//!
//! 少一个组件，而且分支调用退化成**一次函数调用** —— 没有网络、没有序列化。
//!
//! # 用法
//!
//! ```no_run
//! use dtmrs_server::embedded::Embedded;
//! use dtmrs_core::BranchResult;
//!
//! # async fn demo() -> anyhow::Result<()> {
//! let tc = Embedded::builder("sqlite:app.db")
//!     .handler("deduct",      |_ctx| async { BranchResult::Success })
//!     .handler("deduct_undo", |_ctx| async { BranchResult::Success })
//!     .start()
//!     .await?;
//!
//! tc.saga("order-1001")
//!     .step("local://deduct", "local://deduct_undo")
//!     // 可以跟远端服务混用
//!     .step("http://shipment/create", "http://shipment/cancel")
//!     .submit()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # 一个必须知道的约束
//!
//! `local://` 分支存的是**名字**，因为闭包没法持久化。重启后必须注册同名 handler，
//! 否则事务推不动（会当成"结果未知"一直重试，不会误回滚）。
//! `submit` 时会检查名字是否都注册了 —— 宁可提交就报错，也别等副作用落地了才发现。
//!
//! # 五种模式都能用
//!
//! | 模式 | 入口 | 一阶段谁做 |
//! |---|---|---|
//! | SAGA | [`Embedded::saga`] | TC 调正向分支 |
//! | TCC | [`Embedded::tcc`] → [`Tcc::try_branch`] | **调用方**跑 try，TC 只管 confirm / cancel |
//! | XA | [`Embedded::xa`] → [`Xa::prepare_branch`] | **调用方**做业务 SQL + `PREPARE`，TC 只管 commit / rollback |
//! | 二阶段消息 | [`Embedded::msg`] → [`MsgBuilder::do_and_submit`] | **调用方**跑本地事务，TC 负责把消息送达 |
//! | workflow | [`EmbeddedBuilder::workflow`] + [`Embedded::submit_workflow`] | 函数自己 |
//!
//! TCC / XA / msg 的业务判断（能不能登记、能不能 abort、分支号格式）**全在
//! [`crate::api::Api`] 里**，这里只是薄薄一层 —— 跟 HTTP / gRPC 是同一套规则，
//! 不许在嵌入式这边另写一份。

use crate::api::{Api, RegisterBranch};
use crate::driver::Driver;
use crate::registry::{BranchCtx, Registry};
use crate::saga_rows;
use crate::workflow::{WorkflowCtx, WorkflowRegistry, WorkflowResult};
use dtmrs_core::{BranchResult, GlobalStatus, SagaStep, TransType};
use dtmrs_store::Store;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

pub struct EmbeddedBuilder {
    db: String,
    owner: String,
    registry: Registry,
    workflows: WorkflowRegistry,
    tick: Duration,
}

impl EmbeddedBuilder {
    /// 注册一个进程内分支。名字对应 `local://名字`。
    pub fn handler<F, Fut>(mut self, name: &str, f: F) -> Self
    where
        F: Fn(BranchCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = BranchResult> + Send + 'static,
    {
        self.registry.register(name, f);
        self
    }

    pub fn owner(mut self, o: &str) -> Self {
        self.owner = o.to_string();
        self
    }

    /// 推进器轮询间隔。默认 200ms —— 进程内调用很快，不需要像跨网络那样保守
    pub fn tick(mut self, d: Duration) -> Self {
        self.tick = d;
        self
    }

    /// 注册一个 workflow：把整个事务流程写成一个普通函数。
    ///
    /// 跟 saga 的区别是**步骤由函数自己决定** —— 可以有 if、有循环、
    /// 可以依赖前一步的返回值。崩溃后靠重放 + 结果记忆化续跑。
    ///
    /// 详见 [`crate::workflow`]，尤其是「你的函数必须是确定性的」那节。
    pub fn workflow<F, Fut>(mut self, name: &str, f: F) -> Self
    where
        F: Fn(WorkflowCtx) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = WorkflowResult<()>> + Send + 'static,
    {
        self.workflows.register(name, f);
        self
    }

    pub async fn start(self) -> anyhow::Result<Embedded> {
        let store = Store::open(&self.db).await?;
        let registry = Arc::new(self.registry);
        let workflows = Arc::new(self.workflows);
        let driver = Driver::new(store.clone(), self.owner)
            .with_registry(registry.clone())
            .with_workflows(workflows.clone());
        // 常驻推进器。重启后未终结的事务会被它自动捞起继续推 —— 崩溃恢复就靠这个
        let task = tokio::spawn(driver.clone().run_forever(self.tick));
        Ok(Embedded {
            api: Api::new(store.clone()),
            store,
            registry,
            workflows,
            task: Some(task),
        })
    }
}

pub struct Embedded {
    store: Store,
    /// TCC / XA / msg 走的操作层，跟 HTTP / gRPC 共用同一套判断
    api: Api,
    registry: Arc<Registry>,
    workflows: Arc<WorkflowRegistry>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Embedded {
    pub fn builder(db: &str) -> EmbeddedBuilder {
        EmbeddedBuilder {
            db: db.to_string(),
            owner: format!("embedded-{}", std::process::id()),
            registry: Registry::new(),
            workflows: WorkflowRegistry::new(),
            tick: Duration::from_millis(200),
        }
    }

    pub fn saga(&self, gid: &str) -> SagaBuilder<'_> {
        SagaBuilder {
            tc: self,
            gid: gid.to_string(),
            steps: Vec::new(),
        }
    }

    /// 提交一个 workflow 事务。
    ///
    /// `name` 必须是 [`EmbeddedBuilder::workflow`] 注册过的名字 ——
    /// 这里就检查，宁可提交报错，也别等推到一半才发现函数不存在。
    ///
    /// `input` 原样透传给函数（`WorkflowCtx::input`）。gid 本身通常就是业务单号，
    /// 简单场景可以传空串。
    pub async fn submit_workflow(&self, gid: &str, name: &str, input: &str) -> anyhow::Result<()> {
        if !self.workflows.contains(name) {
            anyhow::bail!(
                "workflow「{name}」没注册。已注册的: {:?}",
                self.workflows.names()
            );
        }
        let mut g = crate::tcc_rows(gid);
        g.trans_type = TransType::Workflow;
        g.status = GlobalStatus::Submitted;
        g.payload = crate::workflow::encode_payload(name, input);
        // 重复提交同一个 gid 是幂等的（INSERT OR IGNORE），跟 saga 一致
        self.store.create_global(&g, &[]).await?;
        Ok(())
    }

    /// 开一个 TCC 事务（幂等：同一个 gid 再开一次不报错，也不会覆盖）。
    ///
    /// 之后用 [`Tcc::try_branch`] 逐个分支「先登记、再跑 try」，全成功就
    /// [`Tcc::submit`]，任何一个不是 `Success` 就 [`Tcc::abort`]。
    pub async fn tcc(&self, gid: &str) -> anyhow::Result<Tcc<'_>> {
        self.api.prepare(gid, "tcc", &[], "", None).await?;
        Ok(Tcc {
            tc: self,
            gid: gid.to_string(),
            next: 0,
        })
    }

    /// 开一个 XA 事务。跟 [`Self::tcc`] 同形，op 换成 commit / rollback。
    ///
    /// 一阶段（业务 SQL + `PREPARE TRANSACTION` / `XA PREPARE`）由调用方做，
    /// commit / rollback 分支通常是个 `local://` handler，里面调 `dtmrs-xa` 的
    /// `commit_prepared` / `rollback_prepared`。
    pub async fn xa(&self, gid: &str) -> anyhow::Result<Xa<'_>> {
        self.api.prepare(gid, "xa", &[], "", None).await?;
        Ok(Xa {
            tc: self,
            gid: gid.to_string(),
            next: 0,
        })
    }

    /// 开一个二阶段消息。见 [`MsgBuilder`]。
    pub fn msg(&self, gid: &str) -> MsgBuilder<'_> {
        MsgBuilder {
            tc: self,
            gid: gid.to_string(),
            actions: Vec::new(),
            query_prepared: String::new(),
            grace_secs: None,
        }
    }

    /// 登记一个 TCC 分支，**分支号由调用方给**（`01`、`02`……）。
    ///
    /// 给 FFI 和「一个事务的分支分散在多个进程里登记」的场景用；同一个进程里
    /// 用 [`Tcc::try_branch`] 更省事，分支号自动编。
    ///
    /// 同一个分支号原样再登记一次是幂等的（客户端重试）；**地址不同则报错** ——
    /// 那是两个分支撞了号，后一个的地址根本没写进去，放行会让它的资源没人收尾。
    pub async fn register_tcc_branch(
        &self,
        gid: &str,
        branch_id: &str,
        confirm: &str,
        cancel: &str,
    ) -> anyhow::Result<()> {
        self.check_local(&[confirm, cancel])?;
        self.api
            .register_branch(&RegisterBranch {
                gid: gid.to_string(),
                branch_id: branch_id.to_string(),
                confirm: confirm.to_string(),
                cancel: cancel.to_string(),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// 登记一个 XA 分支。语义同 [`Self::register_tcc_branch`]。
    pub async fn register_xa_branch(
        &self,
        gid: &str,
        branch_id: &str,
        commit: &str,
        rollback: &str,
    ) -> anyhow::Result<()> {
        self.check_local(&[commit, rollback])?;
        self.api
            .register_branch(&RegisterBranch {
                gid: gid.to_string(),
                branch_id: branch_id.to_string(),
                commit: commit.to_string(),
                rollback: rollback.to_string(),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// 二阶段提交 tcc / xa / msg：一阶段全成功了，交给 TC 推 confirm / commit /
    /// 发消息。幂等，重复调不报错。
    pub async fn submit(&self, gid: &str) -> anyhow::Result<()> {
        let Some(g) = self.store.get_global(gid).await? else {
            anyhow::bail!("gid {gid} 不存在，tcc / xa / msg 要先开事务");
        };
        self.api.submit(gid, &g.trans_type.to_string(), &[]).await?;
        Ok(())
    }

    /// 主动中止，TC 会逆序撤销**所有**已登记的分支（没做成的由屏障空转掉）。
    ///
    /// ⚠ tcc / xa / msg **submit 之后不能 abort**（会报错）：方向已定，
    /// 这时 abort 就是一半 confirm 一半 cancel。要回滚必须在 submit 之前。
    pub async fn abort(&self, gid: &str) -> anyhow::Result<()> {
        self.api.abort(gid).await?;
        Ok(())
    }

    /// `local://` 名字都注册了吗？跟 saga 提交前的自查是同一条理由：
    /// 等 TC 去 confirm / cancel 时才发现 handler 不存在，一阶段的副作用已经落地了
    fn check_local(&self, targets: &[&str]) -> anyhow::Result<()> {
        let t: Vec<String> = targets.iter().map(|s| s.to_string()).collect();
        if let Err(missing) = self.registry.check_all(&t) {
            anyhow::bail!("这些本地分支没注册: {}", missing.join(", "));
        }
        Ok(())
    }

    pub async fn status(&self, gid: &str) -> anyhow::Result<Option<GlobalStatus>> {
        Ok(self.store.get_global(gid).await?.map(|g| g.status))
    }

    /// 等到事务落终态。**只是为了测试和"同步等结果"的场景方便** ——
    /// 生产上事务是异步推进的，别在请求路径里等。
    pub async fn wait_final(&self, gid: &str, timeout: Duration) -> anyhow::Result<GlobalStatus> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(s) = self.status(gid).await? {
                if s.is_final() {
                    return Ok(s);
                }
            }
            if std::time::Instant::now() >= deadline {
                anyhow::bail!("等 {gid} 落终态超时");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }
}

impl Drop for Embedded {
    fn drop(&mut self) {
        // 模拟进程退出：停掉推进器。未终结的事务留在库里，下次 start 会接着推
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

pub struct SagaBuilder<'a> {
    tc: &'a Embedded,
    gid: String,
    steps: Vec<SagaStep>,
}

impl SagaBuilder<'_> {
    /// 加一步。两个参数都可以是 `local://名字` 或 `http://...`，可混用。
    pub fn step(mut self, action: &str, compensate: &str) -> Self {
        self.steps.push(SagaStep::new(action, compensate));
        self
    }

    /// 加一步，并带上**这一步自己的**业务数据（发给分支的请求体）。
    ///
    /// 扣款那步要金额、发货那步要地址 —— 它们本来就不该收到同一份数据。
    pub fn step_with(mut self, action: &str, compensate: &str, payload: &str) -> Self {
        self.steps
            .push(SagaStep::with_payload(action, compensate, payload));
        self
    }

    pub async fn submit(self) -> anyhow::Result<()> {
        if self.steps.is_empty() {
            anyhow::bail!("saga 至少要有一步");
        }
        // 提交前自查所有 local:// 名字。等推到一半才发现分支不存在就晚了 ——
        // 那时前几步的副作用已经落地，只能靠补偿收拾。
        let targets: Vec<String> = self
            .steps
            .iter()
            .flat_map(|s| [s.action.clone(), s.compensate.clone()])
            .collect();
        if let Err(missing) = self.tc.registry.check_all(&targets) {
            anyhow::bail!("这些本地分支没注册: {}", missing.join(", "));
        }
        let (g, branches) = saga_rows(&self.gid, &self.steps);
        self.tc.store.create_global(&g, &branches).await?;
        Ok(())
    }
}

/// 一个进行中的 TCC 事务。由 [`Embedded::tcc`] 开。
///
/// ```no_run
/// # use dtmrs_server::embedded::Embedded;
/// # use dtmrs_core::BranchResult;
/// # async fn demo(tc: &Embedded) -> anyhow::Result<()> {
/// let mut t = tc.tcc("order-1").await?;
/// let r = t
///     .try_branch("local://冻结确认", "local://冻结撤销", |_bid| async {
///         BranchResult::Success // 冻结库存
///     })
///     .await?;
/// if r == BranchResult::Success {
///     t.submit().await?;
/// } else {
///     t.abort().await?; // try 超时（Unknown）也是 abort：还没 submit，cancel 兜得住
/// }
/// # Ok(())
/// # }
/// ```
pub struct Tcc<'a> {
    tc: &'a Embedded,
    gid: String,
    next: usize,
}

impl Tcc<'_> {
    pub fn gid(&self) -> &str {
        &self.gid
    }

    /// 先登记分支，**登记成功了**才跑 `try_fn`，返回 try 的结果。
    ///
    /// 顺序是这个方法存在的理由：反过来的话 try 成功但登记失败，TC 不知道有这个
    /// 分支，回滚时不会 cancel 它 —— 冻结的资源永久泄漏。登记失败返回 `Err`，
    /// `try_fn` 一次都不会跑。
    ///
    /// `try_fn` 拿到的是分支号，业务侧用 (gid, 分支号) 做幂等 / 屏障。
    ///
    /// **try 不是 `Success` 就该 abort**，包括 `Unknown`：还没 submit，
    /// cancel 会撤掉每个分支，try 其实成功了的那份也会被撤掉。
    pub async fn try_branch<F, Fut>(
        &mut self,
        confirm: &str,
        cancel: &str,
        try_fn: F,
    ) -> anyhow::Result<BranchResult>
    where
        F: FnOnce(String) -> Fut,
        Fut: Future<Output = BranchResult>,
    {
        let bid = self.register(confirm, cancel).await?;
        Ok(try_fn(bid).await)
    }

    /// 只登记下一个分支，返回分支号。try 自己去跑 —— **必须在这之后**。
    pub async fn register(&mut self, confirm: &str, cancel: &str) -> anyhow::Result<String> {
        let bid = crate::driver::branch_id(self.next);
        self.tc
            .register_tcc_branch(&self.gid, &bid, confirm, cancel)
            .await?;
        // 登记成功才占号：失败了重试这一步还用同一个号，不会留下空洞
        self.next += 1;
        Ok(bid)
    }

    /// 所有 try 都成功了 → 交给 TC 去 confirm。
    pub async fn submit(self) -> anyhow::Result<()> {
        self.tc.submit(&self.gid).await
    }

    /// 有 try 没成功 → TC 逆序 cancel 每个已登记的分支。
    pub async fn abort(self) -> anyhow::Result<()> {
        self.tc.abort(&self.gid).await
    }
}

/// 一个进行中的 XA 事务。由 [`Embedded::xa`] 开，用法跟 [`Tcc`] 一样。
pub struct Xa<'a> {
    tc: &'a Embedded,
    gid: String,
    next: usize,
}

impl Xa<'_> {
    pub fn gid(&self) -> &str {
        &self.gid
    }

    /// 先登记分支，登记成功了才跑一阶段 `prepare_fn`（业务 SQL + PREPARE）。
    ///
    /// 顺序反过来的后果比 TCC 更糟：一阶段 prepare 了但 TC 不知道，
    /// 这个 prepared 事务**永久持锁**（Postgres 上还卡 VACUUM）。
    ///
    /// `prepare_fn` 拿到分支号，用它拼 xid（`dtmrs_xa::xid_for(gid, 分支号)`），
    /// 这样 commit / rollback 分支能按同样的规则找到它。
    pub async fn prepare_branch<F, Fut>(
        &mut self,
        commit: &str,
        rollback: &str,
        prepare_fn: F,
    ) -> anyhow::Result<BranchResult>
    where
        F: FnOnce(String) -> Fut,
        Fut: Future<Output = BranchResult>,
    {
        let bid = self.register(commit, rollback).await?;
        Ok(prepare_fn(bid).await)
    }

    /// 只登记下一个分支，返回分支号。一阶段自己去做 —— **必须在这之后**。
    pub async fn register(&mut self, commit: &str, rollback: &str) -> anyhow::Result<String> {
        let bid = crate::driver::branch_id(self.next);
        self.tc
            .register_xa_branch(&self.gid, &bid, commit, rollback)
            .await?;
        self.next += 1;
        Ok(bid)
    }

    pub async fn submit(self) -> anyhow::Result<()> {
        self.tc.submit(&self.gid).await
    }

    pub async fn abort(self) -> anyhow::Result<()> {
        self.tc.abort(&self.gid).await
    }
}

/// 二阶段消息：「本地事务 + 保证送达的消息」，取代 MQ 的事务消息。
///
/// ```no_run
/// # use dtmrs_server::embedded::Embedded;
/// # use dtmrs_core::BranchResult;
/// # async fn demo(tc: &Embedded) -> anyhow::Result<()> {
/// tc.msg("order-1")
///     .action("local://加积分")
///     .action("http://notify/send")
///     .query_prepared("local://查订单提交了没有")
///     .do_and_submit(|| async {
///         BranchResult::Success // 本地事务：写订单表
///     })
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// **`query_prepared` 是必填的。** 进程崩在「本地事务提交」和「submit」之间，
/// TC 只能靠它问「你那边到底提交了没有」—— 猜「提交了」会重复发，猜「没有」会丢单。
/// 回查 handler 返回 `Success` = 已提交（继续发），`Failure` = 没提交（作废），
/// 其它 = 不知道（过会儿再问）。
pub struct MsgBuilder<'a> {
    tc: &'a Embedded,
    gid: String,
    actions: Vec<String>,
    query_prepared: String,
    grace_secs: Option<i64>,
}

impl MsgBuilder<'_> {
    /// 加一条要送达的消息（正向分支）。msg 没有补偿：送不到就一直重试。
    pub fn action(mut self, target: &str) -> Self {
        self.actions.push(target.to_string());
        self
    }

    /// 回查地址。见类型文档。
    pub fn query_prepared(mut self, target: &str) -> Self {
        self.query_prepared = target.to_string();
        self
    }

    /// prepare 之后隔多久才开始回查，默认 10 秒。正常流程会很快 submit，
    /// 立刻回查是白问一次
    pub fn grace_secs(mut self, s: i64) -> Self {
        self.grace_secs = Some(s);
        self
    }

    /// 只做 prepare。之后自己跑本地事务，成功了 [`Embedded::submit`]，
    /// 明确失败了 [`Embedded::abort`]，不知道就什么都别做（交给回查）。
    pub async fn prepare(self) -> anyhow::Result<()> {
        let mut t: Vec<&str> = self.actions.iter().map(String::as_str).collect();
        t.push(&self.query_prepared);
        self.tc.check_local(&t)?;
        self.tc
            .api
            .prepare(
                &self.gid,
                "msg",
                &self.actions,
                &self.query_prepared,
                self.grace_secs,
            )
            .await?;
        Ok(())
    }

    /// prepare → 跑本地事务 → 按结果收尾，返回本地事务的结果。
    ///
    /// | 本地事务返回 | 这里做什么 |
    /// |---|---|
    /// | `Success` | submit，TC 开始送达 |
    /// | `Failure` | abort，整单作废，一条都不发 |
    /// | `Ongoing` / `Unknown` | **什么都不做**，停在 prepared 等回查决断 |
    ///
    /// prepare 失败时本地事务**不会跑**：跑了就是本地已提交、TC 却不知道
    /// 有这笔消息。submit 失败（比如存储抖了）返回 `Err`，但不要紧 ——
    /// 事务停在 prepared，回查会把它推下去。
    pub async fn do_and_submit<F, Fut>(self, local_tx: F) -> anyhow::Result<BranchResult>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = BranchResult>,
    {
        let tc = self.tc;
        let gid = self.gid.clone();
        self.prepare().await?;
        let r = local_tx().await;
        match r {
            BranchResult::Success => tc.submit(&gid).await?,
            BranchResult::Failure => tc.abort(&gid).await?,
            BranchResult::Ongoing | BranchResult::Unknown => {}
        }
        Ok(r)
    }
}
