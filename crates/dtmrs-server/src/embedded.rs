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

use crate::driver::Driver;
use crate::registry::{BranchCtx, Registry};
use crate::saga_rows;
use dtmrs_core::{BranchResult, GlobalStatus, SagaStep};
use dtmrs_store::Store;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

pub struct EmbeddedBuilder {
    db: String,
    owner: String,
    registry: Registry,
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

    pub async fn start(self) -> anyhow::Result<Embedded> {
        let store = Store::open(&self.db).await?;
        let registry = Arc::new(self.registry);
        let driver = Driver::new(store.clone(), self.owner).with_registry(registry.clone());
        // 常驻推进器。重启后未终结的事务会被它自动捞起继续推 —— 崩溃恢复就靠这个
        let task = tokio::spawn(driver.clone().run_forever(self.tick));
        Ok(Embedded {
            store,
            registry,
            task: Some(task),
        })
    }
}

pub struct Embedded {
    store: Store,
    registry: Arc<Registry>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Embedded {
    pub fn builder(db: &str) -> EmbeddedBuilder {
        EmbeddedBuilder {
            db: db.to_string(),
            owner: format!("embedded-{}", std::process::id()),
            registry: Registry::new(),
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
        self.steps.push(SagaStep {
            action: action.to_string(),
            compensate: compensate.to_string(),
        });
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
