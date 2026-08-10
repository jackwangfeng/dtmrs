//! 事务推进器：把状态机的决策落成真实的 HTTP 调用和状态更新。
//!
//! 这里是**唯一**会修改全局事务状态的地方，决策全部来自 `dtmrs_core::saga_advance`，
//! 本文件只负责 I/O。这样状态迁移的正确性可以在 core 里纯单测覆盖。

use crate::registry::{parse_target, BranchCtx, Registry, Target};
use dtmrs_core::{
    msg_advance, saga_advance, tcc_advance, Advance, BranchOp, BranchResult, BranchStatus,
    GlobalStatus, SagaStep, TransType,
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
    /// 进程内分支注册表。嵌入式模式用，纯 HTTP 部署时是空表
    pub registry: Arc<Registry>,
}

impl Driver {
    pub fn new(store: Store, owner: String) -> Self {
        Self {
            store,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("build http client"),
            owner,
            lease: 30,
            registry: Arc::new(Registry::new()),
        }
    }

    /// 挂上进程内分支注册表 —— 嵌入式模式的入口
    pub fn with_registry(mut self, r: Arc<Registry>) -> Self {
        self.registry = r;
        self
    }

    /// 常驻循环：抢一个到期事务推一下，没活就睡
    pub async fn run_forever(self, tick: Duration) {
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
            TransType::Xa => {
                // XA 要数据库原生 XA 支持，还没实现。**不假装成功** ——
                // 留在库里比错误地标成 succeed 安全得多
                warn!(gid = %g.gid, "xa 模式尚未实现，跳过不处理");
                Ok(())
            }
        }
    }

    // ---------------- SAGA ----------------

    async fn process_saga(&self, g: &GlobalRow) -> anyhow::Result<()> {
        let steps: Vec<SagaStep> = serde_json::from_str(&g.payload).unwrap_or_default();
        if steps.is_empty() {
            self.store.set_global_status(&g.gid, GlobalStatus::Succeed, "").await?;
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
                        self.store.set_global_status(&g.gid, s, "分支已判失败").await?;
                        continue;
                    }
                    info!(gid = %g.gid, status = s.as_str(), "事务终结");
                    self.store.set_global_status(&g.gid, s, "").await?;
                    return Ok(());
                }
                Advance::Wait => return Ok(()),
                Advance::Call { index, op } => {
                    let branch_id = branch_id(index);
                    let url = match op {
                        BranchOp::Action => &steps[index].action,
                        _ => &steps[index].compensate,
                    };
                    match self.call_branch(g, &branch_id, op, url).await {
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
            self.store.set_global_status(&g.gid, s, "").await?;
            return Ok(());
        }

        let status = g.status;
        loop {
            let rows = self.store.list_branches(&g.gid).await?;
            let (confirms, cancels) = split_by_op(&rows, n, BranchOp::Confirm, BranchOp::Cancel);
            match tcc_advance(status, &confirms, &cancels) {
                Advance::Finish(s) => {
                    info!(gid = %g.gid, status = s.as_str(), "TCC 事务终结");
                    self.store.set_global_status(&g.gid, s, "").await?;
                    return Ok(());
                }
                Advance::Wait => return Ok(()),
                Advance::Call { index, op } => {
                    let bid = branch_id(index);
                    let Some(url) = url_of(&rows, &bid, op) else {
                        // 登记时漏了这个 op 的 URL。当未知处理，等人修
                        warn!(gid = %g.gid, branch = %bid, op = op.as_str(),
                              "分支没登记这个操作的 URL，无法调用");
                        self.retry_later(g).await?;
                        return Ok(());
                    };
                    match self.call_branch(g, &bid, op, &url).await {
                        BranchResult::Success => {
                            self.store
                                .set_branch_status(&g.gid, &bid, op, BranchStatus::Succeed)
                                .await?;
                        }
                        // confirm/cancel 失败**绝不改变全局方向**：
                        // try 已经成功、方向已经定了，反向操作会造成不一致。
                        // 唯一正确处理是无限重试 + 报警。
                        BranchResult::Failure => {
                            self.store
                                .set_branch_status(&g.gid, &bid, op, BranchStatus::Failed)
                                .await?;
                            warn!(gid = %g.gid, branch = %bid, op = op.as_str(),
                                  "TCC 二阶段失败，会持续重试，需要人工介入");
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
                .call_branch(g, "00", BranchOp::Action, &g.query_prepared)
                .await
            {
                BranchResult::Success => {
                    info!(gid = %g.gid, "回查：本地事务已提交 → 继续推进");
                    self.store
                        .set_global_status(&g.gid, GlobalStatus::Submitted, "")
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
            self.store.set_global_status(&g.gid, GlobalStatus::Succeed, "").await?;
            return Ok(());
        }
        loop {
            let (actions, _) = self.branch_states(&g.gid, steps.len()).await?;
            match msg_advance(status, &actions) {
                Advance::Finish(s) => {
                    info!(gid = %g.gid, status = s.as_str(), "消息事务终结");
                    self.store.set_global_status(&g.gid, s, "").await?;
                    return Ok(());
                }
                Advance::Wait => return Ok(()),
                Advance::Call { index, op } => {
                    let bid = branch_id(index);
                    match self.call_branch(g, &bid, op, &steps[index].action).await {
                        BranchResult::Success => {
                            self.store
                                .set_branch_status(&g.gid, &bid, op, BranchStatus::Succeed)
                                .await?;
                        }
                        // msg 保证"最终一定送达"，没有补偿一说。失败只能重试。
                        BranchResult::Failure
                        | BranchResult::Ongoing
                        | BranchResult::Unknown => {
                            self.retry_later(g).await?;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn retry_later(&self, g: &GlobalRow) -> anyhow::Result<()> {
        let iv = dtmrs_core::next_interval(g.next_cron_interval);
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
            let Some(i) = index_of(&r.branch_id) else { continue };
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

    /// 调一个分支。`local://名字` 走进程内函数，其它走 HTTP。
    async fn call_branch(
        &self,
        g: &GlobalRow,
        branch_id: &str,
        op: BranchOp,
        url: &str,
    ) -> BranchResult {
        match parse_target(url) {
            Target::Local(name) => self.call_local(g, branch_id, op, &name).await,
            Target::Http(u) => self.call_http(g, branch_id, op, &u).await,
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
            .body(branch_payload(&g.payload));
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
    branch_id.parse::<usize>().ok().and_then(|v| v.checked_sub(1))
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
        let Some(i) = index_of(&r.branch_id) else { continue };
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

fn url_of(rows: &[dtmrs_store::BranchRow], branch_id: &str, op: BranchOp) -> Option<String> {
    rows.iter()
        .find(|r| r.branch_id == branch_id && r.op == op)
        .map(|r| r.url.clone())
}

/// MVP 阶段各分支共用同一份请求体。真实场景应该每步独立 payload，
/// 那是第二版的事（见 DESIGN.md 范围表）。
fn branch_payload(_global_payload: &str) -> String {
    "{}".to_string()
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
