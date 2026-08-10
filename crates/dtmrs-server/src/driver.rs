//! 事务推进器：把状态机的决策落成真实的 HTTP 调用和状态更新。
//!
//! 这里是**唯一**会修改全局事务状态的地方，决策全部来自 `dtmrs_core::saga_advance`，
//! 本文件只负责 I/O。这样状态迁移的正确性可以在 core 里纯单测覆盖。

use crate::registry::{parse_target, BranchCtx, Registry, Target};
use dtmrs_core::{saga_advance, Advance, BranchOp, BranchResult, BranchStatus, GlobalStatus, SagaStep};
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
                    let res = self.call_branch(&g, &branch_id, op, url).await;
                    match res {
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
