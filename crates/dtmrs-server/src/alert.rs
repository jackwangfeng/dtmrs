//! 卡住的事务往外报：重试轮数到了上限就发告警。
//!
//! # 为什么要有
//!
//! 有些「卡住」是**刻意**的：confirm / commit 失败只能无限重试、等人介入
//! （绝不能转 cancel，见 CLAUDE.md「绝对不能破坏的语义」第 2 条）；msg 的订阅方
//! 一直失败也只能重试。语义没错，但**没人知道它卡着**就等于丢了。
//!
//! # 跟 DTM 的对照
//!
//! DTM 是 `AlertRetryLimit`（默认 3）+ `AlertWebHook`，POST 一个
//! `{gid, status, branch, error, retry_count}`。这里同样默认 3、同样 POST JSON，
//! 字段是 [`Alert`]：没给「哪个分支、什么错」，给的是**所有还没成功的分支**——
//! 一笔事务卡住时往往不止一个分支没完成，运维要看的是全貌。
//!
//! 重试到上限之后**每轮都报**（同 DTM）。退避封顶 300 秒，所以一笔事务最多
//! 五分钟一条，不会刷屏；报警系统那边按 gid 去重。
//!
//! 告警发不出去只打日志，**绝不影响推进** —— 监控挂了不能让事务也跟着停。

use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

/// 告警内容。webhook 收到的 JSON 就是它
#[derive(Debug, Clone, Serialize)]
pub struct Alert {
    pub gid: String,
    pub trans_type: String,
    pub status: String,
    /// 已经退避重试了多少轮（包括这一轮）
    pub retry_count: i64,
    pub rollback_reason: String,
    /// 还没成功的分支
    pub pending: Vec<PendingBranch>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingBranch {
    pub branch_id: String,
    pub op: String,
    pub url: String,
}

/// 告警发到哪
#[derive(Clone)]
pub enum AlertSink {
    /// POST JSON 到这个地址（5 秒超时）
    Webhook(String),
    /// 进程内回调（嵌入式用：直接接进宿主自己的监控）。**别在里面阻塞**
    Callback(Arc<dyn Fn(Alert) + Send + Sync>),
}

#[derive(Clone)]
pub struct AlertConfig {
    pub sink: AlertSink,
    /// 重试到第几轮开始报，默认 3（同 DTM 的 AlertRetryLimit）
    pub retry_limit: i64,
}

/// 默认从第几轮重试开始报
pub const DEFAULT_ALERT_RETRY_LIMIT: i64 = 3;

impl AlertConfig {
    /// `DTMRS_ALERT_WEBHOOK`（不配就不告警）+ `DTMRS_ALERT_RETRY_LIMIT`（默认 3）
    pub fn from_env() -> Option<Self> {
        let url = std::env::var("DTMRS_ALERT_WEBHOOK").ok()?;
        let url = url.trim();
        if url.is_empty() {
            return None;
        }
        let retry_limit = std::env::var("DTMRS_ALERT_RETRY_LIMIT")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_ALERT_RETRY_LIMIT);
        Some(Self {
            sink: AlertSink::Webhook(url.to_string()),
            retry_limit,
        })
    }

    /// 这一轮该不该报
    pub fn should_fire(&self, retry_count: i64) -> bool {
        retry_count >= self.retry_limit.max(1)
    }

    /// 发出去。webhook 在后台发，不等结果
    pub fn fire(&self, http: &reqwest::Client, a: Alert) {
        tracing::warn!(gid = %a.gid, status = %a.status, retry_count = a.retry_count,
                       pending = a.pending.len(), "事务重试到告警上限，需要人看一眼");
        match &self.sink {
            AlertSink::Callback(f) => f(a),
            AlertSink::Webhook(url) => {
                let (http, url) = (http.clone(), url.clone());
                tokio::spawn(async move {
                    let r = http
                        .post(&url)
                        .timeout(Duration::from_secs(5))
                        .json(&a)
                        .send()
                        .await;
                    match r {
                        Ok(resp) if resp.status().is_success() => {}
                        Ok(resp) => tracing::error!(gid = %a.gid, status = %resp.status(),
                                                    "告警 webhook 返回非 2xx"),
                        Err(e) => {
                            tracing::error!(gid = %a.gid, error = %e, "告警 webhook 发送失败")
                        }
                    }
                });
            }
        }
    }
}
