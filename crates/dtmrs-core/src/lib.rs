//! 类型与状态机 —— 纯逻辑，不碰 I/O。
//!
//! 分布式事务的 bug 绝大多数在状态迁移上，所以把这层从存储和网络里隔离出来，
//! 可以纯单元测试覆盖。

use serde::{Deserialize, Serialize};
use std::fmt;

/// 分支被调用后的结论。**这四态的区分是整个系统的命门。**
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchResult {
    /// HTTP 200 —— 成功
    Success,
    /// HTTP 409 —— 业务**明确**要求回滚。只有这个才触发补偿
    Failure,
    /// HTTP 425 —— 还在处理中，别当失败
    Ongoing,
    /// 网络错误、超时、5xx —— 结果**未知**
    ///
    /// 绝不能当成失败：超时的时候对方可能已经成功了，贸然补偿会造成不一致。
    /// 正确做法是重试，直到拿到 Success 或 Failure。
    Unknown,
}

impl BranchResult {
    /// 从 HTTP 状态码 + 响应体判定。响应体里的 `dtm_result` 字段优先于状态码，
    /// 这样业务方用 200 返回 `{"dtm_result":"FAILURE"}` 也能表达失败。
    pub fn from_http(status: u16, body: &str) -> Self {
        if body.contains("FAILURE") {
            return Self::Failure;
        }
        if body.contains("ONGOING") {
            return Self::Ongoing;
        }
        match status {
            200..=299 => Self::Success,
            409 => Self::Failure,
            425 => Self::Ongoing,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransType {
    Saga,
    Tcc,
    Msg,
    Xa,
}

impl fmt::Display for TransType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Saga => "saga",
            Self::Tcc => "tcc",
            Self::Msg => "msg",
            Self::Xa => "xa",
        };
        f.write_str(s)
    }
}

impl TransType {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "saga" => Some(Self::Saga),
            "tcc" => Some(Self::Tcc),
            "msg" => Some(Self::Msg),
            "xa" => Some(Self::Xa),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GlobalStatus {
    /// 仅二阶段消息用：TC 收到了但还不知道该不该执行
    Prepared,
    /// 可以推进
    Submitted,
    /// 需要回滚，正在逆序补偿
    Aborting,
    /// 终态
    Succeed,
    /// 终态
    Failed,
}

impl GlobalStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Submitted => "submitted",
            Self::Aborting => "aborting",
            Self::Succeed => "succeed",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "prepared" => Some(Self::Prepared),
            "submitted" => Some(Self::Submitted),
            "aborting" => Some(Self::Aborting),
            "succeed" => Some(Self::Succeed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    /// 终态不再被 cron 调度
    pub fn is_final(&self) -> bool {
        matches!(self, Self::Succeed | Self::Failed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BranchStatus {
    Prepared,
    Succeed,
    Failed,
}

impl BranchStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Succeed => "succeed",
            Self::Failed => "failed",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "prepared" => Some(Self::Prepared),
            "succeed" => Some(Self::Succeed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// 分支操作类型。跟 DTM 的字符串保持一致，方便客户端互通。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BranchOp {
    Action,
    Compensate,
    Try,
    Confirm,
    Cancel,
    Rollback,
}

impl BranchOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Action => "action",
            Self::Compensate => "compensate",
            Self::Try => "try",
            Self::Confirm => "confirm",
            Self::Cancel => "cancel",
            Self::Rollback => "rollback",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "action" => Some(Self::Action),
            "compensate" => Some(Self::Compensate),
            "try" => Some(Self::Try),
            "confirm" => Some(Self::Confirm),
            "cancel" => Some(Self::Cancel),
            "rollback" => Some(Self::Rollback),
            _ => None,
        }
    }

    /// 补偿类操作对应的**正向**操作。屏障判空回滚要用。
    pub fn origin_op(&self) -> Option<BranchOp> {
        match self {
            Self::Cancel => Some(Self::Try),
            Self::Compensate => Some(Self::Action),
            Self::Rollback => Some(Self::Action),
            _ => None,
        }
    }

    pub fn is_compensating(&self) -> bool {
        self.origin_op().is_some()
    }
}

/// 一个 SAGA 步骤：正向动作 + 对应补偿
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaStep {
    pub action: String,
    pub compensate: String,
}

/// 推进全局事务后，状态机给出的下一步指令
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Advance {
    /// 调这个分支
    Call { index: usize, op: BranchOp },
    /// 全部完成，落终态
    Finish(GlobalStatus),
    /// 有分支 Ongoing/Unknown，本轮到此为止，等下次 cron
    Wait,
}

/// SAGA 推进决策。**不碰 I/O，所以可以穷举测试。**
///
/// `actions[i]` / `compensates[i]` 是第 i 步两个分支各自的当前状态。
pub fn saga_advance(
    status: GlobalStatus,
    actions: &[BranchStatus],
    compensates: &[BranchStatus],
) -> Advance {
    debug_assert_eq!(actions.len(), compensates.len());
    match status {
        GlobalStatus::Submitted => {
            // 正向：按序找第一个还没成功的
            for (i, st) in actions.iter().enumerate() {
                match st {
                    BranchStatus::Succeed => continue,
                    BranchStatus::Prepared => {
                        return Advance::Call { index: i, op: BranchOp::Action }
                    }
                    // 有分支被判失败，本该已经转 aborting；防御性处理
                    BranchStatus::Failed => return Advance::Finish(GlobalStatus::Aborting),
                }
            }
            Advance::Finish(GlobalStatus::Succeed)
        }
        GlobalStatus::Aborting => {
            // 逆序补偿。**所有分支都补**，不管它的 action 成没成功 ——
            // action 超时但实际成功的情况必须靠补偿兜住，多余的补偿由屏障空转掉。
            for i in (0..compensates.len()).rev() {
                if compensates[i] == BranchStatus::Prepared {
                    return Advance::Call { index: i, op: BranchOp::Compensate };
                }
            }
            Advance::Finish(GlobalStatus::Failed)
        }
        GlobalStatus::Prepared => Advance::Wait,
        s => Advance::Finish(s),
    }
}

/// 指数退避：10s → 20s → 40s → … → 上限 300s
pub fn next_interval(cur: i64) -> i64 {
    const MAX: i64 = 300;
    if cur <= 0 {
        return 10;
    }
    (cur * 2).min(MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use BranchStatus::{Failed, Prepared, Succeed};

    #[test]
    fn 超时不能当失败() {
        // 这条错了就会造成数据不一致：对方可能已经成功了
        assert_eq!(BranchResult::from_http(504, ""), BranchResult::Unknown);
        assert_eq!(BranchResult::from_http(500, ""), BranchResult::Unknown);
        // 只有明确的 409 / FAILURE 才算失败
        assert_eq!(BranchResult::from_http(409, ""), BranchResult::Failure);
        assert_eq!(
            BranchResult::from_http(200, r#"{"dtm_result":"FAILURE"}"#),
            BranchResult::Failure
        );
        assert_eq!(BranchResult::from_http(425, ""), BranchResult::Ongoing);
        assert_eq!(BranchResult::from_http(200, "ok"), BranchResult::Success);
    }

    #[test]
    fn 正向按序推进() {
        let a = [Prepared, Prepared];
        let c = [Prepared, Prepared];
        assert_eq!(
            saga_advance(GlobalStatus::Submitted, &a, &c),
            Advance::Call { index: 0, op: BranchOp::Action }
        );
        let a = [Succeed, Prepared];
        assert_eq!(
            saga_advance(GlobalStatus::Submitted, &a, &c),
            Advance::Call { index: 1, op: BranchOp::Action }
        );
        let a = [Succeed, Succeed];
        assert_eq!(
            saga_advance(GlobalStatus::Submitted, &a, &c),
            Advance::Finish(GlobalStatus::Succeed)
        );
    }

    #[test]
    fn 补偿必须逆序() {
        let a = [Succeed, Failed];
        let c = [Prepared, Prepared];
        // 先补第 1 步（后执行的先回滚）
        assert_eq!(
            saga_advance(GlobalStatus::Aborting, &a, &c),
            Advance::Call { index: 1, op: BranchOp::Compensate }
        );
        let c = [Prepared, Succeed];
        assert_eq!(
            saga_advance(GlobalStatus::Aborting, &a, &c),
            Advance::Call { index: 0, op: BranchOp::Compensate }
        );
        let c = [Succeed, Succeed];
        assert_eq!(
            saga_advance(GlobalStatus::Aborting, &a, &c),
            Advance::Finish(GlobalStatus::Failed)
        );
    }

    #[test]
    fn 没跑过的分支也要补偿() {
        // action 全没成功，补偿照样得发 —— 因为 action 可能超时但实际成功了。
        // 多余的补偿由子事务屏障空转掉，这是安全的一侧。
        let a = [Prepared, Prepared];
        let c = [Prepared, Prepared];
        assert_eq!(
            saga_advance(GlobalStatus::Aborting, &a, &c),
            Advance::Call { index: 1, op: BranchOp::Compensate }
        );
    }

    #[test]
    fn 终态不再推进() {
        for s in [GlobalStatus::Succeed, GlobalStatus::Failed] {
            assert!(s.is_final());
            assert_eq!(saga_advance(s, &[], &[]), Advance::Finish(s));
        }
    }

    #[test]
    fn 补偿操作能找到正向操作() {
        assert_eq!(BranchOp::Compensate.origin_op(), Some(BranchOp::Action));
        assert_eq!(BranchOp::Cancel.origin_op(), Some(BranchOp::Try));
        assert_eq!(BranchOp::Action.origin_op(), None);
        assert!(BranchOp::Compensate.is_compensating());
        assert!(!BranchOp::Try.is_compensating());
    }

    #[test]
    fn 退避有上限() {
        assert_eq!(next_interval(0), 10);
        assert_eq!(next_interval(10), 20);
        assert_eq!(next_interval(200), 300);
        assert_eq!(next_interval(300), 300);
    }
}
