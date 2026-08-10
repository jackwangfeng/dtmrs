//! 类型与状态机 —— 纯逻辑，不碰 I/O。
//!
//! 分布式事务的 bug 绝大多数在状态迁移上，所以把这层从存储和网络里隔离出来，
//! 可以纯单元测试覆盖。

pub mod dialect;

pub use dialect::Backend;

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

    /// 从 gRPC 状态码判定。取值是 gRPC 规范里的标准编号，跟 DTM 的
    /// `dtmgrpc` 对齐，这样两边的业务服务可以互换。
    ///
    /// # 这个映射为什么是这几个码
    ///
    /// HTTP 那边靠 409/425 表达「明确失败」和「还在处理」，gRPC 没有这两个码，
    /// 得从 16 个标准码里各挑一个**不会被基础设施误用**的：
    ///
    /// | gRPC 码 | 语义 | 对应 HTTP |
    /// |---|---|---|
    /// | `OK`(0) | 成功 | 200 |
    /// | `ABORTED`(10) | 业务**明确**要求回滚 | 409 |
    /// | `FAILED_PRECONDITION`(9) | 还在处理，别当失败 | 425 |
    /// | 其它全部 | 结果**未知**，重试 | 5xx / 超时 |
    ///
    /// 关键在最后一行。`UNAVAILABLE`(14)、`DEADLINE_EXCEEDED`(4)、`INTERNAL`(13)
    /// 这些**都算未知**而不是失败 —— 它们恰恰是网络抖动和超时会产生的码，
    /// 而超时的时候对方可能已经成功了。这跟 HTTP 侧「超时不等于失败」是同一条命门。
    ///
    /// 特别注意 `CANCELLED`(1) 和 `DEADLINE_EXCEEDED`(4)：调用方自己取消/超时
    /// 产生的码，绝不能当成业务失败 —— 那是**我们这边**放弃了，不是对方拒绝了。
    pub fn from_grpc(code: i32) -> Self {
        match code {
            GRPC_OK => Self::Success,
            GRPC_ABORTED => Self::Failure,
            GRPC_FAILED_PRECONDITION => Self::Ongoing,
            // 包括 CANCELLED / DEADLINE_EXCEEDED / UNAVAILABLE / INTERNAL / …
            // 一律按未知处理：只重试，不回滚
            _ => Self::Unknown,
        }
    }
}

/// gRPC 标准状态码。只列用得上的三个，其余一律走 `_ => Unknown`。
pub const GRPC_OK: i32 = 0;
/// 业务明确要求回滚。gRPC 侧的 409
pub const GRPC_ABORTED: i32 = 10;
/// 还在处理中。gRPC 侧的 425
pub const GRPC_FAILED_PRECONDITION: i32 = 9;

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
    /// XA 的二阶段提交
    Commit,
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
            Self::Commit => "commit",
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
            "commit" => Some(Self::Commit),
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
    fn grpc只有aborted才算失败() {
        assert_eq!(BranchResult::from_grpc(0), BranchResult::Success);
        // 业务明确要求回滚 —— gRPC 侧唯一能触发补偿的码
        assert_eq!(BranchResult::from_grpc(10), BranchResult::Failure);
        assert_eq!(BranchResult::from_grpc(9), BranchResult::Ongoing);

        // 穷举 gRPC 全部 16 个标准码：除了这三个，一律是 Unknown。
        // 这条错了就会数据不一致 —— UNAVAILABLE / DEADLINE_EXCEEDED 正是
        // 网络抖动和超时产生的码，当成失败去回滚，对方可能其实已经成功了。
        for code in 0..=15 {
            let want = match code {
                0 => BranchResult::Success,
                10 => BranchResult::Failure,
                9 => BranchResult::Ongoing,
                _ => BranchResult::Unknown,
            };
            assert_eq!(BranchResult::from_grpc(code), want, "gRPC 码 {code} 判错了");
        }
        // 几个最容易写错的，单独钉一遍
        assert_eq!(BranchResult::from_grpc(1), BranchResult::Unknown, "CANCELLED 是我们自己放弃，不是对方拒绝");
        assert_eq!(BranchResult::from_grpc(4), BranchResult::Unknown, "DEADLINE_EXCEEDED 绝不能当失败");
        assert_eq!(BranchResult::from_grpc(14), BranchResult::Unknown, "UNAVAILABLE 绝不能当失败");
        // 不认识的码（未来扩展 / 对方乱返）也必须是 Unknown
        assert_eq!(BranchResult::from_grpc(99), BranchResult::Unknown);
        assert_eq!(BranchResult::from_grpc(-1), BranchResult::Unknown);
    }

    #[test]
    fn grpc与http的判定语义一致() {
        // 同一个业务意图，两种协议必须得到同一个结论 —— 否则同一个服务
        // 换协议接入就会有不同的回滚行为
        for (http, grpc) in [(200u16, 0i32), (409, 10), (425, 9), (500, 13), (503, 14)] {
            assert_eq!(
                BranchResult::from_http(http, ""),
                BranchResult::from_grpc(grpc),
                "HTTP {http} 与 gRPC {grpc} 应当判定一致"
            );
        }
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

/// TCC 推进决策。
///
/// # 跟 SAGA 的关键差别
///
/// SAGA 的正向分支（action）返回 FAILURE 会触发逆序补偿。
/// **TCC 的 confirm 返回 FAILURE 绝不能触发 cancel** —— try 阶段资源已经预留成功、
/// 全局也已经决定提交了，这时候去 cancel 会把已确认的事务撤掉，造成不一致。
/// confirm 失败的唯一正确处理是**无限重试 + 报警等人介入**。
///
/// 所以这个函数在 Submitted 阶段永远不会返回 `Finish(Aborting)`。
///
/// Try 阶段不在这里 —— TCC 的 try 是**客户端自己驱动**的（这也是 TCC 要
/// `registerBranch` 接口的原因），TC 只负责 confirm/cancel。
pub fn tcc_advance(
    status: GlobalStatus,
    confirms: &[BranchStatus],
    cancels: &[BranchStatus],
) -> Advance {
    debug_assert_eq!(confirms.len(), cancels.len());
    match status {
        // 客户端还在跑 try，TC 不插手
        GlobalStatus::Prepared => Advance::Wait,
        GlobalStatus::Submitted => {
            for (i, st) in confirms.iter().enumerate() {
                match st {
                    BranchStatus::Succeed => continue,
                    // Failed 也要继续重试 —— 见上面注释，绝不转 aborting
                    BranchStatus::Prepared | BranchStatus::Failed => {
                        return Advance::Call { index: i, op: BranchOp::Confirm }
                    }
                }
            }
            Advance::Finish(GlobalStatus::Succeed)
        }
        GlobalStatus::Aborting => {
            // 逆序 cancel。全部 cancel，空回滚由屏障负责
            for i in (0..cancels.len()).rev() {
                if cancels[i] != BranchStatus::Succeed {
                    return Advance::Call { index: i, op: BranchOp::Cancel };
                }
            }
            Advance::Finish(GlobalStatus::Failed)
        }
        s => Advance::Finish(s),
    }
}

/// 二阶段消息推进决策。
///
/// # 这个模式解决什么
///
/// "本地事务 + 可靠消息" —— 取代 RocketMQ 那类事务消息，不需要 MQ。
/// 流程：`prepare` 落库 → 业务提交本地事务 → `submit`。
/// 如果进程在两者之间崩了，TC 会回查业务方（`query_prepared`）问这单到底成没成。
///
/// # 没有补偿
///
/// msg 只保证"最终一定送达"，分支必须最终成功（幂等 + 无限重试）。
/// 所以正向分支返回 FAILURE **不触发补偿**（压根没有补偿分支），
/// 只能重试。真要放弃只能靠 `query_prepared` 回答 FAILURE 让整单作废。
pub fn msg_advance(status: GlobalStatus, actions: &[BranchStatus]) -> Advance {
    match status {
        // 等 cron 去回查 query_prepared
        GlobalStatus::Prepared => Advance::Wait,
        GlobalStatus::Submitted => {
            for (i, st) in actions.iter().enumerate() {
                if *st != BranchStatus::Succeed {
                    return Advance::Call { index: i, op: BranchOp::Action };
                }
            }
            Advance::Finish(GlobalStatus::Succeed)
        }
        // 回查得到 FAILURE：整单作废，没有补偿可做
        GlobalStatus::Aborting => Advance::Finish(GlobalStatus::Failed),
        s => Advance::Finish(s),
    }
}

#[cfg(test)]
mod tcc_msg_tests {
    use super::*;
    use BranchStatus::{Failed, Prepared, Succeed};

    #[test]
    fn tcc的try阶段tc不插手() {
        assert_eq!(
            tcc_advance(GlobalStatus::Prepared, &[Prepared], &[Prepared]),
            Advance::Wait
        );
    }

    #[test]
    fn tcc按序confirm() {
        let c = [Prepared, Prepared];
        let x = [Prepared, Prepared];
        assert_eq!(
            tcc_advance(GlobalStatus::Submitted, &c, &x),
            Advance::Call { index: 0, op: BranchOp::Confirm }
        );
        assert_eq!(
            tcc_advance(GlobalStatus::Submitted, &[Succeed, Prepared], &x),
            Advance::Call { index: 1, op: BranchOp::Confirm }
        );
        assert_eq!(
            tcc_advance(GlobalStatus::Submitted, &[Succeed, Succeed], &x),
            Advance::Finish(GlobalStatus::Succeed)
        );
    }

    #[test]
    fn confirm失败绝不能触发cancel() {
        // 这是 TCC 最容易写错的地方：try 已成功、已决定提交，
        // 这时候 cancel 会把已确认的事务撤掉 —— 必须重试而不是回滚
        let c = [Succeed, Failed];
        let x = [Prepared, Prepared];
        assert_eq!(
            tcc_advance(GlobalStatus::Submitted, &c, &x),
            Advance::Call { index: 1, op: BranchOp::Confirm },
            "confirm 失败要继续重试 confirm，不能转 cancel"
        );
        // 穷举：Submitted 阶段永远不会返回 Aborting
        for a in [Prepared, Succeed, Failed] {
            for b in [Prepared, Succeed, Failed] {
                let r = tcc_advance(GlobalStatus::Submitted, &[a, b], &x);
                assert_ne!(r, Advance::Finish(GlobalStatus::Aborting));
                assert_ne!(r, Advance::Finish(GlobalStatus::Failed));
            }
        }
    }

    #[test]
    fn tcc逆序cancel() {
        let c = [Prepared, Prepared];
        assert_eq!(
            tcc_advance(GlobalStatus::Aborting, &c, &[Prepared, Prepared]),
            Advance::Call { index: 1, op: BranchOp::Cancel }
        );
        assert_eq!(
            tcc_advance(GlobalStatus::Aborting, &c, &[Prepared, Succeed]),
            Advance::Call { index: 0, op: BranchOp::Cancel }
        );
        assert_eq!(
            tcc_advance(GlobalStatus::Aborting, &c, &[Succeed, Succeed]),
            Advance::Finish(GlobalStatus::Failed)
        );
        // cancel 失败也要重试，不能就这么算了
        assert_eq!(
            tcc_advance(GlobalStatus::Aborting, &c, &[Succeed, Failed]),
            Advance::Call { index: 1, op: BranchOp::Cancel }
        );
    }

    #[test]
    fn msg等回查而不是自己推() {
        assert_eq!(msg_advance(GlobalStatus::Prepared, &[Prepared]), Advance::Wait);
    }

    #[test]
    fn msg只往前不补偿() {
        assert_eq!(
            msg_advance(GlobalStatus::Submitted, &[Prepared, Prepared]),
            Advance::Call { index: 0, op: BranchOp::Action }
        );
        // 分支失败也只能重试 —— msg 没有补偿分支
        assert_eq!(
            msg_advance(GlobalStatus::Submitted, &[Failed]),
            Advance::Call { index: 0, op: BranchOp::Action }
        );
        assert_eq!(
            msg_advance(GlobalStatus::Submitted, &[Succeed, Succeed]),
            Advance::Finish(GlobalStatus::Succeed)
        );
        // 回查得到 FAILURE → 整单作废，无补偿可做
        assert_eq!(
            msg_advance(GlobalStatus::Aborting, &[Succeed]),
            Advance::Finish(GlobalStatus::Failed)
        );
    }
}

/// XA 推进决策。
///
/// # 跟 TCC 同一条铁律
///
/// 分支一旦 `PREPARE TRANSACTION` 成功、全局又决定了提交，**commit 失败绝不能
/// 转成 rollback** —— 别的分支可能已经 COMMIT PREPARED 了，这时候回滚就是
/// 一半提交一半回滚。只能无限重试 + 报警。
///
/// 所以跟 `tcc_advance` 一样，Submitted 阶段永远不返回 `Finish(Aborting)`。
///
/// # XA 独有的危险
///
/// 已 prepare 未解决的事务会**一直持有锁**，在 Postgres 里还会阻塞 VACUUM
/// 导致事务 ID 回卷风险。所以 XA 的 commit/rollback 必须最终送达 ——
/// 这比 SAGA/TCC 的"补偿没跑成"严重得多。运维上要监控 `pg_prepared_xacts`。
pub fn xa_advance(
    status: GlobalStatus,
    commits: &[BranchStatus],
    rollbacks: &[BranchStatus],
) -> Advance {
    debug_assert_eq!(commits.len(), rollbacks.len());
    match status {
        // 客户端还在各分支上跑业务 SQL + PREPARE，TC 不插手
        GlobalStatus::Prepared => Advance::Wait,
        GlobalStatus::Submitted => {
            for (i, st) in commits.iter().enumerate() {
                if *st != BranchStatus::Succeed {
                    return Advance::Call { index: i, op: BranchOp::Commit };
                }
            }
            Advance::Finish(GlobalStatus::Succeed)
        }
        GlobalStatus::Aborting => {
            for i in (0..rollbacks.len()).rev() {
                if rollbacks[i] != BranchStatus::Succeed {
                    return Advance::Call { index: i, op: BranchOp::Rollback };
                }
            }
            Advance::Finish(GlobalStatus::Failed)
        }
        s => Advance::Finish(s),
    }
}

#[cfg(test)]
mod xa_tests {
    use super::*;
    use BranchStatus::{Failed, Prepared, Succeed};

    #[test]
    fn xa的prepare阶段tc不插手() {
        // 各分支的业务 SQL + PREPARE TRANSACTION 都是客户端自己做的
        assert_eq!(
            xa_advance(GlobalStatus::Prepared, &[Prepared], &[Prepared]),
            Advance::Wait
        );
    }

    #[test]
    fn xa按序commit() {
        let r = [Prepared, Prepared];
        assert_eq!(
            xa_advance(GlobalStatus::Submitted, &[Prepared, Prepared], &r),
            Advance::Call { index: 0, op: BranchOp::Commit }
        );
        assert_eq!(
            xa_advance(GlobalStatus::Submitted, &[Succeed, Prepared], &r),
            Advance::Call { index: 1, op: BranchOp::Commit }
        );
        assert_eq!(
            xa_advance(GlobalStatus::Submitted, &[Succeed, Succeed], &r),
            Advance::Finish(GlobalStatus::Succeed)
        );
    }

    #[test]
    fn commit失败绝不能转rollback() {
        // 别的分支可能已经 COMMIT PREPARED 了，这时候回滚就是一半提交一半回滚
        let r = [Prepared, Prepared];
        assert_eq!(
            xa_advance(GlobalStatus::Submitted, &[Succeed, Failed], &r),
            Advance::Call { index: 1, op: BranchOp::Commit },
            "commit 失败要继续重试 commit"
        );
        // 穷举：Submitted 阶段永远不会走向回滚或失败
        for a in [Prepared, Succeed, Failed] {
            for b in [Prepared, Succeed, Failed] {
                let got = xa_advance(GlobalStatus::Submitted, &[a, b], &r);
                assert_ne!(got, Advance::Finish(GlobalStatus::Aborting));
                assert_ne!(got, Advance::Finish(GlobalStatus::Failed));
            }
        }
    }

    #[test]
    fn xa逆序rollback且失败也要重试() {
        let c = [Prepared, Prepared];
        assert_eq!(
            xa_advance(GlobalStatus::Aborting, &c, &[Prepared, Prepared]),
            Advance::Call { index: 1, op: BranchOp::Rollback }
        );
        assert_eq!(
            xa_advance(GlobalStatus::Aborting, &c, &[Succeed, Succeed]),
            Advance::Finish(GlobalStatus::Failed)
        );
        // rollback 失败也不能就这么算了 —— 那会留下永久持锁的 prepared 事务
        assert_eq!(
            xa_advance(GlobalStatus::Aborting, &c, &[Succeed, Failed]),
            Advance::Call { index: 1, op: BranchOp::Rollback }
        );
    }

    #[test]
    fn commit操作没有反向映射() {
        // XA 的 commit 不是补偿类操作，屏障不该给它做空回滚判定
        assert_eq!(BranchOp::Commit.origin_op(), None);
        assert!(!BranchOp::Commit.is_compensating());
        assert_eq!(BranchOp::parse("commit"), Some(BranchOp::Commit));
        assert_eq!(BranchOp::Commit.as_str(), "commit");
    }
}
