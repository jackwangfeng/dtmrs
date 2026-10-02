//! TC 的可复用部分。做成 lib 是为了让集成测试能直接驱动推进器 ——
//! bin crate 是不能被 tests/ import 的。
//!
//! **两个协议层都放在这儿**（`http` 和 `grpc`），理由同上：它们早先一个在
//! 这里、一个在 bin crate 的 main.rs 里，结果 gRPC 层有 86% 覆盖率而 HTTP 层
//! 是 0% —— 而「两边不许漂移」恰恰是这个项目最要紧的结构约束之一。
//! 现在两边都能被 tests/ 拿到，可以用同一组用例做等价性测试。

pub mod alert;
pub mod api;
pub mod auth;
pub mod driver;
pub mod embedded;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod http;
pub mod registry;
pub mod workflow;

use dtmrs_core::{BranchOp, BranchStatus, GlobalStatus, MsgBranch, SagaStep, TransType};
use dtmrs_store::{BranchRow, GlobalRow};

/// 各模式共用的全局事务骨架
fn global(gid: &str, tt: TransType, status: GlobalStatus, payload: String) -> GlobalRow {
    GlobalRow {
        gid: gid.to_string(),
        trans_type: tt,
        status,
        payload,
        next_cron_time: dtmrs_store::now(),
        next_cron_interval: 0,
        owner: String::new(),
        lease_until: 0,
        rollback_reason: String::new(),
        query_prepared: String::new(),
        create_time: 0,
        finish_time: None,
        timeout_to_fail: 0,
        retry_count: 0,
    }
}

/// TCC / XA 停在 prepared 的全局默认时限（秒）：`DTMRS_TIMEOUT_TO_FAIL`，
/// 没配用 [`dtmrs_core::DEFAULT_TIMEOUT_TO_FAIL`]（35，同 DTM），0 表示不超时。
/// 进程内只读一次
pub fn timeout_to_fail_default() -> i64 {
    static V: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("DTMRS_TIMEOUT_TO_FAIL")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .unwrap_or(dtmrs_core::DEFAULT_TIMEOUT_TO_FAIL)
    })
}

/// 不超时的事务排到哪：远到不会到期，又留足余量不会在加法里溢出
pub const NEVER: i64 = i64::MAX / 4;

/// 把一组 SAGA 步骤展开成"1 个全局事务 + 2N 个分支"。
///
/// HTTP `submit` 和集成测试都走这里，避免两处构造逻辑漂移 ——
/// 分支号规则错位是那种测试全绿、线上补偿补错对象的 bug。
pub fn saga_rows(gid: &str, steps: &[SagaStep]) -> (GlobalRow, Vec<BranchRow>) {
    let payload = serde_json::to_string(steps).unwrap_or_else(|_| "[]".into());
    let g = global(gid, TransType::Saga, GlobalStatus::Submitted, payload);
    let mut branches = Vec::with_capacity(steps.len() * 2);
    for (i, s) in steps.iter().enumerate() {
        let bid = driver::branch_id(i);
        for (op, url) in [
            (BranchOp::Action, &s.action),
            (BranchOp::Compensate, &s.compensate),
        ] {
            branches.push(BranchRow {
                gid: gid.to_string(),
                branch_id: bid.clone(),
                op,
                url: url.clone(),
                // 每步自己的业务数据。正向和补偿共用同一份 ——
                // 补偿需要知道当初做了什么才能撤销
                payload: s.payload.clone(),
                status: BranchStatus::Prepared,
            });
        }
    }
    (g, branches)
}

/// 二阶段消息：**只有正向分支，没有补偿**。
///
/// `grace_secs` 是回查前的宽限期：客户端 prepare 之后正常会很快 submit，
/// 立刻回查是白问一次。给几秒钟等它自己来。
pub fn msg_rows(
    gid: &str,
    actions: &[String],
    query_prepared: &str,
    grace_secs: i64,
) -> (GlobalRow, Vec<BranchRow>) {
    // 用 SagaStep 复用 payload 格式，compensate 留空（msg 没有补偿）
    let steps: Vec<SagaStep> = actions
        .iter()
        .map(|a| SagaStep {
            action: a.clone(),
            compensate: String::new(),
            payload: String::new(),
        })
        .collect();
    let payload = serde_json::to_string(&steps).unwrap_or_else(|_| "[]".into());
    let mut g = global(gid, TransType::Msg, GlobalStatus::Prepared, payload);
    g.query_prepared = query_prepared.to_string();
    g.next_cron_time = dtmrs_store::now() + grace_secs.max(0);
    let branches = actions
        .iter()
        .enumerate()
        .map(|(i, a)| BranchRow {
            gid: gid.to_string(),
            branch_id: driver::branch_id(i),
            op: BranchOp::Action,
            url: a.clone(),
            payload: String::new(),
            status: BranchStatus::Prepared,
        })
        .collect();
    (g, branches)
}

/// 二阶段消息，主题已经展开过（见 [`dtmrs_core::expand_msg_steps`]）。
///
/// 全局事务的 payload 存展开后的 [`MsgBranch`] 列表，推进器按它调分支 ——
/// **不再按下标算分支号**，扇出的分支号是 `01-01` 这种。
pub fn msg_rows_expanded(
    gid: &str,
    branches: &[MsgBranch],
    query_prepared: &str,
    grace_secs: i64,
) -> (GlobalRow, Vec<BranchRow>) {
    let payload = serde_json::to_string(branches).unwrap_or_else(|_| "[]".into());
    let mut g = global(gid, TransType::Msg, GlobalStatus::Prepared, payload);
    g.query_prepared = query_prepared.to_string();
    g.next_cron_time = dtmrs_store::now() + grace_secs.max(0);
    let rows = branches
        .iter()
        .map(|b| BranchRow {
            gid: gid.to_string(),
            branch_id: b.branch_id.clone(),
            op: BranchOp::Action,
            url: b.action.clone(),
            payload: b.payload.clone(),
            status: BranchStatus::Prepared,
        })
        .collect();
    (g, rows)
}

/// TCC：`prepare` 只建全局事务，**分支是客户端在 try 阶段动态登记的**
/// （`Store::register_branch`）。所以这里不产生任何分支行。
///
/// ⚠ 调度时间是「现在」，不是 prepared 时限 —— 这个函数也被当成通用模板用
/// （嵌入式的 workflow 拿它改成 submitted 直接开推）。真正的 TCC / XA prepare 走
/// [`tcc_rows_with_timeout`]。排早了也不出错：推进器捞到没到点的 prepared 会排回时限
pub fn tcc_rows(gid: &str) -> GlobalRow {
    global(gid, TransType::Tcc, GlobalStatus::Prepared, "[]".into())
}

/// TCC / XA prepare 用：带这笔事务自己的 prepared 时限（0 = 全局默认），
/// 调度时间直接排到时限那一刻 —— prepared 的 TCC / XA 要被推进器捞起来的唯一理由
/// 就是超时回滚，排早了只是白抢一次
pub fn tcc_rows_with_timeout(gid: &str, timeout_to_fail: i64) -> GlobalRow {
    let mut g = global(gid, TransType::Tcc, GlobalStatus::Prepared, "[]".into());
    g.timeout_to_fail = timeout_to_fail.max(0);
    g.next_cron_time = dtmrs_core::prepared_deadline(
        dtmrs_store::now(),
        g.timeout_to_fail,
        timeout_to_fail_default(),
    )
    .unwrap_or(NEVER);
    g
}
