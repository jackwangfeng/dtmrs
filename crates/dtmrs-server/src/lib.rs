//! TC 的可复用部分。做成 lib 是为了让集成测试能直接驱动推进器 ——
//! bin crate 是不能被 tests/ import 的。
//!
//! **两个协议层都放在这儿**（`http` 和 `grpc`），理由同上：它们早先一个在
//! 这里、一个在 bin crate 的 main.rs 里，结果 gRPC 层有 86% 覆盖率而 HTTP 层
//! 是 0% —— 而「两边不许漂移」恰恰是这个项目最要紧的结构约束之一。
//! 现在两边都能被 tests/ 拿到，可以用同一组用例做等价性测试。

pub mod api;
pub mod driver;
pub mod embedded;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod http;
pub mod registry;
pub mod workflow;

use dtmrs_core::{BranchOp, BranchStatus, GlobalStatus, SagaStep, TransType};
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
        rollback_reason: String::new(),
        query_prepared: String::new(),
        create_time: 0,
        finish_time: None,
    }
}

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

/// TCC：`prepare` 只建全局事务，**分支是客户端在 try 阶段动态登记的**
/// （`Store::register_branch`）。所以这里不产生任何分支行。
pub fn tcc_rows(gid: &str) -> GlobalRow {
    global(gid, TransType::Tcc, GlobalStatus::Prepared, "[]".into())
}
