//! TC 的可复用部分。做成 lib 是为了让集成测试能直接驱动推进器 ——
//! bin crate 是不能被 tests/ import 的。

pub mod driver;
pub mod embedded;
pub mod registry;

use dtmrs_core::{BranchOp, BranchStatus, GlobalStatus, SagaStep, TransType};
use dtmrs_store::{BranchRow, GlobalRow};

/// 把一组 SAGA 步骤展开成"1 个全局事务 + 2N 个分支"。
///
/// HTTP `submit` 和集成测试都走这里，避免两处构造逻辑漂移 ——
/// 分支号规则错位是那种测试全绿、线上补偿补错对象的 bug。
pub fn saga_rows(gid: &str, steps: &[SagaStep]) -> (GlobalRow, Vec<BranchRow>) {
    let payload = serde_json::to_string(steps).unwrap_or_else(|_| "[]".into());
    let g = GlobalRow {
        gid: gid.to_string(),
        trans_type: TransType::Saga,
        status: GlobalStatus::Submitted,
        payload,
        next_cron_time: dtmrs_store::now(),
        next_cron_interval: 0,
        owner: String::new(),
        rollback_reason: String::new(),
        create_time: 0,
        finish_time: None,
    };
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
                payload: String::new(),
                status: BranchStatus::Prepared,
            });
        }
    }
    (g, branches)
}
