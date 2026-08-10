//! # dtmrs
//!
//! Rust 写的分布式事务管理器，对标 [DTM](https://github.com/dtm-labs/dtm)。
//! 支持 **SAGA / TCC / 二阶段消息 / XA / workflow** 五种模式，
//! 存储可跑 sqlite / postgres / mysql，对外有 HTTP 和 gRPC 两套等价接口。
//!
//! 这个 crate 是**门面**：把各层重新导出到一处，省得挨个加依赖。
//! 想精确控制依赖的话也可以直接用底下的 `dtmrs-core` / `dtmrs-server` /
//! `dtmrs-barrier` / `dtmrs-xa`。
//!
//! ## 两种用法，别装错
//!
//! dtmrs 的两端要的东西完全不同，feature 就是按这个切的：
//!
//! | 你的角色 | 装什么 | 拿到什么 |
//! |---|---|---|
//! | **跑协调器**（TC） | `dtmrs`（默认） | [`Embedded`]、`dtmrs` 二进制 |
//! | **业务服务**（RM） | `dtmrs = { features = ["barrier"], default-features = false }` | [`barrier`] —— 幂等所必需 |
//! | 业务服务 + XA | 再加 `"xa"` | [`xa`] |
//!
//! 业务侧**只需要屏障**，不该把整个协调器和 axum/tonic 拖进去 ——
//! 所以 `barrier` 不在默认 feature 里，`default-features = false` 之后它非常轻。
//!
//! ## 嵌入式：不用单独部署服务
//!
//! 这是相对 DTM 的结构性差异 —— 协调器当库链进你自己的进程，
//! 分支可以直接是进程内的函数：
//!
//! ```no_run
//! use dtmrs::{BranchResult, Embedded};
//!
//! # async fn demo() -> anyhow::Result<()> {
//! let tc = Embedded::builder("sqlite:app.db")
//!     .handler("扣款",     |_ctx| async { BranchResult::Success })
//!     .handler("扣款撤销", |_ctx| async { BranchResult::Success })
//!     .start()
//!     .await?;
//!
//! tc.saga("order-1001")
//!     .step("local://扣款", "local://扣款撤销")
//!     .step("http://shipment/create", "http://shipment/cancel")  // 可跟远端混用
//!     .submit()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! 也可以当独立服务跑：`cargo install dtmrs` 之后
//! `DTMRS_DB=sqlite:dtmrs.db dtmrs`。
//!
//! ## 业务侧接入：屏障不是可选项
//!
//! 分支接口**一定会**被重复调用（TC 重试 + 崩溃恢复），所以必须幂等。
//! 把屏障记录和业务 SQL 放进**同一个本地事务**：
//!
//! ```ignore
//! use dtmrs::barrier::{BranchBarrier, Decision};
//! use dtmrs::Backend;
//!
//! let mut bb = BranchBarrier::new(Backend::from_url(&url), tt, gid, branch_id, op)?;
//! let mut tx = pool.begin().await?;
//! if bb.decide(&mut tx).await? == Decision::Execute {
//!     // 业务 SQL —— 必须在这个 tx 里
//! }
//! tx.commit().await?;   // 原子性的来源
//! ```
//!
//! ## 一条写错就会数据不一致的规矩
//!
//! **超时 ≠ 失败。** 5xx、连接超时、gRPC 的 `UNAVAILABLE` / `DEADLINE_EXCEEDED`
//! 都表示**结果未知** —— 对方可能已经成功了，这时候回滚就是不一致。
//! 只有 HTTP 409、gRPC `ABORTED`、或响应体里的 `FAILURE` 才触发补偿。
//! 见 [`BranchResult`]。

// ---- 状态机与类型：任何 feature 组合下都有 ----
pub use dtmrs_core::*;

/// 存储层：一套 SQL 跑 sqlite / postgres / mysql
#[cfg(feature = "server")]
pub mod store {
    pub use dtmrs_store::*;
}

/// 协调器（TC）本体：推进器、嵌入式门面、HTTP/gRPC 接口
#[cfg(feature = "server")]
pub mod server {
    pub use dtmrs_server::*;
}

/// 子事务屏障 —— 业务侧（RM）做幂等用，**接入 dtmrs 的必需品**
#[cfg(feature = "barrier")]
pub mod barrier {
    pub use dtmrs_barrier::*;
}

/// XA 两阶段提交的业务侧（RM）助手
#[cfg(feature = "xa")]
pub mod xa {
    pub use dtmrs_xa::*;
}

// ---- 最常用的东西提到顶层，省得记路径 ----

#[cfg(feature = "server")]
pub use dtmrs_server::embedded::{Embedded, EmbeddedBuilder};
#[cfg(feature = "server")]
pub use dtmrs_server::registry::BranchCtx;
#[cfg(feature = "server")]
pub use dtmrs_server::workflow::{WorkflowCtx, WorkflowError, WorkflowResult};
#[cfg(feature = "server")]
pub use dtmrs_store::Store;

#[cfg(feature = "barrier")]
pub use dtmrs_barrier::{BranchBarrier, Decision};

#[cfg(test)]
mod tests {
    #[test]
    fn 门面把核心类型透出来了() {
        // 这个 crate 存在的意义就是「加一个依赖就够用」，
        // 所以核心类型必须能从顶层直接拿到
        use crate::{BranchOp, BranchResult, GlobalStatus, TransType};
        assert_eq!(BranchResult::from_http(409, ""), BranchResult::Failure);
        assert_eq!(BranchResult::from_grpc(10), BranchResult::Failure);
        assert_eq!(TransType::parse("workflow"), Some(TransType::Workflow));
        assert!(GlobalStatus::Succeed.is_final());
        assert_eq!(BranchOp::Compensate.origin_op(), Some(BranchOp::Action));
    }

    #[cfg(feature = "server")]
    #[test]
    fn 开了server就能拿到嵌入式门面() {
        // 只是确认路径存在、类型能被引用
        fn _assert<T>() {}
        _assert::<crate::Embedded>();
        _assert::<crate::Store>();
    }
}
