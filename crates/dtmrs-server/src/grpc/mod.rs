//! gRPC 支持。两个方向都有：
//!
//! - [`client`]：TC **去调**业务方的 gRPC 分支（`grpc://` 前缀的分支地址）
//! - [`server`]：TC **对外提供**与 HTTP 对等的 gRPC API
//!
//! 两边共用 `dtmrs-core` 的状态码映射，所以同一个业务服务换协议接入不会
//! 有不同的回滚行为。

pub mod client;
pub mod server;

/// proto 生成的类型。`dtmrs.v1` 是 TC 自己的 API
pub mod pb {
    tonic::include_proto!("dtmrs.v1");
}

/// 业务服务的参考定义，测试和示例用。业务方**不需要**实现这个 ——
/// 任何 gRPC 方法都能直接当分支（见 [`client`] 的说明）
pub mod busi_pb {
    tonic::include_proto!("busi.v1");
}

/// 分支调用时透传给业务方的 metadata 键，跟 DTM 对齐。
///
/// HTTP 那边这四个是 query 参数，gRPC 这边是 metadata —— 业务方拿到的
/// 信息完全一样，屏障库两边都能用。
pub const MD_GID: &str = "dtm-gid";
pub const MD_TRANS_TYPE: &str = "dtm-trans_type";
pub const MD_BRANCH_ID: &str = "dtm-branch_id";
pub const MD_OP: &str = "dtm-op";
