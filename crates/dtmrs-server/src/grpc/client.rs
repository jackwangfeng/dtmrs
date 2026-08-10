//! TC 去调业务方的 gRPC 分支。
//!
//! # 为什么不需要业务方的 proto
//!
//! 这是这一层唯一的技术难点。TC 要能调**任意**业务服务的**任意**方法，
//! 但编译期根本不知道对方的 proto 长什么样。
//!
//! 解法是绕开 protobuf 的类型系统：用一个只会搬字节的 [`BytesCodec`] 替换
//! tonic 默认的 prost codec，请求体发裸字节、响应体收裸字节。gRPC 的方法路径
//! （`/包.服务/方法`）本来就是运行期的字符串，所以整条调用链都不需要类型信息。
//!
//! 换来的好处很实在：**业务方不用为 dtmrs 改接口**，已有的 gRPC 服务直接
//! 就能当分支用。DTM 也是这个路子。
//!
//! # 请求体发什么
//!
//! 空字节。空的 protobuf 消息对**任何** message 类型都是合法的（所有字段取默认值），
//! 所以不管对方方法的入参声明成什么都能解开。
//!
//! 分支的身份（gid / branch_id / op / trans_type）走 metadata，不走请求体 ——
//! 这正是屏障需要的全部信息。跟 HTTP 那边把它们放 query 参数是一回事。
//!
//! （每步独立的业务 payload 是后续版本的事，HTTP 那边目前也统一发 `{}`。）
//!
//! # 结果判定
//!
//! 只看 gRPC 状态码，映射见 [`dtmrs_core::BranchResult::from_grpc`]。
//! **连不上、超时、`UNAVAILABLE` 一律是「结果未知」而不是失败** ——
//! 跟 HTTP 侧「超时不等于失败」是同一条命门。

// 走 prost 的 re-export，不额外引一个 bytes 依赖
use dtmrs_core::BranchResult;
use prost::bytes::{Buf, BufMut};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};
use tonic::Status;
use tracing::{info, warn};

use super::{MD_BRANCH_ID, MD_GID, MD_OP, MD_TRANS_TYPE};
use crate::registry::GrpcTarget;

/// 只搬字节的 codec —— 让 tonic 在不知道消息类型的前提下完成一次 unary 调用。
#[derive(Debug, Default, Clone, Copy)]
pub struct BytesCodec;

impl Codec for BytesCodec {
    type Encode = Vec<u8>;
    type Decode = Vec<u8>;
    type Encoder = BytesCodec;
    type Decoder = BytesCodec;

    fn encoder(&mut self) -> Self::Encoder {
        *self
    }
    fn decoder(&mut self) -> Self::Decoder {
        *self
    }
}

impl Encoder for BytesCodec {
    type Item = Vec<u8>;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put_slice(&item);
        Ok(())
    }
}

impl Decoder for BytesCodec {
    type Item = Vec<u8>;
    type Error = Status;

    /// tonic 保证 `src` 里正好是一条完整消息，不用自己拆帧
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let mut out = vec![0u8; src.remaining()];
        src.copy_to_slice(&mut out);
        Ok(Some(out))
    }
}

/// gRPC 分支调用器，带 channel 缓存。
///
/// 用 `connect_lazy` 而不是 `connect`：连接在首次真正发请求时才建立，
/// 断了之后 tonic 自己重连。所以缓存里的 channel **不会因为对方重启而变成死的**，
/// 不需要额外的健康检查和淘汰逻辑。
#[derive(Clone)]
pub struct GrpcCaller {
    channels: Arc<Mutex<HashMap<String, Channel>>>,
    timeout: Duration,
}

impl GrpcCaller {
    pub fn new(timeout: Duration) -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            timeout,
        }
    }

    fn channel(&self, endpoint: &str) -> Result<Channel, String> {
        // 先查缓存。锁里不做 await，所以用 std 的 Mutex 就够
        if let Some(c) = self.channels.lock().unwrap().get(endpoint) {
            return Ok(c.clone());
        }
        let ch = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| format!("gRPC 地址不合法: {e}"))?
            .timeout(self.timeout)
            .connect_timeout(self.timeout)
            .connect_lazy();
        self.channels
            .lock()
            .unwrap()
            .insert(endpoint.to_string(), ch.clone());
        Ok(ch)
    }

    /// 调一个 gRPC 分支。任何失败都不会返回 [`BranchResult::Failure`] ——
    /// 只有对方**明确**返回 `ABORTED` 才算业务要求回滚。
    pub async fn call(
        &self,
        target: &GrpcTarget,
        gid: &str,
        trans_type: &str,
        branch_id: &str,
        op: &str,
    ) -> BranchResult {
        let channel = match self.channel(&target.endpoint) {
            Ok(c) => c,
            Err(e) => {
                // 地址都拼不出来，是配置错误。但仍然按「未知」处理：
                // 判失败会触发回滚，而这其实是部署问题，改对了重试才对
                warn!(gid, branch = branch_id, endpoint = %target.endpoint, error = %e,
                      "gRPC 分支地址不合法，按结果未知处理（会重试，不回滚）");
                return BranchResult::Unknown;
            }
        };

        let path = match PathAndQuery::try_from(target.path.clone()) {
            Ok(p) => p,
            Err(e) => {
                warn!(gid, branch = branch_id, path = %target.path, error = %e,
                      "gRPC 方法路径不合法，按结果未知处理");
                return BranchResult::Unknown;
            }
        };

        let mut grpc = tonic::client::Grpc::new(channel);
        if let Err(e) = grpc.ready().await {
            warn!(gid, branch = branch_id, error = %e, "gRPC 分支不可达，结果未知");
            return BranchResult::Unknown;
        }

        // 空消息体：对任何 message 类型都合法。分支身份走 metadata
        let mut req = tonic::Request::new(Vec::<u8>::new());
        for (k, v) in [
            (MD_GID, gid),
            (MD_TRANS_TYPE, trans_type),
            (MD_BRANCH_ID, branch_id),
            (MD_OP, op),
        ] {
            match v.parse() {
                Ok(val) => {
                    req.metadata_mut().insert(k, val);
                }
                Err(_) => {
                    // gid 里有非 ASCII 之类。这些值是我们自己生成/客户端给的，
                    // 塞不进 header 就没法让业务方做幂等 —— 宁可不调
                    warn!(
                        gid,
                        branch = branch_id,
                        key = k,
                        "metadata 值不合法（非 ASCII？），无法调用 gRPC 分支"
                    );
                    return BranchResult::Unknown;
                }
            }
        }

        match grpc
            .unary::<Vec<u8>, Vec<u8>, BytesCodec>(req, path, BytesCodec)
            .await
        {
            Ok(_) => {
                info!(gid, branch = branch_id, op, "gRPC 分支返回 OK");
                BranchResult::Success
            }
            Err(status) => {
                let r = BranchResult::from_grpc(status.code() as i32);
                info!(gid, branch = branch_id, op, code = ?status.code(), result = ?r,
                      "gRPC 分支返回");
                r
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 地址不合法时是未知而不是失败() {
        // 判失败会触发回滚，而地址写错是部署问题
        let c = GrpcCaller::new(Duration::from_secs(1));
        assert!(c.channel("这不是个地址").is_err());
    }

    #[tokio::test]
    async fn 连不上的分支不能触发回滚() {
        // 端口上没人听 —— 必须是 Unknown（重试），绝不能是 Failure（回滚）
        let c = GrpcCaller::new(Duration::from_millis(300));
        let t = GrpcTarget {
            endpoint: "http://127.0.0.1:1".into(),
            path: "/a.B/C".into(),
        };
        let r = c.call(&t, "g1", "saga", "01", "action").await;
        assert_eq!(r, BranchResult::Unknown, "连不上必须是未知，不能是失败");
    }

    /// `connect_lazy` 内部要拿 tokio 的 executor，**必须在运行时里调** ——
    /// 普通 `#[test]` 会直接 panic。生产路径上分支调用本来就在运行时里，
    /// 不受影响
    #[tokio::test]
    async fn channel会被缓存复用() {
        let c = GrpcCaller::new(Duration::from_secs(1));
        let a = c.channel("http://127.0.0.1:9").unwrap();
        let b = c.channel("http://127.0.0.1:9").unwrap();
        // 缓存命中时表里只该有一条
        assert_eq!(c.channels.lock().unwrap().len(), 1);
        drop((a, b));
    }
}
