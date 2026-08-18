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
    /// 额外信任的 CA（PEM 内容，不是路径）。内网自签证书用。
    ///
    /// 做成字段而不是每次去读环境变量，是为了能测：改进程级 env 在并行测试里
    /// 会互相打架，而 TLS 这种东西不实际连一次根本不知道配没配对。
    extra_ca: Option<Arc<Vec<u8>>>,
}

/// 检查这份 PEM 里到底有没有证书，没有就**大声拒掉**。
///
/// ⚠ 这个函数存在的理由是一次实测：给 tonic 传一份垃圾 PEM，
/// `tls_config()` **不报任何错** —— rustls 的 `add_parsable_certificates`
/// 会静默跳过认不出的条目。于是运维把 `DTMRS_GRPC_CA` 指错文件（指到了私钥、
/// 指到了不存在的软链、文件被截断）时没有任何提示，只会在握手时收到一个
/// 跟根因毫无关系的错误。
///
/// 这里只做最轻的判断（有没有 BEGIN CERTIFICATE 块），不引入 PEM 解析器 ——
/// 要抓的就是「指错文件」这类现实错误，不是要做完整校验。
fn check_ca_pem(pem: Vec<u8>, from: &str) -> Option<Arc<Vec<u8>>> {
    const MARK: &[u8] = b"-----BEGIN CERTIFICATE-----";
    let has_cert = pem.windows(MARK.len()).any(|w| w == MARK);
    if !has_cert {
        warn!(
            source = from,
            bytes = pem.len(),
            "额外 CA 里没有 BEGIN CERTIFICATE 块，已忽略 —— \
             传了私钥或指错文件？注意 tonic 对这种输入不会报错，只会静默不生效"
        );
        return None;
    }
    Some(Arc::new(pem))
}

impl GrpcCaller {
    /// `DTMRS_GRPC_CA` 指向一个 PEM 文件时，把它加进信任列表。
    ///
    /// 读不到就**只是不加**，不 panic —— 推进器是常驻的，
    /// 因为一个可选配置起不来比连不上更糟。
    pub fn new(timeout: Duration) -> Self {
        let extra_ca = std::env::var("DTMRS_GRPC_CA").ok().and_then(|p| {
            match std::fs::read(&p) {
                Ok(pem) => check_ca_pem(pem, &p),
                Err(e) => {
                    warn!(path = %p, error = %e, "DTMRS_GRPC_CA 读不到，忽略该配置");
                    None
                }
            }
        });
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            timeout,
            extra_ca,
        }
    }

    /// 直接指定额外信任的 CA（PEM 内容）。测试和嵌入式宿主用，绕开环境变量。
    pub fn with_ca_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.extra_ca = check_ca_pem(pem.into(), "<直接传入>");
        self
    }

    fn channel(&self, target: &GrpcTarget) -> Result<Channel, String> {
        let endpoint = target.endpoint.as_str();
        // 先查缓存。锁里不做 await，所以用 std 的 Mutex 就够
        if let Some(c) = self.channels.lock().unwrap().get(endpoint) {
            return Ok(c.clone());
        }
        let mut ep = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| format!("gRPC 地址不合法: {e}"))?
            .timeout(self.timeout)
            .connect_timeout(self.timeout);

        if target.tls {
            // with_enabled_roots()：把编译进来的根证书集合都启用
            // （native = 系统信任库，走内网自签 CA；webpki = 内置 Mozilla 根）。
            // 域名不用手写，tonic 从 uri 的 host 里取，跟证书的 SAN 校验。
            let mut tls = tonic::transport::ClientTlsConfig::new().with_enabled_roots();
            if let Some(pem) = &self.extra_ca {
                tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(pem.as_slice()));
            }
            ep = ep
                .tls_config(tls)
                .map_err(|e| format!("gRPC TLS 配置不可用: {e}"))?;
        }

        let ch = ep.connect_lazy();
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
        let channel = match self.channel(target) {
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

    fn target(endpoint: &str, tls: bool) -> GrpcTarget {
        GrpcTarget {
            endpoint: endpoint.into(),
            path: "/a.B/C".into(),
            tls,
        }
    }

    #[test]
    fn 地址不合法时是未知而不是失败() {
        // 判失败会触发回滚，而地址写错是部署问题
        let c = GrpcCaller::new(Duration::from_secs(1));
        assert!(c.channel(&target("这不是个地址", false)).is_err());
    }

    #[tokio::test]
    async fn 连不上的分支不能触发回滚() {
        // 端口上没人听 —— 必须是 Unknown（重试），绝不能是 Failure（回滚）
        let c = GrpcCaller::new(Duration::from_millis(300));
        let r = c
            .call(&target("http://127.0.0.1:1", false), "g1", "saga", "01", "action")
            .await;
        assert_eq!(r, BranchResult::Unknown, "连不上必须是未知，不能是失败");
    }

    /// TLS 握手失败（对面根本不是 TLS 服务）也必须是 Unknown。
    ///
    /// ⚠ 这条容易想当然：证书错误感觉像「明确的拒绝」，但它跟业务无关 ——
    /// 判成 Failure 会因为一个配置问题去回滚一笔可能已经成功的事务。
    #[tokio::test]
    async fn tls握手失败也只能是未知() {
        let c = GrpcCaller::new(Duration::from_millis(300));
        let r = c
            .call(&target("https://127.0.0.1:1", true), "g1", "saga", "01", "action")
            .await;
        assert_eq!(r, BranchResult::Unknown, "TLS 失败是部署问题，不是业务拒绝");
    }

    #[tokio::test]
    async fn tls端点能建出channel() {
        // connect_lazy 不会真握手，这里验的是 tls_config 本身能装上 ——
        // 根证书集合没编进来的话这一步就会报错
        let c = GrpcCaller::new(Duration::from_secs(1));
        assert!(
            c.channel(&target("https://busi.internal:9000", true)).is_ok(),
            "TLS 配置装不上，多半是 tonic 的 tls feature 没开"
        );
    }

    /// ⚠ 实测发现的坑：给 tonic 传垃圾 PEM，`tls_config()` **不报错** ——
    /// rustls 会静默跳过认不出的条目。所以指错文件时完全没有提示，
    /// 只会在握手阶段收到一个跟根因无关的错误。
    ///
    /// 现在在装载时就挡掉并打警告。这里断言的是「没被当成 CA 收下」，
    /// 而不是「channel 建不出来」—— 建得出来是对的，只是不该多信任什么。
    #[tokio::test]
    async fn 垃圾ca要被挡掉而不是静默收下() {
        for junk in [
            &b"not a pem"[..],
            &b"-----BEGIN PRIVATE KEY-----\nxxx\n-----END PRIVATE KEY-----"[..], // 指到私钥
            &b""[..],
        ] {
            let c = GrpcCaller::new(Duration::from_secs(1)).with_ca_pem(junk.to_vec());
            assert!(
                c.extra_ca.is_none(),
                "垃圾 PEM 被当成 CA 收下了，那它只会静默不生效"
            );
            // 而且不能把推进器搞崩
            assert!(c.channel(&target("https://a:1", true)).is_ok());
        }
    }

    #[tokio::test]
    async fn 像样的ca要能装上() {
        let pem = b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n".to_vec();
        let c = GrpcCaller::new(Duration::from_secs(1)).with_ca_pem(pem);
        assert!(c.extra_ca.is_some(), "守卫不能误伤真的证书");
    }

    /// `connect_lazy` 内部要拿 tokio 的 executor，**必须在运行时里调** ——
    /// 普通 `#[test]` 会直接 panic。生产路径上分支调用本来就在运行时里，
    /// 不受影响
    #[tokio::test]
    async fn channel会被缓存复用() {
        let c = GrpcCaller::new(Duration::from_secs(1));
        let a = c.channel(&target("http://127.0.0.1:9", false)).unwrap();
        let b = c.channel(&target("http://127.0.0.1:9", false)).unwrap();
        // 缓存命中时表里只该有一条
        assert_eq!(c.channels.lock().unwrap().len(), 1);
        drop((a, b));
    }
}
