//! 进程内分支注册表 —— 嵌入式模式的核心。
//!
//! # 为什么分支要用「名字」而不是闭包
//!
//! 事务必须能跨进程重启恢复，而**闭包没法持久化**。所以数据库里存的是名字
//! （`local://deduct`），重启后靠注册表把名字重新解析成函数。
//!
//! 这是持久化执行引擎的通用做法，也是唯一正确的做法：
//!
//! ```text
//! 提交时   steps = ["local://deduct", "local://deduct_undo"]  → 落库
//! 崩溃重启 从库里读出 "local://deduct" → registry 查表 → 拿到函数 → 继续推
//! ```
//!
//! **代价**：注册表在重启后必须注册同样的名字，否则事务推不动。这不是缺陷，
//! 是把"代码版本"这个隐式依赖显式化了 —— 漏注册会明确报错，而不是静默跑错。

use dtmrs_core::{BranchOp, BranchResult};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// 分支被调用时拿到的上下文。业务侧用它做幂等（配合 dtmrs-barrier）。
#[derive(Debug, Clone)]
pub struct BranchCtx {
    pub gid: String,
    pub branch_id: String,
    pub op: BranchOp,
    pub trans_type: String,
}

type BoxFut = Pin<Box<dyn Future<Output = BranchResult> + Send>>;
type Handler = Arc<dyn Fn(BranchCtx) -> BoxFut + Send + Sync>;

/// 分支目标：进程内函数、远端 HTTP，还是远端 gRPC。
///
/// 用 URI 前缀区分而不是加新字段 —— 这样落库格式不变，也跟 DTM 的 http URL
/// 完全兼容，同一个事务里可以三种混用。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// `local://名字`
    Local(String),
    /// `http://...` / `https://...`
    Http(String),
    /// `grpc://host:port/包.服务/方法`
    Grpc(GrpcTarget),
}

/// 拆好的 gRPC 分支地址。
///
/// gRPC 的调用地址天然是两段：连哪个 server（endpoint）+ 调哪个方法（path），
/// 而 HTTP 是一整个 URL。所以这里必须拆开存，不能像 http 那样原样透传。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcTarget {
    /// tonic 连接用，形如 `http://host:port`
    pub endpoint: String,
    /// gRPC 方法路径，形如 `/包.服务/方法`
    pub path: String,
}

pub fn parse_target(s: &str) -> Target {
    if let Some(name) = s.strip_prefix("local://") {
        return Target::Local(name.to_string());
    }
    // grpcs:// 走 TLS，但当前 tonic 依赖没开 tls feature，先只认明文。
    // 认错了不如认不出来 —— 落到 Http 分支会明确报错，而不是静默用错协议。
    if let Some(rest) = s.strip_prefix("grpc://") {
        if let Some(t) = parse_grpc(rest) {
            return Target::Grpc(t);
        }
    }
    Target::Http(s.to_string())
}

/// `host:port/包.服务/方法` → (endpoint, path)
///
/// 必须正好有两段路径（服务名 + 方法名）。少一段或多一段都说明地址写错了，
/// 这时候**不能猜** —— 返回 None 让它落到 Http 分支去明确失败。
fn parse_grpc(rest: &str) -> Option<GrpcTarget> {
    let (authority, path) = rest.split_once('/')?;
    if authority.is_empty() {
        return None;
    }
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segs.len() != 2 {
        return None;
    }
    Some(GrpcTarget {
        endpoint: format!("http://{authority}"),
        path: format!("/{}/{}", segs[0], segs[1]),
    })
}

#[derive(Default)]
pub struct Registry {
    handlers: HashMap<String, Handler>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册一个进程内分支。名字要跟 `local://名字` 对应。
    pub fn register<F, Fut>(&mut self, name: &str, f: F) -> &mut Self
    where
        F: Fn(BranchCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = BranchResult> + Send + 'static,
    {
        let h: Handler = Arc::new(move |ctx| Box::pin(f(ctx)));
        self.handlers.insert(name.to_string(), h);
        self
    }

    pub fn get(&self, name: &str) -> Option<Handler> {
        self.handlers.get(name).cloned()
    }

    pub fn names(&self) -> Vec<&str> {
        self.handlers.keys().map(String::as_str).collect()
    }

    /// 提交前自查：所有 `local://` 分支都注册了吗？
    ///
    /// 宁可在提交时就报错，也不要等事务推到一半才发现分支不存在 ——
    /// 那时候已经有副作用落地了，只能靠补偿收拾。
    pub fn check_all(&self, targets: &[String]) -> Result<(), Vec<String>> {
        let missing: Vec<String> = targets
            .iter()
            .filter_map(|t| match parse_target(t) {
                Target::Local(n) if !self.handlers.contains_key(&n) => Some(n),
                _ => None,
            })
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(missing)
        }
    }
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("handlers", &self.names())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 前缀区分本地与远端() {
        assert_eq!(parse_target("local://deduct"), Target::Local("deduct".into()));
        assert_eq!(
            parse_target("http://busi/deduct"),
            Target::Http("http://busi/deduct".into())
        );
        // 没前缀就当 http，保持跟 DTM 的兼容
        assert_eq!(parse_target("https://a/b"), Target::Http("https://a/b".into()));
    }

    #[test]
    fn grpc地址拆成端点与方法路径() {
        let Target::Grpc(t) = parse_target("grpc://127.0.0.1:9000/busi.Busi/Deduct") else {
            panic!("应该认成 grpc");
        };
        // endpoint 必须带 http:// —— tonic 的 Endpoint 要求是个完整 URI
        assert_eq!(t.endpoint, "http://127.0.0.1:9000");
        assert_eq!(t.path, "/busi.Busi/Deduct");
    }

    #[test]
    fn 畸形grpc地址不猜而是落回http() {
        // 少了方法名、少了服务名、路径多一段、没有 authority ——
        // 全都不能猜。落到 Http 分支会明确失败，比连错服务安全。
        for bad in [
            "grpc://127.0.0.1:9000/onlyservice",
            "grpc://127.0.0.1:9000/",
            "grpc://127.0.0.1:9000/a/b/c",
            "grpc:///a/b",
            "grpc://noslash",
        ] {
            assert!(
                matches!(parse_target(bad), Target::Http(_)),
                "{bad} 不该被当成合法 grpc 地址"
            );
        }
    }

    #[tokio::test]
    async fn 注册与调用() {
        let mut r = Registry::new();
        r.register("ok", |_ctx| async { BranchResult::Success });
        let h = r.get("ok").expect("应该能查到");
        let ctx = BranchCtx {
            gid: "g".into(),
            branch_id: "01".into(),
            op: BranchOp::Action,
            trans_type: "saga".into(),
        };
        assert_eq!(h(ctx).await, BranchResult::Success);
        assert!(r.get("nope").is_none());
    }

    #[test]
    fn 提交前能查出漏注册的分支() {
        let mut r = Registry::new();
        r.register("a", |_| async { BranchResult::Success });
        let targets = vec![
            "local://a".to_string(),
            "local://missing".to_string(),
            "http://x/y".to_string(),
        ];
        let err = r.check_all(&targets).unwrap_err();
        assert_eq!(err, vec!["missing"], "只报本地漏的，http 不管");
        assert!(r.check_all(&["local://a".to_string()]).is_ok());
    }
}
