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

/// 分支目标：进程内函数，还是远端 HTTP。
///
/// 用 URI 前缀区分而不是加新字段 —— 这样落库格式不变，也跟 DTM 的 http URL
/// 完全兼容，同一个事务里可以混用两种分支。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// `local://名字`
    Local(String),
    /// `http://...` / `https://...`
    Http(String),
}

pub fn parse_target(s: &str) -> Target {
    match s.strip_prefix("local://") {
        Some(name) => Target::Local(name.to_string()),
        None => Target::Http(s.to_string()),
    }
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
