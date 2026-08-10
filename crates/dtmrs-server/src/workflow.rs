//! workflow 模式：把事务流程写成一个**普通函数**，崩溃后从断点续跑。
//!
//! # 跟前四种模式的差别
//!
//! SAGA / TCC / msg / XA 都要求提交时就把步骤声明清楚。真实业务经常不满足：
//! 第三步做不做取决于第二步返回了什么，中间还有 `if`、有循环。
//!
//! workflow 模式让你直接写：
//!
//! ```ignore
//! tc.workflow("下单", |mut wf| async move {
//!     let oid = wf.branch("建订单").on_rollback("local://取消订单")
//!         .run_with(|| async { (BranchResult::Success, new_order_id()) }).await?;
//!
//!     wf.branch("扣款").on_rollback("local://退款")
//!         .run(|| async { deduct(&oid).await }).await?;
//!
//!     // 控制流是真的控制流
//!     if need_ship(&oid) {
//!         wf.branch("发货").on_rollback("local://退货")
//!             .run(|| async { ship(&oid).await }).await?;
//!     }
//!     Ok(())
//! })
//! ```
//!
//! # 崩溃恢复靠重放 + 结果记忆化
//!
//! 进程崩了重启，TC 会把这个函数**从头再跑一遍**。已经成功过的分支不重新执行，
//! 而是把上次存的返回值原样还给你 —— 所以函数会沿着上次的路径走到断点，
//! 然后继续往下。
//!
//! ```text
//! 第一次:  建订单(真跑,存 oid) → 扣款(真跑) → 崩溃
//! 重启后:  建订单(记忆化,还回 oid) → 扣款(记忆化) → 发货(真跑) → 完成
//!                    ↑ 副作用不会重做
//! ```
//!
//! # ⚠ 你的函数必须是确定性的
//!
//! 重放是**从头再跑**，所以分支之间的那些代码会被执行多次。它们必须在相同的
//! 分支返回值下走相同的路径：
//!
//! - ❌ `if rand() > 0.5`、`if now().hour() < 12`、读一个会变的全局状态
//! - ❌ 在分支**外面**直接写数据库 —— 那部分不会被记忆化，重放时会重复执行
//! - ✅ 所有副作用都放进 `branch(...).run(...)` 里
//!
//! 写岔了会怎样？本模块会**当场发现并拒绝继续**（见 [`WorkflowError::Diverged`]），
//! 而不是静默补偿错对象。这是刻意的：静默走错比停下来严重得多。
//!
//! # 为什么这个模式只在嵌入式形态下提供
//!
//! 因为「步骤」是**代码**，没法表示成一个 URL 存进数据库。DTM 那边也是同理：
//! workflow 的函数体在客户端进程里，TC 只存状态。
//! 我们把 TC 也放在同一个进程里，所以这件事反而更自然。

use crate::driver::branch_id;
use dtmrs_core::{BranchOp, BranchResult, BranchStatus};
use dtmrs_store::{BranchRow, Store};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// 函数跑不下去的原因。用 `?` 往外抛。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowError {
    /// 业务**明确**要求回滚 —— 会触发逆序补偿。
    /// 只有这一种会回滚，其它都是重试
    Rollback(String),
    /// 结果未知或还在处理中 —— 退避重试，**绝不回滚**。
    /// 分支返回 `Ongoing` / `Unknown` 时自动变成这个
    Retry(String),
    /// **重放走岔了**：这次跑到第 N 个分支时的名字，跟上次记录的对不上。
    ///
    /// 说明函数不是确定性的（或者代码改了步骤顺序又碰上老事务）。
    /// 这时候**不能继续**：按位置记忆化会把 A 的结果当成 B 的，
    /// 回滚时也会补偿错对象。停下来等人比静默走错安全得多。
    Diverged {
        branch_id: String,
        recorded: String,
        got: String,
    },
    /// 存储层出错之类
    Internal(String),
}

impl std::fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rollback(m) => write!(f, "业务要求回滚: {m}"),
            Self::Retry(m) => write!(f, "需要重试: {m}"),
            Self::Diverged {
                branch_id,
                recorded,
                got,
            } => write!(
                f,
                "重放走岔了：分支 {branch_id} 上次记录的是「{recorded}」，这次却是「{got}」。\
                 函数必须是确定性的，副作用都要放进 branch().run() 里"
            ),
            Self::Internal(m) => write!(f, "内部错误: {m}"),
        }
    }
}

impl std::error::Error for WorkflowError {}

pub type WorkflowResult<T> = Result<T, WorkflowError>;

/// 传给 workflow 函数的上下文。分支都从这里开。
pub struct WorkflowCtx {
    pub gid: String,
    /// 提交时带的输入数据，原样透传
    pub input: String,
    store: Store,
    /// 下一个分支的序号
    seq: usize,
    /// 本次运行开始时库里已有的分支行（上次运行留下的），key 是 `branch_id`
    recorded: HashMap<String, BranchRow>,
}

impl WorkflowCtx {
    pub(crate) fn new(gid: &str, input: &str, store: Store, rows: Vec<BranchRow>) -> Self {
        let recorded = rows
            .into_iter()
            .filter(|r| r.op == BranchOp::Action)
            .map(|r| (r.branch_id.clone(), r))
            .collect();
        Self {
            gid: gid.to_string(),
            input: input.to_string(),
            store,
            seq: 0,
            recorded,
        }
    }

    /// 开一个分支。
    ///
    /// `name` 是这个分支的逻辑名字，用来做**重放分岔检测** ——
    /// 重放时第 N 个分支的名字必须跟上次一致。取个稳定的名字，别用时间戳之类。
    pub fn branch(&mut self, name: &str) -> BranchBuilder<'_> {
        BranchBuilder {
            ctx: self,
            name: name.to_string(),
            compensate: None,
        }
    }

    /// 已经登记过的分支数（含本次运行新增的）
    pub fn branch_count(&self) -> usize {
        self.seq
    }
}

pub struct BranchBuilder<'a> {
    ctx: &'a mut WorkflowCtx,
    name: String,
    compensate: Option<String>,
}

impl BranchBuilder<'_> {
    /// 登记这个分支的补偿。地址跟 saga 的补偿一样，可以是
    /// `local://名字` / `http://...` / `grpc://...`。
    ///
    /// 不登记补偿的分支在回滚时**不会被补偿** —— 只适合本来就没副作用的步骤
    /// （比如纯查询）。有副作用就一定要给。
    pub fn on_rollback(mut self, compensate: &str) -> Self {
        self.compensate = Some(compensate.to_string());
        self
    }

    /// 跑这个分支（不带返回数据）。
    pub async fn run<F, Fut>(self, f: F) -> WorkflowResult<()>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = BranchResult>,
    {
        self.run_with(|| async move { (f().await, String::new()) })
            .await
            .map(|_| ())
    }

    /// 跑这个分支，并记住它的返回数据。**重放时原样还回来，不会重新执行。**
    ///
    /// 数据大小受 `trans_branch_op.payload` 列限制（MySQL 上是 VARCHAR），
    /// 放个 id 或一小段 JSON 就好，别塞大对象。
    pub async fn run_with<F, Fut>(self, f: F) -> WorkflowResult<String>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = (BranchResult, String)>,
    {
        let bid = branch_id(self.ctx.seq);
        self.ctx.seq += 1;
        let gid = self.ctx.gid.clone();

        // ---- 1. 重放：这个位置上次是什么？----
        if let Some(row) = self.ctx.recorded.get(&bid) {
            // 分岔检测先做，不管上次成没成 —— 名字对不上就说明函数走了另一条路，
            // 再往下按位置记忆化就是张冠李戴
            if row.url != self.name {
                return Err(WorkflowError::Diverged {
                    branch_id: bid,
                    recorded: row.url.clone(),
                    got: self.name,
                });
            }
            if row.status == BranchStatus::Succeed {
                // 记忆化命中：**不重新执行**，把上次的结果还回去
                return Ok(row.payload.clone());
            }
            // 上次没成（崩在中间 / 超时）—— 往下走，重新执行一遍。
            // 重复执行的安全性由业务侧的子事务屏障保证
        }

        // ---- 2. 先登记补偿，再执行动作 ----
        // 顺序不能反：反过来的话，动作执行完、补偿还没登记时崩溃，
        // 副作用就永远没人收拾了。跟 TCC「先 registerBranch 再调 try」同一条教训。
        let mut ops = Vec::with_capacity(2);
        if let Some(c) = &self.compensate {
            ops.push((BranchOp::Compensate, c.clone()));
        }
        // action 行的 url 存**分支名**，供下次重放做分岔检测
        ops.push((BranchOp::Action, self.name.clone()));
        self.ctx
            .store
            .register_branch(&gid, &bid, &ops)
            .await
            .map_err(|e| WorkflowError::Internal(format!("登记分支 {bid} 失败: {e}")))?;

        // ---- 3. 真正执行 ----
        let (result, data) = f().await;
        match result {
            BranchResult::Success => {
                self.ctx
                    .store
                    .set_branch_result(&gid, &bid, BranchOp::Action, BranchStatus::Succeed, &data)
                    .await
                    .map_err(|e| WorkflowError::Internal(format!("存分支结果失败: {e}")))?;
                Ok(data)
            }
            BranchResult::Failure => {
                let _ = self
                    .ctx
                    .store
                    .set_branch_status(&gid, &bid, BranchOp::Action, BranchStatus::Failed)
                    .await;
                Err(WorkflowError::Rollback(format!(
                    "分支 {bid}（{}）返回 FAILURE",
                    self.name
                )))
            }
            // **绝不能当失败**：对方可能已经成功了，回滚会造成不一致
            BranchResult::Ongoing | BranchResult::Unknown => Err(WorkflowError::Retry(format!(
                "分支 {bid}（{}）结果未知或处理中",
                self.name
            ))),
        }
    }
}

type BoxFut = Pin<Box<dyn Future<Output = WorkflowResult<()>> + Send>>;
type WorkflowFn = Arc<dyn Fn(WorkflowCtx) -> BoxFut + Send + Sync>;

/// 按名字存 workflow 函数。
///
/// 跟 `local://` 分支同一个道理：**函数没法持久化**，库里存的是名字，
/// 重启后靠这张表把名字解析回函数。所以重启后必须注册同名的 workflow。
#[derive(Default)]
pub struct WorkflowRegistry {
    fns: HashMap<String, WorkflowFn>,
}

impl WorkflowRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F, Fut>(&mut self, name: &str, f: F) -> &mut Self
    where
        F: Fn(WorkflowCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = WorkflowResult<()>> + Send + 'static,
    {
        let h: WorkflowFn = Arc::new(move |ctx| Box::pin(f(ctx)));
        self.fns.insert(name.to_string(), h);
        self
    }

    pub fn get(&self, name: &str) -> Option<WorkflowFn> {
        self.fns.get(name).cloned()
    }

    pub fn contains(&self, name: &str) -> bool {
        self.fns.contains_key(name)
    }

    pub fn names(&self) -> Vec<&str> {
        self.fns.keys().map(String::as_str).collect()
    }
}

impl std::fmt::Debug for WorkflowRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowRegistry")
            .field("workflows", &self.names())
            .finish()
    }
}

/// `trans_global.payload` 里存的东西：workflow 名字 + 输入数据
pub(crate) fn encode_payload(name: &str, input: &str) -> String {
    serde_json::json!({ "name": name, "input": input }).to_string()
}

pub(crate) fn decode_payload(payload: &str) -> (String, String) {
    let v: serde_json::Value = serde_json::from_str(payload).unwrap_or_default();
    (
        v.get("name")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string(),
        v.get("input")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload编解码能往返() {
        let p = encode_payload("下单", r#"{"oid":1}"#);
        let (n, i) = decode_payload(&p);
        assert_eq!(n, "下单");
        assert_eq!(i, r#"{"oid":1}"#);
    }

    #[test]
    fn 坏payload不会panic() {
        // 老版本留下的数据、或者被截断的 payload —— 解不出来也不能崩，
        // 崩了整个推进器就停了
        assert_eq!(decode_payload("不是 json"), (String::new(), String::new()));
        assert_eq!(decode_payload(""), (String::new(), String::new()));
        assert_eq!(decode_payload("{}"), (String::new(), String::new()));
    }

    #[test]
    fn 注册表按名字查函数() {
        let mut r = WorkflowRegistry::new();
        r.register("a", |_ctx| async { Ok(()) });
        assert!(r.contains("a"));
        assert!(r.get("a").is_some());
        assert!(r.get("没这个").is_none());
    }

    #[test]
    fn 分岔错误的说明要能指出问题所在() {
        let e = WorkflowError::Diverged {
            branch_id: "02".into(),
            recorded: "扣款".into(),
            got: "发货".into(),
        };
        let s = e.to_string();
        assert!(s.contains("02") && s.contains("扣款") && s.contains("发货"));
        assert!(s.contains("确定性"), "得告诉用户根因是函数不确定");
    }
}
