//! C ABI —— 让 Python / Node / Java / C++ 也能把事务协调器嵌进自己的进程。
//!
//! 这是「嵌入式 TC」的完整形态，也是 Go 做不到的地方：Go 的 `c-shared` 会把整个
//! 运行时（调度器 + GC + 信号处理）拖进宿主进程，跟宿主的信号/线程模型冲突，
//! 实际没人这么用。Rust 编出来就是一个普通的 `.so`，没有运行时包袱。
//!
//! # 三个必须处理对的地方
//!
//! **1. 宿主的回调是同步的，而且可能阻塞。**
//! Python 的 handler 会去查数据库、发 HTTP，动辄几十毫秒；而且 CPython 调回调
//! 要抢 GIL。直接在 tokio worker 线程里调会把整个运行时卡死。
//! 所以每次回调都走 `spawn_blocking`，扔到专门的阻塞线程池。
//!
//! **2. 回调会从任意线程被调用。**
//! 推进器跑在 tokio 的线程上，不是宿主的主线程。宿主的回调必须线程安全。
//! Python 的 `ctypes.CFUNCTYPE` 会自动处理 GIL，可以直接用；
//! 其它语言（比如 JNI）需要自己 attach 线程。
//!
//! **3. 结果码不能把「未知」当成「失败」。**
//! 这是这个领域的头号 bug。宿主返回 `DTMRS_UNKNOWN`（3）时只会重试不会回滚。
//! 宿主代码抛异常/panic 的话，FFI 层也按 UNKNOWN 处理 —— 不知道就别回滚。
//!
//! # 内存约定
//!
//! - 所有 `const char*` 入参：C 侧拥有，调用期间必须有效，本库不接管
//! - 所有输出缓冲区：C 侧分配，本库只写入并保证以 `\0` 结尾
//! - `dtmrs_last_error()` 返回线程局部缓冲区，下次调用本库任何函数即失效

// 这个 crate 存在的意义就是给 C 调：几乎每个导出函数都要解引用宿主传进来的
// 裸指针。安全契约写在 include/dtmrs.h 和各函数的文档里（非空、存活期、
// 线程安全），由调用方保证 —— 这是 C ABI 的常态，不是疏忽。
// 每个函数内部都做了空指针检查，传 NULL 会返回 DTMRS_ERR 而不是崩。
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use dtmrs_core::{BranchResult, SagaStep};
use dtmrs_server::embedded::Embedded;
use dtmrs_server::registry::BranchCtx;
use dtmrs_server::workflow::{WorkflowCtx, WorkflowError, WorkflowResult};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

pub const DTMRS_OK: c_int = 0;
pub const DTMRS_ERR: c_int = -1;

/// 分支返回码。**顺序和语义不能改**，宿主语言按这些数字写死了。
pub const DTMRS_SUCCESS: c_int = 0;
pub const DTMRS_FAILURE: c_int = 1;
pub const DTMRS_ONGOING: c_int = 2;
pub const DTMRS_UNKNOWN: c_int = 3;

/// 宿主提供的分支处理函数。
///
/// 返回 `DTMRS_SUCCESS/FAILURE/ONGOING/UNKNOWN` 之一。
/// **返回不认识的值一律按 UNKNOWN 处理** —— 宁可重试，不可误回滚。
pub type HandlerFn = extern "C" fn(
    gid: *const c_char,
    branch_id: *const c_char,
    op: *const c_char,
    user_data: *mut c_void,
) -> c_int;

/// 带业务数据的分支处理函数（[`dtmrs_register_ex`] 用）。
///
/// 比 [`HandlerFn`] 多一个 `payload`：saga 那步 `step_with` 给的数据，没给就是空串。
/// 老签名没法加参数 —— 已经编好的宿主按四个参数调，改了就是栈错乱 ——
/// 所以另起一个类型，两种可以混用。
pub type HandlerExFn = extern "C" fn(
    gid: *const c_char,
    branch_id: *const c_char,
    op: *const c_char,
    payload: *const c_char,
    user_data: *mut c_void,
) -> c_int;

#[derive(Clone, Copy)]
enum HostFn {
    Plain(HandlerFn),
    Ex(HandlerExFn),
}

/// 裸函数指针 + 用户数据。跨线程传递需要显式声明安全性。
///
/// # Safety 契约（宿主必须保证）
/// - `f` 在 TC 存活期间一直有效
/// - `f` 可以被多个线程并发调用
/// - `ud` 指向的数据在 TC 存活期间有效且线程安全
#[derive(Clone, Copy)]
struct HandlerPtr {
    f: HostFn,
    ud: *mut c_void,
}
unsafe impl Send for HandlerPtr {}
unsafe impl Sync for HandlerPtr {}

/// 宿主的 workflow 函数。在里面用 [`dtmrs_wf_branch`] 开分支。
///
/// 返回 `DTMRS_SUCCESS` = 跑完了；`DTMRS_FAILURE` = 业务要求整单回滚；
/// 其它 = 过会儿重放。**任何一次 `dtmrs_wf_branch` 返回过 `DTMRS_ERR`，
/// 这里的返回值就不再算数** —— 以那次的原因为准（回滚 / 重试 / 分岔停下）。
pub type WorkflowFn = extern "C" fn(
    wf: *mut DtmrsWf,
    gid: *const c_char,
    input: *const c_char,
    user_data: *mut c_void,
) -> c_int;

/// workflow 分支的函数体。结果数据写进 `out`（`\0` 结尾，可以不写），
/// 重放时原样还给调用方，不再执行。
pub type WfBranchFn = extern "C" fn(
    gid: *const c_char,
    branch_id: *const c_char,
    out: *mut c_char,
    out_len: usize,
    user_data: *mut c_void,
) -> c_int;

#[derive(Clone, Copy)]
struct WorkflowPtr {
    f: WorkflowFn,
    ud: *mut c_void,
}
unsafe impl Send for WorkflowPtr {}
unsafe impl Sync for WorkflowPtr {}

thread_local! {
    static LAST_ERR: RefCell<CString> = RefCell::new(CString::new("").unwrap());
}

fn set_err(msg: impl Into<Vec<u8>>) {
    let c = CString::new(msg).unwrap_or_else(|_| CString::new("错误信息含 NUL").unwrap());
    LAST_ERR.with(|e| *e.borrow_mut() = c);
}

fn clear_err() {
    set_err("");
}

/// 取最近一次错误。返回的指针在下次调用本库任何函数后失效。
#[no_mangle]
pub extern "C" fn dtmrs_last_error() -> *const c_char {
    LAST_ERR.with(|e| e.borrow().as_ptr())
}

/// 不透明句柄。C 侧只当 void* 用。
pub struct DtmrsTc {
    rt: tokio::runtime::Runtime,
    /// start 之前收集 handler，start 之后置 None
    pending: Option<Vec<(String, HandlerPtr)>>,
    /// start 之前收集走拉取式的分支名
    pending_pull: Option<Vec<String>>,
    /// start 之前收集的 workflow 函数
    pending_wf: Option<Vec<(String, WorkflowPtr)>>,
    db: String,
    tc: Option<Embedded>,
    pull: Arc<PullQueue>,
}

// ---------------- 拉取式分支分发 ----------------

/// 宿主没回结果的等待上限。到点按「结果未知」处理 —— 只重试不回滚，
/// 因为宿主可能已经把活干完了只是没来得及回话。
const PULL_REPLY_TIMEOUT: Duration = Duration::from_secs(30);

/// 一个等着宿主处理的分支
#[derive(Debug, Clone)]
struct PullTask {
    task_id: u64,
    name: String,
    gid: String,
    branch_id: String,
    op: String,
    payload: String,
}

/// 拉取式分发的队列。
///
/// # 为什么要有这个东西（回调式不够用吗）
///
/// C ABI 的回调必须**同步返回一个 int**。这对 Python/Java 没问题，但对
/// Node 这类宿主是硬伤：它们的业务代码几乎全是异步的（数据库客户端都返回
/// Promise），而同步回调里没法 await。
///
/// 拉取式把控制权交给宿主：宿主在自己的事件循环里取任务、爱怎么异步怎么异步、
/// 完事了再回填结果。**不是回调式的替代品，是另一种接法** ——
/// 同一个进程里两种可以混用，各自负责各自的分支名。
struct PullQueue {
    tx: std::sync::mpsc::Sender<PullTask>,
    rx: Mutex<std::sync::mpsc::Receiver<PullTask>>,
    /// 已发给宿主、还等着回话的任务
    waiting: Mutex<HashMap<u64, oneshot::Sender<BranchResult>>>,
    next_id: AtomicU64,
}

impl PullQueue {
    fn new() -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        Self {
            tx,
            rx: Mutex::new(rx),
            waiting: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// 推进器这边：把分支挂进队列，等宿主回话
    async fn dispatch(&self, name: &str, ctx: &BranchCtx) -> BranchResult {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.waiting.lock().unwrap().insert(id, tx);

        let task = PullTask {
            task_id: id,
            name: name.to_string(),
            gid: ctx.gid.clone(),
            branch_id: ctx.branch_id.clone(),
            op: ctx.op.as_str().to_string(),
            payload: ctx.payload.clone(),
        };
        if self.tx.send(task).is_err() {
            self.waiting.lock().unwrap().remove(&id);
            return BranchResult::Unknown;
        }

        match tokio::time::timeout(PULL_REPLY_TIMEOUT, rx).await {
            Ok(Ok(r)) => r,
            // 超时，或者宿主把句柄丢了。**按未知处理** ——
            // 宿主可能已经执行了业务逻辑，只是没回话
            _ => {
                self.waiting.lock().unwrap().remove(&id);
                eprintln!("[dtmrs] 宿主未在 {PULL_REPLY_TIMEOUT:?} 内回填结果，按结果未知处理");
                BranchResult::Unknown
            }
        }
    }
}

unsafe fn cstr<'a>(p: *const c_char, what: &str) -> Option<&'a str> {
    if p.is_null() {
        set_err(format!("{what} 是空指针"));
        return None;
    }
    match CStr::from_ptr(p).to_str() {
        Ok(s) => Some(s),
        Err(_) => {
            set_err(format!("{what} 不是合法 UTF-8"));
            None
        }
    }
}

fn write_out(s: &str, out: *mut c_char, out_len: usize) -> c_int {
    if out.is_null() || out_len == 0 {
        set_err("输出缓冲区无效");
        return DTMRS_ERR;
    }
    let b = s.as_bytes();
    if b.len() + 1 > out_len {
        set_err(format!("输出缓冲区太小：需要 {} 字节", b.len() + 1));
        return DTMRS_ERR;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(b.as_ptr(), out as *mut u8, b.len());
        *out.add(b.len()) = 0;
    }
    DTMRS_OK
}

/// 创建一个 TC 句柄（还没启动）。`db_url` 形如 `sqlite:/tmp/app.db`，也可以是
/// `postgres://` / `mysql://` / `redis://`（Redis 要 `redis` feature，默认开；
/// 多套环境共用一个 Redis 时带 `?key_prefix=staging:` 隔开）。
///
/// 失败返回 NULL，用 `dtmrs_last_error()` 看原因。
#[no_mangle]
pub extern "C" fn dtmrs_open(db_url: *const c_char) -> *mut DtmrsTc {
    clear_err();
    let Some(db) = (unsafe { cstr(db_url, "db_url") }) else {
        return std::ptr::null_mut();
    };
    // 多线程运行时：推进器和阻塞回调各用各的线程池
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            set_err(format!("创建运行时失败: {e}"));
            return std::ptr::null_mut();
        }
    };
    Box::into_raw(Box::new(DtmrsTc {
        rt,
        pending: Some(Vec::new()),
        pending_pull: Some(Vec::new()),
        pending_wf: Some(Vec::new()),
        db: db.to_string(),
        tc: None,
        pull: Arc::new(PullQueue::new()),
    }))
}

/// 注册一个进程内分支。必须在 `dtmrs_start` 之前调。
///
/// `name` 对应 saga 步骤里的 `local://name`。
#[no_mangle]
pub extern "C" fn dtmrs_register(
    tc: *mut DtmrsTc,
    name: *const c_char,
    f: Option<HandlerFn>,
    user_data: *mut c_void,
) -> c_int {
    clear_err();
    let Some(h) = (unsafe { tc.as_mut() }) else {
        set_err("句柄是空指针");
        return DTMRS_ERR;
    };
    let Some(name) = (unsafe { cstr(name, "name") }) else {
        return DTMRS_ERR;
    };
    let Some(f) = f else {
        set_err("handler 是空指针");
        return DTMRS_ERR;
    };
    push_handler(h, name, HostFn::Plain(f), user_data)
}

/// 跟 [`dtmrs_register`] 一样，只是回调多一个 `payload` 参数（见 [`HandlerExFn`]）。
///
/// 分支要用业务数据（金额、地址）就用这个；只靠 gid/branch_id 做幂等的用哪个都行。
#[no_mangle]
pub extern "C" fn dtmrs_register_ex(
    tc: *mut DtmrsTc,
    name: *const c_char,
    f: Option<HandlerExFn>,
    user_data: *mut c_void,
) -> c_int {
    clear_err();
    let Some(h) = (unsafe { tc.as_mut() }) else {
        set_err("句柄是空指针");
        return DTMRS_ERR;
    };
    let Some(name) = (unsafe { cstr(name, "name") }) else {
        return DTMRS_ERR;
    };
    let Some(f) = f else {
        set_err("handler 是空指针");
        return DTMRS_ERR;
    };
    push_handler(h, name, HostFn::Ex(f), user_data)
}

fn push_handler(h: &mut DtmrsTc, name: &str, f: HostFn, ud: *mut c_void) -> c_int {
    match h.pending.as_mut() {
        Some(v) => {
            v.push((name.to_string(), HandlerPtr { f, ud }));
            DTMRS_OK
        }
        None => {
            // 启动后再注册会有竞态：推进器可能正在查这张表
            set_err("已经 start 了，不能再注册 handler");
            DTMRS_ERR
        }
    }
}

/// 把一个分支名登记成**拉取式**。必须在 `dtmrs_start` 之前调。
///
/// 登记之后，这个名字的分支不会回调宿主，而是进队列等 [`dtmrs_next_task`] 来取。
/// 适合两类宿主：
///
/// - **事件循环型**（Node）：同步回调里没法 await，只能用拉取式
/// - 想用自己的线程池 / 想控制并发度的
///
/// 跟 [`dtmrs_register`] 可以在同一个进程里混用，各管各的名字。
#[no_mangle]
pub extern "C" fn dtmrs_register_pull(tc: *mut DtmrsTc, name: *const c_char) -> c_int {
    clear_err();
    let Some(h) = (unsafe { tc.as_mut() }) else {
        set_err("句柄是空指针");
        return DTMRS_ERR;
    };
    let Some(name) = (unsafe { cstr(name, "name") }) else {
        return DTMRS_ERR;
    };
    match h.pending_pull.as_mut() {
        Some(v) => {
            v.push(name.to_string());
            DTMRS_OK
        }
        None => {
            set_err("已经 start 了，不能再注册分支");
            DTMRS_ERR
        }
    }
}

/// 取一个待办分支，JSON 写进 `out`：
///
/// ```json
/// {"task_id":7,"name":"deduct","gid":"order-1","branch_id":"01","op":"action"}
/// ```
///
/// 返回 `1` = 取到任务，`0` = 这段时间没任务，`DTMRS_ERR` = 出错。
///
/// `timeout_ms` 传 **0 表示不阻塞**（立刻返回）。事件循环型宿主应该传 0 并靠
/// 自己的定时器轮询 —— 阻塞会卡住整个循环，那样连回填结果都做不到。
///
/// 取到任务后**必须**用 [`dtmrs_reply`] 回填，否则这个分支会一直挂到超时
/// （按结果未知处理，会重试）。
#[no_mangle]
pub extern "C" fn dtmrs_next_task(
    tc: *mut DtmrsTc,
    timeout_ms: c_int,
    out: *mut c_char,
    out_len: usize,
) -> c_int {
    clear_err();
    let Some(h) = (unsafe { tc.as_ref() }) else {
        set_err("句柄是空指针");
        return DTMRS_ERR;
    };
    let rx = h.pull.rx.lock().unwrap();
    let got = if timeout_ms <= 0 {
        rx.try_recv().ok()
    } else {
        rx.recv_timeout(Duration::from_millis(timeout_ms as u64))
            .ok()
    };
    drop(rx);

    let Some(task) = got else { return 0 };
    let json = serde_json::json!({
        "task_id": task.task_id,
        "name": task.name,
        "gid": task.gid,
        "branch_id": task.branch_id,
        "op": task.op,
        "payload": task.payload,
    })
    .to_string();
    if write_out(&json, out, out_len) != DTMRS_OK {
        // 缓冲区太小 —— 任务已经出队了，直接回未知让它重试，
        // 否则这个分支会一直挂到超时
        let _ = reply_inner(h, task.task_id, DTMRS_UNKNOWN);
        return DTMRS_ERR;
    }
    1
}

/// 回填一个拉取到的分支的结果。
///
/// `result` 取 `DTMRS_SUCCESS` / `DTMRS_FAILURE` / `DTMRS_ONGOING` / `DTMRS_UNKNOWN`，
/// **不认识的值一律按 UNKNOWN 处理** —— 宁可重试，不可误回滚。
///
/// 宿主自己抛异常时应该回 `DTMRS_UNKNOWN` 而不是 `DTMRS_FAILURE`：
/// 异常意味着不知道业务到底做没做，回滚可能造成不一致。
#[no_mangle]
pub extern "C" fn dtmrs_reply(tc: *mut DtmrsTc, task_id: u64, result: c_int) -> c_int {
    clear_err();
    let Some(h) = (unsafe { tc.as_ref() }) else {
        set_err("句柄是空指针");
        return DTMRS_ERR;
    };
    reply_inner(h, task_id, result)
}

fn reply_inner(h: &DtmrsTc, task_id: u64, result: c_int) -> c_int {
    let Some(tx) = h.pull.waiting.lock().unwrap().remove(&task_id) else {
        // 回晚了（已经超时了），或者 task_id 是编的
        set_err(format!("task_id {task_id} 不存在或已超时"));
        return DTMRS_ERR;
    };
    let _ = tx.send(to_branch_result(result));
    DTMRS_OK
}

/// 启动推进器。之后事务就会被自动推进（包括进程上次留下的未终结事务）。
#[no_mangle]
pub extern "C" fn dtmrs_start(tc: *mut DtmrsTc) -> c_int {
    clear_err();
    let Some(h) = (unsafe { tc.as_mut() }) else {
        set_err("句柄是空指针");
        return DTMRS_ERR;
    };
    let Some(pending) = h.pending.take() else {
        set_err("已经 start 过了");
        return DTMRS_ERR;
    };
    let pending_pull = h.pending_pull.take().unwrap_or_default();
    let pending_wf = h.pending_wf.take().unwrap_or_default();

    let mut b = Embedded::builder(&h.db).tick(Duration::from_millis(50));

    for (name, wp) in pending_wf {
        b = b.workflow(&name, move |ctx: WorkflowCtx| async move {
            // 跟分支回调同一个理由：宿主函数同步、会阻塞、还可能抢 GIL
            let rt = tokio::runtime::Handle::current();
            match tokio::task::spawn_blocking(move || run_host_workflow(wp, ctx, rt)).await {
                Ok(r) => r,
                // 宿主函数 panic 了。不知道跑到哪了 —— **重放，不回滚**
                Err(e) => Err(WorkflowError::Retry(format!(
                    "宿主 workflow 函数异常终止: {e}"
                ))),
            }
        });
    }

    // 拉取式的分支：handler 只负责挂进队列然后等宿主回话
    for name in pending_pull {
        let q = h.pull.clone();
        let n = name.clone();
        b = b.handler(&name, move |ctx: BranchCtx| {
            let q = q.clone();
            let n = n.clone();
            async move { q.dispatch(&n, &ctx).await }
        });
    }

    for (name, hp) in pending {
        b = b.handler(&name, move |ctx: BranchCtx| async move {
            // 宿主回调是同步的、可能阻塞几十毫秒、还可能要抢 GIL。
            // 必须扔到阻塞线程池，否则会卡死 tokio worker。
            let r = tokio::task::spawn_blocking(move || call_host(hp, &ctx)).await;
            match r {
                Ok(v) => v,
                Err(e) => {
                    // 宿主回调 panic 了。**当未知处理，不当失败** ——
                    // 不知道它到底做了没有，回滚可能造成不一致
                    eprintln!("[dtmrs] 宿主回调异常终止: {e}，按结果未知处理");
                    BranchResult::Unknown
                }
            }
        });
    }
    match h.rt.block_on(b.start()) {
        Ok(e) => {
            h.tc = Some(e);
            DTMRS_OK
        }
        Err(e) => {
            set_err(format!("启动失败: {e}"));
            DTMRS_ERR
        }
    }
}

/// 真正打回宿主语言的那一跳
fn call_host(hp: HandlerPtr, ctx: &BranchCtx) -> BranchResult {
    let gid = CString::new(ctx.gid.as_str()).unwrap_or_default();
    let bid = CString::new(ctx.branch_id.as_str()).unwrap_or_default();
    let op = CString::new(ctx.op.as_str()).unwrap_or_default();
    // CString 都在本函数栈上活着，回调返回前不会被释放
    let code = match hp.f {
        HostFn::Plain(f) => f(gid.as_ptr(), bid.as_ptr(), op.as_ptr(), hp.ud),
        HostFn::Ex(f) => {
            // payload 里带 NUL 就传不过去。**不能退化成空串**：宿主拿到空数据
            // 可能照样执行（扣 0 元），按未知处理只重试，让人去看日志
            let Ok(p) = CString::new(ctx.payload.as_str()) else {
                eprintln!(
                    "[dtmrs] 分支 {} 的 payload 含 NUL，C 回调传不了，按结果未知处理",
                    ctx.branch_id
                );
                return BranchResult::Unknown;
            };
            f(gid.as_ptr(), bid.as_ptr(), op.as_ptr(), p.as_ptr(), hp.ud)
        }
    };
    to_branch_result(code)
}

/// 宿主给的返回码 → 内部结论。回调式和拉取式共用，两条路必须一致。
///
/// **不认识的值一律按 Unknown**：宁可重试，不可误回滚。
fn to_branch_result(code: c_int) -> BranchResult {
    match code {
        DTMRS_SUCCESS => BranchResult::Success,
        DTMRS_FAILURE => BranchResult::Failure,
        DTMRS_ONGOING => BranchResult::Ongoing,
        // 包括 DTMRS_UNKNOWN 和任何不认识的值
        _ => BranchResult::Unknown,
    }
}

/// 提交一个 SAGA。`steps_json` 形如：
///
/// ```json
/// [{"action":"local://deduct","compensate":"local://deduct_undo","payload":"{\"amount\":30}"},
///  {"action":"http://svc/ship","compensate":"http://svc/unship"}]
/// ```
///
/// `payload` 可省略，是**字符串**（要传 JSON 就先序列化成字符串）。
/// http 分支收到的是请求体，本地分支从 [`dtmrs_register_ex`] 的回调参数 /
/// 拉取任务的 `payload` 字段拿到。
#[no_mangle]
pub extern "C" fn dtmrs_submit_saga(
    tc: *mut DtmrsTc,
    gid: *const c_char,
    steps_json: *const c_char,
) -> c_int {
    clear_err();
    let Some(h) = (unsafe { tc.as_mut() }) else {
        set_err("句柄是空指针");
        return DTMRS_ERR;
    };
    let Some(inner) = h.tc.as_ref() else {
        set_err("还没 start");
        return DTMRS_ERR;
    };
    let (Some(gid), Some(js)) = (unsafe { cstr(gid, "gid") }, unsafe {
        cstr(steps_json, "steps_json")
    }) else {
        return DTMRS_ERR;
    };
    let steps: Vec<SagaStep> = match serde_json::from_str(js) {
        Ok(v) => v,
        Err(e) => {
            set_err(format!("steps_json 解析失败: {e}"));
            return DTMRS_ERR;
        }
    };
    let mut sb = inner.saga(gid);
    for s in &steps {
        // ⚠ 必须是 step_with。原先调的是 step()，JSON 里的 payload 解析出来
        // 又被丢掉，分支永远收到 {} —— 不报错，所以一直没人发现
        sb = sb.step_with(&s.action, &s.compensate, &s.payload);
    }
    match h.rt.block_on(sb.submit()) {
        Ok(()) => DTMRS_OK,
        Err(e) => {
            set_err(format!("提交失败: {e}"));
            DTMRS_ERR
        }
    }
}

// ---------------- TCC / XA / 二阶段消息 ----------------
//
// 这几种模式的一阶段是**宿主自己做**的（跑 try / 业务 SQL + PREPARE / 本地事务），
// 所以 C 接口是按 gid 的一串无状态调用，不是 saga 那样一次提交：
//
//   TCC:  tcc_begin → (tcc_register 01 → 宿主跑 try) × N → submit 或 abort
//   XA:   xa_begin  → (xa_register  01 → 宿主做 PREPARE) × N → submit 或 abort
//   msg:  msg_prepare → 宿主跑本地事务 → submit / abort / 什么都不做（交给回查）
//
// **分支号由宿主给**（01、02……）。库里没法替宿主编号：两次 register 之间没有
// 共享状态，按「当前最大号 +1」编在并发 try 时会撞号。撞号本身会被 api 层拒掉
// （地址不同时），但地址相同的两个分支撞号是查不出来的 —— 第二个会被当成重试。
//
// 业务判断（分支号格式、重号、终态 / 已 submit 不能 abort）全在 dtmrs-server 的
// api 层，跟 HTTP / gRPC 同一套，这里只做参数搬运。

/// 取已启动的 TC。只读借用 —— 这些调用可能来自宿主的多个线程
fn started<'a>(tc: *mut DtmrsTc) -> Option<&'a DtmrsTc> {
    let Some(h) = (unsafe { tc.as_ref() }) else {
        set_err("句柄是空指针");
        return None;
    };
    if h.tc.is_none() {
        set_err("还没 start");
        return None;
    }
    Some(h)
}

/// 跑一个异步操作，错误写进 last_error
fn run(h: &DtmrsTc, fut: impl std::future::Future<Output = anyhow::Result<()>>) -> c_int {
    match h.rt.block_on(fut) {
        Ok(()) => DTMRS_OK,
        Err(e) => {
            set_err(format!("{e}"));
            DTMRS_ERR
        }
    }
}

/// 开一个 TCC 事务。幂等：同一个 gid 再调一次不报错。
#[no_mangle]
pub extern "C" fn dtmrs_tcc_begin(tc: *mut DtmrsTc, gid: *const c_char) -> c_int {
    clear_err();
    let Some(h) = started(tc) else {
        return DTMRS_ERR;
    };
    let Some(gid) = (unsafe { cstr(gid, "gid") }) else {
        return DTMRS_ERR;
    };
    let inner = h.tc.as_ref().unwrap();
    run(h, async { inner.tcc(gid).await.map(|_| ()) })
}

/// 登记一个 TCC 分支。**返回 OK 之后才能去跑这个分支的 try** ——
/// 反过来的话 try 冻结的资源 TC 不知道，回滚时没人 cancel。
///
/// `branch_id` 从 `"01"` 开始、两位补零、每个分支各用各的。同一个分支原样重试
/// 登记是幂等的；同一个号配了不同的地址会报错（两个分支撞号）。
#[no_mangle]
pub extern "C" fn dtmrs_tcc_register(
    tc: *mut DtmrsTc,
    gid: *const c_char,
    branch_id: *const c_char,
    confirm: *const c_char,
    cancel: *const c_char,
) -> c_int {
    clear_err();
    let Some(h) = started(tc) else {
        return DTMRS_ERR;
    };
    let (Some(gid), Some(bid), Some(c), Some(x)) = (unsafe {
        (
            cstr(gid, "gid"),
            cstr(branch_id, "branch_id"),
            cstr(confirm, "confirm"),
            cstr(cancel, "cancel"),
        )
    }) else {
        return DTMRS_ERR;
    };
    let inner = h.tc.as_ref().unwrap();
    run(h, inner.register_tcc_branch(gid, bid, c, x))
}

/// 开一个 XA 事务。幂等。
#[no_mangle]
pub extern "C" fn dtmrs_xa_begin(tc: *mut DtmrsTc, gid: *const c_char) -> c_int {
    clear_err();
    let Some(h) = started(tc) else {
        return DTMRS_ERR;
    };
    let Some(gid) = (unsafe { cstr(gid, "gid") }) else {
        return DTMRS_ERR;
    };
    let inner = h.tc.as_ref().unwrap();
    run(h, async { inner.xa(gid).await.map(|_| ()) })
}

/// 登记一个 XA 分支。**返回 OK 之后才能做这个分支的一阶段**（业务 SQL + PREPARE）——
/// 反过来会留下 TC 不知道的 prepared 事务，永久持锁。分支号规则同 TCC。
#[no_mangle]
pub extern "C" fn dtmrs_xa_register(
    tc: *mut DtmrsTc,
    gid: *const c_char,
    branch_id: *const c_char,
    commit: *const c_char,
    rollback: *const c_char,
) -> c_int {
    clear_err();
    let Some(h) = started(tc) else {
        return DTMRS_ERR;
    };
    let (Some(gid), Some(bid), Some(c), Some(r)) = (unsafe {
        (
            cstr(gid, "gid"),
            cstr(branch_id, "branch_id"),
            cstr(commit, "commit"),
            cstr(rollback, "rollback"),
        )
    }) else {
        return DTMRS_ERR;
    };
    let inner = h.tc.as_ref().unwrap();
    run(h, inner.register_xa_branch(gid, bid, c, r))
}

/// 二阶段消息的 prepare。`actions_json` 是要送达的分支地址数组：
///
/// ```json
/// ["local://add_points", "http://notify/send"]
/// ```
///
/// `query_prepared` **必填**：进程崩在本地事务和 submit 之间时，TC 靠它问
/// 「本地事务提交了没有」。回查 handler 返回 SUCCESS=已提交（继续发）、
/// FAILURE=没提交（作废），其它=不知道（过会儿再问）。
///
/// `grace_secs` 是 prepare 之后多久才开始回查，传负数用默认值（10 秒）。
///
/// 返回 OK 之后才能跑本地事务。之后：成功 → `dtmrs_submit`；明确失败 →
/// `dtmrs_abort`；不知道 → **什么都别调**，交给回查。
#[no_mangle]
pub extern "C" fn dtmrs_msg_prepare(
    tc: *mut DtmrsTc,
    gid: *const c_char,
    actions_json: *const c_char,
    query_prepared: *const c_char,
    grace_secs: c_int,
) -> c_int {
    clear_err();
    let Some(h) = started(tc) else {
        return DTMRS_ERR;
    };
    let (Some(gid), Some(js), Some(q)) = (unsafe {
        (
            cstr(gid, "gid"),
            cstr(actions_json, "actions_json"),
            cstr(query_prepared, "query_prepared"),
        )
    }) else {
        return DTMRS_ERR;
    };
    let actions: Vec<String> = match serde_json::from_str(js) {
        Ok(v) => v,
        Err(e) => {
            set_err(format!("actions_json 解析失败（要的是字符串数组）: {e}"));
            return DTMRS_ERR;
        }
    };
    let inner = h.tc.as_ref().unwrap();
    let mut b = inner.msg(gid).query_prepared(q);
    for a in &actions {
        b = b.action(a);
    }
    if grace_secs >= 0 {
        b = b.grace_secs(grace_secs as i64);
    }
    run(h, b.prepare())
}

/// 二阶段提交 tcc / xa / msg：一阶段全成功了，交给 TC 推。幂等。
#[no_mangle]
pub extern "C" fn dtmrs_submit(tc: *mut DtmrsTc, gid: *const c_char) -> c_int {
    clear_err();
    let Some(h) = started(tc) else {
        return DTMRS_ERR;
    };
    let Some(gid) = (unsafe { cstr(gid, "gid") }) else {
        return DTMRS_ERR;
    };
    let inner = h.tc.as_ref().unwrap();
    run(h, inner.submit(gid))
}

/// 主动中止：TC 逆序撤销**所有**已登记的分支。
///
/// ⚠ tcc / xa / msg **submit 之后不能 abort**，会返回 DTMRS_ERR ——
/// 方向已定，这时 abort 就是一半 confirm 一半 cancel。
#[no_mangle]
pub extern "C" fn dtmrs_abort(tc: *mut DtmrsTc, gid: *const c_char) -> c_int {
    clear_err();
    let Some(h) = started(tc) else {
        return DTMRS_ERR;
    };
    let Some(gid) = (unsafe { cstr(gid, "gid") }) else {
        return DTMRS_ERR;
    };
    let inner = h.tc.as_ref().unwrap();
    run(h, inner.abort(gid))
}

// ---------------- workflow ----------------
//
// workflow 的「步骤」是宿主的代码，所以是**回调里再调回来**：
//
//   推进器 ──► 宿主的 workflow 函数(wf, gid, input)
//                 ├─ dtmrs_wf_branch(wf, "建订单", "local://取消订单", 函数体, …)
//                 │     重放命中 → 不跑函数体，把上次的结果写进 out
//                 │     没命中   → 先登记补偿，再跑函数体，记下结果
//                 └─ dtmrs_wf_branch(wf, "扣款", …)
//
// workflow 函数跑在阻塞线程池上（spawn_blocking），dtmrs_wf_branch 在那条线程上
// block_on —— 阻塞线程不在异步上下文里，可以这么做。
//
// 只有回调式；**拉取式（Node）不支持 workflow**：函数体必须在 C 回调里同步跑完，
// 而 Node 的业务代码是 async 的，在同步回调里等不了 Promise。

/// 传给宿主 workflow 函数的不透明句柄，只在那次调用期间有效。
pub struct DtmrsWf {
    ctx: WorkflowCtx,
    rt: tokio::runtime::Handle,
    /// 第一次出错就记下来，之后的 `dtmrs_wf_branch` **一律不跑**、直接返回 ERR。
    ///
    /// 宿主可能没检查返回值（或者 Python 里一个裸 `except:` 把停止信号吞了），
    /// 接着去开下一个分支。那样的话：回滚时会多出一个本不该执行的分支；分岔时
    /// 会在已经对不上号的位置上继续执行。都得靠这里挡住，不能指望宿主自觉。
    err: Option<WorkflowError>,
}

/// 分支结果数据的缓冲区。库里那一列上限是 1024 **字符**，UTF-8 最多 4 字节一个
const WF_OUT_CAP: usize = 1024 * 4 + 1;

fn run_host_workflow(
    wp: WorkflowPtr,
    ctx: WorkflowCtx,
    rt: tokio::runtime::Handle,
) -> WorkflowResult<()> {
    let (Ok(gid), Ok(input)) = (
        CString::new(ctx.gid.as_str()),
        CString::new(ctx.input.as_str()),
    ) else {
        return Err(WorkflowError::Retry(
            "gid / input 含 NUL，C 回调传不了".into(),
        ));
    };
    let mut wf = DtmrsWf { ctx, rt, err: None };
    let code = (wp.f)(&mut wf, gid.as_ptr(), input.as_ptr(), wp.ud);
    // 分支出过错就以它为准，宿主的返回值不算数（见 WorkflowFn 的文档）
    if let Some(e) = wf.err {
        return Err(e);
    }
    match code {
        DTMRS_SUCCESS => Ok(()),
        DTMRS_FAILURE => Err(WorkflowError::Rollback("workflow 函数返回 FAILURE".into())),
        // ONGOING / UNKNOWN / 野值：都是重放，绝不回滚
        c => Err(WorkflowError::Retry(format!("workflow 函数返回 {c}"))),
    }
}

/// 注册一个 workflow 函数。必须在 `dtmrs_start` 之前调。
///
/// 跟 `local://` 分支同一个约束：库里存的是**名字**，重启后必须注册同名函数。
#[no_mangle]
pub extern "C" fn dtmrs_register_workflow(
    tc: *mut DtmrsTc,
    name: *const c_char,
    f: Option<WorkflowFn>,
    user_data: *mut c_void,
) -> c_int {
    clear_err();
    let Some(h) = (unsafe { tc.as_mut() }) else {
        set_err("句柄是空指针");
        return DTMRS_ERR;
    };
    let Some(name) = (unsafe { cstr(name, "name") }) else {
        return DTMRS_ERR;
    };
    let Some(f) = f else {
        set_err("workflow 函数是空指针");
        return DTMRS_ERR;
    };
    match h.pending_wf.as_mut() {
        Some(v) => {
            v.push((name.to_string(), WorkflowPtr { f, ud: user_data }));
            DTMRS_OK
        }
        None => {
            set_err("已经 start 了，不能再注册 workflow");
            DTMRS_ERR
        }
    }
}

/// 提交一个 workflow 事务。`name` 没注册会当场报错。`input` 原样传给函数。幂等。
#[no_mangle]
pub extern "C" fn dtmrs_submit_workflow(
    tc: *mut DtmrsTc,
    gid: *const c_char,
    name: *const c_char,
    input: *const c_char,
) -> c_int {
    clear_err();
    let Some(h) = started(tc) else {
        return DTMRS_ERR;
    };
    let (Some(gid), Some(name), Some(input)) =
        (unsafe { (cstr(gid, "gid"), cstr(name, "name"), cstr(input, "input")) })
    else {
        return DTMRS_ERR;
    };
    let inner = h.tc.as_ref().unwrap();
    run(h, inner.submit_workflow(gid, name, input))
}

/// 在 workflow 函数里开一个分支。**只能在 workflow 函数执行期间、用传进来的 `wf` 调。**
///
/// - `name`：分支的逻辑名字，用来做重放分岔检测。取稳定的名字，别带时间戳
/// - `compensate`：回滚时调的地址（`local://…` / `http://…`）；NULL 或空串 = 不补偿
///   （只适合没有副作用的步骤）
/// - `f`：函数体。重放时如果这个分支上次已经成功，**不会被调用**
/// - `out` / `out_len`：函数体写的（或上次记下的）结果数据写到这里；可以传 NULL
///
/// 返回 `DTMRS_OK` 表示分支成功（新跑的或重放命中的）。返回 `DTMRS_ERR` 表示
/// **workflow 要停在这里**（回滚 / 重试 / 分岔），原因在 `dtmrs_last_error()`。
/// 收到 ERR 应该立刻从 workflow 函数返回 —— 不返回的话之后的分支也一律不会执行。
#[no_mangle]
pub extern "C" fn dtmrs_wf_branch(
    wf: *mut DtmrsWf,
    name: *const c_char,
    compensate: *const c_char,
    f: Option<WfBranchFn>,
    user_data: *mut c_void,
    out: *mut c_char,
    out_len: usize,
) -> c_int {
    clear_err();
    let Some(wf) = (unsafe { wf.as_mut() }) else {
        set_err("wf 是空指针");
        return DTMRS_ERR;
    };
    if let Some(e) = &wf.err {
        set_err(format!("workflow 已经要停下了，不再执行新分支: {e}"));
        return DTMRS_ERR;
    }
    let Some(name) = (unsafe { cstr(name, "name") }) else {
        return DTMRS_ERR;
    };
    let compensate = if compensate.is_null() {
        ""
    } else {
        match unsafe { cstr(compensate, "compensate") } {
            Some(c) => c,
            None => return DTMRS_ERR,
        }
    };
    let Some(f) = f else {
        set_err("分支函数是空指针");
        return DTMRS_ERR;
    };

    // run_with 不把分支号交给函数体，按同样的规则先算出来（下一个序号）
    let bid = dtmrs_server::driver::branch_id(wf.ctx.branch_count());
    let (Ok(gid_c), Ok(bid_c)) = (
        CString::new(wf.ctx.gid.as_str()),
        CString::new(bid.as_str()),
    ) else {
        set_err("gid 含 NUL");
        return DTMRS_ERR;
    };
    let mut b = wf.ctx.branch(name);
    if !compensate.is_empty() {
        b = b.on_rollback(compensate);
    }
    let body = || async {
        let mut buf = vec![0u8; WF_OUT_CAP];
        let code = f(
            gid_c.as_ptr(),
            bid_c.as_ptr(),
            buf.as_mut_ptr() as *mut c_char,
            buf.len(),
            user_data,
        );
        // 宿主可能没写 \0 就写满了 —— 兜底截在最后一个字节
        *buf.last_mut().unwrap() = 0;
        let data = unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) }
            .to_string_lossy()
            .into_owned();
        (to_branch_result(code), data)
    };
    match wf.rt.block_on(b.run_with(body)) {
        Ok(data) => {
            if out.is_null() {
                return DTMRS_OK;
            }
            if write_out(&data, out, out_len) != DTMRS_OK {
                // 分支本身已经成功并记下了，只是宿主接不住结果。**重放，不回滚**：
                // 宿主改大缓冲区后重放会命中记忆化，不会重做
                let e = WorkflowError::Retry(format!(
                    "分支 {bid} 的结果写不进 out: {}",
                    last_err_string()
                ));
                set_err(e.to_string());
                wf.err = Some(e);
                return DTMRS_ERR;
            }
            DTMRS_OK
        }
        Err(e) => {
            set_err(e.to_string());
            wf.err = Some(e);
            DTMRS_ERR
        }
    }
}

fn last_err_string() -> String {
    LAST_ERR.with(|e| e.borrow().to_string_lossy().into_owned())
}

/// 查当前状态，写进 `out`（`prepared|submitted|aborting|succeed|failed`）。
/// gid 不存在返回 `DTMRS_ERR`。
#[no_mangle]
pub extern "C" fn dtmrs_status(
    tc: *mut DtmrsTc,
    gid: *const c_char,
    out: *mut c_char,
    out_len: usize,
) -> c_int {
    clear_err();
    let Some(h) = (unsafe { tc.as_mut() }) else {
        set_err("句柄是空指针");
        return DTMRS_ERR;
    };
    let Some(inner) = h.tc.as_ref() else {
        set_err("还没 start");
        return DTMRS_ERR;
    };
    let Some(gid) = (unsafe { cstr(gid, "gid") }) else {
        return DTMRS_ERR;
    };
    match h.rt.block_on(inner.status(gid)) {
        Ok(Some(s)) => write_out(s.as_str(), out, out_len),
        Ok(None) => {
            set_err("gid 不存在");
            DTMRS_ERR
        }
        Err(e) => {
            set_err(format!("查询失败: {e}"));
            DTMRS_ERR
        }
    }
}

/// 阻塞等到事务落终态。**只适合脚本和测试** —— 生产上事务是异步推进的。
#[no_mangle]
pub extern "C" fn dtmrs_wait_final(
    tc: *mut DtmrsTc,
    gid: *const c_char,
    timeout_ms: c_int,
    out: *mut c_char,
    out_len: usize,
) -> c_int {
    clear_err();
    let Some(h) = (unsafe { tc.as_mut() }) else {
        set_err("句柄是空指针");
        return DTMRS_ERR;
    };
    let Some(inner) = h.tc.as_ref() else {
        set_err("还没 start");
        return DTMRS_ERR;
    };
    let Some(gid) = (unsafe { cstr(gid, "gid") }) else {
        return DTMRS_ERR;
    };
    let d = Duration::from_millis(timeout_ms.max(0) as u64);
    match h.rt.block_on(inner.wait_final(gid, d)) {
        Ok(s) => write_out(s.as_str(), out, out_len),
        Err(e) => {
            set_err(format!("{e}"));
            DTMRS_ERR
        }
    }
}

/// 关闭并释放。之后句柄不可再用。
///
/// 未终结的事务留在库里 —— 下次 open+start 会自动接着推。
#[no_mangle]
pub extern "C" fn dtmrs_close(tc: *mut DtmrsTc) {
    if tc.is_null() {
        return;
    }
    let mut h = unsafe { Box::from_raw(tc) };
    // 必须在运行时还活着的时候把存储关干净，再析构运行时。
    // 光 drop 的话：Runtime 的 Drop 只等 tokio 任务，等不到 sqlx-sqlite 的连接线程，
    // close 返回时库文件还在被关 / 被 checkpoint，宿主立刻删目录会撞 ENOTEMPTY。
    if let Some(e) = h.tc.take() {
        h.rt.block_on(e.shutdown());
    }
    drop(h);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 拉取式：宿主自己取任务、自己回结果。
    ///
    /// 这条路存在的理由是 Node 那类宿主 —— 同步回调里没法 await。
    /// 这里用一个后台线程模拟宿主的事件循环。
    #[test]
    fn 拉取式能跑完一笔事务() {
        let db = format!("sqlite:/tmp/dtmrs_pull_{}.db", std::process::id());
        let _ = std::fs::remove_file(db.trim_start_matches("sqlite:"));
        let tc = dtmrs_open(cs(&db).as_ptr());
        assert!(!tc.is_null());

        assert_eq!(dtmrs_register_pull(tc, cs("act").as_ptr()), DTMRS_OK);
        assert_eq!(dtmrs_register_pull(tc, cs("undo").as_ptr()), DTMRS_OK);
        assert_eq!(dtmrs_start(tc), DTMRS_OK);

        // 模拟宿主的事件循环：非阻塞轮询 + 回填
        let addr = tc as usize;
        let worker = std::thread::spawn(move || {
            let tc = addr as *mut DtmrsTc;
            let mut buf = vec![0u8; 512];
            let mut done = 0;
            for _ in 0..600 {
                let r = dtmrs_next_task(tc, 0, buf.as_mut_ptr() as *mut c_char, buf.len());
                if r == 1 {
                    let s = unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) }
                        .to_str()
                        .unwrap()
                        .to_string();
                    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
                    let id = v["task_id"].as_u64().unwrap();
                    assert_eq!(v["name"], "act");
                    assert_eq!(v["op"], "action");
                    assert_eq!(v["branch_id"], "01");
                    assert_eq!(dtmrs_reply(tc, id, DTMRS_SUCCESS), DTMRS_OK);
                    done += 1;
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            done
        });

        let steps = cs(r#"[{"action":"local://act","compensate":"local://undo"}]"#);
        assert_eq!(
            dtmrs_submit_saga(tc, cs("pull-1").as_ptr(), steps.as_ptr()),
            DTMRS_OK
        );

        let mut out = vec![0u8; 64];
        assert_eq!(
            dtmrs_wait_final(
                tc,
                cs("pull-1").as_ptr(),
                8000,
                out.as_mut_ptr() as *mut c_char,
                64
            ),
            DTMRS_OK
        );
        let st = unsafe { CStr::from_ptr(out.as_ptr() as *const c_char) }
            .to_str()
            .unwrap();
        assert_eq!(st, "succeed");
        assert_eq!(worker.join().unwrap(), 1, "宿主应该正好取到 1 个任务");
        dtmrs_close(tc);
    }

    #[test]
    fn 没任务时立刻返回0而不是阻塞() {
        // 事件循环型宿主靠这个：传 0 必须马上回来，否则整个循环就卡住了
        let db = format!("sqlite:/tmp/dtmrs_pull_empty_{}.db", std::process::id());
        let _ = std::fs::remove_file(db.trim_start_matches("sqlite:"));
        let tc = dtmrs_open(cs(&db).as_ptr());
        assert_eq!(dtmrs_register_pull(tc, cs("a").as_ptr()), DTMRS_OK);
        assert_eq!(dtmrs_start(tc), DTMRS_OK);

        let mut buf = vec![0u8; 256];
        let t0 = std::time::Instant::now();
        let r = dtmrs_next_task(tc, 0, buf.as_mut_ptr() as *mut c_char, buf.len());
        assert_eq!(r, 0, "没任务应该返回 0");
        assert!(t0.elapsed() < Duration::from_millis(200), "不该阻塞");
        dtmrs_close(tc);
    }

    #[test]
    fn close返回时sqlite连接必须已经关完() {
        // 问题单 2026-09-26：close 只 drop 句柄，运行时先析构、连接池后丢且从没 close 过；
        // sqlx-sqlite 每条连接一个 OS 线程，drop 不等它 sqlite3_close。
        // 宿主「close 完立刻删目录」会撞上 -wal / -shm 被删了又重建，ENOTEMPTY。
        //
        // 判据：close 返回时目录里只剩主库（WAL 模式下最后一条连接关闭会 checkpoint
        // 并删掉 -wal / -shm），而且**之后不再变** —— 有没被等到的连接线程的话，
        // 它稍后关库时会删 / 建这两个文件。
        //
        // ⚠ 0ms 那档是关键：start 后立刻 close 时 16 个 worker 正在并发建连，
        // abort 掉 connect 中途的 future，sqlx-sqlite 的线程会自己异步关库，
        // Pool::close 根本不知道有这条连接（keel 复核 0cda1a4 时发现的，
        // 只测「sleep 30ms 再 close」测不出来）。
        // （不数 sqlx-sqlite 线程 —— 同进程并行的其它测试也在开 sqlite。）
        fn ls(dir: &std::path::Path) -> Vec<String> {
            let mut v: Vec<String> = std::fs::read_dir(dir)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            v.sort();
            v
        }
        let rounds: Vec<(u64, usize)> = [0u64, 1, 5, 30]
            .iter()
            .flat_map(|&ms| (0..15).map(move |i| (ms, i)))
            .collect();
        let mut dirs = Vec::new();
        for (ms, i) in rounds {
            let dir =
                std::env::temp_dir().join(format!("dtmrs_close_{}_{ms}_{i}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let db = format!("sqlite:{}/dtm.db", dir.display());
            let tc = dtmrs_open(cs(&db).as_ptr());
            assert!(!tc.is_null());
            assert_eq!(dtmrs_start(tc), DTMRS_OK);
            if ms > 0 {
                std::thread::sleep(Duration::from_millis(ms));
            }
            dtmrs_close(tc);
            let at_close = ls(&dir);
            assert_eq!(
                at_close,
                vec!["dtm.db".to_string()],
                "start 后 {ms}ms close（第 {i} 轮）：close 返回时还有没关完的连接"
            );
            dirs.push((ms, i, dir));
        }
        // 统一等一次，再核对每个目录都没被「迟到的关库」动过
        std::thread::sleep(Duration::from_millis(300));
        for (ms, i, dir) in dirs {
            assert_eq!(
                ls(&dir),
                vec!["dtm.db".to_string()],
                "start 后 {ms}ms close（第 {i} 轮）：close 返回后目录内容还在变"
            );
            std::fs::remove_dir_all(&dir).expect("close 返回后删目录必须成功");
        }
    }

    #[test]
    fn 回填不认识的码按未知处理() {
        // 宿主传了野值 —— 宁可重试，不可误回滚
        assert_eq!(to_branch_result(DTMRS_SUCCESS), BranchResult::Success);
        assert_eq!(to_branch_result(DTMRS_FAILURE), BranchResult::Failure);
        assert_eq!(to_branch_result(DTMRS_ONGOING), BranchResult::Ongoing);
        assert_eq!(to_branch_result(DTMRS_UNKNOWN), BranchResult::Unknown);
        assert_eq!(to_branch_result(42), BranchResult::Unknown);
        assert_eq!(to_branch_result(-7), BranchResult::Unknown);
    }

    #[test]
    fn 回填不存在的task_id会报错() {
        let db = format!("sqlite:/tmp/dtmrs_pull_bad_{}.db", std::process::id());
        let _ = std::fs::remove_file(db.trim_start_matches("sqlite:"));
        let tc = dtmrs_open(cs(&db).as_ptr());
        assert_eq!(dtmrs_start(tc), DTMRS_OK);
        // 编的 id / 已超时的 id 都该被拒，而不是静默吞掉
        assert_eq!(dtmrs_reply(tc, 999, DTMRS_SUCCESS), DTMRS_ERR);
        assert_eq!(
            dtmrs_reply(std::ptr::null_mut(), 1, DTMRS_SUCCESS),
            DTMRS_ERR
        );
        assert_eq!(
            dtmrs_register_pull(std::ptr::null_mut(), cs("x").as_ptr()),
            DTMRS_ERR
        );
        dtmrs_close(tc);
    }

    extern "C" fn ok_handler(
        _g: *const c_char,
        _b: *const c_char,
        _o: *const c_char,
        ud: *mut c_void,
    ) -> c_int {
        if !ud.is_null() {
            unsafe { *(ud as *mut c_int) += 1 };
        }
        DTMRS_SUCCESS
    }

    extern "C" fn fail_handler(
        _g: *const c_char,
        _b: *const c_char,
        _o: *const c_char,
        _ud: *mut c_void,
    ) -> c_int {
        DTMRS_FAILURE
    }

    /// 返回一个不认识的码，必须被当成 UNKNOWN（只重试，不回滚）
    extern "C" fn bogus_handler(
        _g: *const c_char,
        _b: *const c_char,
        _o: *const c_char,
        ud: *mut c_void,
    ) -> c_int {
        if !ud.is_null() {
            unsafe { *(ud as *mut c_int) += 1 };
        }
        999
    }

    fn db(name: &str) -> (CString, std::path::PathBuf) {
        let p = std::env::temp_dir().join(format!("dtmrs_ffi_{}_{}.db", name, std::process::id()));
        let _ = std::fs::remove_file(&p);
        (CString::new(format!("sqlite:{}", p.display())).unwrap(), p)
    }

    fn cs(s: &str) -> CString {
        CString::new(s).unwrap()
    }

    #[test]
    fn c接口跑通一个saga() {
        let (url, path) = db("happy");
        let tc = dtmrs_open(url.as_ptr());
        assert!(!tc.is_null());
        let mut calls: c_int = 0;
        let ud = &mut calls as *mut c_int as *mut c_void;
        assert_eq!(
            dtmrs_register(tc, cs("a1").as_ptr(), Some(ok_handler), ud),
            DTMRS_OK
        );
        assert_eq!(
            dtmrs_register(
                tc,
                cs("c1").as_ptr(),
                Some(ok_handler),
                std::ptr::null_mut()
            ),
            DTMRS_OK
        );
        assert_eq!(dtmrs_start(tc), DTMRS_OK);

        let steps = cs(r#"[{"action":"local://a1","compensate":"local://c1"}]"#);
        assert_eq!(
            dtmrs_submit_saga(tc, cs("ffi-1").as_ptr(), steps.as_ptr()),
            DTMRS_OK
        );

        let mut buf = [0i8; 32];
        assert_eq!(
            dtmrs_wait_final(
                tc,
                cs("ffi-1").as_ptr(),
                5000,
                buf.as_mut_ptr() as *mut c_char,
                32
            ),
            DTMRS_OK
        );
        let s = unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) }
            .to_str()
            .unwrap();
        assert_eq!(s, "succeed");
        assert_eq!(calls, 1, "handler 被调一次");
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口的失败会触发补偿() {
        let (url, path) = db("rb");
        let tc = dtmrs_open(url.as_ptr());
        let mut comp: c_int = 0;
        let ud = &mut comp as *mut c_int as *mut c_void;
        dtmrs_register(
            tc,
            cs("a1").as_ptr(),
            Some(fail_handler),
            std::ptr::null_mut(),
        );
        dtmrs_register(tc, cs("c1").as_ptr(), Some(ok_handler), ud);
        assert_eq!(dtmrs_start(tc), DTMRS_OK);
        let steps = cs(r#"[{"action":"local://a1","compensate":"local://c1"}]"#);
        dtmrs_submit_saga(tc, cs("ffi-2").as_ptr(), steps.as_ptr());

        let mut buf = [0i8; 32];
        dtmrs_wait_final(
            tc,
            cs("ffi-2").as_ptr(),
            5000,
            buf.as_mut_ptr() as *mut c_char,
            32,
        );
        let s = unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) }
            .to_str()
            .unwrap();
        assert_eq!(s, "failed");
        assert_eq!(comp, 1, "补偿被调一次");
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn 不认识的返回码按未知处理而不是失败() {
        // 宿主传了个野值。如果被当成 FAILURE，一笔本该成功的事务就被回滚了
        let (url, path) = db("bogus");
        let tc = dtmrs_open(url.as_ptr());
        let mut n: c_int = 0;
        let ud = &mut n as *mut c_int as *mut c_void;
        dtmrs_register(tc, cs("a1").as_ptr(), Some(bogus_handler), ud);
        dtmrs_register(
            tc,
            cs("c1").as_ptr(),
            Some(ok_handler),
            std::ptr::null_mut(),
        );
        dtmrs_start(tc);
        let steps = cs(r#"[{"action":"local://a1","compensate":"local://c1"}]"#);
        dtmrs_submit_saga(tc, cs("ffi-3").as_ptr(), steps.as_ptr());

        std::thread::sleep(Duration::from_millis(500));
        let mut buf = [0i8; 32];
        assert_eq!(
            dtmrs_status(
                tc,
                cs("ffi-3").as_ptr(),
                buf.as_mut_ptr() as *mut c_char,
                32
            ),
            DTMRS_OK
        );
        let s = unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) }
            .to_str()
            .unwrap();
        assert_eq!(s, "submitted", "野值不能触发回滚，只能重试");
        assert!(n >= 1);
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    /// 记下 ex 回调收到的 (branch_id-op, payload)
    extern "C" fn record_ex_handler(
        _g: *const c_char,
        b: *const c_char,
        o: *const c_char,
        p: *const c_char,
        ud: *mut c_void,
    ) -> c_int {
        let s = |x: *const c_char| unsafe { CStr::from_ptr(x) }.to_str().unwrap().to_string();
        let seen = unsafe { &*(ud as *const Mutex<Vec<(String, String)>>) };
        seen.lock()
            .unwrap()
            .push((format!("{}-{}", s(b), s(o)), s(p)));
        DTMRS_SUCCESS
    }

    #[test]
    fn c接口提交的每步payload不能被丢掉() {
        // 原先 dtmrs_submit_saga 解析出 payload 之后调的是 step() 而不是
        // step_with()，JSON 里写了也被静默丢掉，分支永远收到 {}
        let (url, path) = db("payload");
        let tc = dtmrs_open(url.as_ptr());
        let seen: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());
        let ud = &seen as *const _ as *mut c_void;
        assert_eq!(
            dtmrs_register_ex(tc, cs("a").as_ptr(), Some(record_ex_handler), ud),
            DTMRS_OK
        );
        assert_eq!(
            dtmrs_register_ex(tc, cs("c").as_ptr(), Some(record_ex_handler), ud),
            DTMRS_OK
        );
        assert_eq!(dtmrs_start(tc), DTMRS_OK);
        let steps = cs(
            r#"[{"action":"local://a","compensate":"local://c","payload":"{\"amount\":30}"},
                           {"action":"local://a","compensate":"local://c"}]"#,
        );
        assert_eq!(
            dtmrs_submit_saga(tc, cs("ffi-payload").as_ptr(), steps.as_ptr()),
            DTMRS_OK
        );
        assert_eq!(wait(tc, "ffi-payload"), "succeed");
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                ("01-action".to_string(), r#"{"amount":30}"#.to_string()),
                // 没写 payload 的那步收到空串
                ("02-action".to_string(), String::new()),
            ]
        );
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn 拉取式的任务里带着payload() {
        let (url, path) = db("pull_payload");
        let tc = dtmrs_open(url.as_ptr());
        assert_eq!(dtmrs_register_pull(tc, cs("act").as_ptr()), DTMRS_OK);
        assert_eq!(dtmrs_register_pull(tc, cs("undo").as_ptr()), DTMRS_OK);
        assert_eq!(dtmrs_start(tc), DTMRS_OK);
        let steps =
            cs(r#"[{"action":"local://act","compensate":"local://undo","payload":"订单-7"}]"#);
        assert_eq!(
            dtmrs_submit_saga(tc, cs("pull-payload").as_ptr(), steps.as_ptr()),
            DTMRS_OK
        );
        let mut buf = vec![0u8; 512];
        let r = dtmrs_next_task(tc, 5000, buf.as_mut_ptr() as *mut c_char, buf.len());
        assert_eq!(r, 1, "应该取到任务");
        let v: serde_json::Value = serde_json::from_str(
            unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) }
                .to_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(v["payload"], "订单-7");
        dtmrs_reply(tc, v["task_id"].as_u64().unwrap(), DTMRS_SUCCESS);
        assert_eq!(wait(tc, "pull-payload"), "succeed");
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    /// 等终态，返回状态字符串。等不到返回 last_error，让断言信息有用
    fn wait(tc: *mut DtmrsTc, gid: &str) -> String {
        let mut buf = [0u8; 64];
        if dtmrs_wait_final(
            tc,
            cs(gid).as_ptr(),
            8000,
            buf.as_mut_ptr() as *mut c_char,
            64,
        ) != DTMRS_OK
        {
            return format!(
                "ERR: {}",
                unsafe { CStr::from_ptr(dtmrs_last_error()) }
                    .to_str()
                    .unwrap()
            );
        }
        unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) }
            .to_str()
            .unwrap()
            .to_string()
    }

    /// 调用记录，形如 `"confirm@01"`，按调用顺序
    type Log = Arc<Mutex<Vec<String>>>;

    /// 按 (tag@分支号) 记下调用顺序；tag 由 user_data 指向的 Rec 给
    struct Rec {
        tag: &'static str,
        ret: c_int,
        log: Log,
    }

    extern "C" fn rec_handler(
        _g: *const c_char,
        b: *const c_char,
        _o: *const c_char,
        _p: *const c_char,
        ud: *mut c_void,
    ) -> c_int {
        let r = unsafe { &*(ud as *const Rec) };
        let b = unsafe { CStr::from_ptr(b) }.to_str().unwrap();
        r.log.lock().unwrap().push(format!("{}@{b}", r.tag));
        r.ret
    }

    /// 开一个 TC，按 (名字, 返回码) 注册一组记录型 handler
    ///
    /// Box 不是多余的：user_data 存的是 Rec 的地址，Vec 扩容会把裸 Rec 搬走，
    /// 回调拿到的就是野指针
    #[allow(clippy::vec_box)]
    fn tc_with(
        name: &str,
        hs: &[(&'static str, c_int)],
    ) -> (*mut DtmrsTc, Log, Vec<Box<Rec>>, std::path::PathBuf) {
        let (url, path) = db(name);
        let tc = dtmrs_open(url.as_ptr());
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut keep = Vec::new();
        for (n, ret) in hs {
            let r = Box::new(Rec {
                tag: n,
                ret: *ret,
                log: log.clone(),
            });
            let ud = &*r as *const Rec as *mut c_void;
            assert_eq!(
                dtmrs_register_ex(tc, cs(n).as_ptr(), Some(rec_handler), ud),
                DTMRS_OK
            );
            keep.push(r);
        }
        assert_eq!(dtmrs_start(tc), DTMRS_OK);
        (tc, log, keep, path)
    }

    fn last_err() -> String {
        unsafe { CStr::from_ptr(dtmrs_last_error()) }
            .to_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn c接口跑通tcc() {
        let (tc, log, _k, path) = tc_with(
            "tcc",
            &[("confirm", DTMRS_SUCCESS), ("cancel", DTMRS_SUCCESS)],
        );
        let gid = cs("ffi-tcc");
        assert_eq!(dtmrs_tcc_begin(tc, gid.as_ptr()), DTMRS_OK);
        assert_eq!(dtmrs_tcc_begin(tc, gid.as_ptr()), DTMRS_OK, "begin 要幂等");
        for bid in ["01", "02"] {
            assert_eq!(
                dtmrs_tcc_register(
                    tc,
                    gid.as_ptr(),
                    cs(bid).as_ptr(),
                    cs("local://confirm").as_ptr(),
                    cs("local://cancel").as_ptr()
                ),
                DTMRS_OK,
                "{}",
                last_err()
            );
            // （宿主在这里跑 try）
        }
        assert_eq!(dtmrs_submit(tc, gid.as_ptr()), DTMRS_OK);
        assert_eq!(wait(tc, "ffi-tcc"), "succeed");
        assert_eq!(*log.lock().unwrap(), ["confirm@01", "confirm@02"]);
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口tcc_abort后逆序cancel每个分支() {
        let (tc, log, _k, path) = tc_with(
            "tcc_rb",
            &[("confirm", DTMRS_SUCCESS), ("cancel", DTMRS_SUCCESS)],
        );
        let gid = cs("ffi-tcc-rb");
        dtmrs_tcc_begin(tc, gid.as_ptr());
        for bid in ["01", "02"] {
            dtmrs_tcc_register(
                tc,
                gid.as_ptr(),
                cs(bid).as_ptr(),
                cs("local://confirm").as_ptr(),
                cs("local://cancel").as_ptr(),
            );
        }
        // 第 2 个 try 失败了
        assert_eq!(dtmrs_abort(tc, gid.as_ptr()), DTMRS_OK);
        assert_eq!(wait(tc, "ffi-tcc-rb"), "failed");
        assert_eq!(*log.lock().unwrap(), ["cancel@02", "cancel@01"]);
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口tcc_submit之后不能abort_confirm失败也绝不cancel() {
        let (tc, log, _k, path) = tc_with(
            "tcc_cf",
            &[("confirm", DTMRS_FAILURE), ("cancel", DTMRS_SUCCESS)],
        );
        let gid = cs("ffi-tcc-cf");
        dtmrs_tcc_begin(tc, gid.as_ptr());
        dtmrs_tcc_register(
            tc,
            gid.as_ptr(),
            cs("01").as_ptr(),
            cs("local://confirm").as_ptr(),
            cs("local://cancel").as_ptr(),
        );
        assert_eq!(dtmrs_submit(tc, gid.as_ptr()), DTMRS_OK);
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(
            dtmrs_abort(tc, gid.as_ptr()),
            DTMRS_ERR,
            "已 submit 必须拒绝 abort"
        );
        assert!(last_err().contains("submit"), "{}", last_err());
        std::thread::sleep(Duration::from_millis(200));
        let l = log.lock().unwrap().clone();
        assert!(
            l.iter().all(|s| s.starts_with("confirm@")),
            "confirm 失败绝不能转 cancel: {l:?}"
        );
        assert!(!l.is_empty());
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口tcc的登记错误都要报出来() {
        let (tc, _log, _k, path) = tc_with(
            "tcc_err",
            &[("confirm", DTMRS_SUCCESS), ("cancel", DTMRS_SUCCESS)],
        );
        let reg = |gid: &str, bid: &str, c: &str| {
            dtmrs_tcc_register(
                tc,
                cs(gid).as_ptr(),
                cs(bid).as_ptr(),
                cs(c).as_ptr(),
                cs("local://cancel").as_ptr(),
            )
        };
        // 没 begin
        assert_eq!(reg("ffi-nobegin", "01", "local://confirm"), DTMRS_ERR);
        dtmrs_tcc_begin(tc, cs("ffi-tcc-err").as_ptr());
        // 分支号格式不对（会让推进器把事务当成空事务直接判成功）
        assert_eq!(
            reg("ffi-tcc-err", "inventory", "local://confirm"),
            DTMRS_ERR
        );
        // 漏注册的 handler
        assert_eq!(reg("ffi-tcc-err", "01", "local://没注册"), DTMRS_ERR);
        assert!(last_err().contains("没注册"), "{}", last_err());
        // 撞号：同号不同地址
        assert_eq!(reg("ffi-tcc-err", "01", "local://confirm"), DTMRS_OK);
        assert_eq!(
            reg("ffi-tcc-err", "01", "local://confirm"),
            DTMRS_OK,
            "原样重试要幂等"
        );
        assert_eq!(
            reg("ffi-tcc-err", "01", "local://cancel"),
            DTMRS_ERR,
            "撞号必须报错"
        );
        // 空指针
        assert_eq!(
            dtmrs_tcc_begin(std::ptr::null_mut(), cs("x").as_ptr()),
            DTMRS_ERR
        );
        assert_eq!(dtmrs_tcc_begin(tc, std::ptr::null()), DTMRS_ERR);
        assert_eq!(
            dtmrs_tcc_register(
                tc,
                cs("x").as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null()
            ),
            DTMRS_ERR
        );
        assert_eq!(dtmrs_submit(tc, std::ptr::null()), DTMRS_ERR);
        assert_eq!(
            dtmrs_abort(std::ptr::null_mut(), cs("x").as_ptr()),
            DTMRS_ERR
        );
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口没start就调二阶段接口会报错而不是崩() {
        let (url, path) = db("nostart");
        let tc = dtmrs_open(url.as_ptr());
        assert_eq!(dtmrs_tcc_begin(tc, cs("x").as_ptr()), DTMRS_ERR);
        assert_eq!(dtmrs_xa_begin(tc, cs("x").as_ptr()), DTMRS_ERR);
        assert_eq!(dtmrs_submit(tc, cs("x").as_ptr()), DTMRS_ERR);
        assert!(last_err().contains("start"), "{}", last_err());
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口跑通xa_commit和rollback() {
        let (tc, log, _k, path) = tc_with(
            "xa",
            &[("commit", DTMRS_SUCCESS), ("rollback", DTMRS_SUCCESS)],
        );
        for (gid, ok, want, calls) in [
            ("ffi-xa-ok", true, "succeed", vec!["commit@01", "commit@02"]),
            (
                "ffi-xa-rb",
                false,
                "failed",
                vec!["rollback@02", "rollback@01"],
            ),
        ] {
            log.lock().unwrap().clear();
            let g = cs(gid);
            assert_eq!(dtmrs_xa_begin(tc, g.as_ptr()), DTMRS_OK);
            for bid in ["01", "02"] {
                assert_eq!(
                    dtmrs_xa_register(
                        tc,
                        g.as_ptr(),
                        cs(bid).as_ptr(),
                        cs("local://commit").as_ptr(),
                        cs("local://rollback").as_ptr()
                    ),
                    DTMRS_OK,
                    "{}",
                    last_err()
                );
            }
            let r = if ok {
                dtmrs_submit(tc, g.as_ptr())
            } else {
                dtmrs_abort(tc, g.as_ptr())
            };
            assert_eq!(r, DTMRS_OK);
            assert_eq!(wait(tc, gid), want);
            assert_eq!(*log.lock().unwrap(), calls, "{gid}");
        }
        // XA 分支不能拿 tcc 的接口登记（缺 commit/rollback 会留下永久持锁的 prepared）
        dtmrs_xa_begin(tc, cs("ffi-xa-wrong").as_ptr());
        assert_eq!(
            dtmrs_tcc_register(
                tc,
                cs("ffi-xa-wrong").as_ptr(),
                cs("01").as_ptr(),
                cs("local://commit").as_ptr(),
                cs("local://rollback").as_ptr()
            ),
            DTMRS_ERR
        );
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口跑通msg() {
        let (tc, log, _k, path) = tc_with("msg", &[("act", DTMRS_SUCCESS), ("q", DTMRS_SUCCESS)]);
        let g = cs("ffi-msg");
        let actions = cs(r#"["local://act","local://act"]"#);
        assert_eq!(
            dtmrs_msg_prepare(
                tc,
                g.as_ptr(),
                actions.as_ptr(),
                cs("local://q").as_ptr(),
                -1
            ),
            DTMRS_OK,
            "{}",
            last_err()
        );
        // （宿主在这里提交本地事务）
        assert_eq!(dtmrs_submit(tc, g.as_ptr()), DTMRS_OK);
        assert_eq!(wait(tc, "ffi-msg"), "succeed");
        assert_eq!(*log.lock().unwrap(), ["act@01", "act@02"]);
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口msg_宿主不知道本地事务结果时靠回查决断() {
        // 宿主崩在本地事务和 submit 之间 —— 这里就是 prepare 完什么都不调
        let (tc, log, _k, path) = tc_with("msg_q", &[("act", DTMRS_SUCCESS), ("q", DTMRS_SUCCESS)]);
        let g = cs("ffi-msg-q");
        assert_eq!(
            dtmrs_msg_prepare(
                tc,
                g.as_ptr(),
                cs(r#"["local://act"]"#).as_ptr(),
                cs("local://q").as_ptr(),
                0
            ),
            DTMRS_OK
        );
        assert_eq!(wait(tc, "ffi-msg-q"), "succeed");
        assert_eq!(*log.lock().unwrap(), ["q@00", "act@01"], "先回查、再送达");
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口msg的参数错误都要报出来() {
        let (tc, _log, _k, path) =
            tc_with("msg_err", &[("act", DTMRS_SUCCESS), ("q", DTMRS_SUCCESS)]);
        let p = |a: &str, q: &str| {
            dtmrs_msg_prepare(
                tc,
                cs("ffi-msg-err").as_ptr(),
                cs(a).as_ptr(),
                cs(q).as_ptr(),
                -1,
            )
        };
        assert_eq!(p(r#"{"a":1}"#, "local://q"), DTMRS_ERR, "不是数组");
        assert_eq!(p("[]", "local://q"), DTMRS_ERR, "没有消息");
        assert_eq!(
            p(r#"["local://act"]"#, ""),
            DTMRS_ERR,
            "没有回查地址就没法决断"
        );
        assert_eq!(p(r#"["local://没注册"]"#, "local://q"), DTMRS_ERR);
        assert_eq!(p(r#"["local://act"]"#, "local://q"), DTMRS_OK);
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    // ---------------- workflow ----------------

    /// 宿主 workflow 函数的状态（user_data 指向它）
    #[derive(Default)]
    struct Wf {
        runs: AtomicU64,
        /// 每个分支函数体被真正执行的次数，按分支号下标
        body: [AtomicU64; 3],
        /// 第几种剧本
        mode: u8,
        /// 宿主看到的东西：分支 out、input、dtmrs_last_error
        seen: Mutex<Vec<String>>,
    }

    fn wf_of(ud: *mut c_void) -> &'static Wf {
        unsafe { &*(ud as *const Wf) }
    }

    extern "C" fn body_ok(
        _g: *const c_char,
        b: *const c_char,
        out: *mut c_char,
        len: usize,
        ud: *mut c_void,
    ) -> c_int {
        let w = wf_of(ud);
        let b = unsafe { CStr::from_ptr(b) }.to_str().unwrap();
        let i: usize = b.parse::<usize>().unwrap() - 1;
        w.body[i].fetch_add(1, Ordering::SeqCst);
        write_out(&format!("结果-{b}"), out, len);
        DTMRS_SUCCESS
    }

    extern "C" fn body_fail(
        _g: *const c_char,
        _b: *const c_char,
        _o: *mut c_char,
        _l: usize,
        ud: *mut c_void,
    ) -> c_int {
        wf_of(ud).body[1].fetch_add(1, Ordering::SeqCst);
        DTMRS_FAILURE
    }

    /// 第一次结果未知，之后成功
    extern "C" fn body_flaky(
        g: *const c_char,
        b: *const c_char,
        o: *mut c_char,
        l: usize,
        ud: *mut c_void,
    ) -> c_int {
        let w = wf_of(ud);
        if w.body[1].load(Ordering::SeqCst) == 0 {
            w.body[1].fetch_add(1, Ordering::SeqCst);
            return DTMRS_UNKNOWN;
        }
        body_ok(g, b, o, l, ud)
    }

    fn wf_branch(
        wf: *mut DtmrsWf,
        name: &str,
        f: WfBranchFn,
        ud: *mut c_void,
    ) -> Result<String, String> {
        let mut out = [0u8; 256];
        let r = dtmrs_wf_branch(
            wf,
            cs(name).as_ptr(),
            cs("local://undo").as_ptr(),
            Some(f),
            ud,
            out.as_mut_ptr() as *mut c_char,
            out.len(),
        );
        if r == DTMRS_OK {
            Ok(unsafe { CStr::from_ptr(out.as_ptr() as *const c_char) }
                .to_str()
                .unwrap()
                .to_string())
        } else {
            Err(last_err())
        }
    }

    extern "C" fn host_workflow(
        wf: *mut DtmrsWf,
        _gid: *const c_char,
        input: *const c_char,
        ud: *mut c_void,
    ) -> c_int {
        let w = wf_of(ud);
        let run = w.runs.fetch_add(1, Ordering::SeqCst);
        let seen = |s: String| w.seen.lock().unwrap().push(s);
        match w.mode {
            // 正常：两步，第一步的结果拿得到
            0 => {
                seen(format!(
                    "input={}",
                    unsafe { CStr::from_ptr(input) }.to_str().unwrap()
                ));
                let a = wf_branch(wf, "建订单", body_ok, ud).unwrap();
                seen(format!("out={a}"));
                wf_branch(wf, "扣款", body_ok, ud).unwrap();
                DTMRS_SUCCESS
            }
            // 第二步失败 → 宿主**不理会**错误继续开第三步，还返回 SUCCESS
            1 => {
                wf_branch(wf, "建订单", body_ok, ud).unwrap();
                if let Err(e) = wf_branch(wf, "扣款", body_fail, ud) {
                    seen(format!("err={e}"));
                }
                if let Err(e) = wf_branch(wf, "发货", body_ok, ud) {
                    seen(format!("err3={e}"));
                }
                DTMRS_SUCCESS
            }
            // 第二步第一次结果未知 → 重放
            2 => {
                wf_branch(wf, "建订单", body_ok, ud).unwrap();
                match wf_branch(wf, "扣款", body_flaky, ud) {
                    Ok(_) => DTMRS_SUCCESS,
                    Err(_) => DTMRS_SUCCESS, // 返回值不算数，以分支的错误为准
                }
            }
            // 不确定的函数：第一次走 A，重放时走 B
            3 => {
                let name = if run == 0 { "A" } else { "B" };
                match wf_branch(wf, name, body_ok, ud) {
                    Ok(_) if run == 0 => DTMRS_UNKNOWN, // 逼它重放
                    Ok(_) => DTMRS_SUCCESS,
                    Err(e) => {
                        seen(format!("err={e}"));
                        DTMRS_SUCCESS // 同样不算数
                    }
                }
            }
            // 野值
            _ => {
                wf_branch(wf, "建订单", body_ok, ud).unwrap();
                42
            }
        }
    }

    /// 开 TC、注册 undo 补偿和一个 workflow，提交一笔。Box 的理由同 `tc_with`
    #[allow(clippy::vec_box)]
    fn wf_run(
        name: &str,
        mode: u8,
    ) -> (
        *mut DtmrsTc,
        Box<Wf>,
        Log,
        Vec<Box<Rec>>,
        std::path::PathBuf,
    ) {
        let (url, path) = db(name);
        let tc = dtmrs_open(url.as_ptr());
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let undo = Box::new(Rec {
            tag: "undo",
            ret: DTMRS_SUCCESS,
            log: log.clone(),
        });
        dtmrs_register_ex(
            tc,
            cs("undo").as_ptr(),
            Some(rec_handler),
            &*undo as *const Rec as *mut c_void,
        );
        let w = Box::new(Wf {
            mode,
            ..Default::default()
        });
        assert_eq!(
            dtmrs_register_workflow(
                tc,
                cs("下单").as_ptr(),
                Some(host_workflow),
                &*w as *const Wf as *mut c_void
            ),
            DTMRS_OK
        );
        assert_eq!(dtmrs_start(tc), DTMRS_OK);
        assert_eq!(
            dtmrs_submit_workflow(
                tc,
                cs(name).as_ptr(),
                cs("下单").as_ptr(),
                cs("订单-7").as_ptr()
            ),
            DTMRS_OK,
            "{}",
            last_err()
        );
        (tc, w, log, vec![undo], path)
    }

    fn wait_ms(tc: *mut DtmrsTc, gid: &str, ms: c_int) -> String {
        let mut buf = [0u8; 64];
        dtmrs_wait_final(
            tc,
            cs(gid).as_ptr(),
            ms,
            buf.as_mut_ptr() as *mut c_char,
            64,
        );
        let mut st = [0u8; 64];
        dtmrs_status(tc, cs(gid).as_ptr(), st.as_mut_ptr() as *mut c_char, 64);
        unsafe { CStr::from_ptr(st.as_ptr() as *const c_char) }
            .to_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn c接口跑通workflow_分支结果能拿到() {
        let (tc, w, log, _k, path) = wf_run("wf-ok", 0);
        assert_eq!(wait_ms(tc, "wf-ok", 8000), "succeed");
        assert_eq!(*w.seen.lock().unwrap(), ["input=订单-7", "out=结果-01"]);
        assert_eq!(w.body[0].load(Ordering::SeqCst), 1);
        assert_eq!(w.body[1].load(Ordering::SeqCst), 1);
        assert!(log.lock().unwrap().is_empty(), "成功了不该补偿");
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口workflow分支失败则逆序补偿_宿主不理会错误也开不了新分支() {
        let (tc, w, log, _k, path) = wf_run("wf-rb", 1);
        assert_eq!(
            wait_ms(tc, "wf-rb", 8000),
            "failed",
            "宿主返回 SUCCESS 不算数"
        );
        // 失败的那步也补：它的补偿在动作之前就登记了，动作可能做了一半
        assert_eq!(*log.lock().unwrap(), ["undo@02", "undo@01"]);
        assert_eq!(w.body[2].load(Ordering::SeqCst), 0, "第三步绝不能执行");
        let seen = w.seen.lock().unwrap().clone();
        assert!(
            seen.iter()
                .any(|s| s.starts_with("err=") && s.contains("FAILURE")),
            "{seen:?}"
        );
        assert!(
            seen.iter()
                .any(|s| s.starts_with("err3=") && s.contains("停下")),
            "{seen:?}"
        );
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口workflow重放时已成功的分支不重跑() {
        // 第一次跑到第二步结果未知 → 退避（默认 10 秒）后重放。这条要等十来秒
        let (tc, w, log, _k, path) = wf_run("wf-replay", 2);
        assert_eq!(wait_ms(tc, "wf-replay", 20000), "succeed");
        assert_eq!(w.runs.load(Ordering::SeqCst), 2, "函数被从头跑了两次");
        assert_eq!(
            w.body[0].load(Ordering::SeqCst),
            1,
            "第一步重放时命中记忆化，不重做"
        );
        assert_eq!(w.body[1].load(Ordering::SeqCst), 2);
        assert!(log.lock().unwrap().is_empty(), "结果未知绝不能触发补偿");
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口workflow重放走岔时停下_既不成功也不回滚() {
        let (tc, w, log, _k, path) = wf_run("wf-diverge", 3);
        // 等到第二次跑（退避 10 秒）并且确认它停住了
        let t0 = std::time::Instant::now();
        while w.runs.load(Ordering::SeqCst) < 2 && t0.elapsed() < Duration::from_secs(20) {
            std::thread::sleep(Duration::from_millis(50));
        }
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(
            wait_ms(tc, "wf-diverge", 0),
            "submitted",
            "分岔了不能判成功（宿主返回 SUCCESS 也不行）"
        );
        assert!(log.lock().unwrap().is_empty(), "分岔了也不能回滚");
        let seen = w.seen.lock().unwrap().clone();
        assert!(seen.iter().any(|s| s.contains("走岔")), "{seen:?}");
        assert_eq!(
            w.body[0].load(Ordering::SeqCst),
            1,
            "B 不能在 A 的位置上执行"
        );
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口workflow函数返回野值只重放不回滚() {
        let (tc, w, log, _k, path) = wf_run("wf-bogus", 9);
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(wait_ms(tc, "wf-bogus", 0), "submitted");
        assert!(w.runs.load(Ordering::SeqCst) >= 1);
        assert!(log.lock().unwrap().is_empty());
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn c接口workflow的错误路径() {
        let (url, path) = db("wf_err");
        let tc = dtmrs_open(url.as_ptr());
        assert_eq!(
            dtmrs_register_workflow(tc, cs("w").as_ptr(), None, std::ptr::null_mut()),
            DTMRS_ERR
        );
        assert_eq!(
            dtmrs_submit_workflow(tc, cs("g").as_ptr(), cs("w").as_ptr(), cs("").as_ptr()),
            DTMRS_ERR,
            "没 start"
        );
        assert_eq!(dtmrs_start(tc), DTMRS_OK);
        assert_eq!(
            dtmrs_register_workflow(
                tc,
                cs("w").as_ptr(),
                Some(host_workflow),
                std::ptr::null_mut()
            ),
            DTMRS_ERR,
            "start 之后不能再注册"
        );
        assert_eq!(
            dtmrs_submit_workflow(tc, cs("g").as_ptr(), cs("没注册").as_ptr(), cs("").as_ptr()),
            DTMRS_ERR
        );
        assert!(last_err().contains("没注册"), "{}", last_err());
        assert_eq!(
            dtmrs_wf_branch(
                std::ptr::null_mut(),
                cs("a").as_ptr(),
                std::ptr::null(),
                Some(body_ok),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0
            ),
            DTMRS_ERR
        );
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    /// C 接口对着真 Redis 跑：saga（带 payload）、TCC、msg 回查。
    ///
    /// 原先 dtmrs-ffi 没开 redis feature，dtmrs_open("redis://…") 直接报错 ——
    /// 所有语言绑定都用不了 Redis 后端。用自己的 key_prefix，跟别的测试互不干扰。
    #[cfg(feature = "redis")]
    #[test]
    fn c接口能跑在redis后端上() {
        let Ok(base) = std::env::var("DTMRS_TEST_REDIS") else {
            if std::env::var("DTMRS_TEST_REQUIRE_REAL_DB").is_ok() {
                panic!("设了 DTMRS_TEST_REQUIRE_REAL_DB，却没有 DTMRS_TEST_REDIS");
            }
            eprintln!("\n⚠ 跳过 FFI 的 Redis 测试：DTMRS_TEST_REDIS 没配。这不等于通过。\n");
            return;
        };
        let sep = if base.contains('?') { '&' } else { '?' };
        let url = format!("{base}{sep}key_prefix=dtmrs-t-ffi:");
        // 开头清一次：上次跑崩留下的事务会被这次的推进器捡起来
        let rt = tokio::runtime::Runtime::new().unwrap();
        let flush = || {
            rt.block_on(async {
                let s = dtmrs_store::Store::open(&url).await.unwrap();
                s.as_redis().unwrap().flush_prefix().await.unwrap();
            })
        };
        flush();

        let tc = dtmrs_open(cs(&url).as_ptr());
        assert!(!tc.is_null(), "{}", last_err());
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let seen: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());
        let recs: Vec<Box<Rec>> = ["confirm", "cancel", "q"]
            .into_iter()
            .map(|n| {
                Box::new(Rec {
                    tag: n,
                    ret: DTMRS_SUCCESS,
                    log: log.clone(),
                })
            })
            .collect();
        for r in &recs {
            let ud = &**r as *const Rec as *mut c_void;
            assert_eq!(
                dtmrs_register_ex(tc, cs(r.tag).as_ptr(), Some(rec_handler), ud),
                DTMRS_OK
            );
        }
        let sud = &seen as *const _ as *mut c_void;
        assert_eq!(
            dtmrs_register_ex(tc, cs("a").as_ptr(), Some(record_ex_handler), sud),
            DTMRS_OK
        );
        assert_eq!(
            dtmrs_register_ex(tc, cs("c").as_ptr(), Some(record_ex_handler), sud),
            DTMRS_OK
        );
        assert_eq!(dtmrs_start(tc), DTMRS_OK, "{}", last_err());

        // saga + payload：payload 在 Redis 上走的是另一套序列化
        let steps = cs(r#"[{"action":"local://a","compensate":"local://c","payload":"金额=30"}]"#);
        assert_eq!(
            dtmrs_submit_saga(tc, cs("r-saga").as_ptr(), steps.as_ptr()),
            DTMRS_OK
        );
        assert_eq!(wait(tc, "r-saga"), "succeed");
        assert_eq!(
            *seen.lock().unwrap(),
            [("01-action".to_string(), "金额=30".to_string())]
        );

        // TCC
        let g = cs("r-tcc");
        assert_eq!(dtmrs_tcc_begin(tc, g.as_ptr()), DTMRS_OK);
        for bid in ["01", "02"] {
            assert_eq!(
                dtmrs_tcc_register(
                    tc,
                    g.as_ptr(),
                    cs(bid).as_ptr(),
                    cs("local://confirm").as_ptr(),
                    cs("local://cancel").as_ptr()
                ),
                DTMRS_OK,
                "{}",
                last_err()
            );
        }
        assert_eq!(dtmrs_submit(tc, g.as_ptr()), DTMRS_OK);
        assert_eq!(wait(tc, "r-tcc"), "succeed");
        assert_eq!(dtmrs_abort(tc, g.as_ptr()), DTMRS_ERR, "终态不能 abort");

        // msg：prepare 完不 submit，靠回查推下去。prepared 的 msg 能不能被捞起来
        // 取决于 Lua 里那份 schedulable()
        let m = cs("r-msg");
        assert_eq!(
            dtmrs_msg_prepare(
                tc,
                m.as_ptr(),
                cs(r#"["local://confirm"]"#).as_ptr(),
                cs("local://q").as_ptr(),
                0
            ),
            DTMRS_OK
        );
        assert_eq!(wait(tc, "r-msg"), "succeed");

        assert_eq!(
            *log.lock().unwrap(),
            ["confirm@01", "confirm@02", "q@00", "confirm@01"],
            "TCC 两个 confirm，然后 msg 先回查再送达"
        );
        dtmrs_close(tc);
        flush();
    }

    #[test]
    fn 错误路径不会崩() {
        // FFI 最怕的是宿主传错参数直接段错误
        assert!(dtmrs_open(std::ptr::null()).is_null());
        assert!(!unsafe { CStr::from_ptr(dtmrs_last_error()) }
            .to_bytes()
            .is_empty());
        assert_eq!(
            dtmrs_register(
                std::ptr::null_mut(),
                std::ptr::null(),
                None,
                std::ptr::null_mut()
            ),
            DTMRS_ERR
        );
        assert_eq!(dtmrs_start(std::ptr::null_mut()), DTMRS_ERR);
        assert_eq!(
            dtmrs_submit_saga(std::ptr::null_mut(), std::ptr::null(), std::ptr::null()),
            DTMRS_ERR
        );
        assert_eq!(
            dtmrs_status(
                std::ptr::null_mut(),
                std::ptr::null(),
                std::ptr::null_mut(),
                0
            ),
            DTMRS_ERR
        );
        dtmrs_close(std::ptr::null_mut()); // 不该崩

        let (url, path) = db("errs");
        let tc = dtmrs_open(url.as_ptr());
        // start 之前提交
        assert_eq!(
            dtmrs_submit_saga(tc, cs("x").as_ptr(), cs("[]").as_ptr()),
            DTMRS_ERR
        );
        dtmrs_register(
            tc,
            cs("a1").as_ptr(),
            Some(ok_handler),
            std::ptr::null_mut(),
        );
        dtmrs_start(tc);
        // start 之后再注册
        assert_eq!(
            dtmrs_register(tc, cs("z").as_ptr(), Some(ok_handler), std::ptr::null_mut()),
            DTMRS_ERR
        );
        // 坏 JSON
        assert_eq!(
            dtmrs_submit_saga(tc, cs("y").as_ptr(), cs("{坏}").as_ptr()),
            DTMRS_ERR
        );
        // 漏注册的 handler，提交就该被拦住
        let steps = cs(r#"[{"action":"local://a1","compensate":"local://没注册"}]"#);
        assert_eq!(
            dtmrs_submit_saga(tc, cs("w").as_ptr(), steps.as_ptr()),
            DTMRS_ERR
        );
        // 缓冲区太小
        let mut tiny = [0i8; 2];
        dtmrs_submit_saga(
            tc,
            cs("v").as_ptr(),
            cs(r#"[{"action":"local://a1","compensate":"local://a1"}]"#).as_ptr(),
        );
        assert_eq!(
            dtmrs_status(tc, cs("v").as_ptr(), tiny.as_mut_ptr() as *mut c_char, 2),
            DTMRS_ERR
        );
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }
}
