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

use dtmrs_core::{BranchResult, SagaStep};
use dtmrs_server::embedded::Embedded;
use dtmrs_server::registry::BranchCtx;
use std::cell::RefCell;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::time::Duration;

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

/// 裸函数指针 + 用户数据。跨线程传递需要显式声明安全性。
///
/// # Safety 契约（宿主必须保证）
/// - `f` 在 TC 存活期间一直有效
/// - `f` 可以被多个线程并发调用
/// - `ud` 指向的数据在 TC 存活期间有效且线程安全
#[derive(Clone, Copy)]
struct HandlerPtr {
    f: HandlerFn,
    ud: *mut c_void,
}
unsafe impl Send for HandlerPtr {}
unsafe impl Sync for HandlerPtr {}

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
    db: String,
    tc: Option<Embedded>,
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

/// 创建一个 TC 句柄（还没启动）。`db_url` 形如 `sqlite:/tmp/app.db`。
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
        db: db.to_string(),
        tc: None,
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
    match h.pending.as_mut() {
        Some(v) => {
            v.push((name.to_string(), HandlerPtr { f, ud: user_data }));
            DTMRS_OK
        }
        None => {
            // 启动后再注册会有竞态：推进器可能正在查这张表
            set_err("已经 start 了，不能再注册 handler");
            DTMRS_ERR
        }
    }
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

    let mut b = Embedded::builder(&h.db).tick(Duration::from_millis(50));
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
    // 三个 CString 在本函数栈上活着，回调返回前不会被释放
    let code = (hp.f)(gid.as_ptr(), bid.as_ptr(), op.as_ptr(), hp.ud);
    match code {
        DTMRS_SUCCESS => BranchResult::Success,
        DTMRS_FAILURE => BranchResult::Failure,
        DTMRS_ONGOING => BranchResult::Ongoing,
        // 包括 DTMRS_UNKNOWN 和任何不认识的值。宁可重试不可误回滚。
        _ => BranchResult::Unknown,
    }
}

/// 提交一个 SAGA。`steps_json` 形如：
///
/// ```json
/// [{"action":"local://deduct","compensate":"local://deduct_undo"},
///  {"action":"http://svc/ship","compensate":"http://svc/unship"}]
/// ```
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
        sb = sb.step(&s.action, &s.compensate);
    }
    match h.rt.block_on(sb.submit()) {
        Ok(()) => DTMRS_OK,
        Err(e) => {
            set_err(format!("提交失败: {e}"));
            DTMRS_ERR
        }
    }
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
    // Embedded 的 Drop 会停掉推进器；Runtime 的 Drop 等待任务收尾
    drop(unsafe { Box::from_raw(tc) });
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(dtmrs_register(tc, cs("a1").as_ptr(), Some(ok_handler), ud), DTMRS_OK);
        assert_eq!(dtmrs_register(tc, cs("c1").as_ptr(), Some(ok_handler), std::ptr::null_mut()), DTMRS_OK);
        assert_eq!(dtmrs_start(tc), DTMRS_OK);

        let steps = cs(r#"[{"action":"local://a1","compensate":"local://c1"}]"#);
        assert_eq!(dtmrs_submit_saga(tc, cs("ffi-1").as_ptr(), steps.as_ptr()), DTMRS_OK);

        let mut buf = [0i8; 32];
        assert_eq!(
            dtmrs_wait_final(tc, cs("ffi-1").as_ptr(), 5000, buf.as_mut_ptr() as *mut c_char, 32),
            DTMRS_OK
        );
        let s = unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) }.to_str().unwrap();
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
        dtmrs_register(tc, cs("a1").as_ptr(), Some(fail_handler), std::ptr::null_mut());
        dtmrs_register(tc, cs("c1").as_ptr(), Some(ok_handler), ud);
        assert_eq!(dtmrs_start(tc), DTMRS_OK);
        let steps = cs(r#"[{"action":"local://a1","compensate":"local://c1"}]"#);
        dtmrs_submit_saga(tc, cs("ffi-2").as_ptr(), steps.as_ptr());

        let mut buf = [0i8; 32];
        dtmrs_wait_final(tc, cs("ffi-2").as_ptr(), 5000, buf.as_mut_ptr() as *mut c_char, 32);
        let s = unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) }.to_str().unwrap();
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
        dtmrs_register(tc, cs("c1").as_ptr(), Some(ok_handler), std::ptr::null_mut());
        dtmrs_start(tc);
        let steps = cs(r#"[{"action":"local://a1","compensate":"local://c1"}]"#);
        dtmrs_submit_saga(tc, cs("ffi-3").as_ptr(), steps.as_ptr());

        std::thread::sleep(Duration::from_millis(500));
        let mut buf = [0i8; 32];
        assert_eq!(
            dtmrs_status(tc, cs("ffi-3").as_ptr(), buf.as_mut_ptr() as *mut c_char, 32),
            DTMRS_OK
        );
        let s = unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) }.to_str().unwrap();
        assert_eq!(s, "submitted", "野值不能触发回滚，只能重试");
        assert!(n >= 1);
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn 错误路径不会崩() {
        // FFI 最怕的是宿主传错参数直接段错误
        assert!(dtmrs_open(std::ptr::null()).is_null());
        assert!(!unsafe { CStr::from_ptr(dtmrs_last_error()) }.to_bytes().is_empty());
        assert_eq!(dtmrs_register(std::ptr::null_mut(), std::ptr::null(), None, std::ptr::null_mut()), DTMRS_ERR);
        assert_eq!(dtmrs_start(std::ptr::null_mut()), DTMRS_ERR);
        assert_eq!(dtmrs_submit_saga(std::ptr::null_mut(), std::ptr::null(), std::ptr::null()), DTMRS_ERR);
        assert_eq!(dtmrs_status(std::ptr::null_mut(), std::ptr::null(), std::ptr::null_mut(), 0), DTMRS_ERR);
        dtmrs_close(std::ptr::null_mut()); // 不该崩

        let (url, path) = db("errs");
        let tc = dtmrs_open(url.as_ptr());
        // start 之前提交
        assert_eq!(dtmrs_submit_saga(tc, cs("x").as_ptr(), cs("[]").as_ptr()), DTMRS_ERR);
        dtmrs_register(tc, cs("a1").as_ptr(), Some(ok_handler), std::ptr::null_mut());
        dtmrs_start(tc);
        // start 之后再注册
        assert_eq!(dtmrs_register(tc, cs("z").as_ptr(), Some(ok_handler), std::ptr::null_mut()), DTMRS_ERR);
        // 坏 JSON
        assert_eq!(dtmrs_submit_saga(tc, cs("y").as_ptr(), cs("{坏}").as_ptr()), DTMRS_ERR);
        // 漏注册的 handler，提交就该被拦住
        let steps = cs(r#"[{"action":"local://a1","compensate":"local://没注册"}]"#);
        assert_eq!(dtmrs_submit_saga(tc, cs("w").as_ptr(), steps.as_ptr()), DTMRS_ERR);
        // 缓冲区太小
        let mut tiny = [0i8; 2];
        dtmrs_submit_saga(tc, cs("v").as_ptr(), cs(r#"[{"action":"local://a1","compensate":"local://a1"}]"#).as_ptr());
        assert_eq!(dtmrs_status(tc, cs("v").as_ptr(), tiny.as_mut_ptr() as *mut c_char, 2), DTMRS_ERR);
        dtmrs_close(tc);
        let _ = std::fs::remove_file(path);
    }
}
