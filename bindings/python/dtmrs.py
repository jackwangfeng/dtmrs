"""dtmrs 的 Python 绑定 —— 在 Python 进程里嵌一个 Rust 事务协调器。

不需要部署任何服务，不需要 MQ，就一个 .so。

    import dtmrs

    tc = dtmrs.Tc("sqlite:/tmp/app.db")

    @tc.handler("扣款")
    def deduct(ctx):
        db.execute("UPDATE account SET balance = balance - 100 WHERE id = 1")
        return dtmrs.SUCCESS

    @tc.handler("扣款撤销")
    def deduct_undo(ctx):
        db.execute("UPDATE account SET balance = balance + 100 WHERE id = 1")
        return dtmrs.SUCCESS

    tc.start()
    tc.submit_saga("order-1001", [("local://扣款", "local://扣款撤销")])

TCC / XA / 二阶段消息的一阶段是你自己做的：

    # TCC：with 块正常结束 → submit；抛异常（包括某个 try 没成功）→ abort
    with tc.tcc("order-1002") as t:
        t.try_branch("local://冻结确认", "local://冻结撤销", lambda bid: freeze(bid))
        t.try_branch("local://扣款确认", "local://扣款撤销", lambda bid: hold(bid))

    # XA 同形：prepare_branch(commit, rollback, fn)，fn 里做业务 SQL + PREPARE

    # 二阶段消息：本地事务成功就 submit，失败就 abort，不知道就交给回查
    tc.msg_do_and_submit("order-1003", ["local://加积分"], "local://查订单", write_order)

    # workflow：步骤由函数自己决定，崩溃后重放续跑（已成功的分支不重做）
    @tc.workflow("下单")
    def place_order(wf):
        sn = wf.branch("扣款", lambda bid: (dtmrs.SUCCESS, "流水号-1"), on_rollback="local://退款")
        if need_ship(sn):
            wf.branch("发货", ship, on_rollback="local://退货")
    tc.submit_workflow("order-1004", "下单", input='{"sku": 7}')

⚠ 两个必须知道的事情

1. **handler 会被 Rust 侧的任意线程调用**，不是你的主线程。
   ctypes 的 CFUNCTYPE 会自动处理 GIL，所以 Python 代码本身是安全的，
   但你的 handler 里用到的连接/对象必须线程安全（比如每次现取一个 DB 连接）。

2. **返回值区分「失败」和「未知」**。
   - 业务明确不能继续（库存不足、余额不足）→ 返回 FAILURE，触发回滚
   - 超时、下游 5xx、自己抛异常 → 返回 UNKNOWN，只重试不回滚
   handler 抛出的异常会被本模块捕获并转成 UNKNOWN —— 不知道就别回滚。
"""

import ctypes
import json
import os
import sys
import traceback

SUCCESS = 0
FAILURE = 1
ONGOING = 2
UNKNOWN = 3

_OK = 0
_ERR = -1

# workflow 函数：int (*)(DtmrsWf *wf, const char *gid, const char *input, void *ud)
WORKFLOW = ctypes.CFUNCTYPE(
    ctypes.c_int, ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p)

# workflow 分支函数体：int (*)(gid, branch_id, char *out, size_t out_len, void *ud)
# out 声明成 void*（我们要往里写，不是读 C 字符串）
WF_BRANCH = ctypes.CFUNCTYPE(
    ctypes.c_int, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t,
    ctypes.c_void_p)

# 对应 C 的 dtmrs_handler_ex_fn（带 payload 的那个）
HANDLER = ctypes.CFUNCTYPE(
    ctypes.c_int,               # 返回码
    ctypes.c_char_p,            # gid
    ctypes.c_char_p,            # branch_id
    ctypes.c_char_p,            # op
    ctypes.c_char_p,            # payload
    ctypes.c_void_p,            # user_data
)


def _find_lib():
    if env := os.environ.get("DTMRS_LIB"):
        return env
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.abspath(os.path.join(here, "..", ".."))
    names = ["libdtmrs.so", "libdtmrs.dylib", "dtmrs.dll"]
    for profile in ("release", "debug"):
        for n in names:
            p = os.path.join(root, "target", profile, n)
            if os.path.exists(p):
                return p
    raise OSError(
        "找不到 libdtmrs。先跑 `cargo build -p dtmrs-ffi --release`，"
        "或者用 DTMRS_LIB 指定路径。"
    )


def _step(s):
    action, compensate, *rest = s
    d = {"action": action, "compensate": compensate}
    if rest:
        p = rest[0]
        d["payload"] = p if isinstance(p, str) else json.dumps(p, ensure_ascii=False)
    return d


class Ctx:
    """分支调用上下文。业务侧做幂等要用 (gid, branch_id, op)。

    payload 是这一步自己的业务数据（submit_saga 的第三项），没给就是空串。
    """

    __slots__ = ("gid", "branch_id", "op", "payload")

    def __init__(self, gid, branch_id, op, payload=""):
        self.gid = gid
        self.branch_id = branch_id
        self.op = op
        self.payload = payload

    def __repr__(self):
        return f"Ctx(gid={self.gid!r}, branch_id={self.branch_id!r}, op={self.op!r})"


class BranchFailed(Exception):
    """一阶段（try / XA prepare）没返回 SUCCESS。在 with 块里抛出会触发 abort。"""

    def __init__(self, branch_id, code):
        super().__init__(f"分支 {branch_id} 一阶段返回 {code}（不是 SUCCESS），整单回滚")
        self.branch_id = branch_id
        self.code = code


def _call_phase1(fn, branch_id):
    """跑宿主的一阶段。异常 = 不知道做没做 → UNKNOWN（在 TCC/XA 里照样回滚，cancel 兜得住）"""
    try:
        return int(fn(branch_id))
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return UNKNOWN


class _TwoPhase:
    """TCC 和 XA 共用：分支号自动编（01、02……），先登记、登记成功才跑一阶段。"""

    _begin = _register = None  # 子类给 C 函数名

    def __init__(self, tc, gid):
        self._tc = tc
        self.gid = gid
        self._next = 0
        tc._call(self._begin, gid.encode())

    def register(self, fwd, bwd):
        """只登记下一个分支，返回分支号。一阶段自己去做 —— **必须在这之后**。"""
        bid = f"{self._next + 1:02d}"
        self._tc._call(self._register, self.gid.encode(), bid.encode(), fwd.encode(), bwd.encode())
        # 登记成功才占号：失败了重试还用同一个号
        self._next += 1
        return bid

    def _branch(self, fwd, bwd, fn):
        bid = self.register(fwd, bwd)
        code = _call_phase1(fn, bid)
        if code != SUCCESS:
            raise BranchFailed(bid, code)
        return bid

    def submit(self):
        self._tc.submit(self.gid)

    def abort(self):
        self._tc.abort(self.gid)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # 一阶段有任何没成功（包括抛异常）→ abort；全成功 → submit。
        # 超时也是 abort：还没 submit，撤销会覆盖每个分支，多余的由屏障空转掉
        if exc_type is None:
            self.submit()
        else:
            self.abort()
        return False


class Tcc(_TwoPhase):
    """一个进行中的 TCC 事务，由 Tc.tcc() 开。"""

    _begin, _register = "dtmrs_tcc_begin", "dtmrs_tcc_register"

    def try_branch(self, confirm, cancel, try_fn):
        """先登记、再跑 try_fn(branch_id)。try 没返回 SUCCESS（含抛异常）就抛 BranchFailed。"""
        return self._branch(confirm, cancel, try_fn)


class Xa(_TwoPhase):
    """一个进行中的 XA 事务，由 Tc.xa() 开。"""

    _begin, _register = "dtmrs_xa_begin", "dtmrs_xa_register"

    def prepare_branch(self, commit, rollback, prepare_fn):
        """先登记、再跑 prepare_fn(branch_id)（业务 SQL + PREPARE）。没 SUCCESS 就抛 BranchFailed。"""
        return self._branch(commit, rollback, prepare_fn)


class WorkflowStop(BaseException):
    """workflow 要停在这里（回滚 / 重试 / 重放走岔）。由 wf.branch() 抛出。

    **继承 BaseException 而不是 Exception**：业务代码里常见的 `except Exception`
    不会把它吞掉。别捕获它 —— 就算捕获了继续开分支，库那边也不会再执行任何分支。
    """


class Workflow:
    """传给 workflow 函数的上下文。只在那次调用期间有效。"""

    def __init__(self, tc, ptr, gid, input):
        self._tc = tc
        self._ptr = ptr
        self.gid = gid
        self.input = input

    def branch(self, name, fn, on_rollback=None):
        """开一个分支，返回它的结果数据（字符串）。

        name 是逻辑名字，重放时用来做分岔检测，要稳定。fn(branch_id) 返回结果码，
        或 (结果码, 数据)；数据会被记下，**重放时 fn 不会再被调用**，直接返回上次的数据。
        on_rollback 是回滚时调的地址（local:// / http://）；有副作用的分支一定要给。

        分支没成功（或重放走岔）时抛 WorkflowStop —— 让它往外传就好。
        """
        def body(_gid, branch_id, out, out_len, _ud):
            try:
                r = fn(branch_id.decode())
                code, data = (r if isinstance(r, tuple) else (r, ""))
                raw = (data or "").encode()
                if len(raw) + 1 > out_len:
                    # 截断会让记下的数据跟真实结果对不上，重放时就是错的。宁可重试
                    print(f"[dtmrs] 分支 {name} 的结果数据太长（{len(raw)} 字节），按结果未知处理",
                          file=sys.stderr)
                    return UNKNOWN
                ctypes.memmove(out, raw + b"\0", len(raw) + 1)
                return int(code)
            except Exception:
                # 不知道做没做 → 重放，绝不回滚
                traceback.print_exc(file=sys.stderr)
                return UNKNOWN

        cb = WF_BRANCH(body)
        buf = ctypes.create_string_buffer(4097)
        comp = on_rollback.encode() if on_rollback else None
        if self._tc._lib.dtmrs_wf_branch(self._ptr, name.encode(), comp, cb, None, buf, len(buf)) != _OK:
            raise WorkflowStop(self._tc._err())
        return buf.value.decode()


class Tc:
    def __init__(self, db_url, lib_path=None):
        self._lib = ctypes.CDLL(lib_path or _find_lib())
        self._decl()
        self._h = self._lib.dtmrs_open(db_url.encode())
        if not self._h:
            raise RuntimeError(self._err())
        # 必须持有 CFUNCTYPE 对象的引用，否则被 GC 回收后 Rust 侧就是野指针
        self._keep = []
        self._started = False

    def _decl(self):
        L = self._lib
        L.dtmrs_open.argtypes = [ctypes.c_char_p]
        L.dtmrs_open.restype = ctypes.c_void_p
        L.dtmrs_register_ex.argtypes = [ctypes.c_void_p, ctypes.c_char_p, HANDLER, ctypes.c_void_p]
        L.dtmrs_register_ex.restype = ctypes.c_int
        L.dtmrs_start.argtypes = [ctypes.c_void_p]
        L.dtmrs_start.restype = ctypes.c_int
        L.dtmrs_submit_saga.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
        L.dtmrs_submit_saga.restype = ctypes.c_int
        L.dtmrs_status.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t]
        L.dtmrs_status.restype = ctypes.c_int
        L.dtmrs_wait_final.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_size_t]
        L.dtmrs_wait_final.restype = ctypes.c_int
        for n in ("dtmrs_tcc_begin", "dtmrs_xa_begin", "dtmrs_submit", "dtmrs_abort"):
            getattr(L, n).argtypes = [ctypes.c_void_p, ctypes.c_char_p]
            getattr(L, n).restype = ctypes.c_int
        for n in ("dtmrs_tcc_register", "dtmrs_xa_register"):
            getattr(L, n).argtypes = [ctypes.c_void_p] + [ctypes.c_char_p] * 4
            getattr(L, n).restype = ctypes.c_int
        L.dtmrs_msg_prepare.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
        L.dtmrs_msg_prepare.restype = ctypes.c_int
        L.dtmrs_register_workflow.argtypes = [ctypes.c_void_p, ctypes.c_char_p, WORKFLOW, ctypes.c_void_p]
        L.dtmrs_register_workflow.restype = ctypes.c_int
        L.dtmrs_submit_workflow.argtypes = [ctypes.c_void_p] + [ctypes.c_char_p] * 3
        L.dtmrs_submit_workflow.restype = ctypes.c_int
        L.dtmrs_wf_branch.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, WF_BRANCH, ctypes.c_void_p,
            ctypes.c_char_p, ctypes.c_size_t]
        L.dtmrs_wf_branch.restype = ctypes.c_int
        L.dtmrs_close.argtypes = [ctypes.c_void_p]
        L.dtmrs_close.restype = None
        L.dtmrs_last_error.argtypes = []
        L.dtmrs_last_error.restype = ctypes.c_char_p

    def _err(self):
        p = self._lib.dtmrs_last_error()
        return (p or b"").decode(errors="replace") or "未知错误"

    def handler(self, name):
        """装饰器：注册一个进程内分支。必须在 start() 之前。"""

        def deco(fn):
            self.register(name, fn)
            return fn

        return deco

    def register(self, name, fn):
        def bridge(gid, branch_id, op, payload, _ud):
            try:
                ctx = Ctx(gid.decode(), branch_id.decode(), op.decode(), payload.decode())
                return int(fn(ctx))
            except Exception:
                # 异常 = 不知道到底做了没有。**当 UNKNOWN，绝不当 FAILURE** ——
                # 误判失败会把一笔本该成功的事务回滚掉。
                traceback.print_exc(file=sys.stderr)
                return UNKNOWN

        cb = HANDLER(bridge)
        self._keep.append(cb)          # 防 GC
        if self._lib.dtmrs_register_ex(self._h, name.encode(), cb, None) != _OK:
            raise RuntimeError(self._err())

    def workflow(self, name):
        """装饰器：注册一个 workflow 函数 fn(wf)。必须在 start() 之前。

        函数正常返回（或返回 SUCCESS）= 跑完了；返回 FAILURE = 整单回滚；
        抛异常 = 重放（不回滚）。**函数会被从头跑多次**，必须是确定性的，
        副作用都放进 wf.branch() 里。重启后要注册同名函数。
        """

        def deco(fn):
            def bridge(wf_ptr, gid, input, _ud):
                wf = Workflow(self, wf_ptr, gid.decode(), input.decode())
                try:
                    r = fn(wf)
                    return SUCCESS if r is None else int(r)
                except WorkflowStop:
                    return UNKNOWN  # 库里已经记下了真正的原因，这个返回值不算数
                except Exception:
                    traceback.print_exc(file=sys.stderr)
                    return UNKNOWN

            cb = WORKFLOW(bridge)
            self._keep.append(cb)  # 防 GC
            self._call("dtmrs_register_workflow", name.encode(), cb, None)
            return fn

        return deco

    def submit_workflow(self, gid, name, input=""):
        """提交一个 workflow 事务。name 没注册会当场报错。幂等。"""
        self._call("dtmrs_submit_workflow", gid.encode(), name.encode(), input.encode())

    def start(self):
        if self._lib.dtmrs_start(self._h) != _OK:
            raise RuntimeError(self._err())
        self._started = True

    def submit_saga(self, gid, steps):
        """steps: [(action, compensate), ...] 或 [(action, compensate, payload), ...]

        地址可以是 local:// 或 http://。payload 是这一步自己的数据（字符串；
        传 dict/list 会被转成 JSON），正向和补偿共用。
        """
        body = json.dumps([_step(s) for s in steps], ensure_ascii=False)
        if self._lib.dtmrs_submit_saga(self._h, gid.encode(), body.encode()) != _OK:
            raise RuntimeError(self._err())

    def _call(self, fn_name, *args):
        if getattr(self._lib, fn_name)(self._h, *args) != _OK:
            raise RuntimeError(self._err())

    def tcc(self, gid):
        """开一个 TCC 事务（幂等）。推荐当 with 用，见模块文档。"""
        return Tcc(self, gid)

    def xa(self, gid):
        """开一个 XA 事务（幂等）。用法同 tcc()，一阶段是业务 SQL + PREPARE。"""
        return Xa(self, gid)

    def msg_prepare(self, gid, actions, query_prepared, grace_secs=-1):
        """二阶段消息的 prepare。之后自己跑本地事务，再 submit / abort / 什么都不做。

        query_prepared 必填：崩在本地事务和 submit 之间时 TC 靠它回查。回查 handler
        返回 SUCCESS=本地已提交、FAILURE=没提交、其它=过会再问。grace_secs<0 用默认 10 秒。
        """
        self._call("dtmrs_msg_prepare", gid.encode(),
                   json.dumps(list(actions), ensure_ascii=False).encode(),
                   query_prepared.encode(), int(grace_secs))

    def msg_do_and_submit(self, gid, actions, query_prepared, local_tx, grace_secs=-1):
        """prepare → 跑 local_tx() → SUCCESS 就 submit、FAILURE 就 abort、
        其它（含抛异常）什么都不做，交给回查决断。返回 local_tx 的结果码。

        prepare 失败会直接抛异常，local_tx **不会跑** —— 跑了就是本地已提交、
        TC 却不知道有这笔消息。
        """
        self.msg_prepare(gid, actions, query_prepared, grace_secs)
        try:
            code = int(local_tx())
        except Exception:
            # 本地事务抛异常 = 不知道提交了没有。不能猜，交给回查
            traceback.print_exc(file=sys.stderr)
            return UNKNOWN
        if code == SUCCESS:
            self.submit(gid)
        elif code == FAILURE:
            self.abort(gid)
        return code

    def submit(self, gid):
        """tcc / xa / msg 的二阶段提交（幂等）"""
        self._call("dtmrs_submit", gid.encode())

    def abort(self, gid):
        """主动中止。tcc / xa / msg 已 submit 的会抛异常 —— 方向已定，不能再回滚"""
        self._call("dtmrs_abort", gid.encode())

    def status(self, gid):
        buf = ctypes.create_string_buffer(64)
        if self._lib.dtmrs_status(self._h, gid.encode(), buf, len(buf)) != _OK:
            raise RuntimeError(self._err())
        return buf.value.decode()

    def wait_final(self, gid, timeout_ms=10000):
        buf = ctypes.create_string_buffer(64)
        if self._lib.dtmrs_wait_final(self._h, gid.encode(), timeout_ms, buf, len(buf)) != _OK:
            raise RuntimeError(self._err())
        return buf.value.decode()

    def close(self):
        if getattr(self, "_h", None):
            self._lib.dtmrs_close(self._h)
            self._h = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
