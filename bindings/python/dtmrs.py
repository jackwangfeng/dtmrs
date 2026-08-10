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
import os
import sys
import traceback

SUCCESS = 0
FAILURE = 1
ONGOING = 2
UNKNOWN = 3

_OK = 0
_ERR = -1

HANDLER = ctypes.CFUNCTYPE(
    ctypes.c_int,               # 返回码
    ctypes.c_char_p,            # gid
    ctypes.c_char_p,            # branch_id
    ctypes.c_char_p,            # op
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


class Ctx:
    """分支调用上下文。业务侧做幂等要用 (gid, branch_id, op)。"""

    __slots__ = ("gid", "branch_id", "op")

    def __init__(self, gid, branch_id, op):
        self.gid = gid
        self.branch_id = branch_id
        self.op = op

    def __repr__(self):
        return f"Ctx(gid={self.gid!r}, branch_id={self.branch_id!r}, op={self.op!r})"


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
        L.dtmrs_register.argtypes = [ctypes.c_void_p, ctypes.c_char_p, HANDLER, ctypes.c_void_p]
        L.dtmrs_register.restype = ctypes.c_int
        L.dtmrs_start.argtypes = [ctypes.c_void_p]
        L.dtmrs_start.restype = ctypes.c_int
        L.dtmrs_submit_saga.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
        L.dtmrs_submit_saga.restype = ctypes.c_int
        L.dtmrs_status.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t]
        L.dtmrs_status.restype = ctypes.c_int
        L.dtmrs_wait_final.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_size_t]
        L.dtmrs_wait_final.restype = ctypes.c_int
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
        def bridge(gid, branch_id, op, _ud):
            try:
                return int(fn(Ctx(gid.decode(), branch_id.decode(), op.decode())))
            except Exception:
                # 异常 = 不知道到底做了没有。**当 UNKNOWN，绝不当 FAILURE** ——
                # 误判失败会把一笔本该成功的事务回滚掉。
                traceback.print_exc(file=sys.stderr)
                return UNKNOWN

        cb = HANDLER(bridge)
        self._keep.append(cb)          # 防 GC
        if self._lib.dtmrs_register(self._h, name.encode(), cb, None) != _OK:
            raise RuntimeError(self._err())

    def start(self):
        if self._lib.dtmrs_start(self._h) != _OK:
            raise RuntimeError(self._err())
        self._started = True

    def submit_saga(self, gid, steps):
        """steps: [(action, compensate), ...]，每项可以是 local:// 或 http://"""
        import json

        payload = json.dumps(
            [{"action": a, "compensate": c} for a, c in steps], ensure_ascii=False
        )
        if self._lib.dtmrs_submit_saga(self._h, gid.encode(), payload.encode()) != _OK:
            raise RuntimeError(self._err())

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
