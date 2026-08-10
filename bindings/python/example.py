#!/usr/bin/env python3
"""Python 进程里嵌一个 Rust 事务协调器 —— 不部署任何服务。

跑之前先编：cargo build -p dtmrs-ffi --release
"""
import os
import sqlite3
import time
import tempfile
import threading

import dtmrs

DB = os.path.join(tempfile.gettempdir(), "dtmrs_py_demo.db")
BIZ = os.path.join(tempfile.gettempdir(), "dtmrs_py_biz.db")
for f in (DB, BIZ):
    if os.path.exists(f):
        os.remove(f)

# 业务库：一个账户表。注意 handler 会被 Rust 的任意线程调用，
# 所以每次现开连接，不共享 —— sqlite 的连接不是线程安全的。
with sqlite3.connect(BIZ) as c:
    c.execute("CREATE TABLE account(id INTEGER PRIMARY KEY, balance INT)")
    c.execute("INSERT INTO account VALUES (1, 1000), (2, 0)")


def balances():
    with sqlite3.connect(BIZ) as c:
        return dict(c.execute("SELECT id, balance FROM account").fetchall())


def move(frm, to, amt):
    with sqlite3.connect(BIZ) as c:
        c.execute("UPDATE account SET balance=balance-? WHERE id=?", (amt, frm))
        c.execute("UPDATE account SET balance=balance+? WHERE id=?", (amt, to))


tc = dtmrs.Tc(f"sqlite:{DB}")
seen = []
lock = threading.Lock()


def log(tag, ctx):
    with lock:
        seen.append(tag)
    print(f"  [{tag}] gid={ctx.gid} branch={ctx.branch_id} op={ctx.op} "
          f"线程={threading.current_thread().name}")


@tc.handler("转出")
def transfer_out(ctx):
    log("转出", ctx)
    move(1, 2, 100)
    return dtmrs.SUCCESS


@tc.handler("转出撤销")
def transfer_out_undo(ctx):
    log("转出撤销", ctx)
    move(2, 1, 100)
    return dtmrs.SUCCESS


@tc.handler("风控拒绝")
def risk_reject(ctx):
    log("风控拒绝", ctx)
    # 业务明确不能继续 → FAILURE，会触发逆序补偿
    return dtmrs.FAILURE


@tc.handler("空补偿")
def noop_undo(ctx):
    log("空补偿", ctx)
    return dtmrs.SUCCESS


@tc.handler("下游超时")
def downstream_timeout(ctx):
    log("下游超时", ctx)
    # 超时 = 不知道成没成 → UNKNOWN，只重试不回滚
    return dtmrs.UNKNOWN


tc.start()
print(f"TC 已在本进程内启动（库: {DB}）")
print("初始余额:", balances())

print("\n① 正常转账")
tc.submit_saga("py-1", [("local://转出", "local://转出撤销")])
print("  结果:", tc.wait_final("py-1", 5000), " 余额:", balances())

print("\n② 风控拒绝 → 逆序补偿，钱要退回来")
tc.submit_saga("py-2", [
    ("local://转出", "local://转出撤销"),
    ("local://风控拒绝", "local://空补偿"),
])
print("  结果:", tc.wait_final("py-2", 5000), " 余额:", balances())

print("\n③ 下游超时 → 只重试，不回滚")
tc.submit_saga("py-3", [("local://下游超时", "local://空补偿")])
time.sleep(0.6)
print("  状态:", tc.status("py-3"), "（应为 submitted，不是 failed）")
assert "空补偿" not in seen[seen.index("下游超时"):], "超时绝不能触发补偿"

print("\n④ start 之后再注册 handler 会被拒（有竞态）")
try:
    tc.handler("会炸")(lambda ctx: dtmrs.SUCCESS)
    print("  不该走到这儿")
except RuntimeError as e:
    print("  被拒:", e)

print("\n⑤ 漏注册的分支在提交时就被拦住")
try:
    tc.submit_saga("py-9", [("local://转出", "local://还没写")])
    print("  不该走到这儿")
except RuntimeError as e:
    print("  提交被拒:", e)

tc.close()
print("\n最终余额:", balances(), "（应回到 1000/0 的净效果：只有 ① 成功转了 100）")
