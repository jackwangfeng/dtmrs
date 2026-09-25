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


# ---- TCC：try 由我们自己跑，TC 只管 confirm / cancel ----
frozen = {}   # (gid, branch_id) → 冻结的金额。真实业务里这是一张表


@tc.handler("冻结确认")
def freeze_confirm(ctx):
    log("冻结确认", ctx)
    amt = frozen.pop((ctx.gid, ctx.branch_id), 0)
    move(1, 2, amt)
    return dtmrs.SUCCESS


@tc.handler("冻结撤销")
def freeze_cancel(ctx):
    log("冻结撤销", ctx)
    # try 可能根本没跑成（空回滚），pop 不到也要返回成功
    frozen.pop((ctx.gid, ctx.branch_id), None)
    return dtmrs.SUCCESS


# ---- 二阶段消息：本地事务 + 保证送达 ----
orders = set()


@tc.handler("加积分")
def add_points(ctx):
    log("加积分", ctx)
    return dtmrs.SUCCESS


@tc.handler("查订单")
def query_order(ctx):
    # 回查：本地事务到底提交了没有
    log("查订单", ctx)
    return dtmrs.SUCCESS if ctx.gid in orders else dtmrs.FAILURE


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

print("\n⑥ TCC：两个 try 都成功 → confirm")
def freeze(gid, amt):
    def try_fn(bid):
        frozen[(gid, bid)] = amt
        return dtmrs.SUCCESS
    return try_fn

with tc.tcc("py-tcc-1") as t:
    t.try_branch("local://冻结确认", "local://冻结撤销", freeze("py-tcc-1", 10))
    t.try_branch("local://冻结确认", "local://冻结撤销", freeze("py-tcc-1", 20))
print("  结果:", tc.wait_final("py-tcc-1", 5000), " 余额:", balances())

print("\n⑦ TCC：第二个 try 失败 → with 块自动 abort，两个分支都 cancel")
try:
    with tc.tcc("py-tcc-2") as t:
        t.try_branch("local://冻结确认", "local://冻结撤销", freeze("py-tcc-2", 10))
        t.try_branch("local://冻结确认", "local://冻结撤销", lambda bid: dtmrs.FAILURE)
except dtmrs.BranchFailed as e:
    print("  ", e)
print("  结果:", tc.wait_final("py-tcc-2", 5000), " 余额:", balances(), " 残留冻结:", frozen)
assert not frozen, "cancel 必须清掉所有冻结"

print("\n⑧ 二阶段消息：本地事务成功 → 消息送达")
def write_order():
    orders.add("py-msg-1")
    return dtmrs.SUCCESS

tc.msg_do_and_submit("py-msg-1", ["local://加积分"], "local://查订单", write_order)
print("  结果:", tc.wait_final("py-msg-1", 5000))

print("\n⑨ 二阶段消息：本地事务提交了但进程「崩」在 submit 之前 → 靠回查继续推")
tc.msg_prepare("py-msg-2", ["local://加积分"], "local://查订单", grace_secs=0)
orders.add("py-msg-2")   # 本地事务提交了，然后……没调 submit
print("  结果:", tc.wait_final("py-msg-2", 5000), "（回查说已提交，消息照样送达）")

tc.close()
print("\n最终余额:", balances(), "（① 转了 100，⑥ 转了 30，其余都被补偿/撤销抹平）")
