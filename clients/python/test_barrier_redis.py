"""Redis 屏障：跟 Rust / Go / Node / Java 各版**同名同义**的用例。

没配 DTMRS_TEST_REDIS_PY 就跳过 —— **跳过不等于通过**。

    DTMRS_TEST_REDIS_PY='127.0.0.1:16379' python3 test_barrier_redis.py

这里手写了一个最小 RESP 客户端，是为了**不给这个包引入 redis-py 依赖** ——
dtmrs_barrier_redis 只要求传进来的对象有 `.eval(...)`，测试自然也不该多要什么。
"""

import os
import socket
import sys

from dtmrs_barrier_redis import RedisBarrier, RedisOutcome


class Resp:
    """够用就好的 RESP 客户端：只支持我们要发的那几条命令"""

    def __init__(self, addr):
        host, port = addr.split(":")
        self.s = socket.create_connection((host, int(port)), timeout=5)
        self.f = self.s.makefile("rb")

    def cmd(self, *args):
        out = f"*{len(args)}\r\n".encode()
        for a in args:
            a = str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.s.sendall(out)
        return self._read()

    def _read(self):
        line = self.f.readline().rstrip(b"\r\n")
        if not line:
            raise RuntimeError("空回复")
        tag, rest = line[:1], line[1:]
        if tag == b"+":
            return rest.decode()
        if tag == b"-":
            raise RuntimeError("redis: " + rest.decode())
        if tag == b":":
            return int(rest)
        if tag == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self.f.read(n + 2)[:n]
            return data.decode()
        if tag == b"*":
            return [self._read() for _ in range(int(rest))]
        raise RuntimeError("看不懂的回复: " + line.decode())

    # dtmrs_barrier_redis 要求的接口就这一个方法（跟 redis-py 同签名）
    def eval(self, script, numkeys, *keys_and_args):
        return self.cmd("EVAL", script, numkeys, *keys_and_args)


def fixture():
    addr = os.environ.get("DTMRS_TEST_REDIS_PY")
    if not addr:
        print("⚠ 跳过 Redis 屏障测试：DTMRS_TEST_REDIS_PY 没配（跳过不等于通过）")
        sys.exit(0)
    r = Resp(addr)
    for pat in ("dtmrs:bar:*", "pt:stock:*"):
        for k in r.cmd("KEYS", pat) or []:
            r.cmd("DEL", k)
    return r


def stock(r, key):
    v = r.cmd("GET", key)
    return -1 if v is None else int(v)


def 重复调用同一分支只执行一次(r):
    r.cmd("SET", "pt:stock:1", 100)
    for want in (RedisOutcome.EXECUTED, RedisOutcome.DUPLICATED):
        # 每次新建 barrier —— 模拟 TC 重试时是两个独立请求
        b = RedisBarrier("saga", "pg-idem", "01", "action")
        got = b.check_adjust_amount(r, "pt:stock:1", -10)
        assert got is want, f"想要 {want}，得到 {got}"
    assert stock(r, "pt:stock:1") == 90, "只该扣一次"


def 正向没跑过时补偿要空转(r):
    r.cmd("SET", "pt:stock:2", 100)
    b = RedisBarrier("saga", "pg-null", "01", "compensate")
    assert b.check_adjust_amount(r, "pt:stock:2", 10) is RedisOutcome.NULL_COMPENSATION
    assert stock(r, "pt:stock:2") == 100, "空回滚不能动数据"


def 补偿先到时晚到的正向必须被丢弃(r):
    r.cmd("SET", "pt:stock:3", 100)
    b = RedisBarrier("saga", "pg-hang", "01", "compensate")
    assert b.check_adjust_amount(r, "pt:stock:3", 10) is RedisOutcome.NULL_COMPENSATION
    # 迟到的正向必须被丢弃，否则扣了款没人补
    b2 = RedisBarrier("saga", "pg-hang", "01", "action")
    assert b2.check_adjust_amount(r, "pt:stock:3", -10) is RedisOutcome.DUPLICATED
    assert stock(r, "pt:stock:3") == 100, "悬挂的正向不能生效"


def 库存不足要明确失败(r):
    r.cmd("SET", "pt:stock:4", 5)
    b = RedisBarrier("saga", "pg-low", "01", "action")
    assert b.check_adjust_amount(r, "pt:stock:4", -10) is RedisOutcome.FAILURE
    assert stock(r, "pt:stock:4") == 5, "失败了不能动数据"


def 键不存在不能凭空创建库存(r):
    b = RedisBarrier("saga", "pg-nokey", "01", "action")
    assert b.check_adjust_amount(r, "pt:stock:404", -1) is RedisOutcome.FAILURE
    assert stock(r, "pt:stock:404") == -1, "INCRBY 会把不存在的键当 0，必须先挡住"


def 业务lua可以自定义(r):
    r.cmd("SET", "pt:stock:5", 3)
    b = RedisBarrier("saga", "pg-cas", "01", "action")
    got = b.call(r, """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 'FAILURE' end
redis.call('SET', KEYS[1], ARGV[2])
""", ["pt:stock:5"], ["3", "7"])
    assert got is RedisOutcome.EXECUTED, got
    assert stock(r, "pt:stock:5") == 7


def 回查没见过的单子要固化成回滚(r):
    b = RedisBarrier("msg", "pg-query", "01", "action")
    assert b.query_prepared(r) is RedisOutcome.FAILURE
    # 回查完之后，晚到的正向必须被挡住：
    # 否则 TC 已经按「没提交」回滚了，业务这边却又执行了一次
    r.cmd("SET", "pt:stock:6", 100)
    b2 = RedisBarrier("msg", "pg-query", "01", "action")
    assert b2.check_adjust_amount(r, "pt:stock:6", -10) is RedisOutcome.DUPLICATED
    assert stock(r, "pt:stock:6") == 100


if __name__ == "__main__":
    cases = [
        重复调用同一分支只执行一次,
        正向没跑过时补偿要空转,
        补偿先到时晚到的正向必须被丢弃,
        库存不足要明确失败,
        键不存在不能凭空创建库存,
        业务lua可以自定义,
        回查没见过的单子要固化成回滚,
    ]
    for fn in cases:
        conn = fixture()
        fn(conn)
        print(f"  ✓ {fn.__name__}")
    print(f"Redis 屏障：{len(cases)} 条全过")
