#!/usr/bin/env python3
"""dtmrs 吞吐压测。

    python3 bench/bench.py --db sqlite --n 2000 --concurrency 50

测的是**端到端完成一笔两步 SAGA 的吞吐**：
提交 → TC 调两个分支 → 落终态。业务分支是本地零操作的 HTTP 服务，
所以测出来的基本是 TC + 存储的开销。

# 这个数字**不能**用来跟 DTM 比

没有跑同一套硬件、同一个业务服务、同一种存储配置的 DTM 对照组，
所以这里只报 dtmrs 自己在不同存储上的相对表现，以及绝对量级。
拿它去说「比 X 快」是不诚实的。

# 影响结果的因素（报数时必须一起报）

- 存储（sqlite 本地文件 / Postgres / Redis 差一个量级）
- TC 的 tick 间隔：推进器空转时的轮询周期，直接决定延迟下限
- 业务分支耗时：这里是 ~0，真实业务会大得多
- 机器：CPU 核数、磁盘、是否与数据库同机
"""

import argparse
import asyncio
import json
import os
import platform
import statistics
import subprocess
import sys
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

BUSI_PORT = 8899
TC_HTTP = "127.0.0.1:36700"


class Busi(BaseHTTPRequestHandler):
    """零操作业务服务 —— 让测出来的数字尽量只反映 TC 和存储的开销"""

    def do_POST(self):
        self.send_response(200)
        self.send_header("content-length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *a):
        pass


def start_busi():
    s = ThreadingHTTPServer(("127.0.0.1", BUSI_PORT), Busi)
    Thread(target=s.serve_forever, daemon=True).start()
    return s


def wait_http(url, timeout=30):
    for _ in range(timeout * 10):
        try:
            urllib.request.urlopen(url, timeout=1)
            return True
        except Exception:
            time.sleep(0.1)
    return False


async def submit_all(n, concurrency, prefix):
    """并发提交 n 笔事务，返回提交阶段耗时"""
    sem = asyncio.Semaphore(concurrency)
    url = f"http://{TC_HTTP}/api/dtmsvr/submit"
    body_tpl = {
        "gid": "",
        "steps": [
            {"action": f"http://127.0.0.1:{BUSI_PORT}/a1",
             "compensate": f"http://127.0.0.1:{BUSI_PORT}/c1"},
            {"action": f"http://127.0.0.1:{BUSI_PORT}/a2",
             "compensate": f"http://127.0.0.1:{BUSI_PORT}/c2"},
        ],
    }

    async def one(i):
        async with sem:
            b = dict(body_tpl, gid=f"{prefix}-{i}")
            data = json.dumps(b).encode()
            req = urllib.request.Request(
                url, data=data, headers={"content-type": "application/json"})
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: urllib.request.urlopen(req, timeout=30).read())

    t0 = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(n)))
    return time.perf_counter() - t0


def count_final(prefix, n):
    """数一下有多少笔已经落终态"""
    done = 0
    for i in range(n):
        try:
            r = urllib.request.urlopen(
                f"http://{TC_HTTP}/api/dtmsvr/query?gid={prefix}-{i}", timeout=5)
            if json.load(r)["status"] in ("succeed", "failed"):
                done += 1
        except Exception:
            pass
    return done


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="sqlite",
                    help="sqlite | postgres | mysql | redis，或直接给完整 DSN")
    ap.add_argument("--n", type=int, default=1000, help="事务笔数")
    ap.add_argument("--concurrency", type=int, default=50, help="提交并发")
    ap.add_argument("--tick", default="5", help="推进器轮询间隔（毫秒）")
    ap.add_argument("--bin", default="target/release/dtmrs")
    args = ap.parse_args()

    dsn = {
        "sqlite": "sqlite:/tmp/bench.db",
        "postgres": os.environ.get("BENCH_PG", "postgres://postgres:dtmrs@127.0.0.1:55434/dtmrs"),
        "mysql": os.environ.get("BENCH_MYSQL", "mysql://root:dtmrs@127.0.0.1:33306/dtmrs"),
        "redis": os.environ.get("BENCH_REDIS", "redis://127.0.0.1:16379/0"),
    }.get(args.db, args.db)

    if dsn.startswith("sqlite"):
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove("/tmp/bench.db" + suffix)
            except FileNotFoundError:
                pass

    start_busi()
    env = dict(os.environ,
               DTMRS_DB=dsn,
               DTMRS_ADDR=TC_HTTP,
               DTMRS_GRPC_ADDR="127.0.0.1:36701",
               DTMRS_TICK_MS=args.tick,
               RUST_LOG="warn")
    # TC 日志留着 —— 出问题时没日志等于瞎猜
    tc_log = open("/tmp/bench_tc.log", "w")
    tc = subprocess.Popen([args.bin], env=env, stdout=tc_log, stderr=tc_log)
    try:
        if not wait_http(f"http://{TC_HTTP}/health"):
            print("TC 起不来", file=sys.stderr)
            return 1

        prefix = f"b{int(time.time())}"
        submit_secs = asyncio.run(submit_all(args.n, args.concurrency, prefix))

        # 等全部落终态
        t0 = time.perf_counter()
        done, last = 0, 0
        stalled = 0
        while done < args.n and time.perf_counter() - t0 < 300:
            done = count_final(prefix, args.n)
            if done == last:
                stalled += 1
                if stalled > 20:
                    break
            else:
                stalled = 0
            last = done
            if done < args.n:
                time.sleep(0.3)
        total_secs = time.perf_counter() - t0 + submit_secs

        print(f"\n=== dtmrs 压测 · 存储={args.db} ===")
        print(f"  机器          : {platform.processor() or platform.machine()}, "
              f"{os.cpu_count()} 核, {platform.system()}")
        print(f"  事务          : {args.n} 笔 × 2 步（业务分支零操作）")
        print(f"  提交并发      : {args.concurrency}")
        print(f"  推进器 tick   : {args.tick} ms")
        print(f"  ---")
        print(f"  提交阶段      : {submit_secs:.2f}s  →  {args.n/submit_secs:.0f} 笔/秒")
        print(f"  全部落终态    : {total_secs:.2f}s  →  {done/total_secs:.0f} 笔/秒")
        print(f"  分支调用总数  : {done*2}  →  {done*2/total_secs:.0f} 次/秒")
        print(f"  完成          : {done}/{args.n}")
        if done < args.n:
            print(f"  ⚠ 有 {args.n-done} 笔没在时限内完成，数字仅供参考")
        return 0
    finally:
        tc.terminate()
        try:
            tc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            tc.kill()


if __name__ == "__main__":
    sys.exit(main())
