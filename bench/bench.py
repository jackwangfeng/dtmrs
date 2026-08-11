#!/usr/bin/env python3
"""dtmrs 吞吐压测。

    python3 bench/bench.py --db redis --n 2000 --concurrency 50 --workers 8

测的是**端到端完成一笔两步 SAGA 的吞吐**：
提交 → TC 调两个分支 → 落终态。业务分支是本地零操作的 HTTP 服务，
所以测出来的基本是 TC + 存储的开销。

# 这个数字**不能**用来跟 DTM 比

没有跑同一套硬件、同一个业务服务、同一种存储配置的 DTM 对照组，
所以这里只报 dtmrs 自己在不同存储上的相对表现，以及绝对量级。
拿它去说「比 X 快」是不诚实的。

# 压测脚本自己不能成为瓶颈 —— 这里踩过一次

第一版把「零操作业务服务」和「轮询完成情况」放在同一个 Python 进程里，
结果是：不管 TC 开几个推进 worker，吞吐都卡在 ~157 笔/秒纹丝不动。
量了才知道那是**脚本自己的天花板**：单进程的 ThreadingHTTPServer
实测只有 781 req/s，而一笔两步事务要占掉 2 次；同一个 GIL 上还挂着
几十个查询线程在每 50ms 把上千个 gid 全部重查一遍。测的是 Python，
不是 dtmrs。

所以现在：

1. **业务服务开 K 个独立进程**，靠 `SO_REUSEPORT` 共享同一个端口，
   由内核做负载均衡。K 个进程 = K 份 GIL。
2. **完成判定不再靠轮询 HTTP**。业务服务在第二步动作里给一个
   `multiprocessing.Value` 原子加一 —— 主进程直接读共享内存，
   零网络、零 GIL 争抢。
3. 提交和最后的核对都用 **keep-alive 长连接**，不再每个请求重开 TCP。

代价是主指标从「落终态」变成「最后一步动作成功」，两者差一次存储写入。
所以跑完还会**全量核对一遍真实终态**（不计时），对不上就明确报出来 ——
只报一个自己没验过的数字是不诚实的。

# ⚠ docker 的端口映射会吃掉 36%

存储跑在 docker 里、靠 `-p 16379:6379` 发布端口的话，每次往返要多走一层
NAT / docker-proxy。实测：

| 路径 | redis-benchmark | 单连接 p50 | 本压测（msg 1 步）|
|---|---|---|---|
| `--network host` | 187k req/s | 0.015 ms | **11573 笔/秒** |
| `-p 16379:6379` | 151k req/s | 0.023 ms | 8500 笔/秒 |

每次往返只多 ~8µs，但推一笔事务要串行打几十次 Redis，累积就是 36%。
**报数时必须说明用的是哪种**。想复现 host 那一列：

    docker run -d --name dtmrs-redis-host --network host redis:7 \
        redis-server --port 16999 --save ''
    BENCH_REDIS=redis://127.0.0.1:16999/1 python3 bench/bench.py --db redis ...

# 影响结果的因素（报数时必须一起报）

- 存储（sqlite 本地文件 / Postgres / MySQL / Redis 差一个量级）
- **存储的网络路径**：见上面那节，docker 端口映射差 36%
- 事务模式（`--mode saga|msg`）和步数（`--steps`）
- 推进 worker 数（`--workers`）：单进程内并行抢占的协程数
- TC 的 tick 间隔：推进器空转时的轮询周期，直接决定延迟下限
- 业务分支耗时：这里是 ~0，真实业务会大得多
- 机器：CPU 核数、磁盘、是否与数据库同机
- 压测脚本的进程数（`--busi-procs` / `--client-procs`）：不够就是在测脚本
"""

import argparse
import http.client
import json
import multiprocessing as mp
import os
import platform
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BUSI_PORT = 8899
TC_HOST, TC_PORT = "127.0.0.1", 36700
TC_HTTP = f"{TC_HOST}:{TC_PORT}"

# 最后一步的正向动作路径。命中它 == 这笔事务的最后一步业务动作成功了。
# 由 --steps 决定，main() 里会改写
FINAL_ACTION = "/a2"


class Busi(BaseHTTPRequestHandler):
    """零操作业务服务 —— 让测出来的数字尽量只反映 TC 和存储的开销。

    唯一的副作用是给 `DONE` 加一，用来做完成判定（见模块头注释）。
    """

    protocol_version = "HTTP/1.1"  # 长连接，省掉每次调用的 TCP 握手
    # ⚠ 必须关 Nagle。http.server 的响应分两次 write（头一次、体一次），
    # 开着 Nagle 时第二段要等对端的延迟 ACK —— 实测每次分支调用固定多出
    # ~40ms，一笔两步事务就是 ~83ms，把吞吐死死压在 12 笔/秒/worker。
    # 真实业务服务（Go / Java / nginx）默认就是 TCP_NODELAY，所以这纯粹是
    # 压测脚本的伪影，不修就是在给 dtmrs 栽赃
    disable_nagle_algorithm = True
    DONE = None

    def do_POST(self):
        n = int(self.headers.get("content-length") or 0)
        if n:
            self.rfile.read(n)
        # ⚠ 必须切掉 query —— TC 调分支时会带上 ?gid=..&branch_id=..&op=..，
        # 拿整个 path 去比对永远不相等（第一版就是这么静默算出 0 笔/秒的）
        if self.path.split("?", 1)[0] == FINAL_ACTION:
            with Busi.DONE.get_lock():
                Busi.DONE.value += 1
        self.send_response(200)
        self.send_header("content-length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *a):
        pass


class ReusePortServer(ThreadingHTTPServer):
    # 3.11+ 才有。K 个进程绑同一个端口，内核按连接分发
    allow_reuse_port = True
    daemon_threads = True


def busi_main(done):
    Busi.DONE = done
    ReusePortServer(("127.0.0.1", BUSI_PORT), Busi).serve_forever()


def start_busi(procs, done):
    """起 K 个业务服务进程。返回句柄列表，跑完要 terminate"""
    ps = []
    for _ in range(procs):
        p = mp.Process(target=busi_main, args=(done,), daemon=True)
        p.start()
        ps.append(p)
    # 等端口真的能连上，否则前几笔提交会打空
    for _ in range(200):
        try:
            socket.create_connection(("127.0.0.1", BUSI_PORT), timeout=0.5).close()
            return ps
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("业务服务起不来")


def wait_http(path, timeout=30):
    for _ in range(timeout * 10):
        try:
            c = http.client.HTTPConnection(TC_HOST, TC_PORT, timeout=1)
            c.request("GET", path)
            c.getresponse().read()
            return True
        except Exception:
            time.sleep(0.1)
    return False


def client_calls(mode, gid, steps):
    """一笔事务客户端要按顺序发的请求，`[(path, body), ...]`。

    两种模式的**客户端往返次数不一样**，这是它们本质的差别之一：

    - `saga`：一次 submit 就带上全部步骤和补偿
    - `msg`：prepare →（业务自己的本地事务）→ submit，**两次**。
      换来的是没有补偿分支：秒杀这类场景本来也没有「反向扣库存」这回事，
      一致性靠「本地事务成功了就一定把消息投出去」保证
    """
    acts = [f"http://127.0.0.1:{BUSI_PORT}/a{i + 1}" for i in range(steps)]
    if mode == "saga":
        body = {
            "gid": gid,
            "steps": [
                {"action": a, "compensate": f"http://127.0.0.1:{BUSI_PORT}/c{i + 1}"}
                for i, a in enumerate(acts)
            ],
        }
        return [("/api/dtmsvr/submit", json.dumps(body).encode())]

    prepare = {
        "gid": gid,
        "trans_type": "msg",
        "actions": acts,
        # 崩在 prepare 和 submit 之间时 TC 靠它决断，这里跑不到
        "query_prepared": f"http://127.0.0.1:{BUSI_PORT}/query",
    }
    return [
        ("/api/dtmsvr/prepare", json.dumps(prepare).encode()),
        ("/api/dtmsvr/submit", json.dumps({"gid": gid, "trans_type": "msg"}).encode()),
    ]


def submit_all(n, concurrency, prefix, mode, steps, procs):
    """并发提交 n 笔事务，返回提交阶段耗时。

    ⚠ **提交端必须是多进程的**，理由和业务服务那边一样：单个 Python 进程
    的 HTTP 客户端实测只能推 ~5500 req/s（打 `/health` 这种零成本端点也一样），
    再往上就是 GIL 在挡。

    这个坑差点让人得出错误结论：比较 saga 和 msg 时，msg 每笔要发两个请求
    （prepare + submit），于是它「看起来慢 45%」—— 其实两种模式的提交阶段
    都顶在客户端的 5500 req/s 上，测的根本不是 dtmrs。
    """
    per = (n + procs - 1) // procs
    t0 = time.perf_counter()
    ps = []
    for k in range(procs):
        lo, hi = k * per, min((k + 1) * per, n)
        if lo >= hi:
            break
        p = mp.Process(target=submit_slice,
                       args=(lo, hi, concurrency, prefix, mode, steps), daemon=True)
        p.start()
        ps.append(p)
    for p in ps:
        p.join()
    return time.perf_counter() - t0


def submit_slice(lo, hi, concurrency, prefix, mode, steps):
    """一个提交进程负责 `[lo, hi)` 这一段。

    每个线程一条 keep-alive 长连接 —— 早期版本每笔都重开 TCP，
    提交阶段测的一半是握手
    """
    work = [client_calls(mode, f"{prefix}-{i}", steps) for i in range(lo, hi)]

    local = threading.local()

    def conn():
        if not hasattr(local, "c"):
            local.c = http.client.HTTPConnection(TC_HOST, TC_PORT, timeout=30)
        return local.c

    def send(path, body):
        for attempt in range(2):
            try:
                c = conn()
                c.request("POST", path, body, {"content-type": "application/json"})
                c.getresponse().read()
                return
            except Exception:
                # 长连接被对端关掉是正常的，重建一次再试
                try:
                    local.c.close()
                except Exception:
                    pass
                del local.c
                if attempt:
                    raise

    def one(calls):
        for path, body in calls:
            send(path, body)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(one, work))


def verify_final(prefix, n, concurrency=16, patience=30):
    """全量核对真实终态。**不计时** —— 这是正确性核对，不是性能指标。

    ⚠ 必须重试着等。主指标是「最后一步动作成功」，它比「落终态」早一次
    存储写入；Postgres / MySQL 上这个尾巴能拖好几秒。一跑完就立刻快照，
    会把还在收尾的事务误报成「没落终态」。

    返回 (终态笔数, 状态分布)
    """
    local = threading.local()

    def q(i):
        if not hasattr(local, "c"):
            local.c = http.client.HTTPConnection(TC_HOST, TC_PORT, timeout=10)
        try:
            local.c.request("GET", f"/api/dtmsvr/query?gid={prefix}-{i}")
            return json.loads(local.c.getresponse().read())["status"]
        except Exception:
            try:
                local.c.close()
            except Exception:
                pass
            del local.c
            return "查询失败"

    deadline = time.perf_counter() + patience
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        while True:
            dist = {}
            for st in pool.map(q, range(n)):
                dist[st] = dist.get(st, 0) + 1
            final = dist.get("succeed", 0) + dist.get("failed", 0)
            if final == n or time.perf_counter() > deadline:
                return final, dist
            time.sleep(0.5)


def reset_db(dsn):
    """把库清空，返回一句「做了什么」的说明。

    ⚠ 这一步不能省，也不能失败了当没事：早期版本只清 sqlite 文件，
    Postgres / MySQL / Redis 的数据一轮轮往上堆。跑到 4 万笔存量之后，
    同一条命令的结果从 2430 笔/秒掉到 1317 —— **报出来的数字不可复现**，
    还会让人误以为是代码变慢了。

    清不掉就明确说清不掉，让报数的人自己判断，别默默给个脏数字。
    """
    import shlex
    import urllib.parse as up

    if dsn.startswith("sqlite"):
        path = dsn.split(":", 1)[1].split("?")[0]
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(path + suffix)
            except FileNotFoundError:
                pass
        return f"已删除 {path}"

    u = up.urlparse(dsn)
    if dsn.startswith("redis"):
        db = (u.path or "/0").lstrip("/") or "0"
        cmd = f"redis-cli -h {u.hostname} -p {u.port or 6379} -n {db} flushdb"
    elif dsn.startswith("postgres"):
        env_pw = f"PGPASSWORD={shlex.quote(up.unquote(u.password or ''))} "
        cmd = (f"{env_pw}psql -h {u.hostname} -p {u.port or 5432} "
               f"-U {u.username} -d {u.path.lstrip('/')} -q -c "
               f"'TRUNCATE trans_global, trans_branch_op'")
    elif dsn.startswith("mysql"):
        cmd = (f"mysql -h {u.hostname} -P {u.port or 3306} -u {u.username} "
               f"-p{up.unquote(u.password or '')} {u.path.lstrip('/')} "
               f"-e 'TRUNCATE trans_global; TRUNCATE trans_branch_op'")
    else:
        return "⚠ 不认识的 DSN，没清库 —— 数字会受存量数据影响"

    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if r.returncode == 0:
        return "已清空"
    # 表还不存在是正常的（第一次跑），别当错误
    err = (r.stderr or "").strip().splitlines()
    if any("does not exist" in e or "Unknown table" in e or "doesn't exist" in e
           for e in err):
        return "库是空的（表还没建）"
    return f"⚠ 没清掉（{err[-1] if err else r.returncode}）—— 数字会受存量数据影响"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="sqlite",
                    help="sqlite | postgres | mysql | redis，或直接给完整 DSN")
    ap.add_argument("--n", type=int, default=1000, help="事务笔数")
    ap.add_argument("--concurrency", type=int, default=50, help="提交并发")
    ap.add_argument("--tick", default="5", help="推进器轮询间隔（毫秒）")
    ap.add_argument("--workers", default="8", help="推进器并行 worker 数")
    ap.add_argument("--busi-procs", type=int, default=8,
                    help="零操作业务服务的进程数。太少就是在测压测脚本自己")
    ap.add_argument("--client-procs", type=int, default=8,
                    help="提交端的进程数。单进程只能推 ~5500 req/s，太少同样是在测脚本")
    ap.add_argument("--mode", default="saga", choices=("saga", "msg"),
                    help="saga（带补偿）| msg（二阶段消息，秒杀那类场景用的）")
    ap.add_argument("--steps", type=int, default=2, help="一笔事务几个正向分支")
    ap.add_argument("--bin", default="target/release/dtmrs")
    ap.add_argument("--quiet", action="store_true", help="只打一行结果，方便扫参数")
    ap.add_argument("--no-verify", action="store_true",
                    help="跳过终态核对。**只在数存储命令次数时用** —— 核对本身要把\n                         每笔事务都查一遍，会把每笔的命令数算多（踩过）")
    args = ap.parse_args()

    # 完成判定盯的是最后一步正向动作
    global FINAL_ACTION
    FINAL_ACTION = f"/a{args.steps}"

    dsn = {
        "sqlite": "sqlite:/tmp/bench.db",
        "postgres": os.environ.get("BENCH_PG", "postgres://postgres:dtmrs@127.0.0.1:55434/dtmrs"),
        "mysql": os.environ.get("BENCH_MYSQL", "mysql://root:dtmrs@127.0.0.1:33306/dtmrs"),
        # ⚠ 刻意用 db 1，别改回 0：Redis 测试跑在 db 0，开头要 flush_prefix()
        # 扫全库。压测一跑就是几十万个 key，测试会直接扫到超时（踩过）
        "redis": os.environ.get("BENCH_REDIS", "redis://127.0.0.1:16379/1"),
    }.get(args.db, args.db)

    reset = reset_db(dsn)

    done = mp.Value("i", 0)
    busi = start_busi(args.busi_procs, done)

    env = dict(os.environ,
               DTMRS_DB=dsn,
               DTMRS_ADDR=TC_HTTP,
               DTMRS_GRPC_ADDR="127.0.0.1:36701",
               DTMRS_TICK_MS=args.tick,
               DTMRS_WORKERS=args.workers,
               RUST_LOG="warn")
    # TC 日志留着 —— 出问题时没日志等于瞎猜
    tc_log = open("/tmp/bench_tc.log", "w")
    tc = subprocess.Popen([args.bin], env=env, stdout=tc_log, stderr=tc_log)
    try:
        if not wait_http("/health"):
            print("TC 起不来，看 /tmp/bench_tc.log", file=sys.stderr)
            return 1

        prefix = f"b{int(time.time())}"
        t_start = time.perf_counter()
        submit_secs = submit_all(args.n, args.concurrency, prefix,
                                 args.mode, args.steps, args.client_procs)

        # 完成判定：直接读共享内存里的计数，不发一个请求（见模块头注释）
        stalled = 0
        last = 0
        while done.value < args.n and time.perf_counter() - t_start < 300:
            time.sleep(0.02)
            if done.value == last:
                stalled += 1
                if stalled > 500:  # 连续 10 秒没有任何进展
                    break
            else:
                stalled, last = 0, done.value
        drive_secs = time.perf_counter() - t_start
        finished = done.value

        # 不计时的正确性核对
        if args.no_verify:
            final_n, dist = -1, {"跳过核对": args.n}
        else:
            final_n, dist = verify_final(prefix, args.n)

        if args.quiet:
            # 清库失败一定要带出来 —— 存量数据会让数字慢慢往下漂
            warn = "" if reset.startswith(("已", "库是")) else f"  {reset}"
            print(f"{args.mode:5s} {args.db:9s} workers={args.workers:>3s}  "
                  f"{finished/drive_secs:7.0f} 笔/秒  "
                  f"终态 {final_n}/{args.n}{warn}")
            return 0

        print(f"\n=== dtmrs 压测 · 存储={args.db} ===")
        print(f"  机器          : {platform.processor() or platform.machine()}, "
              f"{os.cpu_count()} 核, {platform.system()}")
        print(f"  模式          : {args.mode}")
        print(f"  事务          : {args.n} 笔 × {args.steps} 步（业务分支零操作）")
        print(f"  提交并发      : {args.concurrency}")
        print(f"  推进器 tick   : {args.tick} ms")
        print(f"  推进 worker   : {args.workers}")
        print(f"  业务服务进程  : {args.busi_procs}")
        print(f"  提交端进程    : {args.client_procs}")
        print(f"  跑之前清库    : {reset}")
        print(f"  ---")
        print(f"  提交阶段      : {submit_secs:.2f}s  →  {args.n/submit_secs:.0f} 笔/秒")
        print(f"  提交+推完     : {drive_secs:.2f}s  →  {finished/drive_secs:.0f} 笔/秒")
        print(f"  分支调用      : {finished*2} 次  →  {finished*2/drive_secs:.0f} 次/秒")
        print(f"  ---")
        print(f"  终态核对      : {final_n}/{args.n}  {dist}")
        if finished < args.n:
            print(f"  ⚠ 有 {args.n-finished} 笔的最后一步没在时限内跑完，数字仅供参考")
        if final_n < args.n:
            print(f"  ⚠ 有 {args.n-final_n} 笔没落终态")
        return 0
    finally:
        tc.terminate()
        try:
            tc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            tc.kill()
        for p in busi:
            p.terminate()


if __name__ == "__main__":
    sys.exit(main())
