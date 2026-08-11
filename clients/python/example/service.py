#!/usr/bin/env python3
"""可运行的分支服务示例 —— 用标准库 http.server，无框架依赖。
屏障那部分的逻辑跟你用 Flask/FastAPI 时完全一样。"""
import json, os, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import pymysql
from dtmrs_barrier import Barrier, Decision, MYSQL

CONN = dict(host=os.environ["EX_MYSQL_HOST"], port=int(os.environ["EX_MYSQL_PORT"]),
            user=os.environ["EX_MYSQL_USER"], password=os.environ["EX_MYSQL_PASS"],
            database=os.environ["EX_MYSQL_DB"])

# 启动时建表一次
with pymysql.connect(**CONN) as c:
    Barrier.migrate(c, MYSQL)

# 每条路由：正负号表示账户怎么动（0 = 不动账）
ROUTES = {"/deduct": -1, "/refund": +1, "/ok": 0, "/noop": 0}


class H(BaseHTTPRequestHandler):
    def reply(self, code, result):
        body = json.dumps({"dtm_result": result}).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}

        if u.path == "/reject":
            # 业务**明确**拒绝 → 409，TC 会逆序补偿
            return self.reply(409, "FAILURE")
        if u.path not in ROUTES:
            return self.reply(404, "FAILURE")

        sign = ROUTES[u.path]
        amount = int(q.get("amount") or 100)
        conn = pymysql.connect(**CONN)
        try:
            b = Barrier(MYSQL, q["trans_type"], q["gid"], q["branch_id"], q["op"])
            cur = conn.cursor()
            if b.decide(cur) == Decision.EXECUTE and sign != 0:
                n = cur.execute(
                    "UPDATE ex_account SET balance = balance + %s "
                    "WHERE id = 1 AND balance + %s >= 0",
                    (sign * amount, sign * amount))
                if n == 0:
                    conn.rollback()
                    # 余额不足 = 业务明确拒绝 → 409
                    return self.reply(409, "FAILURE")
            # 空回滚 / 重复请求走到这里，什么都没做，同样返回成功
            conn.commit()
            self.reply(200, "SUCCESS")
        except Exception as e:
            conn.rollback()
            # 异常 = 结果**未知** → 5xx 让 TC 重试。绝不能返回 409
            print("branch error:", e, flush=True)
            self.reply(500, "ONGOING")
        finally:
            conn.close()

    def log_message(self, *a):
        pass


ThreadingHTTPServer(("127.0.0.1", int(os.environ["EX_PORT"])), H).serve_forever()
