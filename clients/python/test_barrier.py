#!/usr/bin/env python3
"""三个场景对着真数据库跑。没配环境变量就跳过 —— 跳过不等于通过。"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dtmrs_barrier import Barrier, Decision, MYSQL, POSTGRES

failed = 0
def check(ok, what):
    global failed
    print(("  ✓ " if ok else "  ✗ ") + what)
    if not ok: failed += 1

def run(conn, dialect, ph):
    Barrier.migrate(conn, dialect)
    cur = conn.cursor()
    cur.execute("DELETE FROM barrier")
    cur.execute("DROP TABLE IF EXISTS acct_py")
    cur.execute("CREATE TABLE acct_py (id INT PRIMARY KEY, bal BIGINT)")
    cur.execute("INSERT INTO acct_py VALUES (1, 1000)")
    conn.commit(); cur.close()

    def bal():
        c = conn.cursor(); c.execute("SELECT bal FROM acct_py WHERE id=1")
        v = c.fetchone()[0]; conn.commit(); c.close(); return v

    def once(gid, branch, op, delta):
        b = Barrier(dialect, "saga", gid, branch, op)
        c = conn.cursor()
        try:
            dec = b.decide(c)
            if dec == Decision.EXECUTE:
                c.execute(f"UPDATE acct_py SET bal = bal + {ph} WHERE id = 1", (delta,))
            conn.commit()
            return dec
        except Exception:
            conn.rollback(); raise
        finally:
            c.close()

    check(once("p-1","01","action",-100) == Decision.EXECUTE, "首次调用要执行")
    check(bal() == 900, f"余额扣掉了：{bal()}")
    check(once("p-1","01","action",-100) == Decision.DUPLICATED, "重复调用要被识破")
    check(bal() == 900, f"余额没有被扣第二次：{bal()}")
    check(once("p-2","01","compensate",100) == Decision.NULL_COMPENSATION, "正向没跑过时补偿必须空转")
    check(bal() == 900, f"空回滚不该动余额：{bal()}")
    check(once("p-2","01","action",-100) == Decision.DUPLICATED, "补偿之后迟到的正向必须被丢弃（悬挂）")
    check(bal() == 900, f"悬挂的正向不该扣款：{bal()}")
    check(once("p-3","01","action",-100) == Decision.EXECUTE, "正向执行")
    check(once("p-3","01","compensate",100) == Decision.EXECUTE, "正向跑过之后补偿要真执行")
    check(bal() == 900, f"补偿把钱退回来了：{bal()}")
    check(once("p-3","01","compensate",100) == Decision.DUPLICATED, "补偿自己也要幂等")
    check(bal() == 900, f"补偿没有退第二次：{bal()}")

ran = 0
if os.environ.get("DTMRS_TEST_PG_PY"):
    import psycopg2
    print("\n===== postgres =====")
    with psycopg2.connect(os.environ["DTMRS_TEST_PG_PY"]) as c:
        run(c, POSTGRES, "%s"); ran += 1
else:
    print("⚠ 跳过 postgres：DTMRS_TEST_PG_PY 没配（跳过不等于通过）")

if os.environ.get("DTMRS_TEST_MYSQL_PY"):
    import pymysql
    print("\n===== mysql =====")
    u = os.environ["DTMRS_TEST_MYSQL_PY"].split("|")
    c = pymysql.connect(host=u[0], port=int(u[1]), user=u[2], password=u[3], database=u[4])
    run(c, MYSQL, "%s"); c.close(); ran += 1
else:
    print("⚠ 跳过 mysql：DTMRS_TEST_MYSQL_PY 没配（跳过不等于通过）")

if ran == 0: print("\n⚠ 一个库都没配，什么都没验到"); sys.exit(1)
print("\n✓ 全部通过" if failed == 0 else f"\n✗ {failed} 项失败"); sys.exit(1 if failed else 0)
