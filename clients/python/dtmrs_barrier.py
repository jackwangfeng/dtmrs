"""dtmrs 子事务屏障 —— 业务服务（RM）侧接入用。

分支接口**一定会**被重复调用（TC 重试 + 崩溃恢复），而且可能乱序到达。
这个模块用一张表 + 一条 INSERT IGNORE 同时解决三个问题：

    幂等    同一分支被调两次        → 只执行一次
    空回滚  正向没跑过就来了补偿    → 补偿空转
    悬挂    补偿先到、正向后到      → 丢弃迟到的正向

# 用法

    from dtmrs_barrier import Barrier, Decision, MYSQL

    # 启动时建表一次
    Barrier.migrate(conn, MYSQL)

    # 每次处理分支请求（gid / branch_id / op / trans_type 由 TC 传进来）
    b = Barrier(MYSQL, trans_type, gid, branch_id, op)
    try:
        with conn.cursor() as cur:
            if b.decide(cur) == Decision.EXECUTE:
                # 业务 SQL —— 必须用这个 cur，跟屏障记录同一个事务
                cur.execute("UPDATE account SET balance = balance - %s WHERE id = %s",
                            (amt, uid))
        conn.commit()      # 原子性的来源：屏障记录与业务变更同生共死
    except Exception:
        conn.rollback()
        raise

⚠ 两条不能违反的前提：

1. 屏障表必须和业务表在**同一个数据库实例** —— 不同实例没法共用一个本地事务，
   这个方案直接失效。不是实现限制，是它成立的根本条件。
2. 业务 SQL 必须用传给 decide 的那个 cursor，且同一个事务提交。

返回值语义：NULL_COMPENSATION 和 DUPLICATED 都是**正常路径**，你的接口应该
返回**成功**而不是失败 —— 返回失败会让 TC 以为分支出错了。

本类**不是线程安全的**，每次请求新建一个实例。
"""

import time
from enum import Enum

__all__ = ["Barrier", "Decision", "MYSQL", "POSTGRES", "SQLITE"]

MYSQL = "mysql"
POSTGRES = "postgres"
SQLITE = "sqlite"

_KNOWN_OPS = {"action", "compensate", "try", "confirm", "cancel", "commit", "rollback"}

# 补偿类操作 → 对应的正向操作。判空回滚全靠它
_ORIGIN = {"compensate": "action", "rollback": "action", "cancel": "try"}


class Decision(Enum):
    """屏障给出的判定"""

    #: 该干活。调用方在同一个事务里执行业务 SQL
    EXECUTE = "execute"
    #: **空回滚**：正向分支从没执行过，补偿直接空转。接口应返回成功
    NULL_COMPENSATION = "null_compensation"
    #: **重复或悬挂**：这次调用已处理过，跳过。接口应返回成功
    DUPLICATED = "duplicated"


class Barrier:
    def __init__(self, dialect, trans_type, gid, branch_id, op, table="barrier"):
        if not gid or not branch_id:
            raise ValueError("gid / branch_id 不能为空")
        if op not in _KNOWN_OPS:
            raise ValueError(f"未知 op: {op}")
        self.dialect = dialect
        self.trans_type = trans_type
        self.gid = gid
        self.branch_id = branch_id
        self.op = op
        self.table = table
        self._counter = 0

    # ---------------- 建表 ----------------

    @staticmethod
    def migrate(conn, dialect, table="barrier"):
        """建屏障表。启动时调一次即可，重复调用无害。"""
        # MySQL 不能对 TEXT 建索引（1170 要 key length），必须定长
        id_text = "VARCHAR(128)" if dialect == MYSQL else "TEXT"
        id_short = "VARCHAR(45)" if dialect == MYSQL else "TEXT"
        sql = f"""CREATE TABLE IF NOT EXISTS {table} (
  trans_type  {id_short} NOT NULL,
  gid         {id_text} NOT NULL,
  branch_id   {id_text} NOT NULL,
  op          {id_short} NOT NULL,
  barrier_id  {id_short} NOT NULL,
  reason      {id_short} NOT NULL,
  create_time BIGINT NOT NULL,
  PRIMARY KEY (gid, branch_id, op, barrier_id)
)"""
        cur = conn.cursor()
        try:
            cur.execute(sql)
            conn.commit()
        finally:
            cur.close()

    # ---------------- 判定 ----------------

    def decide(self, cur) -> Decision:
        """做出判定。**必须在业务事务里调用**，业务 SQL 要用同一个 cursor。

        算法：补偿方先用「正向分支」的名义插一行去占坑。占成功了说明正向从没
        来过（空回滚）；占失败了说明正向真跑过（是真补偿）。而这个坑一旦被占，
        迟到的正向分支就再也插不进来（悬挂被丢弃）。
        """
        self._counter += 1
        bid = f"{self._counter:02d}"

        origin = _ORIGIN.get(self.op)
        origin_affected = self._insert(cur, origin, bid) if origin else 0
        current_affected = self._insert(cur, self.op, bid)

        if origin and origin_affected > 0:
            # 正向分支从没跑过（否则那行早被它自己占了）→ 空回滚
            return Decision.NULL_COMPENSATION
        if current_affected == 0:
            # 这个 (gid, branch, op, bid) 已经处理过了 → 重复请求或悬挂
            return Decision.DUPLICATED
        return Decision.EXECUTE

    def _insert(self, cur, op, bid) -> int:
        if self.dialect == MYSQL:
            # ⚠ 这里**绝不能**用 ON DUPLICATE KEY UPDATE：
            # 它在重复时 rowcount 返回 1（不是 0），整个算法就废了。
            # INSERT IGNORE 重复时返回 0，跟另外两家一致。
            sql = (
                f"INSERT IGNORE INTO {self.table}"
                " (trans_type,gid,branch_id,op,barrier_id,reason,create_time)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s)"
            )
        elif self.dialect == POSTGRES:
            sql = (
                f"INSERT INTO {self.table}"
                " (trans_type,gid,branch_id,op,barrier_id,reason,create_time)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
            )
        else:  # sqlite 用 ? 占位符
            sql = (
                f"INSERT INTO {self.table}"
                " (trans_type,gid,branch_id,op,barrier_id,reason,create_time)"
                " VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING"
            )
        cur.execute(
            sql,
            (
                self.trans_type,
                self.gid,
                self.branch_id,
                op,
                bid,
                self.op,  # reason = 是哪个分支插的这行，排查用
                int(time.time()),
            ),
        )
        # DB-API 的 rowcount：插入成功是 1，被忽略是 0
        return cur.rowcount
