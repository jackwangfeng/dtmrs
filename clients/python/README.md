# dtmrs-barrier

[dtmrs](https://github.com/jackwangfeng/dtmrs) 的子事务屏障 —— 业务服务（RM）侧接入用。

分支接口**一定会**被重复调用（TC 重试 + 崩溃恢复），而且可能乱序到达。
一张表 + 一条 `INSERT IGNORE` 同时解决三个问题：

| 难题 | 场景 |
|---|---|
| **幂等** | 同一分支被调两次 → 只执行一次 |
| **空回滚** | 正向没跑过就来了补偿 → 补偿空转 |
| **悬挂** | 补偿先到、正向后到 → 丢弃迟到的正向 |

零运行时依赖，只需要一个 DB-API 2.0 游标（psycopg2 / pymysql / sqlite3 都行）。

```python
from dtmrs_barrier import Barrier, Decision, MYSQL

Barrier.migrate(conn, MYSQL)          # 启动时一次

b = Barrier(MYSQL, trans_type, gid, branch_id, op)
cur = conn.cursor()
try:
    if b.decide(cur) == Decision.EXECUTE:
        # 业务 SQL —— 必须用这个 cur，跟屏障记录同一个事务
        cur.execute("UPDATE account SET balance = balance - %s WHERE id = %s", (amt, uid))
    conn.commit()      # 原子性的来源
except Exception:
    conn.rollback()
    raise
```

`NULL_COMPENSATION` 和 `DUPLICATED` 都是**正常路径**，接口应返回**成功**。

## ⚠ 三条不能违反的

1. **屏障表必须和业务表在同一个数据库实例** —— 不同实例没法共用本地事务，方案失效
2. **业务 SQL 必须用传给 `decide` 的那个 cursor**
3. **MySQL 上绝不能用 `ON DUPLICATE KEY UPDATE`** —— 它重复时 rowcount 返回 1 不是 0，
   整个算法依赖「冲突时必须是 0」

完整文档见 [接入指南](https://github.com/jackwangfeng/dtmrs/blob/master/docs/integration.md)。

Apache-2.0
