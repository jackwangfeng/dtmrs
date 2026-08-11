# dtmrs-barrier

[dtmrs](https://github.com/jackwangfeng/dtmrs) 的子事务屏障 —— 业务服务（RM）侧接入用。

分支接口**一定会**被重复调用（TC 重试 + 崩溃恢复），而且可能乱序到达。
一张表 + 一条 `INSERT IGNORE` 同时解决三个问题：**幂等 / 空回滚 / 悬挂**。

零运行时依赖，适配 `pg` 和 `mysql2` 的返回格式，自带 TypeScript 类型。

```js
const { Barrier, Decision, POSTGRES } = require('dtmrs-barrier');

await Barrier.migrate(client, POSTGRES);        // 启动时一次

await client.query('BEGIN');
try {
  const b = new Barrier(POSTGRES, transType, gid, branchId, op);
  if (await b.decide(client) === Decision.EXECUTE) {
    // 业务 SQL —— 必须用这个 client，跟屏障记录同一个事务
    await client.query('UPDATE account SET balance = balance - $1 WHERE id = $2', [amt, uid]);
  }
  await client.query('COMMIT');    // 原子性的来源
} catch (e) {
  await client.query('ROLLBACK');
  throw e;
}
```

`NULL_COMPENSATION` 和 `DUPLICATED` 都是**正常路径**，接口应返回**成功**。

## ⚠ 三条不能违反的

1. **屏障表必须和业务表在同一个数据库实例** —— 不同实例没法共用本地事务，方案失效
2. **业务 SQL 必须用传给 `decide` 的那个连接**，且事务要在同一个连接上跑完
   （用连接池记得 `pool.connect()` 拿独占连接，别直接用 pool）
3. **MySQL 上绝不能用 `ON DUPLICATE KEY UPDATE`** —— 它重复时 affectedRows 返回 1
   不是 0，整个算法依赖「冲突时必须是 0」

完整文档见 [接入指南](https://github.com/jackwangfeng/dtmrs/blob/master/docs/integration.md)。

Apache-2.0
