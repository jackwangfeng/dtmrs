// 可运行的分支服务示例 —— 用标准库 node:http，无框架依赖。
// 屏障那部分的逻辑跟你用 Express/Fastify 时完全一样。
'use strict';
const http = require('node:http');
const { URL } = require('node:url');
const mysql = require('mysql2/promise');
const { Barrier, Decision, MYSQL } = require('../barrier');

const pool = mysql.createPool(process.env.EX_MYSQL_NODE);
// 每条路由：正负号表示账户怎么动（0 = 不动账）
const ROUTES = { '/deduct': -1, '/refund': +1, '/ok': 0, '/noop': 0 };

const reply = (res, code, result) =>
  res.writeHead(code, { 'content-type': 'application/json' })
     .end(JSON.stringify({ dtm_result: result }));

(async () => {
  // 启动时建表一次
  const c0 = await pool.getConnection();
  await Barrier.migrate(c0, MYSQL);
  c0.release();

  http.createServer(async (req, res) => {
    const u = new URL(req.url, 'http://x');
    const q = Object.fromEntries(u.searchParams);

    if (u.pathname === '/reject') return reply(res, 409, 'FAILURE');  // 业务明确拒绝
    if (!(u.pathname in ROUTES)) return reply(res, 404, 'FAILURE');

    const sign = ROUTES[u.pathname];
    const amount = Number(q.amount || 100);

    // ⚠ 必须拿独占连接：事务要在同一个连接上跑完。
    //   直接用 pool 的话每条语句可能落到不同连接，事务就散了
    const conn = await pool.getConnection();
    try {
      await conn.query('BEGIN');
      const b = new Barrier(MYSQL, q.trans_type, q.gid, q.branch_id, q.op);
      if (await b.decide(conn) === Decision.EXECUTE && sign !== 0) {
        const [r] = await conn.query(
          'UPDATE ex_account SET balance = balance + ? WHERE id = 1 AND balance + ? >= 0',
          [sign * amount, sign * amount]);
        if (r.affectedRows === 0) {
          await conn.query('ROLLBACK');
          return reply(res, 409, 'FAILURE');   // 余额不足 = 业务明确拒绝
        }
      }
      // 空回滚 / 重复请求走到这里，什么都没做，同样返回成功
      await conn.query('COMMIT');
      reply(res, 200, 'SUCCESS');
    } catch (e) {
      await conn.query('ROLLBACK').catch(() => {});
      // 异常 = 结果**未知** → 5xx 让 TC 重试。绝不能返回 409
      console.error('branch error:', e.message);
      reply(res, 500, 'ONGOING');
    } finally {
      conn.release();
    }
  }).listen(Number(process.env.EX_PORT), '127.0.0.1');
})();
