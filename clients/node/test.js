// 三个场景对着真数据库跑。没配环境变量就跳过 —— 跳过不等于通过。
const { Barrier, Decision, MYSQL, POSTGRES } = require('./barrier');
let failed = 0;
const check = (ok, what) => { console.log((ok ? '  ✓ ' : '  ✗ ') + what); if (!ok) failed++; };

async function run(conn, dialect, ph) {
  await Barrier.migrate(conn, dialect);
  await conn.query('DELETE FROM barrier');
  await conn.query('DROP TABLE IF EXISTS acct_node');
  await conn.query('CREATE TABLE acct_node (id INT PRIMARY KEY, bal BIGINT)');
  await conn.query('INSERT INTO acct_node VALUES (1, 1000)');

  const bal = async () => {
    const r = await conn.query('SELECT bal FROM acct_node WHERE id=1');
    const rows = Array.isArray(r) ? r[0] : r.rows;
    return Number(rows[0].bal);
  };
  const once = async (gid, branch, op, delta) => {
    await conn.query('BEGIN');
    try {
      const b = new Barrier(dialect, 'saga', gid, branch, op);
      const dec = await b.decide(conn);
      if (dec === Decision.EXECUTE) {
        await conn.query(`UPDATE acct_node SET bal = bal + ${ph} WHERE id = 1`, [delta]);
      }
      await conn.query('COMMIT');
      return dec;
    } catch (e) { await conn.query('ROLLBACK'); throw e; }
  };

  check(await once('n-1','01','action',-100) === Decision.EXECUTE, '首次调用要执行');
  check(await bal() === 900, `余额扣掉了：${await bal()}`);
  check(await once('n-1','01','action',-100) === Decision.DUPLICATED, '重复调用要被识破');
  check(await bal() === 900, `余额没有被扣第二次：${await bal()}`);
  check(await once('n-2','01','compensate',100) === Decision.NULL_COMPENSATION, '正向没跑过时补偿必须空转');
  check(await bal() === 900, `空回滚不该动余额：${await bal()}`);
  check(await once('n-2','01','action',-100) === Decision.DUPLICATED, '补偿之后迟到的正向必须被丢弃（悬挂）');
  check(await bal() === 900, `悬挂的正向不该扣款：${await bal()}`);
  check(await once('n-3','01','action',-100) === Decision.EXECUTE, '正向执行');
  check(await once('n-3','01','compensate',100) === Decision.EXECUTE, '正向跑过之后补偿要真执行');
  check(await bal() === 900, `补偿把钱退回来了：${await bal()}`);
  check(await once('n-3','01','compensate',100) === Decision.DUPLICATED, '补偿自己也要幂等');
  check(await bal() === 900, `补偿没有退第二次：${await bal()}`);
}

(async () => {
  let ran = 0;
  if (process.env.DTMRS_TEST_PG_NODE) {
    console.log('\n===== postgres ====='); const { Client } = require('pg');
    const c = new Client({ connectionString: process.env.DTMRS_TEST_PG_NODE });
    await c.connect(); await run(c, POSTGRES, '$1'); await c.end(); ran++;
  } else console.log('⚠ 跳过 postgres：DTMRS_TEST_PG_NODE 没配（跳过不等于通过）');

  if (process.env.DTMRS_TEST_MYSQL_NODE) {
    console.log('\n===== mysql ====='); const mysql = require('mysql2/promise');
    const c = await mysql.createConnection(process.env.DTMRS_TEST_MYSQL_NODE);
    await run(c, MYSQL, '?'); await c.end(); ran++;
  } else console.log('⚠ 跳过 mysql：DTMRS_TEST_MYSQL_NODE 没配（跳过不等于通过）');

  if (!ran) { console.log('\n⚠ 一个库都没配，什么都没验到'); process.exit(1); }
  console.log(failed ? `\n✗ ${failed} 项失败` : '\n✓ 全部通过');
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
