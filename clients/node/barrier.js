/**
 * dtmrs 子事务屏障 —— 业务服务（RM）侧接入用。
 *
 * 分支接口**一定会**被重复调用（TC 重试 + 崩溃恢复），而且可能乱序到达。
 * 这个模块用一张表 + 一条 INSERT IGNORE 同时解决三个问题：
 *
 *   幂等    同一分支被调两次        → 只执行一次
 *   空回滚  正向没跑过就来了补偿    → 补偿空转
 *   悬挂    补偿先到、正向后到      → 丢弃迟到的正向
 *
 * ## 用法（以 pg 为例）
 *
 *   const { Barrier, Decision, POSTGRES } = require('./barrier');
 *
 *   await Barrier.migrate(client, POSTGRES);        // 启动时一次
 *
 *   // 每次处理分支请求（gid / branchId / op / transType 由 TC 传进来）
 *   await client.query('BEGIN');
 *   try {
 *     const b = new Barrier(POSTGRES, transType, gid, branchId, op);
 *     if (await b.decide(client) === Decision.EXECUTE) {
 *       // 业务 SQL —— 必须用这个 client，跟屏障记录同一个事务
 *       await client.query('UPDATE account SET balance = balance - $1 WHERE id = $2', [amt, uid]);
 *     }
 *     await client.query('COMMIT');   // 原子性的来源
 *   } catch (e) {
 *     await client.query('ROLLBACK');
 *     throw e;
 *   }
 *
 * ## ⚠ 两条不能违反的前提
 *
 * 1. 屏障表必须和业务表在**同一个数据库实例** —— 不同实例没法共用一个本地
 *    事务，这个方案直接失效。不是实现限制，是它成立的根本条件。
 * 2. 业务 SQL 必须用传给 decide 的那个连接，且同一个事务提交。
 *
 * ## 返回值语义
 *
 * NULL_COMPENSATION 和 DUPLICATED 都是**正常路径**，你的接口应该返回**成功**
 * 而不是失败 —— 返回失败会让 TC 以为分支出错了。
 *
 * ## 连接要求
 *
 * 事务必须在**同一个连接**上跑完。用连接池的话记得 `pool.connect()` 拿到
 * 独占连接（pg）或 `pool.getConnection()`（mysql2），别直接用 pool ——
 * 那样每条语句可能落到不同连接上，事务就散了。
 */
'use strict';

const MYSQL = 'mysql';
const POSTGRES = 'postgres';
const SQLITE = 'sqlite';

/** 屏障给出的判定 */
const Decision = Object.freeze({
  /** 该干活。调用方在同一个事务里执行业务 SQL */
  EXECUTE: 'EXECUTE',
  /** **空回滚**：正向分支从没执行过，补偿直接空转。接口应返回成功 */
  NULL_COMPENSATION: 'NULL_COMPENSATION',
  /** **重复或悬挂**：这次调用已处理过，跳过。接口应返回成功 */
  DUPLICATED: 'DUPLICATED',
});

const KNOWN_OPS = new Set([
  'action', 'compensate', 'try', 'confirm', 'cancel', 'commit', 'rollback',
]);

/** 补偿类操作 → 对应的正向操作。判空回滚全靠它 */
const ORIGIN = { compensate: 'action', rollback: 'action', cancel: 'try' };

/** 不同驱动拿「影响行数」的字段不一样，统一在这里取 */
function affected(res, dialect) {
  if (res == null) return 0;
  if (dialect === MYSQL) {
    // mysql2 的 execute 返回 [rows, fields]，rows.affectedRows
    const r = Array.isArray(res) ? res[0] : res;
    return (r && r.affectedRows) || 0;
  }
  // pg 是 rowCount；node:sqlite / better-sqlite3 是 changes
  if (typeof res.rowCount === 'number') return res.rowCount;
  if (typeof res.changes === 'number') return res.changes;
  return 0;
}

class Barrier {
  /**
   * @param {string} dialect MYSQL / POSTGRES / SQLITE
   * @param {string} transType saga / tcc / msg / xa
   * @param {string} gid 全局事务号
   * @param {string} branchId 分支号
   * @param {string} op action / compensate / try / confirm / cancel / commit / rollback
   */
  constructor(dialect, transType, gid, branchId, op, table = 'barrier') {
    if (!gid || !branchId) throw new Error('gid / branchId 不能为空');
    if (!KNOWN_OPS.has(op)) throw new Error(`未知 op: ${op}`);
    this.dialect = dialect;
    this.transType = transType;
    this.gid = gid;
    this.branchId = branchId;
    this.op = op;
    this.table = table;
    this.counter = 0;
  }

  /** 建屏障表。启动时调一次即可，重复调用无害 */
  static async migrate(conn, dialect, table = 'barrier') {
    // MySQL 不能对 TEXT 建索引（1170 要 key length），必须定长
    const idText = dialect === MYSQL ? 'VARCHAR(128)' : 'TEXT';
    const idShort = dialect === MYSQL ? 'VARCHAR(45)' : 'TEXT';
    const sql = `CREATE TABLE IF NOT EXISTS ${table} (
      trans_type  ${idShort} NOT NULL,
      gid         ${idText} NOT NULL,
      branch_id   ${idText} NOT NULL,
      op          ${idShort} NOT NULL,
      barrier_id  ${idShort} NOT NULL,
      reason      ${idShort} NOT NULL,
      create_time BIGINT NOT NULL,
      PRIMARY KEY (gid, branch_id, op, barrier_id)
    )`;
    await conn.query(sql);
  }

  /**
   * 做出判定。**必须在业务事务里调用**，业务 SQL 要用同一个连接。
   *
   * 算法：补偿方先用「正向分支」的名义插一行去占坑。占成功了说明正向从没来过
   * （空回滚）；占失败了说明正向真跑过（是真补偿）。而这个坑一旦被占，
   * 迟到的正向分支就再也插不进来（悬挂被丢弃）。
   *
   * @returns {Promise<string>} Decision 之一
   */
  async decide(conn) {
    this.counter += 1;
    const bid = String(this.counter).padStart(2, '0');

    const origin = ORIGIN[this.op];
    const originAffected = origin ? await this._insert(conn, origin, bid) : 0;
    const currentAffected = await this._insert(conn, this.op, bid);

    if (origin && originAffected > 0) {
      // 正向分支从没跑过（否则那行早被它自己占了）→ 空回滚
      return Decision.NULL_COMPENSATION;
    }
    if (currentAffected === 0) {
      // 这个 (gid, branch, op, bid) 已经处理过了 → 重复请求或悬挂
      return Decision.DUPLICATED;
    }
    return Decision.EXECUTE;
  }

  async _insert(conn, op, bid) {
    const cols = '(trans_type,gid,branch_id,op,barrier_id,reason,create_time)';
    // reason = 是哪个分支插的这行，排查用
    const vals = [this.transType, this.gid, this.branchId, op, bid, this.op,
      Math.floor(Date.now() / 1000)];
    let sql;
    if (this.dialect === MYSQL) {
      // ⚠ 这里**绝不能**用 ON DUPLICATE KEY UPDATE：
      // 它在重复时 affectedRows 返回 1（不是 0），整个算法就废了。
      // INSERT IGNORE 重复时返回 0，跟另外两家一致。
      sql = `INSERT IGNORE INTO ${this.table} ${cols} VALUES (?,?,?,?,?,?,?)`;
    } else if (this.dialect === POSTGRES) {
      sql = `INSERT INTO ${this.table} ${cols} VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`;
    } else {
      sql = `INSERT INTO ${this.table} ${cols} VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`;
    }
    const res = await conn.query(sql, vals);
    return affected(res, this.dialect);
  }
}

module.exports = { Barrier, Decision, MYSQL, POSTGRES, SQLITE };
