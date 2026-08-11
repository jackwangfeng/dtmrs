'use strict';
/**
 * 业务数据在 Redis 里时的子事务屏障。
 *
 * # 为什么不能用 barrier.js
 *
 * 那个的原子性来自「屏障记录和业务 SQL 在同一个本地事务里提交」。秒杀那类
 * 场景库存本身就在 Redis，没有 SQL 事务可以加入。
 *
 * 这里换一个原子性来源：**屏障判定和业务操作写在同一个 Lua 脚本里**，
 * Redis 执行脚本单线程且不可打断。
 *
 * 代价是**业务逻辑必须能用 Lua 表达**。扣库存、扣余额这类计数操作正好可以，
 * 复杂业务请继续用 SQL 屏障。
 *
 * # 用法
 *
 *   const { RedisBarrier, RedisOutcome } = require('dtmrs-barrier/barrier-redis');
 *
 *   const b = new RedisBarrier(transType, gid, branchId, op);
 *   const out = await b.checkAdjustAmount(evalFn, 'stock:1001', -1);
 *   if (out === RedisOutcome.FAILURE) return 失败;   // 库存不足 → 让 TC 回滚
 *   return 成功;                                      // 其余三种都是正常路径
 *
 * # ⚠ evalFn 要你自己给
 *
 * **不依赖任何 Redis 库** —— node-redis 和 ioredis 的 eval 签名不一样，
 * 与其猜，不如让调用方给一个 `(script, keys, args) => Promise<any>`：
 *
 *   // ioredis
 *   const evalFn = (s, keys, args) => redis.eval(s, keys.length, ...keys, ...args);
 *
 *   // node-redis v4
 *   const evalFn = (s, keys, args) => client.eval(s, { keys, arguments: args });
 *
 * # 跟 SQL 版的两处行为差异（介质决定的，不是 bug）
 *
 * 1. **屏障键会过期**（默认 7 天）。SQL 版的屏障行永久保留，Redis 不挂 TTL
 *    内存会撑爆。⚠ TTL 必须**长于事务可能的最大生命周期**（含重试退避），
 *    否则正向的键先过期、补偿再来会以为「正向没跑过」而空转，副作用就漏补了。
 * 2. **业务失败要由脚本自己表达**（`return 'FAILURE'`）。SQL 版里业务失败是
 *    调用方自己的事（不提交就行），这里业务跑在屏障的脚本内。
 */

/** 屏障键的默认存活时间（秒） */
const DEFAULT_BARRIER_TTL = 7 * 24 * 3600;

/**
 * Redis 屏障的判定结果。
 *
 * 比 SQL 版的 Decision 多一个 FAILURE：业务逻辑跑在屏障的脚本里，
 * 它的拒绝只能从这里带出来。
 */
const RedisOutcome = Object.freeze({
  /** 业务操作已经执行 */
  EXECUTED: 'EXECUTED',
  /** 空回滚：正向分支从没执行过，业务操作没跑。接口应返回成功 */
  NULL_COMPENSATION: 'NULL_COMPENSATION',
  /** 重复或悬挂：这次调用之前处理过，业务操作没跑。接口应返回成功 */
  DUPLICATED: 'DUPLICATED',
  /** 业务逻辑自己拒绝了（比如库存不足）。屏障键仍然留下 ——
   *  这次调用确实被处理过了，重试不该再跑一遍 */
  FAILURE: 'FAILURE',
});

const ORIGIN = { compensate: 'action', cancel: 'try', rollback: 'commit' };
const KNOWN = new Set([
  'action', 'compensate', 'try', 'confirm', 'cancel', 'commit', 'rollback',
]);

class RedisBarrier {
  constructor(transType, gid, branchId, op, opts = {}) {
    if (!transType || !gid || !branchId || !op) {
      throw new Error('trans_type / gid / branch_id / op 都不能为空');
    }
    if (!KNOWN.has(op)) throw new Error(`未知 op: ${op}`);
    this.transType = transType;
    this.gid = gid;
    this.branchId = branchId;
    this.op = op;
    this.prefix = opts.prefix || 'dtmrs:bar:';
    this.ttl = opts.ttl || DEFAULT_BARRIER_TTL;
    this._counter = 0;
  }

  _nextBarrierId() {
    this._counter += 1;
    return String(this._counter).padStart(2, '0');
  }

  _bkey(op, bid) {
    return `${this.prefix}${this.gid}-${this.branchId}-${op}-${bid}`;
  }

  /**
   * 带屏障保护地跑一段业务 Lua。
   *
   * busi 里可以用 KEYS[1..] / ARGV[1..]，编号就是你传进来的顺序 ——
   * 屏障自己的键和参数追加在**后面**，不会打乱你的编号。
   *
   * 业务想拒绝就 `return 'FAILURE'`。
   */
  async call(evalFn, busi, keys, args) {
    const bid = this._nextBarrierId();
    const nk = keys.length;
    const na = args.length;
    // 屏障键接在用户的键后面，用户脚本里的编号才不会被打乱
    const kCur = nk + 1;
    const kOrg = nk + 2;
    const aTtl = na + 1;
    const aHasOrigin = na + 2;

    // ⚠ 这段的结构必须跟 SQL 版的 decide 一一对应，也必须跟 Rust / Go /
    // Python / Java 各版逐字一致 —— 五份实现的判定语义不能漂。
    //  1. 补偿类操作先「假装自己是正向分支」占位，占到了 = 正向没来过 = 空回滚
    //  2. 再以自己的身份占位，占不到 = 重复请求或悬挂
    // SET NX 占不到时返回 false，对应 SQL 那边的 rows_affected = 0
    const script = `
local origin_got = false
if ARGV[${aHasOrigin}] == '1' then
    origin_got = redis.call('SET', KEYS[${kOrg}], 'origin', 'NX', 'EX', ARGV[${aTtl}])
end
local cur_got = redis.call('SET', KEYS[${kCur}], 'cur', 'NX', 'EX', ARGV[${aTtl}])
if ARGV[${aHasOrigin}] == '1' and origin_got then
    return 'NULL_COMPENSATION'
end
if not cur_got then
    return 'DUPLICATED'
end
local function busi()
${busi}
end
local r = busi()
if r == 'FAILURE' then return 'FAILURE' end
return 'EXECUTED'
`;
    const origin = ORIGIN[this.op];
    const allKeys = [
      ...keys,
      this._bkey(this.op, bid),
      // 非补偿操作用不到这个键，但 KEYS 个数要固定，塞个同前缀的占位
      this._bkey(origin || 'none', bid),
    ];
    const allArgs = [...args, String(this.ttl), origin ? '1' : '0'];

    const v = await evalFn(script, allKeys, allArgs);
    const s = Buffer.isBuffer(v) ? v.toString() : String(v);
    // 认不出来的一律当失败 —— 宁可让 TC 回滚，也不能把未知当成功
    return Object.values(RedisOutcome).includes(s) ? s : RedisOutcome.FAILURE;
  }

  /**
   * 秒杀那个形状：检查余额够不够，够就调整。
   *
   * amount 传负数表示扣减。余额不足（会变成负数）或键不存在都返回 FAILURE ——
   * 调用方该把它翻译成「业务明确失败」让 TC 回滚，而不是当成未知去重试。
   */
  async checkAdjustAmount(evalFn, key, amount) {
    // 键不存在 → 失败（INCRBY 会把不存在的键当 0，不挡就凭空创建库存了）
    return this.call(evalFn, `
local v = redis.call('GET', KEYS[1])
if v == false or tonumber(v) + tonumber(ARGV[1]) < 0 then
    return 'FAILURE'
end
redis.call('INCRBY', KEYS[1], ARGV[1])
`, [key], [String(amount)]);
  }

  /**
   * 二阶段消息的回查。
   *
   * 规则：**没见过就地写一个 rollback 标记**，让后来的正向分支看到标记后放弃 ——
   * 把「不知道」固化成「没提交」，避免两边各自猜出不同的结论。
   *
   * 返回 FAILURE 表示「这单没提交，TC 该回滚」。
   */
  async queryPrepared(evalFn) {
    // msg 的正向分支固定是 01/action/01，回查看的就是它那个键
    const key = `${this.prefix}${this.gid}-01-action-01`;
    const v = await evalFn(`
local v = redis.call('GET', KEYS[1])
if v == false then
    redis.call('SET', KEYS[1], 'rollback', 'EX', ARGV[1])
    v = 'rollback'
end
if v == 'rollback' then return 'FAILURE' end
return 'EXECUTED'
`, [key], [String(this.ttl)]);
    const s = Buffer.isBuffer(v) ? v.toString() : String(v);
    return s === 'FAILURE' ? RedisOutcome.FAILURE : RedisOutcome.EXECUTED;
  }
}

module.exports = { RedisBarrier, RedisOutcome, DEFAULT_BARRIER_TTL };
