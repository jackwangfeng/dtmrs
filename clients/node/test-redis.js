'use strict';
/**
 * Redis 屏障：跟 Rust / Go / Python / Java 各版**同名同义**的用例。
 *
 * 没配 DTMRS_TEST_REDIS_NODE 就跳过 —— **跳过不等于通过**。
 *
 *   DTMRS_TEST_REDIS_NODE='127.0.0.1:16379' node test-redis.js
 *
 * 这里手写了一个最小 RESP 客户端，是为了**不给这个包引入 Redis 依赖** ——
 * barrier-redis.js 只要求调用方给一个 evalFn，测试自然也不该多要什么。
 */
const net = require('net');
const assert = require('assert');
const { RedisBarrier, RedisOutcome } = require('./barrier-redis');

/** 够用就好的 RESP 客户端：只支持我们要发的那几条命令 */
class Resp {
  constructor(sock) {
    this.sock = sock;
    this.buf = Buffer.alloc(0);
    this.waiters = [];
    sock.on('data', (d) => {
      this.buf = Buffer.concat([this.buf, d]);
      this._drain();
    });
  }

  static connect(addr) {
    const [host, port] = addr.split(':');
    return new Promise((resolve, reject) => {
      const s = net.createConnection({ host, port: Number(port) }, () => resolve(new Resp(s)));
      s.on('error', reject);
    });
  }

  cmd(...args) {
    let out = `*${args.length}\r\n`;
    for (const a of args) {
      const s = String(a);
      out += `$${Buffer.byteLength(s)}\r\n${s}\r\n`;
    }
    return new Promise((resolve, reject) => {
      this.waiters.push({ resolve, reject });
      this.sock.write(out);
      this._drain();
    });
  }

  /** 试着从缓冲区解析出一个完整回复；解析不出就等更多数据 */
  _drain() {
    while (this.waiters.length) {
      const r = this._parse(0);
      if (!r) return;
      this.buf = this.buf.subarray(r.next);
      const w = this.waiters.shift();
      if (r.err) w.reject(new Error(r.err));
      else w.resolve(r.val);
    }
  }

  _parse(i) {
    const end = this.buf.indexOf('\r\n', i);
    if (end < 0) return null;
    const tag = String.fromCharCode(this.buf[i]);
    const body = this.buf.subarray(i + 1, end).toString();
    if (tag === '+') return { val: body, next: end + 2 };
    if (tag === '-') return { err: body, next: end + 2 };
    if (tag === ':') return { val: Number(body), next: end + 2 };
    if (tag === '$') {
      const n = Number(body);
      if (n < 0) return { val: null, next: end + 2 };
      if (this.buf.length < end + 2 + n + 2) return null;
      return { val: this.buf.subarray(end + 2, end + 2 + n).toString(), next: end + 2 + n + 2 };
    }
    if (tag === '*') {
      const n = Number(body);
      let cur = end + 2;
      const out = [];
      for (let k = 0; k < n; k++) {
        const r = this._parse(cur);
        if (!r) return null;
        out.push(r.val);
        cur = r.next;
      }
      return { val: out, next: cur };
    }
    return { err: `看不懂的回复: ${tag}${body}`, next: end + 2 };
  }

  /** barrier-redis 要求的就这一个东西 */
  evalFn() {
    return (script, keys, args) => this.cmd('EVAL', script, keys.length, ...keys, ...args);
  }
}

async function fixture() {
  const addr = process.env.DTMRS_TEST_REDIS_NODE;
  if (!addr) {
    console.log('⚠ 跳过 Redis 屏障测试：DTMRS_TEST_REDIS_NODE 没配（跳过不等于通过）');
    process.exit(0);
  }
  const r = await Resp.connect(addr);
  for (const pat of ['dtmrs:bar:*', 'nt:stock:*']) {
    const keys = await r.cmd('KEYS', pat);
    for (const k of keys || []) await r.cmd('DEL', k);
  }
  return r;
}

const stock = async (r, key) => {
  const v = await r.cmd('GET', key);
  return v === null ? -1 : Number(v);
};

const cases = {
  async 重复调用同一分支只执行一次(r, e) {
    await r.cmd('SET', 'nt:stock:1', 100);
    for (const want of [RedisOutcome.EXECUTED, RedisOutcome.DUPLICATED]) {
      // 每次新建 barrier —— 模拟 TC 重试时是两个独立请求
      const b = new RedisBarrier('saga', 'ng-idem', '01', 'action');
      assert.strictEqual(await b.checkAdjustAmount(e, 'nt:stock:1', -10), want);
    }
    assert.strictEqual(await stock(r, 'nt:stock:1'), 90, '只该扣一次');
  },

  async 正向没跑过时补偿要空转(r, e) {
    await r.cmd('SET', 'nt:stock:2', 100);
    const b = new RedisBarrier('saga', 'ng-null', '01', 'compensate');
    assert.strictEqual(
      await b.checkAdjustAmount(e, 'nt:stock:2', 10), RedisOutcome.NULL_COMPENSATION);
    assert.strictEqual(await stock(r, 'nt:stock:2'), 100, '空回滚不能动数据');
  },

  async 补偿先到时晚到的正向必须被丢弃(r, e) {
    await r.cmd('SET', 'nt:stock:3', 100);
    const b = new RedisBarrier('saga', 'ng-hang', '01', 'compensate');
    assert.strictEqual(
      await b.checkAdjustAmount(e, 'nt:stock:3', 10), RedisOutcome.NULL_COMPENSATION);
    // 迟到的正向必须被丢弃，否则扣了款没人补
    const b2 = new RedisBarrier('saga', 'ng-hang', '01', 'action');
    assert.strictEqual(
      await b2.checkAdjustAmount(e, 'nt:stock:3', -10), RedisOutcome.DUPLICATED);
    assert.strictEqual(await stock(r, 'nt:stock:3'), 100, '悬挂的正向不能生效');
  },

  async 库存不足要明确失败(r, e) {
    await r.cmd('SET', 'nt:stock:4', 5);
    const b = new RedisBarrier('saga', 'ng-low', '01', 'action');
    assert.strictEqual(await b.checkAdjustAmount(e, 'nt:stock:4', -10), RedisOutcome.FAILURE);
    assert.strictEqual(await stock(r, 'nt:stock:4'), 5, '失败了不能动数据');
  },

  async 键不存在不能凭空创建库存(r, e) {
    const b = new RedisBarrier('saga', 'ng-nokey', '01', 'action');
    assert.strictEqual(await b.checkAdjustAmount(e, 'nt:stock:404', -1), RedisOutcome.FAILURE);
    assert.strictEqual(await stock(r, 'nt:stock:404'), -1,
      'INCRBY 会把不存在的键当 0，必须先挡住');
  },

  async 业务lua可以自定义(r, e) {
    await r.cmd('SET', 'nt:stock:5', 3);
    const b = new RedisBarrier('saga', 'ng-cas', '01', 'action');
    const got = await b.call(e, `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 'FAILURE' end
redis.call('SET', KEYS[1], ARGV[2])
`, ['nt:stock:5'], ['3', '7']);
    assert.strictEqual(got, RedisOutcome.EXECUTED);
    assert.strictEqual(await stock(r, 'nt:stock:5'), 7);
  },

  async 回查没见过的单子要固化成回滚(r, e) {
    const b = new RedisBarrier('msg', 'ng-query', '01', 'action');
    assert.strictEqual(await b.queryPrepared(e), RedisOutcome.FAILURE);
    // 回查完之后，晚到的正向必须被挡住：
    // 否则 TC 已经按「没提交」回滚了，业务这边却又执行了一次
    await r.cmd('SET', 'nt:stock:6', 100);
    const b2 = new RedisBarrier('msg', 'ng-query', '01', 'action');
    assert.strictEqual(
      await b2.checkAdjustAmount(e, 'nt:stock:6', -10), RedisOutcome.DUPLICATED);
    assert.strictEqual(await stock(r, 'nt:stock:6'), 100);
  },
};

(async () => {
  let n = 0;
  for (const [name, fn] of Object.entries(cases)) {
    const r = await fixture();
    await fn(r, r.evalFn());
    r.sock.destroy();
    console.log(`  ✓ ${name}`);
    n++;
  }
  console.log(`Redis 屏障：${n} 条全过`);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
