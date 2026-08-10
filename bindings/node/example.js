#!/usr/bin/env node
/**
 * Node 进程里嵌一个 Rust 事务协调器 —— 不部署任何服务。
 *
 * 跑之前：
 *   cargo build -p dtmrs-ffi --release
 *   cd bindings/node && npm install && node example.js
 *
 * 三个场景，账户余额是真的在动：
 *   ① 正常转账
 *   ② 风控拒绝 → 逆序补偿，钱退回来
 *   ③ 下游超时 → 只重试，不回滚
 */
'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const dtmrs = require('./dtmrs');

const DB = path.join(os.tmpdir(), 'dtmrs_node_demo.db');
const BIZ = path.join(os.tmpdir(), 'dtmrs_node_biz.json');
for (const f of [DB, BIZ]) if (fs.existsSync(f)) fs.unlinkSync(f);

// 业务「库」：为了不引数据库依赖，这里用一个 JSON 文件当账本。
// 真实项目请用 postgres/mysql，并且**把业务 SQL 和子事务屏障放进同一个事务**
fs.writeFileSync(BIZ, JSON.stringify({ 1: 1000, 2: 0 }));
const balances = () => JSON.parse(fs.readFileSync(BIZ, 'utf8'));
const move = (from, to, amt) => {
  const b = balances();
  b[from] -= amt;
  b[to] += amt;
  fs.writeFileSync(BIZ, JSON.stringify(b));
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  const tc = new dtmrs.Tc(`sqlite:${DB}`);
  const seen = [];

  // handler 是 **async** 的 —— 这正是拉取式换来的好处，
  // 里头可以正常 await 数据库、HTTP、任何异步操作
  tc.handler('转出', async (ctx) => {
    await sleep(5); // 假装这里在 await 一次数据库往返
    move(1, 2, 100);
    seen.push(`[转出] gid=${ctx.gid} branch=${ctx.branchId} op=${ctx.op}`);
    return dtmrs.SUCCESS;
  });

  tc.handler('转出撤销', async (ctx) => {
    move(2, 1, 100);
    seen.push(`[转出撤销] gid=${ctx.gid} branch=${ctx.branchId} op=${ctx.op}`);
    return dtmrs.SUCCESS;
  });

  tc.handler('风控拒绝', async (ctx) => {
    seen.push(`[风控拒绝] gid=${ctx.gid} branch=${ctx.branchId} op=${ctx.op}`);
    // 业务**明确**要求回滚 —— 只有这个返回值会触发逆序补偿
    return dtmrs.FAILURE;
  });

  tc.handler('空补偿', async (ctx) => {
    seen.push(`[空补偿] gid=${ctx.gid} branch=${ctx.branchId} op=${ctx.op}`);
    return dtmrs.SUCCESS;
  });

  tc.handler('下游超时', async (ctx) => {
    seen.push(`[下游超时] gid=${ctx.gid} 结果未知`);
    // 超时**不等于失败**：对方可能已经成功了，回滚会造成不一致
    return dtmrs.UNKNOWN;
  });

  await tc.start();
  console.log('初始余额:', balances());

  console.log('\n① 正常转账');
  await tc.submitSaga('node-1', [['local://转出', 'local://转出撤销']]);
  let st = await tc.waitFinal('node-1', 8000);
  seen.splice(0).forEach((s) => console.log('  ' + s));
  console.log(`  结果: ${st}  余额:`, balances());

  console.log('\n② 风控拒绝 → 逆序补偿，钱要退回来');
  await tc.submitSaga('node-2', [
    ['local://转出', 'local://转出撤销'],
    ['local://风控拒绝', 'local://空补偿'],
  ]);
  st = await tc.waitFinal('node-2', 8000);
  seen.splice(0).forEach((s) => console.log('  ' + s));
  console.log(`  结果: ${st}  余额:`, balances(), ' ← 转出被补偿抹平了');

  console.log('\n③ 下游超时 → 只重试，不回滚');
  await tc.submitSaga('node-3', [['local://下游超时', 'local://空补偿']]);
  await sleep(600);
  console.log(`  状态: ${await tc.status('node-3')}  ← 停在 submitted 等重试，没有转 aborting`);
  seen.splice(0).forEach((s) => console.log('  ' + s));

  await tc.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
