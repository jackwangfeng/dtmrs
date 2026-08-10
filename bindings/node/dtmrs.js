/**
 * dtmrs 的 Node 绑定 —— 把 Rust 写的事务协调器嵌进 Node 进程，不部署任何服务。
 *
 *   const dtmrs = require('./dtmrs');
 *   const tc = new dtmrs.Tc('sqlite:/tmp/app.db');
 *
 *   tc.handler('转出', async (ctx) => {
 *     await db.query('UPDATE account SET balance = balance - 100 WHERE id = 1');
 *     return dtmrs.SUCCESS;
 *   });
 *
 *   await tc.start();
 *   await tc.submitSaga('order-1', [['local://转出', 'local://转出撤销']]);
 *
 * 跑之前先编：cargo build -p dtmrs-ffi --release
 *
 * ## 为什么用「拉取式」而不是回调
 *
 * C ABI 的回调必须**同步返回**一个 int。Python / Java 能接受，Node 不行 ——
 * Node 的业务代码几乎全是异步的（数据库客户端都返回 Promise），
 * 同步回调里没法 await。
 *
 * 所以这里走 C ABI 的拉取式接口：库把待办分支放进队列，我们在自己的
 * 事件循环里取出来、正常 async/await 干活、完事回填结果。
 * handler 因此可以是 async 函数，这才是 Node 该有的样子。
 *
 * ## 一个必须知道的约束
 *
 * **绝不能阻塞事件循环。** 轮询用的是非阻塞调用（timeout=0），但如果你的
 * handler 里有同步的重活（大 JSON.parse、同步 fs、死循环），分支分发就会跟着停。
 */
'use strict';

const koffi = require('koffi');
const fs = require('fs');
const path = require('path');

/** 分支返回码。**数值语义固定，跟 C 头文件对齐，不要改。** */
const SUCCESS = 0;
/** 业务**明确**要求回滚。只有这个会触发逆序补偿 */
const FAILURE = 1;
/** 还在处理中，别当失败 */
const ONGOING = 2;
/** 结果**未知**（超时、下游 5xx、自己抛异常）—— 只重试，不回滚 */
const UNKNOWN = 3;

const OK = 0;
const ERR = -1;

function findLib() {
  if (process.env.DTMRS_LIB) return process.env.DTMRS_LIB;
  const name =
    process.platform === 'darwin'
      ? 'libdtmrs.dylib'
      : process.platform === 'win32'
        ? 'dtmrs.dll'
        : 'libdtmrs.so';
  // 从仓库里找：bindings/node → ../../target/{release,debug}
  const roots = [
    path.join(__dirname, '..', '..', 'target', 'release'),
    path.join(__dirname, '..', '..', 'target', 'debug'),
    __dirname,
  ];
  for (const r of roots) {
    const p = path.join(r, name);
    if (fs.existsSync(p)) return p;
  }
  throw new Error(
    `找不到 ${name}。先编：cargo build -p dtmrs-ffi --release\n` +
      `或者用环境变量 DTMRS_LIB 指定路径。`
  );
}

/** 分支上下文。业务侧用它做幂等（配合子事务屏障） */
class Ctx {
  constructor(t) {
    this.taskId = t.task_id;
    this.name = t.name;
    this.gid = t.gid;
    this.branchId = t.branch_id;
    /** action | compensate | try | confirm | cancel | commit | rollback */
    this.op = t.op;
  }
  toString() {
    return `Ctx(gid=${this.gid} branch=${this.branchId} op=${this.op})`;
  }
}

class Tc {
  /**
   * @param {string} dbUrl 形如 'sqlite:/tmp/app.db'，也支持 postgres:// / mysql://
   * @param {object} [opts]
   * @param {string} [opts.libPath] .so 路径，默认自动找
   * @param {number} [opts.pollMs=20] 轮询间隔。越小越跟手，也越费 CPU
   */
  constructor(dbUrl, opts = {}) {
    this.lib = koffi.load(opts.libPath || findLib());
    this.pollMs = opts.pollMs ?? 20;
    this._decl();

    this.handlers = new Map();
    this.tc = this.f.open(dbUrl);
    if (!this.tc) throw new Error(`打开失败: ${this.lastError()}`);
    this.timer = null;
    this.closed = false;
    /** 正在处理中的任务数，close 时要等它们收尾 */
    this.inflight = 0;
  }

  _decl() {
    const L = this.lib;
    this.f = {
      open: L.func('void *dtmrs_open(const char *db_url)'),
      registerPull: L.func('int dtmrs_register_pull(void *tc, const char *name)'),
      start: L.func('int dtmrs_start(void *tc)'),
      nextTask: L.func('int dtmrs_next_task(void *tc, int timeout_ms, _Out_ char *out, size_t out_len)'),
      reply: L.func('int dtmrs_reply(void *tc, unsigned long long task_id, int result)'),
      submitSaga: L.func('int dtmrs_submit_saga(void *tc, const char *gid, const char *steps_json)'),
      status: L.func('int dtmrs_status(void *tc, const char *gid, _Out_ char *out, size_t out_len)'),
      close: L.func('void dtmrs_close(void *tc)'),
      lastError: L.func('const char *dtmrs_last_error()'),
    };
  }

  lastError() {
    return this.f.lastError() || '';
  }

  /**
   * 注册一个进程内分支。名字对应 saga 步骤里的 `local://名字`。
   * 必须在 `start()` 之前调。
   *
   * handler 可以是 async 的 —— 这正是拉取式换来的好处。
   * 返回 SUCCESS / FAILURE / ONGOING / UNKNOWN 之一。
   *
   * **handler 抛异常按 UNKNOWN 处理**（只重试不回滚）：
   * 异常意味着不知道业务到底做没做，回滚可能造成不一致。
   */
  handler(name, fn) {
    if (this.timer) throw new Error('已经 start 了，不能再注册 handler');
    if (this.f.registerPull(this.tc, name) !== OK) {
      throw new Error(`注册失败: ${this.lastError()}`);
    }
    this.handlers.set(name, fn);
    return this;
  }

  /** 启动推进器，并开始在事件循环里轮询待办分支 */
  async start() {
    if (this.f.start(this.tc) !== OK) {
      throw new Error(`启动失败: ${this.lastError()}`);
    }
    // unref：光有这个定时器不应该把进程吊住不退
    this.timer = setInterval(() => this._drain(), this.pollMs);
    this.timer.unref?.();
    return this;
  }

  /** 把队列里当前所有待办一次性取干净，各自异步执行 */
  _drain() {
    if (this.closed) return;
    const buf = Buffer.alloc(2048);
    // 循环取到空为止：一次 tick 里可能积压了多个分支
    for (;;) {
      let r;
      try {
        // timeout=0 → 不阻塞。**这是不卡死事件循环的关键**
        r = this.f.nextTask(this.tc, 0, buf, buf.length);
      } catch (e) {
        return;
      }
      if (r !== 1) return;
      let task;
      try {
        task = JSON.parse(koffi.decode(buf, 'char', -1));
      } catch (e) {
        return;
      }
      this._run(task);
    }
  }

  async _run(task) {
    const ctx = new Ctx(task);
    const fn = this.handlers.get(task.name);
    this.inflight++;
    let result = UNKNOWN;
    try {
      if (!fn) {
        // 漏注册（比如新版本删了 handler）。**按未知处理，不是失败** ——
        // 这是部署问题，改回来重试就好；判失败会白白触发回滚
        console.error(`[dtmrs] 分支 ${task.name} 没注册，按结果未知处理（会重试，不回滚）`);
      } else {
        const v = await fn(ctx);
        result = typeof v === 'number' ? v : UNKNOWN;
      }
    } catch (e) {
      // 宿主抛异常 = 不知道业务做没做。只重试，绝不回滚
      console.error(`[dtmrs] handler ${task.name} 抛异常，按结果未知处理:`, e);
      result = UNKNOWN;
    } finally {
      this.inflight--;
      try {
        this.f.reply(this.tc, task.task_id, result);
      } catch (e) {
        /* 句柄已关，忽略 */
      }
    }
  }

  /**
   * 提交一个 SAGA。
   * @param {string} gid 全局事务号。建议直接用业务单号 —— 那样天然幂等
   * @param {Array<[string,string]>|Array<{action:string,compensate:string}>} steps
   *   每步是 [正向, 补偿]。地址可以是 local:// 、http:// 或 grpc:// ，能混用
   */
  async submitSaga(gid, steps) {
    const norm = steps.map((s) =>
      Array.isArray(s) ? { action: s[0], compensate: s[1] } : s
    );
    if (this.f.submitSaga(this.tc, gid, JSON.stringify(norm)) !== OK) {
      throw new Error(`提交失败: ${this.lastError()}`);
    }
  }

  /** 查状态：prepared | submitted | aborting | succeed | failed */
  async status(gid) {
    const buf = Buffer.alloc(64);
    if (this.f.status(this.tc, gid, buf, buf.length) !== OK) {
      throw new Error(`查询失败: ${this.lastError()}`);
    }
    return koffi.decode(buf, 'char', -1);
  }

  /**
   * 等到终态。
   *
   * 注意这里是**轮询实现**而不是调 C 的 `dtmrs_wait_final` ——
   * 那个函数会阻塞调用线程，在 Node 里等于冻结整个事件循环，
   * 连分支都没法分发了，必然超时。
   */
  async waitFinal(gid, timeoutMs = 10000) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const s = await this.status(gid);
      if (s === 'succeed' || s === 'failed') return s;
      if (Date.now() > deadline) return s;
      await new Promise((r) => setTimeout(r, 20));
    }
  }

  /** 关闭。未终结的事务留在库里，下次 open + start 会接着推 */
  async close() {
    if (this.closed) return;
    this.closed = true;
    if (this.timer) clearInterval(this.timer);
    // 等在跑的 handler 收尾，别把它们的 reply 丢了
    for (let i = 0; i < 200 && this.inflight > 0; i++) {
      await new Promise((r) => setTimeout(r, 10));
    }
    this.f.close(this.tc);
    this.tc = null;
  }
}

module.exports = { Tc, Ctx, SUCCESS, FAILURE, ONGOING, UNKNOWN };
