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
 *   // TCC：回调正常返回 → submit；抛异常（包括某个 try 没成功）→ abort
 *   await tc.tccGlobal('order-2', async (t) => {
 *     await t.tryBranch('local://冻结确认', 'local://冻结撤销', async (bid) => freeze(bid));
 *   });
 *   // XA 同形：xaGlobal + prepareBranch
 *
 *   // 二阶段消息：本地事务成功就 submit，失败就 abort，不知道就交给回查
 *   await tc.msgDoAndSubmit('order-3', ['local://加积分'], 'local://查订单', writeOrder);
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
 * ## 没有 workflow
 *
 * 五种模式里只有 workflow 不能用：它的函数体得在 C 回调里**同步**跑完
 * （库在回调里等你一个个开分支），而 Node 的业务代码是 async 的。
 * 要写「步骤取决于上一步结果」的流程，把那段逻辑放到 Rust / Python / Java 的进程里。
 * （别拆成几笔 saga 顺序提交 —— 那样就不是一个事务了，后一笔失败补不到前一笔。）
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
    /** 这一步自己的业务数据（submitSaga 每步的第三项），没给就是空串 */
    this.payload = t.payload ?? '';
  }
  toString() {
    return `Ctx(gid=${this.gid} branch=${this.branchId} op=${this.op})`;
  }
}

/** 一阶段（try / XA prepare）没返回 SUCCESS。在 tccGlobal / xaGlobal 里抛出会触发 abort */
class BranchFailed extends Error {
  constructor(branchId, code) {
    super(`分支 ${branchId} 一阶段返回 ${code}（不是 SUCCESS），整单回滚`);
    this.branchId = branchId;
    this.code = code;
  }
}

/** 跑宿主的一阶段。抛异常 = 不知道做没做 → UNKNOWN（TCC/XA 里照样回滚，cancel 兜得住） */
async function callPhase1(fn, bid) {
  try {
    const v = await fn(bid);
    return typeof v === 'number' ? v : UNKNOWN;
  } catch (e) {
    console.error(`[dtmrs] 分支 ${bid} 的一阶段抛异常，按结果未知处理:`, e);
    return UNKNOWN;
  }
}

/** TCC 和 XA 共用：分支号自动编（01、02……），先登记、登记成功才跑一阶段 */
class TwoPhase {
  constructor(tc, gid, registerFn) {
    this.tc = tc;
    this.gid = gid;
    this._reg = registerFn;
    this._next = 0;
  }

  /** 只登记下一个分支，返回分支号。一阶段自己去做 —— **必须在这之后** */
  async register(fwd, bwd) {
    const bid = String(this._next + 1).padStart(2, '0');
    this.tc._check(this._reg(this.tc.tc, this.gid, bid, fwd, bwd), '登记分支');
    // 登记成功才占号：失败了重试还用同一个号
    this._next++;
    return bid;
  }

  async _branch(fwd, bwd, fn) {
    const bid = await this.register(fwd, bwd);
    const code = await callPhase1(fn, bid);
    if (code !== SUCCESS) throw new BranchFailed(bid, code);
    return bid;
  }

  async submit() {
    return this.tc.submit(this.gid);
  }

  async abort() {
    return this.tc.abort(this.gid);
  }
}

class Tcc extends TwoPhase {
  /** 先登记、再跑 tryFn(branchId)。没返回 SUCCESS（含抛异常）就抛 BranchFailed */
  async tryBranch(confirm, cancel, tryFn) {
    return this._branch(confirm, cancel, tryFn);
  }
}

class Xa extends TwoPhase {
  /** 先登记、再跑 prepareFn(branchId)（业务 SQL + PREPARE）。没 SUCCESS 就抛 BranchFailed */
  async prepareBranch(commit, rollback, prepareFn) {
    return this._branch(commit, rollback, prepareFn);
  }
}

class Tc {
  /**
   * @param {string} dbUrl 形如 'sqlite:/tmp/app.db'，也支持 postgres:// / mysql:// / redis://
   *   （Redis 可带 ?key_prefix=app1: 跟共用同一个 Redis 的别的环境隔开）
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
      tccBegin: L.func('int dtmrs_tcc_begin(void *tc, const char *gid)'),
      tccRegister: L.func(
        'int dtmrs_tcc_register(void *tc, const char *gid, const char *branch_id, const char *confirm, const char *cancel)'
      ),
      xaBegin: L.func('int dtmrs_xa_begin(void *tc, const char *gid)'),
      xaRegister: L.func(
        'int dtmrs_xa_register(void *tc, const char *gid, const char *branch_id, const char *commit, const char *rollback)'
      ),
      msgPrepare: L.func(
        'int dtmrs_msg_prepare(void *tc, const char *gid, const char *actions_json, const char *query_prepared, int grace_secs)'
      ),
      submit: L.func('int dtmrs_submit(void *tc, const char *gid)'),
      abort: L.func('int dtmrs_abort(void *tc, const char *gid)'),
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
   * @param {Array<[string,string,any?]>|Array<{action:string,compensate:string,payload?:any}>} steps
   *   每步是 [正向, 补偿, 数据?]。地址可以是 local:// 、http:// 或 grpc:// ，能混用。
   *   数据是这一步自己的（正向和补偿共用），不是字符串会被 JSON.stringify
   */
  async submitSaga(gid, steps) {
    const norm = steps.map((s) => {
      const o = Array.isArray(s) ? { action: s[0], compensate: s[1], payload: s[2] } : { ...s };
      if (o.payload === undefined) delete o.payload;
      else if (typeof o.payload !== 'string') o.payload = JSON.stringify(o.payload);
      return o;
    });
    if (this.f.submitSaga(this.tc, gid, JSON.stringify(norm)) !== OK) {
      throw new Error(`提交失败: ${this.lastError()}`);
    }
  }

  _check(rc, what) {
    if (rc !== OK) throw new Error(`${what}失败: ${this.lastError()}`);
  }

  /** 开一个 TCC 事务（幂等）。多数时候用 tccGlobal 更省心 */
  async tcc(gid) {
    this._check(this.f.tccBegin(this.tc, gid), '开 TCC 事务');
    return new Tcc(this, gid, this.f.tccRegister);
  }

  /** 开一个 XA 事务（幂等）。一阶段是业务 SQL + PREPARE */
  async xa(gid) {
    this._check(this.f.xaBegin(this.tc, gid), '开 XA 事务');
    return new Xa(this, gid, this.f.xaRegister);
  }

  /**
   * TCC 全局事务：body 正常返回 → submit；抛异常（包括 BranchFailed）→ abort，
   * 然后把异常原样抛出去。超时也是 abort：还没 submit，cancel 会覆盖每个分支
   */
  async tccGlobal(gid, body) {
    return this._global(await this.tcc(gid), body);
  }

  /** XA 全局事务，语义同 tccGlobal */
  async xaGlobal(gid, body) {
    return this._global(await this.xa(gid), body);
  }

  async _global(t, body) {
    let r;
    try {
      r = await body(t);
    } catch (e) {
      await t.abort();
      throw e;
    }
    await t.submit();
    return r;
  }

  /**
   * 二阶段消息的 prepare。之后自己跑本地事务，再 submit / abort / 什么都不做。
   * queryPrepared 必填：崩在本地事务和 submit 之间时 TC 靠它回查 ——
   * 回查 handler 返回 SUCCESS=本地已提交、FAILURE=没提交、其它=过会再问。
   * graceSecs < 0 用默认 10 秒
   */
  async msgPrepare(gid, actions, queryPrepared, graceSecs = -1) {
    this._check(
      this.f.msgPrepare(this.tc, gid, JSON.stringify(actions), queryPrepared, graceSecs),
      'msg prepare'
    );
  }

  /**
   * prepare → 跑 localTx() → SUCCESS 就 submit、FAILURE 就 abort、
   * 其它（含抛异常）什么都不做，交给回查决断。返回 localTx 的结果码。
   * prepare 失败会直接抛异常，localTx **不会跑**
   */
  async msgDoAndSubmit(gid, actions, queryPrepared, localTx, graceSecs = -1) {
    await this.msgPrepare(gid, actions, queryPrepared, graceSecs);
    let code;
    try {
      const v = await localTx();
      code = typeof v === 'number' ? v : UNKNOWN;
    } catch (e) {
      // 本地事务抛异常 = 不知道提交了没有。不能猜，交给回查
      console.error(`[dtmrs] ${gid} 的本地事务抛异常，交给回查决断:`, e);
      return UNKNOWN;
    }
    if (code === SUCCESS) await this.submit(gid);
    else if (code === FAILURE) await this.abort(gid);
    return code;
  }

  /** tcc / xa / msg 的二阶段提交（幂等） */
  async submit(gid) {
    this._check(this.f.submit(this.tc, gid), '提交');
  }

  /** 主动中止。tcc / xa / msg 已 submit 的会抛异常 —— 方向已定，不能再回滚 */
  async abort(gid) {
    this._check(this.f.abort(this.tc, gid), '中止');
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

module.exports = { Tc, Ctx, Tcc, Xa, BranchFailed, SUCCESS, FAILURE, ONGOING, UNKNOWN };
