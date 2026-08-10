# dtmrs

**简体中文** | [English](README.md)

[![CI](https://github.com/jackwangfeng/dtmrs/actions/workflows/ci.yml/badge.svg)](https://github.com/jackwangfeng/dtmrs/actions/workflows/ci.yml) [![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Rust 写的分布式事务管理器，对标 [DTM](https://github.com/dtm-labs/dtm)（Go，10.9k★）。

**为什么重做**：Rust 生态里没有能用的分布式事务管理器。唯一对标项目
[rseata](https://github.com/oulover/rseata) 只有 88★、1 个贡献者、33 次提交、
**而且没有 LICENSE 文件** —— 无许可证等于保留全部版权，法律上不能商用。
其它 Rust 方案（restate/obelisk）是持久化执行范式，且分别受 BUSL / AGPL 限制。

dtmrs 是 **Apache-2.0**，商用闭源接入没有障碍。

## 差异化：嵌入式 TC（DTM 做不到的形态）

TC 当库链进你自己的进程，**不需要单独部署服务**：

```text
DTM:    你的服务 ──HTTP──► 独立部署的 TC 进程 ──► DB
                           （要运维、要高可用、要监控）
dtmrs:  你的服务（TC 就在进程里）──► DB
```

分支不用是 HTTP URL，可以直接是进程内的函数 —— 没有网络、没有序列化：

```rust
let tc = Embedded::builder("sqlite:app.db")
    .handler("扣款",     |ctx| async move { /* 业务逻辑 */ BranchResult::Success })
    .handler("扣款撤销", |ctx| async move { BranchResult::Success })
    .start().await?;

tc.saga("order-1001")
    .step("local://扣款", "local://扣款撤销")
    .step("http://shipment/create", "http://shipment/cancel")   // 可跟远端混用
    .submit().await?;
```

`cargo run --example embedded` 的实际输出：

```
② 库存不足（第 2 步要求回滚，应逆序补偿）
  [扣款]      gid=order-2 branch=01
  [库存不足]  gid=order-2 → 明确要求回滚
  [发货撤销]  gid=order-2 ← 补偿
  [扣款撤销]  gid=order-2 ← 补偿
  结果: Failed
```

**Go 做不到这个形态**：`c-shared` 会把整个运行时（调度器 + GC + 信号处理）拖进
宿主进程，跟宿主的线程/信号模型冲突，实际没人这么用。所以 DTM 结构上必须独立部署。

### 任何语言都能嵌（C ABI）

编出来就是一个普通 `.so`（约 10 MB，无运行时包袱）：

```bash
cargo build -p dtmrs-ffi --release      # → target/release/libdtmrs.so
```

Python（`bindings/python/`，纯 ctypes，零依赖）：

```python
import dtmrs
tc = dtmrs.Tc("sqlite:/tmp/app.db")

@tc.handler("转出")
def transfer_out(ctx):
    move(1, 2, 100)                     # 你自己的业务 SQL
    return dtmrs.SUCCESS

@tc.handler("转出撤销")
def transfer_out_undo(ctx):
    move(2, 1, 100)
    return dtmrs.SUCCESS

tc.start()
tc.submit_saga("order-1", [("local://转出", "local://转出撤销")])
```

`python bindings/python/example.py` 的实际输出（账户余额真的在动）：

```
初始余额: {1: 1000, 2: 0}
① 正常转账
  [转出] gid=py-1 branch=01 op=action 线程=Dummy-1
  结果: succeed  余额: {1: 900, 2: 100}

② 风控拒绝 → 逆序补偿，钱要退回来
  [转出] gid=py-2 branch=01 op=action
  [风控拒绝] gid=py-2 branch=02 op=action
  [空补偿] gid=py-2 branch=02 op=compensate      ← 逆序
  [转出撤销] gid=py-2 branch=01 op=compensate
  结果: failed  余额: {1: 900, 2: 100}          ← 转出被补偿抹平了

③ 下游超时 → 只重试，不回滚
  状态: submitted
```

C 也验过（`examples/c/demo.c` + `include/dtmrs.h`）：

```bash
gcc -I include examples/c/demo.c -L target/release -ldtmrs -o demo && ./demo
```

#### Node（`bindings/node/`）

handler 可以是 **async** 的 —— 里头正常 await 数据库、HTTP 都行：

```js
const dtmrs = require('./dtmrs');
const tc = new dtmrs.Tc('sqlite:/tmp/app.db');

tc.handler('转出', async (ctx) => {
  await db.query('UPDATE account SET balance = balance - 100 WHERE id = 1');
  return dtmrs.SUCCESS;
});

await tc.start();
await tc.submitSaga('order-1', [['local://转出', 'local://转出撤销']]);
```

`cd bindings/node && npm install && node example.js` 的实际输出：

```
初始余额: { '1': 1000, '2': 0 }

① 正常转账
  [转出] gid=node-1 branch=01 op=action
  结果: succeed  余额: { '1': 900, '2': 100 }

② 风控拒绝 → 逆序补偿，钱要退回来
  [转出] gid=node-2 branch=01 op=action
  [风控拒绝] gid=node-2 branch=02 op=action
  [空补偿] gid=node-2 branch=02 op=compensate      ← 逆序
  [转出撤销] gid=node-2 branch=01 op=compensate
  结果: failed  余额: { '1': 900, '2': 100 }  ← 转出被补偿抹平了

③ 下游超时 → 只重试，不回滚
  状态: submitted  ← 停在 submitted 等重试，没有转 aborting
```

#### JVM（`bindings/java/`，JNA，Java 8+）

```java
try (Dtmrs tc = new Dtmrs("sqlite:/tmp/app.db")) {
    tc.handler("转出", ctx -> {
        jdbc.update("UPDATE account SET balance = balance - 100 WHERE id = 1");
        return Dtmrs.SUCCESS;
    });
    tc.start();
    tc.submitSaga("order-1", Dtmrs.step("local://转出", "local://转出撤销"));
}
```

`cd bindings/java && ./run.sh` 的实际输出（不需要 maven/gradle，只要一个 jna jar）：

```
① 正常转账
  [转出] gid=java-1 branch=01 op=action 线程=Thread-0     ← Rust 的线程回调进 JVM
  结果: succeed  余额: {1=900, 2=100}

② 风控拒绝 → 逆序补偿
  [空补偿] gid=java-2 branch=02 op=compensate
  [转出撤销] gid=java-2 branch=01 op=compensate
  结果: failed  余额: {1=900, 2=100}

④ handler 抛异常 → 也是「结果未知」，绝不回滚
  状态: submitted
```

#### 两种宿主，两种接法（这是实测出来的，不是设计出来的）

| | 接法 | 为什么 |
|---|---|---|
| Python / Java / C | **回调式**（`dtmrs_register`） | 宿主能在任意线程同步执行 handler |
| **Node** | **拉取式**（`dtmrs_register_pull`） | 同步回调里没法 `await` |

原本以为 Node 的障碍是「JS 不能被外来线程回调」。实测推翻了 —— 事件循环空闲时
koffi 会把外来线程的回调排到主线程执行，跑得通。

真正的障碍是另一件事：**C ABI 的回调必须同步返回一个 int**，而 Node 的业务代码
几乎全是异步的（数据库客户端都返回 Promise）。这个用文档绕不过去，所以 C ABI
加了第二种分发方式：

```c
int dtmrs_register_pull(DtmrsTc *tc, const char *name);
int dtmrs_next_task(DtmrsTc *tc, int timeout_ms, char *out, size_t out_len);
int dtmrs_reply(DtmrsTc *tc, unsigned long long task_id, int result);
```

库把待办分支放进队列，宿主在自己的事件循环里取出来、爱怎么异步怎么异步、
完事回填。**跟回调式不是替代关系**，同一进程可以混用，各管各的分支名。

两个必须知道的点：

- `timeout_ms` 传 **0 表示不阻塞**。事件循环型宿主必须传 0 —— 阻塞会卡死循环，
  那样连回填结果都做不到
- 宿主 **30 秒不回填**就按「结果未知」处理。它可能已经把活干完了只是没回话，
  判失败会误触发回滚

同理，Node 绑定的 `waitFinal` 是轮询实现而不是调 C 的 `dtmrs_wait_final` ——
后者会冻结整个事件循环，连分支都分发不出去，必然超时。

**JVM 侧的两个坑**（都在绑定里处理了）：JNA 的回调对象必须被强引用住，
被 GC 掉之后 Rust 那边就是野指针，直接段错误；异常绝不能穿回 Rust
（跨 FFI 边界抛异常是未定义行为），统一按未知处理。

选 JNA 不选 FFM（`java.lang.foreign`）是因为后者到 JDK 22 才转正
（21 是预览、17 是 incubator），要求用户升到 22+ 太苛刻。

#### 跨语言这一跳的三个坑（都处理了）

**1. 宿主回调是同步的，还可能抢 GIL。**
Python handler 去查库、发 HTTP 动辄几十毫秒。直接在 tokio worker 线程里调会把
运行时卡死 —— 所以每次回调都走 `spawn_blocking`，扔进专门的阻塞线程池。

**2. 回调来自任意线程。**
看上面输出里的 `线程=Dummy-1/2/3...` —— 那是 Rust 侧的线程回调进 Python。
所以宿主 handler 必须线程安全（示例里每次现开 sqlite 连接，不共享）。
ctypes 的 `CFUNCTYPE` 自动处理 GIL；JNI 需要自己 attach。

**3. 宿主抛异常 = 结果未知，不是失败。**
Python handler 抛异常、返回野值、甚至回调 panic —— 全都按 `UNKNOWN` 处理，
只重试不回滚。不知道它到底做了没有，误判失败会把本该成功的事务毁掉。

### 嵌入式没有牺牲持久性

TC 在进程里，但状态在 DB 里。进程死了事务不丢 —— 新进程启动后自动接着推，
**不需要客户端重新提交，也不会重做已完成的步骤**。
（测试：`跨进程重启_事务不丢且已完成的步骤不重做`）

### 一个必须知道的约束

`local://` 分支存的是**名字**，因为闭包没法持久化。重启后必须注册同名 handler。

- 漏注册 → 按「结果未知」处理，**只重试不回滚**（这是部署问题，不是业务失败）
- `submit()` 时就检查名字是否都注册了 —— 宁可提交报错，也别等副作用落地才发现

## 现在能用什么

| 能力 | 状态 |
|---|---|
| SAGA（正向提交 + 逆序补偿） | ✅ 可用 |
| **嵌入式 TC（进程内分支 + 无需部署）** | ✅ 可用 |
| **C ABI + Python / Node / JVM 绑定（任何语言可嵌）** | ✅ 可用 |
| 子事务屏障（幂等 / 空回滚 / 悬挂） | ✅ 可用 |
| 崩溃恢复（未终结事务自动续推） | ✅ 可用 |
| 多 TC 实例（DB 租约防重复推进） | ✅ 可用 |
| 指数退避重试 | ✅ 可用 |
| HTTP API（路径与 DTM 对齐） | ✅ 可用 |
| SQLite 存储 | ✅ 可用 |
| **Postgres 存储（多实例生产部署）** | ✅ 可用 |
| **MySQL 存储** | ✅ 可用 |
| **TCC（try/confirm/cancel）** | ✅ 可用 |
| **二阶段消息（取代 MQ 事务消息）** | ✅ 可用 |
| **XA（Postgres + MySQL 两阶段提交）** | ✅ 可用 |
| **gRPC（分支调用 + TC 服务端 API）** | ✅ 可用 |

**105 个测试全绿**：27 个状态机/方言单测 + 7 个存储 + 10 个屏障 + 6 个 XA 工具
+ 8 个 C ABI + 9 个服务端单元 + 6 个 SAGA 端到端 + 12 个 TCC/msg + 5 个嵌入式
+ 6 个 XA 端到端 + **8 个 gRPC 端到端**。
存储和屏障的 17 个会在 sqlite / 真 Postgres / 真 MySQL 上**各跑一遍**；
XA 那 6 个必须有真 Postgres 或真 MySQL（两个都配就都跑）。
Python、Node、Java、C 四个示例都实际跑通（CI 里每次都跑）。

真库测试靠环境变量开关，**没配就是没跑，不是通过**：

```bash
DTMRS_TEST_PG='postgres://postgres:pw@127.0.0.1:5432/dtmrs' \
DTMRS_TEST_MYSQL='mysql://root:pw@127.0.0.1:3306/dtmrs' \
DTMRS_TEST_XA_PG='postgres://postgres:pw@127.0.0.1:5432/dtmrs' \
DTMRS_TEST_XA_MYSQL='mysql://root:pw@127.0.0.1:3306/dtmrs' \
cargo test --workspace
```

## 存储：sqlite / Postgres / MySQL 一套 SQL

```bash
DTMRS_DB='postgres://user:pass@host:5432/dtm' ./dtmrs
DTMRS_DB='mysql://user:pass@host:3306/dtm'    ./dtmrs
DTMRS_DB='sqlite:dtmrs.db'                    ./dtmrs
```

**三种库一份实现**，没有抽 `Store` trait、没有三份 SQL。靠 `sqlx::Any` 加一层
薄薄的方言渲染（`dtmrs-core/src/dialect.rs`）。

只有两种后端的时候 `sqlx::Any` 配 `$N` 就够了，**MySQL 一进来同时打破三条**，
才不得不有这一层。全部实测（sqlx 0.8 / pg16 / mysql 8.0.44）：

| | sqlite | postgres | mysql |
|---|---|---|---|
| `$1` 占位符 | ✅ | ✅ | ❌ `Unknown column '$1'` |
| `?` 占位符 | ✅ | ❌ 语法错误 | ✅ |
| `ON CONFLICT DO NOTHING` | ✅ | ✅ | ❌ 1064 语法错误 |
| `INSERT IGNORE` | ❌ | ❌ | ✅ |
| `TEXT PRIMARY KEY` | ✅ | ✅ | ❌ 1170 要 key length |
| `CREATE INDEX IF NOT EXISTS` | ✅ | ✅ | ❌ 1064 语法错误 |

所以模板统一写 `?`（跟 MySQL 一致），非 MySQL 后端由 `Backend::q` 转成 `$1..$n`。
代价是**模板的字符串字面量里不能出现 `?`**，否则会被当成占位符。

### MySQL 上三个必须知道的坑

**1. `ON DUPLICATE KEY UPDATE` 重复时 `rows_affected` 返回 1，不是 0。**
拿它做幂等判断会把"已存在"误判成"刚插入"。所以 MySQL 必须用 `INSERT IGNORE`
（重复时返回 0，跟另外两家一致）。

**2. 经 `sqlx::Any` 读 MySQL 的 `TEXT` 一律报类型不匹配。**

```
mismatched types; Rust type String is not compatible with SQL type BLOB
```

`LONGTEXT`、`MEDIUMTEXT`、显式 `CHARACTER SET utf8mb4` 都一样，
五种写法里只有 `VARCHAR` 能解成 String。所以自由文本列在 MySQL 上是
`VARCHAR(n)`，n 要够装最长的内容（单行还有 65535 字节总限制，utf8mb4 每字符 4 字节）。

**3. 索引写法是二选一的。**
MySQL 没有 `CREATE INDEX IF NOT EXISTS`，只能建表时内联 `KEY`；
另外两家反过来。dialect 层用 `create_index()` / `inline_index()` 一对返回空值来切。

整数列一律 `BIGINT` —— postgres 的 `INTEGER` 只有 4 字节，装不下时间戳。

### 实测：两个实例并发，没有重复推进

Postgres 的意义就在这儿（sqlite 撑不住多实例写）。20 笔事务、两个实例、
业务端故意每次 sleep 50ms 制造撞车窗口：

```
被调用的分支数: 40 （20 笔 × 2 步）
调用次数分布: {1: 40}          ← 每个分支正好 1 次
重复调用: 无 ✓

事务归属:  succeed|tc-1|10
          succeed|tc-2|10     ← 两个实例各推了一半
```

### 踩到的坑：Postgres 的 CREATE TABLE IF NOT EXISTS 不是并发安全的

两个实例同时启动，实例 1 直接崩了：

```
duplicate key value violates unique constraint "pg_type_typname_nsp_index"
```

`CREATE TABLE IF NOT EXISTS` 在 Postgres 里会在系统目录上撞唯一键 ——
sqlite 单写永远不会暴露这个问题，**只有真起两个实例才能撞出来**。
现在建表带重试（输的那个重试时表已经建好，`IF NOT EXISTS` 正常跳过）。

## TCC

try 阶段由**客户端**驱动（先 `registerBranch` 再调 try），TC 只管 confirm/cancel：

```bash
curl -XPOST :36789/api/dtmsvr/prepare -d '{"gid":"tcc-A","trans_type":"tcc"}'
curl -XPOST :36789/api/dtmsvr/registerBranch -d '{"gid":"tcc-A","branch_id":"01",
  "try":"http://b/try1","confirm":"http://b/confirm1","cancel":"http://b/cancel1"}'
# ← 客户端在这里自己调 try。全成功则 submit，任一失败则 abort
curl -XPOST :36789/api/dtmsvr/submit -d '{"gid":"tcc-A","trans_type":"tcc"}'
```

**必须先登记再调 try**。反过来的话 try 成功但登记失败，TC 不知道有这个分支，
回滚时不会 cancel 它 —— 预留的资源永久泄漏。

## TCC 跟 SAGA 的关键语义差别

SAGA 的 action 返回 FAILURE → 逆序补偿。
**TCC 的 confirm 返回 FAILURE 绝不能触发 cancel** —— try 已经成功、资源已预留、
全局已决定提交，这时候 cancel 会把已确认的事务撤掉。唯一正确处理是无限重试 + 报警。

`tcc_advance` 在 Submitted 阶段**永远不会**返回 `Finish(Aborting)`，
测试里穷举了 3×3 种分支状态组合来钉住这条。
（测试：`confirm失败绝不能触发cancel`、`tcc_confirm失败只重试绝不转cancel`）

## 二阶段消息：不用 MQ 也能保证消息必达

流程：`prepare` 落库 → 业务提交本地事务 → `submit`。
**如果进程崩在这两步之间**，TC 会回查业务方问"你那个本地事务到底提交了没有"：

| 回查回答 | TC 动作 |
|---|---|
| SUCCESS | 本地事务已提交 → 继续推正向分支 |
| FAILURE | 本地事务没提交 → 整单作废 |
| ONGOING / 超时 | **不能当成"没提交"** → 退避重试 |

实测（客户端 prepare 后永不 submit）：

```
prepare → SUCCESS
（不调 submit，等 TC 自己回查…）
状态: succeed  分支: [('action', 'succeed')]
业务侧调用: {'/query': 1, '/notify': 1}     ← 回查一次，然后消息发出去了
```

`prepare` 不给 `query_prepared` 会被**直接拒绝** —— 没有回查地址，客户端崩了
就没人能决断这单，猜"已提交"会重复扣款，猜"没提交"会丢单。

msg 没有补偿分支，只保证最终送达，所以分支必须幂等 + 可无限重试。

## XA：数据库原生两阶段提交

不靠补偿，靠数据库原生的两阶段提交。分支的业务 SQL 跑完就 prepare，
改动**已持久化但对外不可见**，等 TC 统一决定 commit 还是 rollback。
好处是强一致（没有中间态可见），代价是**全程持锁**。

**Postgres 和 MySQL 都支持**（各跑同一套 6 个端到端测试）：

```rust
// 业务方（RM）的一阶段
let xa = Xa::from_url(&db_url)?;           // 认库，两种语法自动分流
let mut br = xa.begin(&pool, gid, "01").await?;
sqlx::query("UPDATE acct SET bal = bal - ? WHERE id = ?")   // 占位符按你的库来
    .bind(100i64).bind(1i32).execute(br.conn()).await?;
let xid = br.prepare().await?;             // 一阶段完成
// 然后把 (xid, commit/rollback 回调地址) 登记给 TC
```

### 两种库的语法完全不同（都实测过）

| | Postgres 16 | MySQL 8.0 |
|---|---|---|
| 开始 | `BEGIN` | `XA START 'xid'` |
| 一阶段 | `PREPARE TRANSACTION 'xid'` | `XA END 'xid'` + `XA PREPARE 'xid'` |
| 提交 | `COMMIT PREPARED 'xid'` | `XA COMMIT 'xid'` |
| 回滚 | `ROLLBACK PREPARED 'xid'` | `XA ROLLBACK 'xid'` |
| 列出悬挂的 | `pg_prepared_xacts` | `XA RECOVER` |
| 重复解决的错误码 | `42704` | `XAE04` |
| 默认是否开启 | ❌ `max_prepared_transactions=0` | ✅ 开着 |
| 能看到 prepare 时长 | ✅ | ❌ `XA RECOVER` 不给时间 |
| xid 长度上限 | 200 字节 | gtrid **64 字节** |

SQLite 根本没有两阶段提交，`Xa::from_url` 直接拒掉。

实测（真 Postgres，两个分支模拟跨库转账；MySQL 上同一套测试同样绿）：

```
prepare 后余额: [1000, 0]        ← 改动已持久化但不可见
挂着的 prepared 事务: 2
submit → COMMIT PREPARED
提交后余额: [900, 100]           ← 两个分支的改动一起生效
挂着的 prepared 事务: 0
```

### ⚠ XA 的三条硬约束（都是实测撞出来的，各有测试钉着）

**1. 没解决的 prepared 事务会永久持锁**（Postgres 里还阻塞 VACUUM）。
写这套测试时，一个测试中途失败留下一个 prepared 事务，**后面完全不相关的
UPDATE 就无限期阻塞了**，整个测试进程卡死两分钟。生产上这会放大成整库不可写。
两种库都一样：`Xa::list_prepared` 必须上监控。
（测试：`xa_没解决的prepared事务会阻塞无关写入`，pg / mysql 各跑一遍）

⚠ **MySQL 上 `age_secs` 恒为 0** —— `XA RECOVER` 不提供 prepare 时间，
所以"挂了多久"这个指标在 MySQL 上拿不到，只能靠"这个 xid 还在不在"报警。

**2. XA 的分支必须操作不相交的数据。**
第一版把两个分支写成改同几行，分支 02 直接被锁死（`55P03 lock timeout`）——
分支 01 已经 prepare，行锁一直持着。这不是 bug，是 XA 的本质：
**一个分支对应一个资源管理器**，真实场景里天然分布在不同库。
（MySQL 上对应 `innodb_lock_wait_timeout`，测试里设 5 秒把"卡住"变成"报错"）

**3. 两种库的可用性检查不是一回事，但都得在启动时做。**
`ensure_enabled()` 分流：

| | 检查什么 | 不合格的后果 |
|---|---|---|
| Postgres | `max_prepared_transactions` > 0 | 默认就是 **0**，XA 完全用不了 |
| MySQL | 版本 ≥ **5.7.7** | XA 能用，但 prepared 的事务**重启后会丢** —— 等于没有持久性 |

别等第一笔事务才发现（那时可能已经有别的分支 prepare 成功了）。

```bash
postgres -c max_prepared_transactions=32     # MySQL 8.0 默认就行，不用配
```

### MySQL 特有的两个坑

**1. XA 语句不能走预处理协议。**

```
1295 This command is not supported in the prepared statement protocol yet
```

`sqlx::query()` 默认走预处理，所以两阶段相关的语句一律改用 `sqlx::raw_sql`
（文本协议）。**连带后果**：xid 没法当参数绑定，只能拼进 SQL 字面量 ——
注入防护全靠 `xid_for()` 里的字符白名单（只留字母数字和 `_-`）。

**2. xid 只有 64 字节，比 Postgres 的 200 严得多。**
gid 稍微长一点就超。直接截断是**灾难性的** —— 两个不相关的长 gid 会撞成同一个
xid，然后互相提交对方的事务。所以超长时截断 + 拼 16 位 FNV-1a 摘要。
（测试：`mysql的xid上限更严且截断不撞车`）

### 二阶段幂等靠状态机保证

重复解决同一个 xid 会报错：Postgres `42704 undefined_object`，
MySQL `XAE04 XAER_NOTA`。TC 一定会重试，所以这两个错误都被当成
`AlreadyResolved`（成功）。

**这个"找不到就算成功"之所以安全**，靠的是 `xa_advance` 的保证：
Submitted 阶段永远不返回 `Finish(Aborting)`，所以"找不到"只可能是之前已经
commit 过，不可能是被 rollback 了。用 SQLSTATE 判断而不是匹配错误文本 ——
文本会随版本和语言变。（测试：`错误码按方言区分`）

## gRPC：两个方向都支持

分支地址前缀决定走什么协议，**同一笔事务里可以混用**：

```json
{"action": "grpc://ship:9000/busi.Busi/Ship", "compensate": "grpc://ship:9000/busi.Busi/Unship"}
{"action": "http://pay/deduct",               "compensate": "http://pay/deduct-undo"}
{"action": "local://扣款",                     "compensate": "local://扣款撤销"}
```

TC 自己也提供与 HTTP 对等的 gRPC API（`dtmrs.v1.Tc`，默认 36790 端口）：

```bash
DTMRS_ADDR=0.0.0.0:36789 DTMRS_GRPC_ADDR=0.0.0.0:36790 ./dtmrs
```

两套接口共用同一份逻辑（`api.rs`），所以 gRPC 提交的事务能用 HTTP 查，反之亦然。

### 业务方不用为 dtmrs 改接口

TC 调 gRPC 分支时**不需要知道业务方的 proto**。实现上用一个只搬字节的自定义
codec 替换 tonic 默认的 prost codec：请求体发裸字节、响应体收裸字节，
方法路径本来就是运行期字符串。

所以**任何已有的 gRPC 方法都能直接当分支**。请求体发空字节（空 protobuf 消息
对任何 message 类型都合法），分支身份走 metadata：

| metadata 键 | 内容 |
|---|---|
| `dtm-gid` | 全局事务号 |
| `dtm-trans_type` | saga / tcc / msg / xa |
| `dtm-branch_id` | 分支号（`01`、`02`…） |
| `dtm-op` | action / compensate / confirm / cancel / … |

这四个正是子事务屏障需要的全部信息，跟 HTTP 那边放 query 参数是一回事。

### 状态码映射：唯一会造成数据不一致的地方

HTTP 靠 409 / 425 表达「明确失败」和「还在处理」，gRPC 得从 16 个标准码里
各挑一个不会被基础设施误用的：

| gRPC 码 | 语义 | 对应 HTTP |
|---|---|---|
| `OK`(0) | 成功 | 200 |
| `ABORTED`(10) | 业务**明确**要求回滚 | 409 |
| `FAILED_PRECONDITION`(9) | 还在处理，别当失败 | 425 |
| **其它全部** | 结果**未知**，只重试不回滚 | 5xx / 超时 |

关键在最后一行。`UNAVAILABLE`(14)、`DEADLINE_EXCEEDED`(4)、`INTERNAL`(13)
恰恰是网络抖动和超时产生的码 —— 而超时的时候对方可能已经成功了，
判失败去补偿就是数据不一致。

`CANCELLED`(1) 尤其容易写错：那是**我们自己**放弃了，不是对方拒绝了。

（测试：`grpc只有aborted才算失败` 穷举了 16 个码；`grpc与http的判定语义一致`
钉住同一意图下两种协议结论必须相同。）

## 跑起来

```bash
cargo build --release
DTMRS_DB=sqlite:dtmrs.db DTMRS_ADDR=127.0.0.1:36789 ./target/release/dtmrs
```

提交一个两步 SAGA：

```bash
curl -XPOST localhost:36789/api/dtmsvr/submit -H 'content-type: application/json' -d '{
  "gid": "order-1001",
  "steps": [
    {"action": "http://busi/deduct",  "compensate": "http://busi/deduct-undo"},
    {"action": "http://busi/shipment","compensate": "http://busi/shipment-undo"}
  ]}'

curl 'localhost:36789/api/dtmsvr/query?gid=order-1001'
```

实测的回滚链路（`/a2fail` 返回 409）：

```
状态: failed   回滚原因: 分支 02 返回 FAILURE
  01 action      succeed
  01 compensate  succeed     ← 逆序补偿
  02 action      failed
  02 compensate  succeed
```

## 业务侧接入：必须用屏障

分支接口**一定会**被重复调用（TC 重试 + 崩溃恢复），所以业务侧必须幂等。
用 `dtmrs-barrier`，把屏障记录和业务 SQL 放进**同一个本地事务**：

```rust
use dtmrs_barrier::{BranchBarrier, Decision};
use dtmrs_core::Backend;

let be = Backend::from_url(&db_url);        // 屏障表的 DDL 和 SQL 都按它渲染
BranchBarrier::migrate(&pool, be).await?;   // 启动时建表一次

// gid / branch_id / op / trans_type 由 TC 通过 query 参数传进来
let mut bb = BranchBarrier::new(be, trans_type, gid, branch_id, op)?;
let mut tx = pool.begin().await?;

if bb.decide(&mut tx).await? == Decision::Execute {
    // 业务 SQL —— 必须在这个 tx 里
    sqlx::query("UPDATE account SET balance = balance - ? WHERE id = ?")
        .bind(amount).bind(uid).execute(&mut *tx).await?;
}
tx.commit().await?;   // 原子性的来源：屏障记录与业务变更同生共死
```

`decide` 返回三种结论：

| 结论 | 含义 |
|---|---|
| `Execute` | 该干活 |
| `NullCompensation` | **空回滚** —— 正向分支从没跑过，补偿空转 |
| `Duplicated` | **重复或悬挂** —— 这次调用已处理过，跳过 |

**前提：barrier 表必须和业务表在同一个数据库实例**，否则共用不了本地事务。
这不是实现限制，是这个方案成立的根本条件。

## 三个容易写错的地方（都有测试守着）

**1. 超时不等于失败。**
500 / 连接超时代表**结果未知** —— 对方可能已经成功了。这时候回滚会造成
不一致，正确做法是退避重试，直到拿到明确的 SUCCESS 或 FAILURE。
只有 HTTP 409 或响应体里的 `FAILURE` 才触发补偿。
（测试：`超时不能触发回滚而要重试`）

**2. 补偿要发给所有分支，不只是成功的那些。**
某个 action 超时但实际上执行成功了 —— 如果因为"它没成功"就跳过它的补偿，
钱就漏出去了。所以回滚时逆序补偿**全部**分支，多余的那些由屏障空转掉。
宁可多发补偿，不可漏发。
（测试：`没跑过的分支也要补偿`、`主动中止会触发补偿`）

**3. 重复提交必须成功而不是报错。**
客户端网络抖动重试提交同一个 gid，返回错误会让客户端以为没受理。
`INSERT OR IGNORE` + 返回 SUCCESS。
（测试：`重复提交同一个gid是幂等的`）

## 结构

```
crates/
  dtmrs-core/     状态机，纯逻辑无 I/O —— 状态迁移的 bug 全在这层测
                  dialect.rs：sqlite / postgres / mysql 的 SQL 方言渲染
  dtmrs-store/    存储（三种库一套 SQL）+ 租约抢占
  dtmrs-server/   TC：api.rs（协议无关的操作层）
                      main.rs（axum HTTP）/ grpc/（tonic 两个方向）
                      driver.rs（常驻推进器）/ registry.rs + embedded.rs（嵌入式）
  dtmrs-barrier/  客户端子事务屏障
  dtmrs-xa/       业务方（RM）的 XA 助手：pg 的 PREPARE TRANSACTION / mysql 的 XA
  dtmrs-ffi/      C ABI（cdylib + staticlib），回调式 + 拉取式两种分发
include/dtmrs.h            C 头文件
bindings/python/            Python 绑定（纯 ctypes，零依赖）
bindings/node/              Node 绑定（koffi，handler 可 async）
bindings/java/              JVM 绑定（JNA，Java 8+，不需要 maven/gradle）
examples/c/demo.c           C 示例
```

HTTP 和 gRPC 都只是 `api.rs` 的薄封装 —— 两套协议各写一遍逻辑迟早漂移，
而这里漂移的后果是「同一个请求走 HTTP 被拒、走 gRPC 却受理了」。

把状态机跟 I/O 分开是刻意的：分布式事务的 bug 大多出在状态迁移上，
隔离出来就能穷举测试，不用起网络。

## 协议出处

状态机、表结构、屏障算法都是**逐条核对 DTM 源码**得出的，不是二手资料：

- `sqls/dtmsvr.storage.mysql.sql` —— 全局/分支状态与字段
- `sqls/dtmcli.barrier.mysql.sql` —— 屏障表与唯一键
- `client/dtmcli/barrier.go` 的 `BranchBarrier.Call` —— 屏障算法
- `dtmsvr/trans_type_{saga,tcc,msg,xa}.go` —— 各模式实现

只实现协议，不抄代码。DTM 是 BSD-3，与 Apache-2.0 兼容。

详细设计见 [DESIGN.md](DESIGN.md)。
