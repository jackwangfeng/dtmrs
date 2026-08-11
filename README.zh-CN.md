# dtmrs

**简体中文** | [English](README.md)

[![CI](https://github.com/jackwangfeng/dtmrs/actions/workflows/ci.yml/badge.svg)](https://github.com/jackwangfeng/dtmrs/actions/workflows/ci.yml) [![crates.io](https://img.shields.io/crates/v/dtmrs.svg?logo=rust)](https://crates.io/crates/dtmrs) [![Stars](https://img.shields.io/github/stars/jackwangfeng/dtmrs?style=flat&logo=github)](https://github.com/jackwangfeng/dtmrs/stargazers) [![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

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

## 文档

README 讲的是「这东西是什么」，**怎么用看 [docs/](docs/)**：

| 我想… | 看这篇 |
|---|---|
| 先跑起来看看效果 | [五分钟快速上手](docs/quickstart.md) |
| **搞清楚该用哪种模式** | [五种模式怎么选](docs/choosing-a-mode.md) |
| **把我的服务接进去** | [业务侧接入指南](docs/integration.md) |
| 上生产 / 多实例 / 配监控 | [部署与运维](docs/deployment.md) |
| 查接口字段和返回码 | [API 参考](docs/api.md) |
| 出问题了 | [排错](docs/troubleshooting.md) |
| 查 Rust 库 API | [docs.rs/dtmrs](https://docs.rs/dtmrs) |

只读两篇的话：**选型**和**接入**。这两件事做错的后果不是报错，而是静默的数据不一致。


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
| **workflow 模式（流程写成函数，崩溃后断点续跑）** | ✅ 可用 |
| **Redis 存储（秒杀类尖峰，⚠ 持久性弱于 SQL）** | ✅ 可用 |

**139 个测试全绿**：33 个状态机/方言单测 + 7 个存储 + 10 个屏障 + 6 个 XA 工具
+ 8 个 C ABI + 13 个服务端单元 + 6 个 SAGA 端到端 + 12 个 TCC/msg + 5 个嵌入式
+ 6 个 XA 端到端 + 8 个 gRPC 端到端 + 12 个 workflow 端到端 + **10 个 Redis**。
存储和屏障的 17 个会在 sqlite / 真 Postgres / 真 MySQL 上**各跑一遍**；
XA 那 6 个必须有真 Postgres 或真 MySQL（两个都配就都跑）。
Python、Node、Java、C 四个示例都实际跑通（CI 里每次都跑）。

真库测试靠环境变量开关，**没配就是没跑，不是通过**：

```bash
DTMRS_TEST_PG='postgres://postgres:pw@127.0.0.1:5432/dtmrs' \
DTMRS_TEST_MYSQL='mysql://root:pw@127.0.0.1:3306/dtmrs' \
DTMRS_TEST_XA_PG='postgres://postgres:pw@127.0.0.1:5432/dtmrs' \
DTMRS_TEST_XA_MYSQL='mysql://root:pw@127.0.0.1:3306/dtmrs' \
DTMRS_TEST_REDIS='redis://127.0.0.1:6379/0' \
cargo test --workspace --features dtmrs/redis,dtmrs-server/redis,dtmrs-store/redis
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

## workflow 模式：把流程写成一个普通函数

前四种模式都要求**提交时就把步骤声明清楚**。真实业务常常不满足：第三步做不做
取决于第二步返回了什么，中间还有 `if`、有循环。

workflow 模式让你直接写：

```rust
let tc = Embedded::builder("sqlite:app.db")
    .handler("退款", |_| async { BranchResult::Success })
    .workflow("下单", |mut wf| async move {
        // 第一步的返回值会被记住，重放时原样还回来
        let oid = wf.branch("建订单").on_rollback("local://取消订单")
            .run_with(|| async { (BranchResult::Success, new_order_id()) }).await?;

        wf.branch("扣款").on_rollback("local://退款")
            .run(|| async { deduct(&oid).await }).await?;

        // 控制流是真的控制流
        if need_ship(&oid) {
            wf.branch("发货").on_rollback("local://退货")
                .run(|| async { ship(&oid).await }).await?;
        }
        Ok(())
    })
    .start().await?;

tc.submit_workflow("order-1001", "下单", r#"{"amount":100}"#).await?;
```

### 崩溃恢复靠重放 + 结果记忆化

进程崩了重启，TC 把这个函数**从头再跑一遍**。已经成功过的分支不重新执行，
而是把上次存的返回值原样还回来——所以函数沿着上次的路径走到断点，然后继续往下：

```text
第一次:  建订单(真跑,存 oid) → 扣款(真跑) → 崩溃
重启后:  建订单(记忆化,还回 oid) → 扣款(记忆化) → 发货(真跑) → 完成
                  ↑ 副作用不会重做
```

`cargo run --example workflow -p dtmrs-server` 的实际输出：

```
② 崩溃重启 → 已完成的步骤不重做
  --- 进程 A ---
  [扣款] 真的执行了（累计第 1 次）
  [入账] 超时，结果未知 → 只重试，不回滚
  状态: Some(Submitted)（进程 A 到此被杀）
  --- 进程 B（同一个库，全新的 TC，客户端没有重新提交）---
  [入账] 成功
  结果: Succeed   扣款总共执行了 1 次 ← 重启没有重做它
```

### ⚠ 你的函数必须是确定性的

重放是**从头再跑**，所以分支之间那些代码会被执行多次。它们必须在相同的分支
返回值下走相同的路径：

- ❌ `if rand() > 0.5`、`if now().hour() < 12`、读一个会变的全局状态
- ❌ 在分支**外面**直接写数据库——那部分不会被记忆化，重放时会重复执行
- ✅ 所有副作用都放进 `branch(...).run(...)` 里

写岔了会怎样？**当场发现并停下**，而不是静默补偿错对象：

```
重放走岔了：分支 02 上次记录的是「扣款」，这次却是「发货」。
函数必须是确定性的，副作用都要放进 branch().run() 里
```

这时候既不落成功也不回滚——已经不知道真实进度了，硬回滚更危险。停在原地等人
改回确定性的代码，重启就能接着推。
（测试：`重放走岔了会被当场发现`）

### 补偿只发给「已经跑到」的分支

跟 SAGA「补偿所有分支」看着不同，其实是同一条规则：没跑到的分支压根没登记，
也就没有副作用要收拾。

关键在于**补偿先于正向动作登记**——这样即使正向动作超时、或者进程当场崩了，
补偿也已经在库里了，不会漏。这跟 TCC「必须先 registerBranch 再调 try」是同一条教训。

没给 `on_rollback` 的分支不会被补偿，只适合本来就没副作用的步骤（比如纯查询）。

### 为什么这个模式只在嵌入式形态下提供

因为「步骤」是**代码**，没法表示成一个 URL 存进数据库。DTM 那边同理：workflow
的函数体在客户端进程里，TC 只存状态。我们把 TC 也放在同一个进程里，这件事反而更自然。

HTTP / gRPC 提交 `trans_type=workflow` 会被明确拒绝，而不是假装受理。

## Redis 后端：为秒杀那类流量尖峰而生

```bash
DTMRS_DB='redis://127.0.0.1:6379/0' ./dtmrs
```

需要开 feature（默认是关的）：`cargo build --features redis`。

短时间涌进海量事务时，SQL 库的写入和行锁扛不住 —— DTM 支持 Redis 也是这个动机。

### ⚠ 三条跟 SQL 后端不一样的语义，用之前必须知道

**1. 持久性弱一档。** Redis 默认 `appendfsync everysec`，崩溃可能丢掉最后一秒的写入。
对协调器来说丢的是**事务状态**：可能出现「业务侧已经扣了款，但 TC 这边没有这笔事务」
的悬挂。SQL 后端不会（提交即落盘）。

要么接受它（秒杀场景常常能接受，且有对账兜底），要么配 `appendfsync always`
（吞吐会掉，但仍比 SQL 快）。**别在默认配置下跑资金类强一致场景。**

**2. 终态事务会过期消失**（默认 7 天 TTL）。不然秒杀几千万笔之后内存就没了。
SQL 后端里已完成的事务是永久留着的。要长期审计记录得自己往别处归档。

**3. `list_recent` 只保留最近 1000 笔**，是管理视图不是全量历史。

### 原子性靠 Lua，不靠行锁

多实例抢同一笔事务时，SQL 那边靠「先 SELECT 再带条件 UPDATE」两步法 + 行锁；
Redis 这边整段逻辑在一个 Lua 脚本里跑完 —— Redis 执行脚本**单线程且不可打断**，
所以「找到到期事务 + 抢占它」天然原子，反而比 SQL 那套更直接。

脚本里还会顺手清掉索引中已经不该调度的成员，索引不会因为别处漏维护而慢慢腐烂。

实测（3 个 TC 实例、20 笔事务、业务端故意每次 sleep 30ms 制造撞车窗口）：

```
各实例处理数: [(0, 9), (1, 6), (2, 5)]    ← 活确实分散在三个实例上
重复推进的分支: 无 ✓                        ← 每个分支正好被调用 1 次
```

（测试：`redis_多实例并发不重复推进`）

### 这件事推翻了一个原先写下的设计决定

DESIGN.md 里原本明确写着**不抽 `Store` trait**，理由是「三种库的差异小到一层模板
就能吸收，抽 trait 是过早抽象」。那个判断在当时是对的。

**Redis 让前提不成立了** —— 它不是 SQL，没有表、没有事务、没有 WHERE，模板吸收不了。
所以现在有了一层后端分发。用的是 enum 而不是 trait：调用方拿到的还是同一个 `Store`
具体类型，四十多个调用点一行没改，也不用到处写泛型或 `dyn`。

## 性能：实测数字，以及它们的边界

**先说清楚这些数字不能干什么**：没有跑同一套硬件、同一个业务服务、同一种存储
配置的 DTM 对照组，所以**不能拿来说「比 DTM 快」**。这里只报 dtmrs 自己在不同
存储上的相对表现和绝对量级。

复现：`python3 bench/bench.py --db redis --n 3000 --concurrency 100 --workers 16`

测的是**端到端**：提交 → TC 依次调两个分支 → 落终态。业务分支是本地零操作的
HTTP 服务，所以数字基本反映 TC + 存储的开销。

| 存储 | 串行推进（`workers=1`） | 默认（`workers=16`） |
|---|---|---|
| Redis | 965 笔/秒 | **4695 笔/秒** |
| Postgres | 267 笔/秒 | **3424 笔/秒** |
| sqlite（WAL） | 435 笔/秒 | **682 笔/秒** |
| MySQL | 19 笔/秒 | **129 笔/秒** |

两步 SAGA，提交并发 100，推进器 tick 5ms，**取三次的中位数**，
每次跑之前**都把库清空**（见下面「这些数字为什么会漂」）。
机器：12th Gen Intel(R) Core(TM) i7-12700，20 核，Linux；数据库都在本机 docker 里。
这台机器上还跑着别的东西，单次结果能差 ±20%，别把个位数当真。

几点值得说明：

- **默认的 16 个 worker 基本就到顶了。** Postgres 上 16 个 worker 3196 笔/秒、
  64 个 3184 —— 一样。连接池也不是瓶颈（`DTMRS_DB_POOL` 从 32 加到 64 没区别）。
  想再快得减少每笔事务的存储往返次数，加并发没用。
- **sqlite 从并行里拿不到多少收益**，因为它的写本来就是全库串行的。
- **MySQL 慢是它自己的默认配置**：`innodb_flush_log_at_trx_commit=1` +
  `sync_binlog=1`，每次提交两次 fsync，而推一笔事务要好几次提交。
  把 `innodb_flush_log_at_trx_commit` 设成 2，同样的代码就从 123 涨到 341 笔/秒 ——
  这是持久性取舍，不是 dtmrs 的开销。
- **Postgres 不是 fsync 瓶颈**：库干净时关掉 `synchronous_commit` 是
  3100 → 2970，也就是没区别。

### 跟 DTM 的对照

同一台机器、同一个 Redis（`--network host`）、同一个零操作业务服务、
同一个压测客户端，2 万笔，三次中位数：

| 模式 | dtmrs | DTM v1.19 | |
|---|---|---|---|
| msg，1 个正向步骤 | **12540 笔/秒** | 10299 | dtmrs 快 22% |
| saga，2 个步骤 | 7630 | **9339 笔/秒** | DTM 快 22% |

**各赢一半，而且原因是架构差异，不是谁的代码更细致：**

DTM 在 **submit 请求里同步把事务推完**，不入队；dtmrs 是 submit 立刻返回、
由推进器从队列里抢。抢占那一下（`lock_one_due`）在 Redis 上是一次
Lua 脚本往返，**每笔事务固定要付**。saga 只有一次客户端请求，这个固定成本
占比就高；msg 有 prepare + submit 两次请求，摊薄了，我们的批量脚本反而占优。

两个诚实的注脚：

- 这个对比测了**三次才做对**。第一版 DTM 只跑出 1500 笔/秒，是因为压测脚本的
  业务服务 accept 队列只有 5（Python `socketserver` 的默认值）—— DTM 不复用
  连接，被内核成片 RST；我们用连接池所以几乎没事。改成 4096 之后 DTM 直接
  到 9600、零错误。**跨实现的压测，先怀疑自己的脚本。**
- DTM 默认 `UpdateBranchSync: 0`（分支状态异步落盘），上表已改成 1 对齐，
  实测差别不大。

复现方法和另外两个必须先确认的坑，见 `bench/bench.py` 的文件头。

### 秒杀场景：二阶段消息 + Redis

秒杀用的是**二阶段消息（msg）**，不是 SAGA —— 卖出去的东西没有「反向扣库存」
这回事，本来就不需要补偿。它的形状是：`prepare` →（自己的本地事务，扣库存）→
`submit`，然后 TC 保证下游那一步最终一定送达。

同一台机器，Redis 存储，2 万笔，三次中位数：

| 模式 | 正向步数 | 吞吐 |
|---|---|---|
| **msg** | 1（秒杀的典型形状） | **11573 笔/秒** |
| saga | 2 | 8653 笔/秒 |
| msg | 2 | 7903 笔/秒 |

两点值得注意：

- **步数比模式更重要。** msg 在同为 2 步时反而比 saga 略慢 —— 因为 msg 的客户端
  要发两次请求（prepare + submit），saga 只发一次。msg 快是快在秒杀本来
  只需要 1 个下游步骤。
- **选 msg 不是为了吞吐，是为了它是这个场景下唯一正确的模式。** 没有补偿分支，
  存储也更省：Redis 上每笔 1450 字节，2 步 SAGA 是 1961 字节。

### ⚠ 存储的网络路径：docker 端口映射差 36%

上面所有数字都是存储跑在 docker 里、用 `-p` 发布端口测的。换成
`--network host`（等于存储和 TC 同机直连）：

| 路径 | redis-benchmark | 单连接 p50 | msg 1 步 |
|---|---|---|---|
| `--network host` | 187k req/s | 0.015 ms | **11573 笔/秒** |
| `-p 16379:6379` | 151k req/s | 0.023 ms | 8500 笔/秒 |

每次往返只多 ~8µs，但推一笔事务要串行打几十次 Redis，累积就是 36%。
**所以本节的数字取决于你的部署拓扑，报数时要说清楚。**

### ⚠ 这些数字为什么会漂：库里的存量数据

同一条命令，空库跑出 3424 笔/秒，库里堆了 4 万笔历史事务之后只有 777 ——
**差 4.4 倍**。跑得越久越慢，一次跑的笔数越多也越慢：

| 一次跑的笔数 | Postgres 吞吐 |
|---|---|
| 5000 | 3424 笔/秒 |
| 20000 | 3196 |
| 40000 | 2713 |

原因是每笔事务要 UPDATE 好几次，死元组堆积得比 autovacuum 收得快。

由此带出一个**已知的产品缺口**：Redis 后端给终态记录挂了 7 天 TTL
（`DEFAULT_FINAL_TTL`），**SQL 后端没有任何保留策略** —— 已完成的事务会永远留着。
现在得自己写清理任务：

```sql
DELETE FROM trans_branch_op WHERE gid IN (
  SELECT gid FROM trans_global
  WHERE status IN ('succeed','failed') AND finish_time < <7天前的unix秒>);
DELETE FROM trans_global
  WHERE status IN ('succeed','failed') AND finish_time < <7天前的unix秒>;
```

（先删分支再删主表，顺序反了会留下孤儿行。）

### 压测抓出来的三个真问题

都是压测之前谁也没想到的：

**① sqlite 没开 WAL —— 40 倍。** 默认的 rollback journal + `synchronous=FULL`
是每笔事务一次 fsync：

| 并发 | 开 WAL 前 | 开 WAL 后 |
|---|---|---|
| 1 | 13 笔/秒 | **541** |
| 10 | 55 | **944** |
| 20 | **崩溃**（`database is locked` → 请求超时） | **1162** |

现在 sqlite 连接会自动设 `journal_mode=WAL` / `synchronous=NORMAL` /
`busy_timeout=5000`。

**②' 压测脚本自己就是瓶颈 —— 33 倍。**
零操作业务服务和「每 50ms 重查上千个 gid」挤在同一个 Python 进程里，
而且 `http.server` 的响应分两次 write，开着 Nagle 时第二段要等对端延迟 ACK，
每次分支调用固定多 ~40ms。修完这一条：96 → 3216 笔/秒。
**先确认自己没在测压测脚本**，否则后面所有结论都是假的。

**② 抢占待办时的锁车队 —— Postgres 上并行度几乎为 0。**
抢占是「SELECT 出最该跑的那笔 → UPDATE 占坑」。N 个 worker 的 SELECT
会**全部选中同一行**，然后挤在 UPDATE 上排队，只有一个能成、其余白跑一轮。
症状是加 worker 不涨：Postgres 1 个 worker 71 笔/秒，8 个也才 127。
修法是 `FOR UPDATE SKIP LOCKED`（sqlite 没有行锁，那边是空串）。

**③ 同一条 SQL 在 MySQL 上还要去掉 `ORDER BY`。**
索引是 `(status, next_cron_time)`，而 WHERE 里 status 是个 IN 范围，
所以按 `next_cron_time` 排序用不上索引 —— 执行计划里是 `Using filesort`，
意味着 MySQL 要**把所有命中的行读出来并加锁**才能排序，然后才 `LIMIT 1`。
结果第一个 worker 锁光全部待办，其余 worker 一笔都抢不到（6 并发只成 1 笔）。
去掉 `ORDER BY` 走索引范围扫描，同一状态内仍然是最早到期的先跑，
而且抢到的行会被推到队尾，不会饿死。

这三条现在都有测试钉着（`并发抢占要各拿各的不能全挤在同一笔上`）。

### 顺带被压测抓出来的一个真问题

sqlite 原来没开 WAL，走默认的 rollback journal + `synchronous=FULL`，
**每笔事务一次 fsync**：

| 并发 | 开 WAL 前 | 开 WAL 后 |
|---|---|---|
| 1 | 13 笔/秒 | **541** |
| 10 | 55 | **944** |
| 20 | **崩溃**（`database is locked` → 请求超时） | **1162** |

40 倍差距，而且并发 20 直接不可用。这种问题不压测永远发现不了。
现在 sqlite 连接会自动设 `journal_mode=WAL` / `synchronous=NORMAL` /
`busy_timeout=5000`。


## 安装

```bash
cargo install dtmrs                 # 协调器二进制
DTMRS_DB=sqlite:dtmrs.db dtmrs
```

当库用的话，`dtmrs` 是各实现 crate 的**门面**——加一个依赖，不用挨个加五个。
**按你在系统里的角色选 feature**，两端要的东西完全不同：

```toml
# 跑协调器（或者把它嵌进自己进程）
dtmrs = "0.2"

# 业务服务（RM）：只需要屏障做幂等。
# 别把整个协调器（axum、tonic 那一堆）拖进去
dtmrs = { version = "0.2", default-features = false, features = ["barrier"] }
```

| Feature | 给你什么 |
|---|---|
| `server`（默认） | 协调器本体、`Embedded`、`dtmrs` 二进制 |
| `grpc` | gRPC 服务端接口 + 调 `grpc://` 分支 |
| `barrier` | 子事务屏障——任何业务服务接入都必需 |
| `xa` | 业务侧（RM）的 XA 助手 |
| `full` | 全都要 |

实测这个切分是有意义的：barrier-only 的依赖树里 **axum / tonic / reqwest 一个都没有**
（436 个依赖 vs 默认的 634 个）。

想更精细地控制依赖，底下那六个 crate（`dtmrs-core` / `dtmrs-server` / `dtmrs-store` /
`dtmrs-barrier` / `dtmrs-xa` / `dtmrs-ffi`）也都单独发布。

## 从源码跑

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
  dtmrs/         门面 crate + dtmrs 二进制（用户加这一个依赖就够）
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
