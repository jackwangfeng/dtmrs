# dtmrs

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

编出来就是一个普通 `.so`（8.5 MB，无运行时包袱）：

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
| **C ABI + Python 绑定（任何语言可嵌）** | ✅ 可用 |
| 子事务屏障（幂等 / 空回滚 / 悬挂） | ✅ 可用 |
| 崩溃恢复（未终结事务自动续推） | ✅ 可用 |
| 多 TC 实例（DB 租约防重复推进） | ✅ 可用 |
| 指数退避重试 | ✅ 可用 |
| HTTP API（路径与 DTM 对齐） | ✅ 可用 |
| SQLite 存储 | ✅ 可用 |
| **Postgres 存储（多实例生产部署）** | ✅ 可用 |
| **TCC（try/confirm/cancel）** | ✅ 可用 |
| **二阶段消息（取代 MQ 事务消息）** | ✅ 可用 |
| XA | ⬜ 未做（`submit` 明确报错，推进器也不假装成功） |
| gRPC | ⬜ 未做 |

**58 个测试全绿**（存储层和屏障的 17 个会在 sqlite 和真 Postgres 上各跑一遍）：6 个 SAGA 端到端 + 11 个 TCC/msg + 5 个嵌入式（含跨进程重启恢复）
+ 4 个 C ABI（含空指针/坏参数不崩）+ 10 个屏障 + 13 个状态机单测。
Python 和 C 的示例都实际跑通。

## Postgres：多实例生产部署

```bash
DTMRS_DB='postgres://user:pass@host:5432/dtm' ./dtmrs
```

**一套 SQL 跑两种库**，没有抽 `Store` trait、没有两份实现。靠的是 `sqlx::Any`，
但有两条实测出来的硬规矩（写在 `dtmrs-store` 文件头）：

| | sqlite | postgres |
|---|---|---|
| `$1` 占位符 | ✅ | ✅ |
| `?` 占位符 | ✅ | ❌ 语法错误 |
| `ON CONFLICT DO NOTHING` | ✅ | ✅ |
| 冲突时 `rows_affected` | 1 → 0 | 1 → 0（一致） |

1. **只用 `$N`**，永不用 `?`
2. **同一个 `$N` 不复用** —— sqlite 把 `$4` 当命名参数，复用会让位置绑定错位
3. 整数列用 `BIGINT` —— postgres 的 `INTEGER` 只有 4 字节，装不下时间戳

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

### Postgres：多实例生产部署

```bash
DTMRS_DB='postgres://user:pass@host:5432/dtm' ./dtmrs
```

**一套 SQL 跑两种库**，没有抽 `Store` trait、没有两份实现。靠的是 `sqlx::Any`，
但有两条实测出来的硬规矩（写在 `dtmrs-store` 文件头）：

| | sqlite | postgres |
|---|---|---|
| `$1` 占位符 | ✅ | ✅ |
| `?` 占位符 | ✅ | ❌ 语法错误 |
| `ON CONFLICT DO NOTHING` | ✅ | ✅ |
| 冲突时 `rows_affected` | 1 → 0 | 1 → 0（一致） |

1. **只用 `$N`**，永不用 `?`
2. **同一个 `$N` 不复用** —— sqlite 把 `$4` 当命名参数，复用会让位置绑定错位
3. 整数列用 `BIGINT` —— postgres 的 `INTEGER` 只有 4 字节，装不下时间戳

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

// gid / branch_id / op / trans_type 由 TC 通过 query 参数传进来
let mut bb = BranchBarrier::new(trans_type, gid, branch_id, op)?;
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
  dtmrs-store/    存储（sqlite）+ 租约抢占
  dtmrs-server/   TC：axum HTTP + 常驻推进器 + 嵌入式门面（registry / embedded）
  dtmrs-barrier/  客户端子事务屏障
  dtmrs-ffi/      C ABI（cdylib + staticlib）
include/dtmrs.h            C 头文件
bindings/python/dtmrs.py   Python 绑定（纯 ctypes）
examples/c/demo.c          C 示例
```

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
