# dtmrs

Rust 写的分布式事务管理器，对标 [DTM](https://github.com/dtm-labs/dtm)（Go，10.9k★）。

**为什么重做**：Rust 生态里没有能用的分布式事务管理器。唯一对标项目
[rseata](https://github.com/oulover/rseata) 只有 88★、1 个贡献者、33 次提交、
**而且没有 LICENSE 文件** —— 无许可证等于保留全部版权，法律上不能商用。
其它 Rust 方案（restate/obelisk）是持久化执行范式，且分别受 BUSL / AGPL 限制。

dtmrs 是 **Apache-2.0**，商用闭源接入没有障碍。

## 现在能用什么

| 能力 | 状态 |
|---|---|
| SAGA（正向提交 + 逆序补偿） | ✅ 可用 |
| 子事务屏障（幂等 / 空回滚 / 悬挂） | ✅ 可用 |
| 崩溃恢复（未终结事务自动续推） | ✅ 可用 |
| 多 TC 实例（DB 租约防重复推进） | ✅ 可用 |
| 指数退避重试 | ✅ 可用 |
| HTTP API（路径与 DTM 对齐） | ✅ 可用 |
| SQLite 存储 | ✅ 可用 |
| Postgres 存储 | ⬜ 未做 |
| TCC / 二阶段消息 / XA | ⬜ 未做（`submit` 会明确报错，不假装支持） |
| gRPC | ⬜ 未做 |

**28 个测试全绿**，包含 6 个走真实 HTTP 的端到端用例。

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
  dtmrs-server/   TC：axum HTTP + 常驻推进器
  dtmrs-barrier/  客户端子事务屏障
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
