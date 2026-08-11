# 业务侧接入指南

这篇讲的是**你的服务**要怎么改，才能安全地当 dtmrs 的一个分支。

接错的后果不是报错，是**静默的数据不一致**——所以这篇每一节都值得读完。

## 一、先接受这个前提：你的接口一定会被重复调用

不是「可能」，是**一定**。至少四种情况会导致重复：

1. TC 调用超时但你其实执行成功了 → TC 会重试
2. TC 进程崩溃重启 → 未终结的事务会被重新捞起推进
3. 多个 TC 实例（虽然有租约防重，但租约过期时会接手）
4. 客户端网络抖动重试提交

所以：**分支接口必须幂等**。这不是可选项。

## 二、子事务屏障：一张表解决三个问题

光做幂等还不够。还有两个更隐蔽的问题：

| 难题 | 场景 | 不处理的后果 |
|---|---|---|
| **幂等** | 同一分支被调两次 | 重复扣款 |
| **空回滚** | action 还没执行（网络丢包）就来了 compensate | 凭空退款 |
| **悬挂** | compensate 先到、action 后到 | 补偿完又扣了一笔，永远补不回来 |

一张表 + 一条 `INSERT IGNORE` 全解决。

### 用法

```rust
use dtmrs::barrier::{BranchBarrier, Decision};
use dtmrs::Backend;

// 启动时建一次表
let be = Backend::from_url(&db_url);
BranchBarrier::migrate(&pool, be).await?;

// 每次处理分支请求
// gid / branch_id / op / trans_type 由 TC 传进来（HTTP 走 query 参数，gRPC 走 metadata）
let mut bb = BranchBarrier::new(be, trans_type, gid, branch_id, op)?;
let mut tx = pool.begin().await?;

if bb.decide(&mut tx).await? == Decision::Execute {
    // 你的业务 SQL —— 必须在这个 tx 里
    sqlx::query("UPDATE account SET balance = balance - ? WHERE id = ?")
        .bind(amount).bind(uid).execute(&mut *tx).await?;
}

tx.commit().await?;   // 原子性的来源：屏障记录与业务变更同生共死
```

`decide` 返回三种结论：

| 结论 | 你该做什么 |
|---|---|
| `Execute` | 执行业务逻辑 |
| `NullCompensation` | **空回滚**——正向分支从没跑过，补偿直接空转，返回成功 |
| `Duplicated` | **重复或悬挂**——这次调用已处理过，跳过，返回成功 |

后两种都要**返回成功**，不是失败——它们是正常路径，不是错误。

### ⚠ 两条不能违反的前提

**1. 屏障表必须和业务表在同一个数据库实例。**

不是同一个库就没法共用一个本地事务，屏障记录和业务变更就不再原子——这个方案直接失效。这不是实现限制，是它成立的根本条件。

### 业务数据在 Redis 里怎么办（秒杀）

上面那两条前提有个直接后果：**库存在 Redis 里的话，这套屏障用不了**——
根本没有 SQL 本地事务可以加入。而秒杀恰恰就是这个形状。

开 `barrier-redis` feature，换成 `RedisBarrier`：原子性来源从「同一个本地事务」
变成「**同一个 Lua 脚本**」（Redis 执行脚本单线程且不可打断）。

```rust
use dtmrs::barrier::{RedisBarrier, RedisOutcome};

// 从 TC 传来的 query 参数构造，跟 SQL 版一样
let mut bb = RedisBarrier::new(trans_type, gid, branch_id, op)?;

// 检查库存够不够，够就扣 —— 屏障判定和扣减在同一个脚本里
match bb.check_adjust_amount(&mut conn, "stock:1001", -1).await? {
    RedisOutcome::Executed         => Ok(()),           // 扣成功
    RedisOutcome::Failure          => Err(失败),        // 库存不足 → 让 TC 回滚
    RedisOutcome::NullCompensation => Ok(()),           // 空回滚，正常路径
    RedisOutcome::Duplicated       => Ok(()),           // 重复/悬挂，正常路径
}
```

不是加减法的业务用 `call()` 传自己的 Lua（`KEYS`/`ARGV` 编号从 1 开始，
屏障自己的键追加在后面，不打乱你的编号）。

判定语义跟 SQL 版**逐条一致**，测试用例名都是一一对应的。但有两处
**介质决定的行为差异**：

- **屏障键会过期**（默认 7 天）。SQL 版的屏障行是永久的，Redis 里不挂 TTL
  内存会撑爆。⚠ **TTL 必须长于事务可能的最大生命周期**（含重试退避）——
  短了的话正向分支的键先过期，补偿再来会以为「正向没跑过」而空转，副作用就漏补了。
- **业务失败要由脚本自己表达**。SQL 版里业务失败是你自己的事（不提交就行）；
  这里业务跑在我们的脚本内，约定 `return 'FAILURE'` 表示拒绝。

**2. 业务 SQL 必须在 `decide` 拿到的那个事务里。**

写成这样就白做了：

```rust
// ✗ 错的：业务 SQL 用了另一个连接
if bb.decide(&mut tx).await? == Decision::Execute {
    other_pool.execute("UPDATE ...").await?;   // 不在 tx 里！
}
tx.commit().await?;
```

崩在 commit 之前，屏障记录没落但业务改了——下次重试会再执行一遍。

## 三、返回值语义（写错就会数据不一致）

**这是整个接入里最容易错的地方。**

直觉上返回值是二分：成功 / 失败。**但正确的划分是四分**：

| 你的返回 | HTTP | gRPC | dtmrs 的动作 |
|---|---|---|---|
| 成功 | 200 | `OK` | 推进下一个分支 |
| **业务明确要求回滚** | **409** | **`ABORTED`** | **逆序补偿** |
| 还在处理中 | 425 | `FAILED_PRECONDITION` | 保持现状，下轮再来 |
| **结果未知** | 5xx / 超时 | 其它任何码 | **重试，绝不回滚** |

HTTP 也可以用 200 + 响应体带 `{"dtm_result":"FAILURE"}` 表达失败。

### 关键：什么时候该返回「失败」

**只有你能确定业务规则不允许时**，才返回 409 / `ABORTED`。比如：

- ✅ 库存不足、余额不够、风控拒绝 → 409
- ❌ 数据库连接超时 → **不要**返回 409，让它超时或返回 5xx
- ❌ 调用下游超时 → **不要**返回 409
- ❌ 自己代码抛异常 → **不要**返回 409

为什么？因为**超时的时候你可能已经执行成功了**。你返回 409，TC 就去补偿；而如果那笔操作其实没执行，你就凭空退了一笔钱出去。

gRPC 侧尤其注意：`CANCELLED` 和 `DEADLINE_EXCEEDED` 是**调用方自己**放弃了，不是你拒绝了，dtmrs 一律按「未知」处理。

## 四、TC 传给你的参数

### HTTP

query 参数：

| 参数 | 例 | 说明 |
|---|---|---|
| `gid` | `order-1001` | 全局事务号 |
| `trans_type` | `saga` | saga / tcc / msg / xa |
| `branch_id` | `01` | 分支号 |
| `op` | `action` | action / compensate / try / confirm / cancel / commit / rollback |

请求方法是 `POST`，`content-type: application/json`。

### gRPC

同样四个值走 **metadata**（与 DTM 对齐）：

| metadata 键 |
|---|
| `dtm-gid` |
| `dtm-trans_type` |
| `dtm-branch_id` |
| `dtm-op` |

请求体是**空字节**（空 protobuf 消息对任何 message 类型都合法）。所以**你不需要为 dtmrs 改接口**——任何已有的 gRPC 方法都能直接当分支用。

## 五、各语言怎么接

### 远端服务（HTTP / gRPC）

各语言的屏障实现见 [`clients/`](../clients/)：**Java / Go / Python / Node 各一份单文件、零框架依赖的参考实现**，直接复制进你的项目即可。四个语言 × Postgres / MySQL 都对着真库跑过同一套五场景测试。

### 进程内分支（嵌入式）

如果你的服务本身就是 Rust / Python / Node / JVM，可以把 TC 嵌进去，分支直接是函数——没有网络、没有序列化：

```rust
let tc = Embedded::builder("sqlite:app.db")
    .handler("扣款", |ctx| async move {
        // ctx.gid / ctx.branch_id / ctx.op 直接可用
        BranchResult::Success
    })
    .start().await?;
```

Python / Node / JVM 见 [README 的绑定章节](../README.zh-CN.md#任何语言都能嵌c-abi)。

> **一个必须知道的约束**：`local://` 分支存的是**名字**（闭包没法持久化），重启后必须注册同名 handler。漏注册会按「结果未知」处理——只重试不回滚，因为这是部署问题不是业务失败。

## 六、自检清单

上线前对着过一遍：

- [ ] 分支接口幂等，且用了子事务屏障
- [ ] 屏障表和业务表在**同一个数据库实例**
- [ ] 业务 SQL 在 `decide` 那个事务里
- [ ] 只有业务规则拒绝才返回 409 / `ABORTED`；超时和异常返回 5xx 或让它超时
- [ ] `NullCompensation` / `Duplicated` 返回**成功**而不是失败
- [ ] 补偿接口自己也是幂等的（它同样会被重复调用）
- [ ] 补偿接口能处理「正向分支从没跑过」的情况（空回滚）
- [ ] 如果用 msg：提供了 `query_prepared` 回查接口，且它能准确回答「本地事务提交了没有」
- [ ] 如果用 TCC：`confirm` 能最终成功（它失败时不会触发 cancel，只会无限重试）

## 相关

- [五种模式怎么选](choosing-a-mode.md)
- [部署与运维](deployment.md)
- [排错](troubleshooting.md)
