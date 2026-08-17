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

## 五、TCC 的接入顺序（写错会永久泄漏资源）

前面几节讲的是「你作为分支怎么写」。TCC 多一件事：**发起方的调用顺序**，
而且这个顺序写反了会静默泄漏资源。

### 顺序只有一种是对的

```
prepare(gid)
  ├─ registerBranch(分支1)  →  调分支1的 try
  ├─ registerBranch(分支2)  →  调分支2的 try
  └─ registerBranch(分支3)  →  调分支3的 try
submit(gid)        ← 只有全部 try 明确成功才走到这
```

**先登记，再 try。** 不是「try 成功了再登记」——那个直觉很自然，但是错的。

### 为什么，两种失败的代价不对称

| 崩在中间时 | 后果 |
|---|---|
| **先登记再 try**：登记完、try 前崩了 | TC 发来 cancel，分支发现正向没跑过 → **空回滚，什么都不做** |
| **先 try 再登记**：try 成功、登记前崩了 | **TC 眼里这笔事务一个分支都没有** —— 既不 confirm 也不 cancel，那份冻结的资源永久泄漏 |

也就是说：

> **登记多了无害**（空回滚兜住），**登记漏了致命**（资源永久泄漏，且无法补救 ——
> TC 连那个分支的地址都没有）。

顺序就由这个不对称决定。它跟「回滚时补偿所有分支，包括没成功的那些」是同一条
原则的两种表现：**在「不知道对方状态」时，永远选那个多做一次但无害的动作。**

「try 成功了再登记」这个直觉隐含了一个在分布式环境里不成立的假设 ——
**你能知道 try 成没成功**。try 超时的时候你不知道，而对方可能真的冻结了。

> ⚠ XA 上同一条规则，代价高一档：先 `PREPARE TRANSACTION` 再登记的话，
> 崩在中间会留下**永久持锁的 prepared 事务**，在 Postgres 上还阻塞 VACUUM，
> 能把整个库搞成不可写。

**这不是 dtmrs 自己的约定，是 TCC 的通行做法。** DTM 的 `Tcc.CallBranch` 就是
先 `registerBranch`（注册失败直接返回，不执行 Try）再调 Try；Seata 的
`TCCServiceProxy` 也是在 `Prepare` 内部先 `registeBranch`。

顺带一个实现细节：**一次 registerBranch 在 TC 侧会落两条记录**（Confirm 和
Cancel 各一条，状态 `prepared`），而 **Try 本身不落库** —— 它由客户端直调业务服务。
所以登记时传的 `try` 地址只是存一份备查，TC 不会去调它。

### 什么时候 submit，什么时候 abort

> **只有全部 try 明确成功才 submit。其余一切情况都 abort。**

「其余一切」有三种，别只想到第一种：

| try 的结果 | 怎么办 |
|---|---|
| 明确失败（库存不足、风控拒绝） | **abort** |
| **超时 / 连不上 / 5xx** | **abort** |
| 成功了但你的进程随后崩了 | 靠监控发现（见下） |

第二行最容易写错。超时之后**不能重试 try 然后继续 submit** —— 你不知道那个分支
到底冻结成功没有，而 submit 意味着「决议提交、TC 会一直 confirm 到成功」。
如果那个分支根本没冻结，confirm 会永远失败，你就卡在无限重试加告警里
（`confirm` 失败**绝不会**转成 `cancel`，这是铁律）。

abort 反而总是安全的：冻结了就释放，没冻结就被屏障空转掉。

```java
tc.prepareTcc(gid);
try {
    for (每个分支) {
        tc.registerTccBranch(gid, id, tryUrl, confirm, cancel);  // 先登记
        callTry(...);          // 再 try，任何异常都往外抛
    }
    tc.submitTcc(gid);         // 只有走到这儿才提交
} catch (Exception e) {
    tc.abort(gid);             // 明确失败和超时都走这里
    throw e;
}
```

`abort` 可以在**任何时刻**调用，哪怕后面几个分支还没登记 ——
TC 只 cancel 它知道的那些，没登记的本来也没执行过。

### ⚠ TCC 的 prepared 没有任何自动回收

`catch` 挡不住进程被 kill。而 **TCC 停在 `prepared` 的事务不会被调度器捞起来** ——
调度条件里只有 `submitted` / `aborting`，外加「`prepared` 且是 msg」
（msg 有回查地址，TC 能主动问业务方；TCC 没有这个概念，TC 不知道你的 try
做了什么，也就无从判断该 confirm 还是 cancel）。

实测：登记两个分支、try 了一个，然后不 submit 也不 abort ——
**12 秒后仍是 prepared，冻结的库存一直占着**；手工 abort 之后才释放。

所以需要第二道防线，定时扫或者在管理台上处理：

```sql
SELECT gid FROM trans_global
WHERE trans_type = 'tcc' AND status = 'prepared'
  AND create_time < extract(epoch from now())::bigint - 600;
```

十分钟还停在 prepared 的，基本可以判定发起方已经不在了，直接 abort。

## 六、各语言怎么接

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

## 七、自检清单

上线前对着过一遍：

- [ ] 分支接口幂等，且用了子事务屏障
- [ ] 屏障表和业务表在**同一个数据库实例**
- [ ] 业务 SQL 在 `decide` 那个事务里
- [ ] 只有业务规则拒绝才返回 409 / `ABORTED`；超时和异常返回 5xx 或让它超时
- [ ] `NullCompensation` / `Duplicated` 返回**成功**而不是失败
- [ ] 补偿接口自己也是幂等的（它同样会被重复调用）
- [ ] 补偿接口能处理「正向分支从没跑过」的情况（空回滚）
- [ ] 如果用 msg：提供了 `query_prepared` 回查接口，且它能准确回答「本地事务提交了没有」
- [ ] 如果用 TCC：**先 registerBranch 再调 try**（顺序反了会永久泄漏资源，见第五节）
- [ ] 如果用 TCC：try 失败**或超时**都要 abort，只有全部明确成功才 submit
- [ ] 如果用 TCC：`confirm` 能最终成功（它失败时不会触发 cancel，只会无限重试）
- [ ] 如果用 TCC：监控停在 `prepared` 太久的事务 —— **没有任何自动回收机制**

## 相关

- [五种模式怎么选](choosing-a-mode.md)
- [部署与运维](deployment.md)
- [排错](troubleshooting.md)

## 可运行的例子

`examples/java/` 有一套完整的三分支演示，**零依赖、JDK 17+ 直接跑**：

```bash
DTMRS_URL=http://127.0.0.1:36789 examples/java/run.sh
```

四个场景各自**断言最终状态数值**（不是只看接口返回码）：saga 全成功、
业务拒绝触发逆序补偿、超时只重试不回滚、TCC 的登记顺序。
里面还有「怎么接进 Spring Boot」的样例代码 —— 发起方和分支方各一段。

用其它语言的话，`clients/` 下四个语言的子事务屏障实现都可以直接用。
