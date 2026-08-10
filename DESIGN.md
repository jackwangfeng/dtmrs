# dtmrs —— Rust 版分布式事务管理器

对标 [dtm-labs/dtm](https://github.com/dtm-labs/dtm)（Go，10.9k★，BSD-3）。
DTM 的价值不在算法难度，在**协议齐全 + 跨语言 + 单体部署**。这三点 Rust 都能做得更好：
单个静态二进制、没有 GC 停顿、内存占用低一个量级。

本文是**协议规格**，不是设想 —— 状态机和屏障算法是从 DTM 源码逐条核对出来的，
见文末「出处」。

## 一、核心概念

| 角色 | 职责 |
|---|---|
| **TM**（事务管理器，客户端库） | 定义全局事务边界，把步骤提交给 TC |
| **TC**（事务协调器，就是本服务） | 持久化全局/分支状态，驱动正向提交与反向补偿，超时重试 |
| **RM**（资源管理器，业务服务） | 提供 action/compensate 或 try/confirm/cancel 接口，用屏障保证幂等 |

TC 是**无状态的**（状态全在 DB），可多实例，靠 DB 行锁 + `owner` 租约抢活。

## 二、状态机

### 全局事务 `trans_global.status`

```
                    ┌──────────────► succeed
                    │ 所有分支 succeed
 prepared ──submit──► submitted
    │                   │ 某分支返回 FAILURE（业务明确要求回滚）
    │ abort             ▼
    └──────────────► aborting ──所有补偿完成──► failed
```

- **prepared**：仅二阶段消息（msg）用。TC 已收到事务但尚未确认该不该执行，
  靠 `query_prepared` 回调问业务方"这单到底成不成"
- **submitted**：可以推进了。TC 按序执行分支
- **aborting**：需要回滚。TC 逆序执行补偿
- **succeed / failed**：终态，不再调度

关键区分（DTM 的核心设计，照抄）：

| 分支返回 | 含义 | TC 动作 |
|---|---|---|
| HTTP 200 / `SUCCESS` | 成功 | 推进下一个分支 |
| HTTP 409 / `FAILURE` | **业务明确失败，别再试了** | 转 aborting，逆序补偿 |
| HTTP 425 / `ONGOING` | 还在处理，别当失败 | 保持现状，下轮 cron 再来 |
| 其它错误 / 超时 | 不确定 | **重试**（指数退避），不回滚 |

「超时不等于失败」这条是分布式事务的命门 —— 网络超时时对方可能已经成功了，
贸然补偿会造成不一致。所以只有业务显式说 FAILURE 才回滚。

### 分支 `trans_branch_op.status`

`prepared → succeed | failed`，唯一键 `(gid, branch_id, op)` 保证不重复登记。

## 三、五种模式

### SAGA（最常用，MVP 先做）
提交时一次性给出所有 `(action, compensate)` 对。TC 顺序调 action；
任一 action 返回 FAILURE 就逆序调**已执行过的**分支的 compensate。

### TCC
两阶段：业务先调 `try` 占资源（TC 端 registerBranch 登记 confirm/cancel），
全部 try 成功后 TC 调 confirm，否则调 cancel。比 SAGA 多一次网络往返，
但中间态对外不可见。

### 二阶段消息（msg）
最轻量：本地事务 + 消息表。`prepare` 落库 → 本地事务提交 → `submit`。
若进程在两者之间崩了，TC 靠 `query_prepared` 回查业务方决定提交还是丢弃。
**取代了 MQ 事务消息**，不需要 RocketMQ。

### XA
依赖数据库原生 XA（`XA START/END/PREPARE/COMMIT`）。强一致但全程持锁，
放最后做。

### workflow
前四种都要求**提交时声明好步骤**。workflow 让业务把流程写成一个普通函数，
崩溃后**从头重放**该函数：已成功的分支不重跑，直接还回上次存的返回值，
于是函数沿原路走到断点再继续。

代价是函数必须**确定性**（相同分支返回值 → 相同路径）。写岔了会被分岔检测
当场拦下（对比每个位置记录的分支名），既不落成功也不回滚 —— 那时已经不知道
真实进度，硬回滚更危险。

「步骤」是代码而不是 URL，所以这个模式只在嵌入式形态下提供。

## 四、子事务屏障（最值钱的部分）

一张表 + `insert ignore`，同时解决三个经典难题：

| 难题 | 场景 |
|---|---|
| **幂等** | TC 重试导致同一分支被调两次 |
| **空回滚** | action 还没执行（网络丢包）就来了 compensate，补偿必须空转 |
| **悬挂** | compensate 先到、action 后到，晚到的 action 必须被丢弃 |

表结构（唯一键是全部机关所在）：

```sql
create table barrier(
  id bigint primary key auto_increment,
  trans_type varchar(45), gid varchar(128), branch_id varchar(128),
  op varchar(45), barrier_id varchar(45),
  reason varchar(45),                       -- 是哪个分支插的这行
  create_time datetime, update_time datetime,
  unique key(gid, branch_id, op, barrier_id)
);
```

算法（逐行对照 DTM 的 `BranchBarrier.Call`）：

```
在业务自己的数据库事务里执行：
  bid = 本次调用序号（"01","02",…）
  originOp = { cancel→try, compensate→action, rollback→action }[当前op]

  originAffected  = INSERT IGNORE barrier(gid, branch_id, originOp,   bid, reason=当前op)
  currentAffected = INSERT IGNORE barrier(gid, branch_id, 当前op,      bid, reason=当前op)

  若 当前op 是补偿类 且 originAffected > 0：
      → 空回滚：正向分支从没跑过（否则那行早被它自己占了），补偿直接空转返回
  若 currentAffected == 0：
      → 重复请求或悬挂：这个 (gid,branch,op,bid) 已经处理过，跳过
  否则：
      → 真正执行业务逻辑 busiCall(tx)

  business SQL 与 barrier 插入在同一个本地事务里提交 —— 这是原子性的来源
```

**为什么能work**：补偿方先插一行"假装自己是正向分支"的记录。
如果插进去了（affected>0），说明正向分支从来没来过 → 空回滚。
如果没插进去，说明正向分支已经占了那个位置 → 是真补偿，该执行。
悬挂的正向请求到达时，发现自己那行已被补偿方占了 → currentAffected=0 → 丢弃。

**关键前提**：barrier 表必须和业务表在同一个数据库实例，才能共用一个本地事务。

## 五、可靠性设计

### 重试与租约
`trans_global` 上两个字段撑起整个调度：

```
next_cron_time      下次该处理的时间
next_cron_interval  当前退避间隔（指数增长，有上限）
owner               哪个 TC 实例正在处理（租约，防并发重复推进）
```

cron 轮询走 `key(status, next_cron_time)` 索引：

```sql
UPDATE trans_global SET owner=?, next_cron_time=now()+interval
WHERE status IN ('submitted','aborting') AND next_cron_time < now()
ORDER BY next_cron_time LIMIT 1     -- 抢到即持有租约
```

抢占式更新是**原子的**，所以多个 TC 实例不会重复推进同一个事务。
如果持有租约的实例崩了，租约到期后别的实例接手 —— 这就是崩溃恢复。

### 崩溃恢复的正确性
TC 崩溃后重启，所有未终结事务会被 cron 重新捞起继续推进。
因为分支调用**必须幂等**（靠屏障），重复推进是安全的。

## 六、MVP 范围（第一版做什么）

| 项 | 状态 |
|---|---|
| SAGA 模式（正向 + 逆序补偿） | ✅ 第一版 |
| 子事务屏障客户端库 | ✅ 第一版 |
| HTTP API（DTM 兼容路径） | ✅ 第一版 |
| SQLite 存储（零依赖起步） | ✅ 第一版 |
| cron 重试 + owner 租约 | ✅ 第一版 |
| 崩溃恢复集成测试 | ✅ 第一版 |
| Postgres 存储 | ✅ 第二版 |
| TCC | ✅ 第二版 |
| 二阶段消息 + query_prepared | ✅ 第二版 |
| 嵌入式 TC + C ABI / Python 绑定 | ✅ 第二版（DTM 没有的形态） |
| XA（Postgres） | ✅ 第三版 |
| **MySQL 存储 + MySQL XA** | ✅ 第三版 |
| **gRPC（分支调用 + TC 服务端 API）** | ✅ 第四版 |
| **Node / JVM 绑定** | ✅ 第四版 |
| **workflow 模式（重放 + 结果记忆化）** | ✅ 第五版 |

## 七、工程结构

```
dtmrs/
  crates/
    dtmrs-core/      类型 + 状态机（纯逻辑，无 I/O，好测）+ SQL 方言层
    dtmrs-store/     存储层（sqlx::Any + 方言渲染，一套 SQL 跑
                     sqlite / postgres / mysql）。**没有抽 Store trait** ——
                     三种库的差异小到一层模板就能吸收，抽 trait 是过早抽象
    dtmrs-server/    api.rs（协议无关的操作层，HTTP 与 gRPC 共用）
                     axum HTTP + tonic gRPC + cron 调度器 + 嵌入式门面
    dtmrs-barrier/   客户端子事务屏障库
    dtmrs-xa/        业务方 XA 助手（pg / mysql 两套语法）
    dtmrs-ffi/       C ABI（cdylib + staticlib），回调式 + 拉取式两种分发
  tests/             端到端：正常提交 / 分支失败补偿 / 崩溃恢复 / 幂等
```

`dtmrs-core` 不碰 I/O，状态机可以纯单元测试 —— 分布式事务的 bug 大多在状态迁移上，
把它隔离出来是值得的。

## 八、许可证

**Apache-2.0**。DTM 是 BSD-3，协议兼容；我们不抄代码，只实现同一套协议
（协议本身不受版权保护）。这样商用闭源接入没有障碍 —— 这正是 restate(BUSL)
和 obelisk(AGPL) 做不到的事。

## 出处（都是源码核对，不是二手资料）

- 状态机与字段：`sqls/dtmsvr.storage.mysql.sql`
- 屏障表：`sqls/dtmcli.barrier.mysql.sql`
- 屏障算法：`client/dtmcli/barrier.go` 的 `BranchBarrier.Call`
- 模式实现：`dtmsvr/trans_type_{saga,tcc,msg,xa}.go`
