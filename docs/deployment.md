# 部署与运维

## 起一个 TC

```bash
cargo install dtmrs
DTMRS_DB=sqlite:dtmrs.db dtmrs
```

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `DTMRS_DB` | `sqlite:dtmrs.db` | 存储 DSN，见下面「存储选型」 |
| `DTMRS_ADDR` | `0.0.0.0:36789` | HTTP 监听地址 |
| `DTMRS_GRPC_ADDR` | `0.0.0.0:36790` | gRPC 监听地址 |
| `DTMRS_OWNER` | `tc-<pid>` | 实例标识，多实例部署时用来区分租约持有者 |
| `DTMRS_BRANCH_TIMEOUT` | `10` | 调一个分支最多等几秒。**分支慢就要调大这个** |
| `DTMRS_LEASE` | `30` | 租约时长（秒） |
| `DTMRS_RETRY_INTERVAL` | `10` | 首次重试间隔（秒） |
| `DTMRS_RETRY_MAX_INTERVAL` | `300` | 退避上限（秒） |
| `DTMRS_WORKERS` | `16` | 并行推进的 worker 数，见下面「吞吐不够怎么调」 |
| `DTMRS_DB_POOL` | `32` | 存储连接池上限。**要和 `DTMRS_WORKERS` 一起调** |
| `DTMRS_INLINE_SUBMIT` | 开 | 提交后直接开推，省掉每笔一次抢占往返。`0` 关掉 |

### 内联提交（默认开）

建事务的那条写入里顺便把租约占在自己手上，写成功就直接推 ——
**省掉每笔事务一次抢占往返**。提交仍然立刻返回，不会因此变慢。

实测（两万笔，关 → 开）：

| | Redis | Postgres | sqlite | MySQL |
|---|---|---|---|---|
| saga 2 步 | 7654 → **13105** | 3396 → **4982** | 673 → **1798** | 122 → **242** |
| msg 1 步 | 12377 → **20095** | — → **5371** | — → **2350** | — |

saga 和 msg 两条路都走内联。msg 的事务体是 prepare 时建的、不在提交方手上，
所以 `submit_prepared` 会把它一起带回来 —— Redis 是脚本尾巴上加一个 `HGETALL`，
SQL 是把本来就要发的那条 SELECT 从「只取 status」改成取全行，两边都没多付往返。

两件要知道的事：

- **租约一占就是 `DTMRS_LEASE` 秒。** 进程在「写完」和「推完」之间挂掉的话，
  这笔要等租约到期才被别的实例接手，而不是下一个 tick。这跟「推进器抢到之后
  崩了」是同一种情形，不是新风险；但如果你把 `DTMRS_LEASE` 调得很大，
  这个窗口也会跟着变大。
- **`DTMRS_WORKERS` 的含义变了。** 正常路径的推进发生在提交那条链路上，
  worker 只负责重试和崩溃恢复。实测 `workers=1` 和 `workers=16` 吞吐几乎一样。

配置**非法值一律退回默认**，不会因为写错就让推进器起不来。启动日志会把生效的
配置打出来，可以确认。

### 吞吐不够怎么调

推进器是 N 个并行的抢占循环，每个循环抢一笔推一笔。**一笔事务内部仍然按序**
（SAGA 就是顺序语义），并行只发生在事务之间。

⚠ **开了内联提交（默认）之后，调 `DTMRS_WORKERS` 基本没用** —— 正常路径的
推进不走 worker。要它变大只有一种情形：积压了大量待重试的事务，需要更快地消化。

关掉内联时的实测（Postgres，20 核机器，空库）：

| worker | 连接池 | 吞吐 |
|---|---|---|
| 1 | 32 | 267 笔/秒 |
| 16 | 32 | 3196 笔/秒（默认） |
| 64 | 32 | 3184 笔/秒 |
| 64 | 64 | 3227 笔/秒 |

即使在那种情形下，16 也基本到顶了。

真要调的话记住：TC 常常和业务共用一个数据库，而 **Postgres 默认只有 100 条连接**，
多实例部署时占用是 `实例数 × DTMRS_DB_POOL`。
**sqlite 调了也没用**，它的写是全库串行的。

### ⚠ SQL 后端要自己清理历史数据

Redis 后端给终态记录挂了 7 天 TTL，**SQL 后端没有任何保留策略** ——
已完成的事务永远留着，磁盘一直涨。

存量数据原来还会拖慢吞吐（4 万笔存量时从 3424 掉到 777，**慢 4.4 倍**），
内联提交之后这条基本消失了（空库 5147 vs 存量 4 万笔 5036）——
因为抢占那条查询不再在正常路径上。但**重试和崩溃恢复走的还是抢占**，
积压多的时候仍然吃存量数据。磁盘那条则完全没变。

上线前记得配个定时清理（`finish_time` 是 unix 秒）：

```sql
-- 先删分支再删主表，顺序反了会留下孤儿行
DELETE FROM trans_branch_op WHERE gid IN (
  SELECT gid FROM trans_global
  WHERE status IN ('succeed','failed') AND finish_time < <7天前的unix秒>);
DELETE FROM trans_global
  WHERE status IN ('succeed','failed') AND finish_time < <7天前的unix秒>;
```

各存储后端的完整数字见 README 的「性能」一节。

HTTP 和 gRPC 各占一个端口，**任一个挂了整个进程退出**——不会出现「HTTP 还活着但 gRPC 已经死了」这种半可用状态。

健康检查：`GET /health` 返回 `ok`。

**管理台**：浏览器打开 `http://<DTMRS_ADDR>/` 就是。能看最近事务、展开分支明细、
对未终结的事务手动「立刻重试」或「中止」。单文件内嵌在二进制里，
没有构建步骤也不依赖 CDN —— 内网离线环境可用。

## 存储选型

TC 本身是**无状态的**，所有状态都在存储里。所以选存储 = 选可用性和持久性。

| 存储 | 适合 | 注意 |
|---|---|---|
| **sqlite** | 单机、开发、嵌入式 | **撑不住多实例写**，别用于多实例部署 |
| **Postgres** | 生产默认选它 | 需要 `max_prepared_transactions>0` 才能用 XA |
| **MySQL** | 已有 MySQL 基础设施 | 见下面「MySQL 注意事项」 |
| **Redis** | 秒杀类流量尖峰 | ⚠ **持久性弱于 SQL**，见下 |

```bash
DTMRS_DB='postgres://user:pass@host:5432/dtm'  dtmrs
DTMRS_DB='mysql://user:pass@host:3306/dtm'     dtmrs
DTMRS_DB='redis://127.0.0.1:6379/0'            dtmrs   # 需 --features redis
```

表会在启动时自动建，不需要手动跑 DDL。

### Redis 的三条语义差异

Redis 后端不是「换个 URL 就行」，它跟 SQL 后端有实打实的行为差异：

**1. 持久性弱一档，而且默认配置比你以为的差得多。**

⚠ **Redis 的默认是 `appendonly no` —— AOF 根本是关的**，只有 RDB 快照。
默认的 `save 3600 1 300 100 60 10000` 意味着：低峰期最坏能丢**一小时**的事务状态。
（`appendfsync everysec` 这个值在 AOF 关闭时完全不起作用，别看到它就以为有保障。）

| 配置 | 崩溃最多丢 |
|---|---|
| 纯内存（`--save '' --appendonly no`） | 全部 |
| RDB 默认 | **1 小时** |
| `appendonly yes` + `appendfsync everysec` | ~1 秒 |
| `appendonly yes` + `appendfsync always` | 理论上不丢 |

**丢哪一段状态，后果完全不同** —— 这决定了你该选哪一档：

| 丢掉的是 | 后果 |
|---|---|
| 事务记录，分支**还没开始执行** | 客户端收到 SUCCESS 但什么都没发生 —— 丢单，但不产生不一致 |
| 事务记录，分支**已经跑了一部分** | **那些分支永远不会被补偿或确认** → 真正的数据不一致 |
| 完成状态（分支都跑完了） | TC 重试分支 → 屏障空转掉 → **安全** |

中间那一行是唯一致命的，而它恰好是**最近写入**的那部分 —— 正是崩溃时最容易丢的。
开了内联提交之后更紧：`submit` 写完库立刻就地调分支，「写入被确认」到
「分支开始执行」之间几乎没有间隔，崩溃基本必然逮到在途事务。

所以：

- **最低要求 `appendonly yes` + `appendfsync everysec`**，别用默认
- **资金类强一致场景干脆别用 Redis 做 TC 存储** —— 即使 `appendfsync always`，
  Redis 主从复制是**异步**的，failover 一样会丢已确认的写入，这是 AOF 救不了的。
  这种场景把 TC 存储放 Postgres/MySQL，Redis 只放业务数据；
  TC 的吞吐要求通常远低于业务侧，不值得为它牺牲持久性
- **配了要验**：`INFO persistence` 看 `aof_enabled:1`，别只看配置文件

#### 那秒杀场景到底能不能用 Redis 存 TC

能，但**决定因素不是刷盘配置，是「事务的参与方在不在同一个 Redis 实例里」**。

先看丢状态在秒杀里意味着什么：库存已扣、订单没建、TC 记录丢了 ——
**那个名额永久消失**，也就是「少卖」。秒杀能容忍它，是因为少卖几件商业上
通常可接受、有对账兜底，而且**峰值窗口很短**（几秒到几分钟），
崩溃恰好落在这个窗口的概率不高。

真正的分水岭在这儿：

**所有参与方都在同一个 Redis 实例** → 崩溃时**一起丢**（AOF 是共享的）。
库存扣减没了，TC 的事务记录也没了，净效果等于「这笔从没发生过」——**是一致的**。

**事务跨越两种介质**（库存在 Redis、订单在 MySQL）→ 数据丢失会制造出
**不对称的不一致**：

| 崩溃时的进度 | 后果 |
|---|---|
| 只扣了库存（Redis），订单没建 | Redis 丢掉库存扣减 + TC 记录 → 一致，等于没发生 ✓ |
| 库存扣了（Redis），订单也建了（MySQL） | Redis 回退库存、MySQL 保留订单 → **超卖** ✗ |

**超卖比少卖严重得多** —— 少卖是商业损失，超卖是履约违约。

所以判据是：

> 一笔事务的参与方**都在同一个 Redis 实例**里 → 用 Redis 存 TC 没问题（同生共死）。
> 一旦**跨越 Redis 和数据库两种介质** → 数据丢失会把「少卖」变成「超卖」。

对应的四条做法：

1. **AOF 照样要开。** 它消除不了风险，但把窗口从 1 小时压到 1 秒，几乎免费
2. **TC 存储和业务数据放同一个实例**（不同 db 无所谓）。别为了「隔离」把它们拆到
   两个实例 —— 那恰恰破坏了同生共死这个前提
3. **对账是必需的不是可选的**，秒杀本来就该有活动后的库存对账
4. **跨介质的分支放流程靠后**，让不可逆、跨介质的操作尽量晚发生
   （同 [选型文档](choosing-a-mode.md) 里「把不可撤销的操作放到最后」那条）

**2. 终态事务会过期消失**（默认 7 天 TTL）。不然跑几千万笔之后内存就没了。SQL 后端是永久保留的。需要长期审计记录的话自己往别处归档。

**3. `/api/dtmsvr/all` 只返回最近 1000 笔**，是管理视图不是全量历史。

### MySQL 注意事项

- 自由文本列用 `VARCHAR` 而非 `TEXT`（经 `sqlx::Any` 读 MySQL 的 `TEXT` 会报类型不匹配）
- 超长的 `gid` / `payload` 会在应用层被拒绝而不是静默截断——如果你的 gid 很长（>128 字符）会提交失败，请缩短

## 多实例部署

TC 无状态，直接起多个就行，**不需要选主、不需要额外协调组件**。

防重复推进靠数据库租约：

```sql
-- 大意（真实实现见 dtmrs-store）
UPDATE trans_global SET owner=?, next_cron_time=now()+lease
WHERE status IN ('submitted','aborting') AND next_cron_time <= now()
ORDER BY next_cron_time LIMIT 1
```

抢占式更新是原子的，所以多个实例不会推同一笔。持租约的实例崩了，**租约到期后别的实例自动接手**——这就是崩溃恢复。

给每个实例配不同的 `DTMRS_OWNER`（默认按 pid 生成，容器里建议显式设成 pod 名），排查问题时能看出是谁推的。

> ⚠ **sqlite 不要用于多实例**。它撑不住并发写，会大量 `database is locked`。

### 实测数据

3 个实例、20 笔事务、业务端故意每次 sleep 制造撞车窗口：

```
各实例处理数: [(0, 9), (1, 6), (2, 5)]   ← 活分散在三个实例上
重复推进的分支: 无 ✓                      ← 每个分支正好调用 1 次
```

Postgres 和 Redis 各有一套这样的并发测试（机制不同，各证各的）。

## 该监控什么

### 必须监控

**1. 长时间停在非终态的事务。**

```sql
SELECT gid, trans_type, status, rollback_reason,
       (extract(epoch from now())::bigint - create_time) AS age_secs
FROM trans_global
WHERE status IN ('submitted','aborting','prepared')
  AND create_time < extract(epoch from now())::bigint - 3600
ORDER BY create_time;
```

超过一小时还没终结的，基本都需要人看一眼。常见原因见 [排错](troubleshooting.md)。

**2. XA 的 prepared 事务**（如果你用 XA）—— 见下一节，这条最要命。

**3. 用 Redis 时：内存和持久化配置**。`INFO persistence` 里的 `aof_enabled` / `appendfsync`。

### XA 的运维红线

**没解决的 prepared 事务会永久持锁。** 在 Postgres 上还会阻塞 VACUUM，带来事务 ID 回卷风险。

这比 SAGA「补偿没跑成」严重得多：SAGA 顶多数据不一致，**XA 能把整个库搞成不可写**。

```sql
-- Postgres：挂着的 prepared 事务，以及挂了多久
SELECT gid, prepared, age(now(), prepared) AS age FROM pg_prepared_xacts ORDER BY prepared;
```

```sql
-- MySQL
XA RECOVER;
```

告警建议：**存在超过 5 分钟未解决的 prepared 事务就报警**。

> ⚠ MySQL 上 `XA RECOVER` **不提供 prepare 时间**，所以「挂了多久」这个指标在 MySQL 上拿不到，只能靠「这个 xid 还在不在」报警。

代码里 `Xa::list_prepared` 提供了跨两种库的统一查询。

### 启动前的自检

用 XA 的话，`Xa::ensure_enabled()` 会在启动时检查：

| | 检查什么 | 不合格的后果 |
|---|---|---|
| Postgres | `max_prepared_transactions > 0` | 默认就是 **0**，XA 完全用不了 |
| MySQL | 版本 ≥ **5.7.7** | XA 能用，但 prepared 事务**重启后会丢**——等于没有持久性 |

```bash
postgres -c max_prepared_transactions=32     # MySQL 8.0 默认就行
```

**别等第一笔事务才发现**——那时可能已经有别的分支 prepare 成功了。

## 重试行为

分支返回「结果未知」时，TC 按**指数退避**重试：默认 10s → 20s → 40s → … 上限 300s，
用 `DTMRS_RETRY_INTERVAL` / `DTMRS_RETRY_MAX_INTERVAL` 调。

分支调用超时默认 **10 秒**（`DTMRS_BRANCH_TIMEOUT`）。分支耗时长的话有两个选择：
调大这个值，或者让分支立刻返回 `ONGOING`（HTTP 425）让 TC 下轮再来 —— 后者更好，
因为占着连接干等会拖慢整个推进器。

## 升级

TC 是无状态的，直接滚动重启即可：

1. 未终结的事务留在存储里
2. 新进程启动后自动捞起继续推
3. **不需要客户端重新提交，也不会重做已完成的步骤**

⚠ 如果用了嵌入式的 `local://` 分支或 workflow：**新版本必须注册同名的 handler / workflow**。漏注册会按「结果未知」处理（只重试不回滚），事务会卡住直到你补回来。

## 相关

- [五种模式怎么选](choosing-a-mode.md)
- [业务侧接入](integration.md)
- [排错](troubleshooting.md)
