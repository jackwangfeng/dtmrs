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

配置**非法值一律退回默认**，不会因为写错就让推进器起不来。启动日志会把生效的
配置打出来，可以确认。

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

**1. 持久性弱一档。** Redis 默认 `appendfsync everysec`，崩溃可能丢掉最后一秒的写入。对 TC 来说丢的是**事务状态**——可能出现「业务侧已经扣了款，但 TC 这边没有这笔事务」的悬挂。

要么接受（秒杀场景常常能接受，且有对账兜底），要么配 `appendfsync always`。**别在默认配置下跑资金类强一致场景。**

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

分支返回「结果未知」时，TC 按**指数退避**重试：10s → 20s → 40s → … 上限 300s。

这个策略目前不可配置。如果你的分支耗时很长，注意 TC 的分支调用超时是 **10 秒**——超过这个时间应该让分支立刻返回 `ONGOING`（HTTP 425），而不是让 TC 干等。

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
