# API 参考

HTTP 和 gRPC 两套接口**语义完全对等**（共用同一份实现），可以混着用：gRPC 提交的事务能用 HTTP 查，反之亦然。

- HTTP：`DTMRS_ADDR`，默认 `0.0.0.0:36789`。路径与 DTM 对齐
- gRPC：`DTMRS_GRPC_ADDR`，默认 `0.0.0.0:36790`。服务 `dtmrs.v1.Tc`

## 通用响应

HTTP 的写操作统一返回：

```json
{"dtm_result": "SUCCESS"}
{"dtm_result": "FAILURE", "message": "错误说明"}
```

错误码对应关系：

| 情况 | HTTP | gRPC |
|---|---|---|
| 参数不合法 | 400 | `INVALID_ARGUMENT` |
| gid 不存在 | 404 | `NOT_FOUND` |
| 状态不允许（如已终结的事务再 abort） | **200 + `FAILURE` 体** | `FAILED_PRECONDITION` |
| 内部错误 | 500 | `INTERNAL` |

> ⚠ 「状态不允许」在 HTTP 上返回 **200** 是刻意保留的历史行为（与 DTM 兼容），别只看状态码，要看 `dtm_result`。

---

## `POST /api/dtmsvr/submit`

提交事务。saga 在这里一次性给出全部步骤；tcc / msg / xa 是把 `prepare` 建好的事务推成可执行。

```json
{
  "gid": "order-1001",
  "trans_type": "saga",
  "steps": [
    {"action": "http://pay/deduct", "compensate": "http://pay/deduct-undo"}
  ]
}
```

| 字段 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `gid` | 是 | | 全局事务号，≤128 字符。**建议直接用业务单号**——那样天然幂等 |
| `trans_type` | 否 | `saga` | `saga` / `tcc` / `msg` / `xa` |
| `steps` | saga 必填 | `[]` | 每步 `{action, compensate, payload}` |

`payload` 是**这一步自己**的请求体（扣款那步要金额、发货那步要地址）。
留空则发 `{}`。正向和补偿共用同一份——补偿需要知道当初做了什么才能撤销。

分支地址支持三种前缀，**可在同一笔事务里混用**：

| 前缀 | 说明 |
|---|---|
| `http://` / `https://` | 远端 HTTP 服务 |
| `grpc://host:port/包.服务/方法` | 远端 gRPC 服务（明文） |
| `grpcs://host:port/包.服务/方法` | 远端 gRPC 服务（TLS），见下 |
| `local://名字` | 进程内函数（仅嵌入式形态） |

gRPC 分支**不需要你的 proto**：TC 用动态转发调用，请求体发空字节（空 protobuf
消息对任何 message 类型都合法），分支身份走 metadata。所以已有的 gRPC 服务
不用改接口就能当分支。

### grpcs：TLS 的根证书从哪来

按顺序都会被信任：

1. **系统信任库**——内网自签 CA 通常装在这里，多数情况下不用额外配
2. **内置的 Mozilla 根**——scratch 容器里没装 `ca-certificates` 时的兜底
3. **`DTMRS_GRPC_CA`**——指向一个 PEM 文件，用于前两者都不认的证书

```bash
DTMRS_GRPC_CA=/etc/dtmrs/internal-ca.pem ./dtmrs
```

嵌入式形态用 `Driver::with_grpc_ca_pem(pem)`，不必设进程级环境变量。

⚠ **PEM 里没有 `BEGIN CERTIFICATE` 块时会被忽略并打警告。** 这个检查是必要的：
底层的 rustls 对认不出的证书条目是**静默跳过**的，指错文件（指到私钥、指到
被截断的文件）不会有任何报错，只会在握手时收到一个跟根因毫不相干的错误。

⚠ **证书问题一律按「结果未知」处理**——只重试，绝不回滚。证书错误看着像
「明确的拒绝」，但它跟业务无关：判成失败会因为一个部署配置问题去回滚一笔
**可能已经成功**的事务。同理，把 `grpc://`（明文）打到 TLS 端口上也只是重试。

**重复提交同一个 gid 返回成功而不是报错**——客户端网络抖动重试时，返回错误会让它以为没受理。

`trans_type=workflow` 会被拒绝：workflow 的「步骤」是代码，没法表示成 URL，只能在嵌入式形态下提交。

gRPC：`Tc.Submit(SubmitRequest) → Empty`

---

## `POST /api/dtmsvr/prepare`

第一阶段。msg 建 prepared 事务 + 正向分支；tcc / xa 只建空事务（分支随后登记）。

```json
{
  "gid": "msg-1001",
  "trans_type": "msg",
  "actions": ["http://busi/notify"],
  "query_prepared": "http://busi/query",
  "grace_secs": 10
}
```

| 字段 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `gid` | 是 | | |
| `trans_type` | 是 | | `tcc` / `msg` / `xa`。**saga 不用 prepare，直接 submit** |
| `actions` | msg 必填 | `[]` | 正向分支列表（msg 没有补偿） |
| `query_prepared` | **msg 必填** | | 回查地址，见下 |
| `grace_secs` | 否 | `10` | 回查前的宽限秒数 |
| `payloads` | 否 | `[]` | msg 每个 action 的请求体，跟 `actions` 等长（字段名同 DTM）。不给则分支收到 `{}` |
| `allow_empty_topic` | 否 | `false` | `topic://` 没有订阅者时放行而不是报错，见[按主题投递](#按主题投递topic) |

> ⚠ **msg 不给 `query_prepared` 会被直接拒绝。** 客户端崩在 prepare 和 submit 之间时，没有回查地址就没人能决断这单——猜「已提交」会重复执行，猜「没提交」会丢单。

回查接口要回答「你那个本地事务到底提交了没有」：

| 你的回答 | TC 动作 |
|---|---|
| 成功（200） | 已提交 → 继续推正向分支 |
| `FAILURE`（409） | 没提交 → 整单作废 |
| `ONGOING`（425）/ 超时 | **不能当成「没提交」** → 退避重试 |

gRPC：`Tc.Prepare(PrepareRequest) → Empty`

### 按主题投递（topic）

`actions` 里写 `topic://名字`，就不用在发送方写死下游地址：订阅方自己登记到主题上，
发送方只认主题名。协议、接口、错误文案都对齐 DTM（`msg.AddTopic` 就是拼这个前缀）。

```json
{
  "gid": "stock-7-a1b2c3",
  "trans_type": "msg",
  "actions": ["topic://stock.zero_crossing"],
  "payloads": ["{\"store_id\":3,\"sku_ids\":[11,12]}"],
  "query_prepared": "http://inventory/query",
  "allow_empty_topic": true
}
```

**语义（每条都有测试钉着，见 `crates/dtmrs-server/tests/topic.rs`）：**

| 情形 | 行为 |
|---|---|
| 展开时机 | **prepare 那一刻**展开成当时的全部订阅者，各自一个分支、收到同一份 payload。之后的订阅 / 退订不影响这条消息 |
| 分支号 | 多个订阅者是 `01-01`、`01-02`……，只有一个时是 `01`（同 DTM）。订阅方的屏障按 gid + branch_id 去重，互不冲突 |
| 一个订阅者一直失败 | **不挡别的**：同一主题的订阅者每轮并发投递、各自记成败，成功的不重发，失败的单独退避重试 |
| 订阅者返回 `FAILURE` | 一直重试（msg 没有补偿）。⚠ 跟 DTM 不同：DTM 会把这个分支标成 failed 就算完成 |
| 多个 action | action 之间仍然保序：前一个 action 的订阅者全部送达，才轮到下一个 |
| 订阅变更生效 | 下一次 prepare 立刻生效（不缓存；DTM 要等 `ConfigUpdateInterval`，默认 3 秒） |
| **后来才订阅的** | **不补发**历史消息（同 DTM）。订阅生效之前发出的消息它收不到 —— 需要的话配一个对账兜底 |
| 进程崩溃 | 分支在 prepare 时就落库了，重启后照常推，已送达的不重发 |
| **主题没有订阅者** | 默认 prepare 失败：`topic not found`（同 DTM）—— 二阶段消息的意义就是保证送达，悄悄丢掉比报错糟。`allow_empty_topic: true` 时照常受理、这一步展开成 0 个分支、提交后直接完成，打一条 WARN 并计数（嵌入式 `Embedded::empty_topic_count()` / C 的 `dtmrs_empty_topic_count`） |

> 什么时候开 `allow_empty_topic`：发送方是在**业务本地事务里** prepare 的，而这条消息只是
> 通知（晚到、漏一次都能靠对账补）。不开的话订阅方没登记就会让 prepare 失败，
> 业务本身跟着失败 —— 一个下游没到位，把上游堵死了。

#### 订阅管理

都走 **query string**（同 DTM），成功返回 `{"dtm_result":"SUCCESS"}`。

| 接口 | 参数 | 失败时的 message（同 DTM 原文） |
|---|---|---|
| `GET /api/dtmsvr/subscribe` | `topic`、`url`、`remark`（可选） | `empty topic` / `empty url` / `this url exists` |
| `GET /api/dtmsvr/unsubscribe` | `topic`、`url` | `no such a topic` / `no such an url `（末尾空格是原文） |
| `DELETE /api/dtmsvr/topic/{名字}` | | `storage: NotFound` |
| `GET /api/dtmsvr/queryKV` | `cat=topics`，`key`（可选，某个主题） | |
| `GET /api/dtmsvr/scanKV` | `cat=topics`，`position`（上一页最后的主题名）、`limit`（默认 100） | |

`queryKV` / `scanKV` 返回 DTM 的 KV 形状：`{"kv":[{"id","cat":"topics","k":主题,"v":"[{\"url\":..,\"remark\":..}]","version","create_time","update_time"}]}`，
**`v` 是 JSON 字符串**（DTM 原样如此）；`scanKV` 另带 `next_position`，为空表示到底。

跟 DTM 的状态码不同：DTM 一律 500，这里参数错 400、找不到 404 —— 都是非 2xx，
按「失败」处理的客户端不受影响。

gRPC：`Tc.Subscribe` / `Tc.Unsubscribe` / `Tc.DeleteTopic`（`TopicRequest{topic, url, remark}`），
查询走 HTTP（DTM 的 gRPC 也没有查询）。

#### 嵌入式：让订阅方自己登记

发布方进程里嵌着协调器时，用 `Embedded::serve_topic_api(地址, 共享密钥)`
（C 是 `dtmrs_serve_topic_api`）在内网端口上开放**只有上面这几个订阅接口**的 HTTP 服务，
每个请求要带 `Authorization: Bearer <密钥>`。订阅方启动时调 subscribe 把自己登记上去
（拿到 `this url exists` 说明已经在了，按成功处理）。这样发布方的代码和配置里都不出现下游地址。

单体部署（发布方和订阅方在同一个进程）用 `EmbeddedBuilder::subscribe(主题, "local://函数")`
（C 是 start 之前调 `dtmrs_topic_static`）静态登记就够了。静态订阅跟存储里的取并集、排在前面，
不能通过接口退订。

---

## `POST /api/dtmsvr/registerBranch`

登记分支。TCC 用 `confirm`/`cancel`，XA 用 `commit`/`rollback`。

```json
{
  "gid": "tcc-1001",
  "branch_id": "01",
  "try": "http://busi/try",
  "confirm": "http://busi/confirm",
  "cancel": "http://busi/cancel"
}
```

| 字段 | 必填 | 说明 |
|---|---|---|
| `gid` | 是 | |
| `branch_id` | 是 | **必须是 `01`、`02`…`99`、`100` 这个形式**，见下 |
| `confirm` / `cancel` | TCC 必填 | 缺任一个会被拒 |
| `commit` / `rollback` | XA 必填 | 缺任一个会被拒 |
| `try` | 否 | 只为可观测性存一份 |

> ⚠ **必须先登记再做一阶段。** 反过来的话，一阶段成功了但登记失败，TC 就不知道有这个分支——回滚时会漏掉它。TCC 是预留资源永久泄漏，XA 更糟：留下一个永久持锁的 prepared 事务。

### branch_id 的格式是硬性要求，不是建议

从 1 开始、**至少补零到两位**的十进制序号：`01`、`02` …… `99`、`100`、`101`，上限 `10000`。

不合规的一律返回 `FAILURE`。这个校验是后加的——因为不校验的后果全都很难查：

| 你写 | 不校验的话会发生什么 |
|---|---|
| `inventory` | 解析不出下标，TC 把整笔事务当成**空事务直接判 succeed**，confirm 一次都不会调。你拿到「事务成功」，而 try 冻结的资源永久泄漏，监控上看也完全正常 |
| `1`、`001` | 存进去是 `1`，TC 反查时找的是 `01`，状态更新静默落空，事务无限重试且日志里看不出原因 |
| `2000000000` | TC 推进时按这个下标开数组，**一次请求把 RSS 从 38 MB 顶到 3.4 GB** |

根因是 TC 推进时不用你存的那个字符串，而是拿下标重新生成分支号去反查行。
所以判据就一句：**还原不出原样的写法一律不收。**

多数情况下你不用关心这条——SAGA / msg / workflow 的分支号是 TC 自己生成的。
只有 TCC 和 XA 是你自己给。照着循环下标生成即可：

```java
String bid = String.format("%02d", i + 1);   // 01, 02, ... 99, 100
```

重复登记是幂等的。

gRPC：`Tc.RegisterBranch(RegisterBranchRequest) → Empty`

---

## `POST /api/dtmsvr/abort`

主动中止，触发逆序补偿。

```json
{"gid": "order-1001"}
```

已终结的事务返回 200 + `FAILURE` 体（gRPC 是 `FAILED_PRECONDITION`）。

**已 submit 的 tcc / xa / msg 同样拒绝**（同样的返回）：submit 意味着一阶段全成功、
方向已定，这时 abort 会造成一半 confirm 一半 cancel（msg 则是本地已提交、消息被作废）。
TCC / XA 要回滚，必须在 submit **之前** abort。saga 不受此限。

gRPC：`Tc.Abort(AbortRequest) → Empty`

---

## `POST /api/dtmsvr/retry`

立刻重试：把事务排到调度队首，并清掉退避累积。管理台的「立刻重试」按钮走的就是它。

```json
{"gid": "order-1001"}
```

**只是排到队首，不跳过任何安全检查** —— 分支该幂等还是要幂等。
已终结的事务会被拒（200 + `FAILURE` 体 / gRPC `FAILED_PRECONDITION`）。

gRPC：`Tc.Retry(RetryRequest) → Empty`

## `GET /api/dtmsvr/query?gid=<gid>`

查一笔事务的完整状态。

```json
{
  "gid": "order-1001",
  "trans_type": "saga",
  "status": "failed",
  "rollback_reason": "分支 02 返回 FAILURE",
  "create_time": 1786400000,
  "finish_time": 1786400012,
  "branches": [
    {"branch_id": "01", "op": "action",     "url": "http://pay/deduct", "status": "succeed"},
    {"branch_id": "01", "op": "compensate", "url": "http://pay/undo",   "status": "succeed"}
  ]
}
```

| 全局 `status` | 含义 |
|---|---|
| `prepared` | 仅 msg / tcc / xa 的第一阶段，还没决定执行 |
| `submitted` | 正在推正向分支 |
| `aborting` | 正在逆序补偿 |
| `succeed` / `failed` | 终态，不再调度 |

分支 `status`：`prepared`（还没成功）/ `succeed` / `failed`。

`finish_time` 只有终态才有。gid 不存在返回 404。

gRPC：`Tc.Query(QueryRequest) → TransView`

---

## `GET /api/dtmsvr/all`

最近的事务列表（最多 100 条，不含分支明细）。管理用。

> ⚠ Redis 后端下只保留最近 1000 笔，不是全量历史。

---

## `GET /api/dtmsvr/newGid`

生成一个事务号：`{"gid": "1786400000-0"}`

生产上**更建议直接用业务单号当 gid**——那样天然幂等，客户端重试不会变成两笔。

gRPC：`Tc.NewGid(NewGidRequest) → NewGidReply`

---

## `GET /` 和 `/console`

管理台页面。看最近事务、展开分支明细、手动重试/中止。

## `GET /health`

返回 `ok`。给负载均衡和探针用。

---

## gRPC proto

完整定义见 [`crates/dtmrs-server/proto/dtmrs.proto`](../crates/dtmrs-server/proto/dtmrs.proto)。

```protobuf
service Tc {
  rpc NewGid(NewGidRequest) returns (NewGidReply);
  rpc Prepare(PrepareRequest) returns (Empty);
  rpc RegisterBranch(RegisterBranchRequest) returns (Empty);
  rpc Submit(SubmitRequest) returns (Empty);
  rpc Abort(AbortRequest) returns (Empty);
  rpc Retry(RetryRequest) returns (Empty);
  rpc Query(QueryRequest) returns (TransView);
}
```

## Rust API

嵌入式用法和库 API 见 [docs.rs/dtmrs](https://docs.rs/dtmrs)。

---

## 从 DTM 迁过来要注意的三处差异

路径、字段、`dtm_result` 的语义都跟 DTM 对齐，直接换个地址通常就能跑。
但下面三处**行为**不一样，都跟 `branch_id` 有关，而且都是实测比对过真 DTM 的。

差异的根源是一处设计分歧：**DTM 取分支时按 `ORDER BY id asc`（自增主键，
也就是登记顺序），全程不解析 `branch_id`；dtmrs 把 `branch_id` 解析成整数下标，
用它索引数组。** 对 DTM 来说 branch_id 只是个标识串，对 dtmrs 来说它是承重的。

### ① 执行顺序：DTM 按登记顺序，dtmrs 按分支号数值序

SAGA / msg / workflow 不受影响——分支号由 TC 自己按步序生成，两者必然一致。

**只有 TCC 和 XA 会踩到**，因为分支号是你自己给的。如果你先登记 `02` 再登记 `01`：

| | 执行顺序 |
|---|---|
| DTM | `02` → `01`（你登记的顺序） |
| dtmrs | `01` → `02`（分支号的数值顺序） |

正常写法下两者一致（循环里递增生成分支号）。会出问题的是那种「按业务条件
决定登记哪些分支」而分支号又不连续的写法——迁过来之前确认一下顺序。

### ② 分支数上限：DTM 卡在 99，dtmrs 是 10000

DTM 的客户端 SDK 直接 panic：

```go
func (g *BranchIDGen) NewSubBranchID() string {
	if g.subBranchID >= 99 { panic(fmt.Errorf("branch id is larger than 99")) }
```

dtmrs 没这个限制，分支号超过 99 就变成三位（`100`、`101`…），上限 `10000`。
SAGA 另有 payload 的 8192 字符上限兜着，实测短 URL 能装进 101 步。

所以：**DTM → dtmrs 不会有问题，反向迁移要留意**。

### ③ 重复登记：DTM 一律报错，dtmrs 只拒绝真冲突

DTM 靠 `UNIQUE KEY (gid, branch_id, op)` + 直接 INSERT，撞了就把数据库错误
原样抛给你。实测——**即使两次请求完全一样**：

```
登记 01 (kucun)  → {"dtm_result":"SUCCESS"}
再登记 01 (kucun) → {"message":"Error 1062 (23000): Duplicate entry ..."}
```

也就是说 DTM 的 registerBranch **不是幂等的**，网络抖动重发会拿到错误。

dtmrs 把两种长得一样、结论相反的情况分开了：

| 第二次登记 | dtmrs | DTM |
|---|---|---|
| 分支号和 URL **都一样**（客户端重试） | ✅ 放行 | ❌ `Error 1062` |
| 分支号一样但 URL **不同**（两个分支撞号） | ❌ `Conflict` | ❌ `Error 1062` |

第二行必须拒绝——放行的话第二个分支的 URL 根本存不进去，而客户端会以为
登记成功并去调它的 try 把资源冻结上，TC 却不知道有这个分支，
confirm / cancel 都不会调，**那份资源永久泄漏**。

### 顺带：dtmrs 对 branch_id 的格式校验比 DTM 严

DTM 服务端不校验 `branch_id`（因为它不解析，写什么都无害）。
dtmrs 必须校验——格式不对会导致很难查的故障，详见上面
[branch_id 的格式是硬性要求](#branch_id-的格式是硬性要求不是建议)。

对用官方 SDK 的人没影响（DTM 的分支号本来就由 SDK 生成成 `01`、`02`…）；
**手写 HTTP 调用的要注意**，dtmrs 会明确拒绝而不是默默收下。
