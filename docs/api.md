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
| `grpc://host:port/包.服务/方法` | 远端 gRPC 服务 |
| `local://名字` | 进程内函数（仅嵌入式形态） |

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

> ⚠ **msg 不给 `query_prepared` 会被直接拒绝。** 客户端崩在 prepare 和 submit 之间时，没有回查地址就没人能决断这单——猜「已提交」会重复执行，猜「没提交」会丢单。

回查接口要回答「你那个本地事务到底提交了没有」：

| 你的回答 | TC 动作 |
|---|---|
| 成功（200） | 已提交 → 继续推正向分支 |
| `FAILURE`（409） | 没提交 → 整单作废 |
| `ONGOING`（425）/ 超时 | **不能当成「没提交」** → 退避重试 |

gRPC：`Tc.Prepare(PrepareRequest) → Empty`

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
