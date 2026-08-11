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
| `steps` | saga 必填 | `[]` | 每步 `{action, compensate}` |

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
| `gid` / `branch_id` | 是 | branch_id 建议用 `01`、`02`… |
| `confirm` / `cancel` | TCC 必填 | 缺任一个会被拒 |
| `commit` / `rollback` | XA 必填 | 缺任一个会被拒 |
| `try` | 否 | 只为可观测性存一份 |

> ⚠ **必须先登记再做一阶段。** 反过来的话，一阶段成功了但登记失败，TC 就不知道有这个分支——回滚时会漏掉它。TCC 是预留资源永久泄漏，XA 更糟：留下一个永久持锁的 prepared 事务。

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
  rpc Query(QueryRequest) returns (TransView);
}
```

## Rust API

嵌入式用法和库 API 见 [docs.rs/dtmrs](https://docs.rs/dtmrs)。
