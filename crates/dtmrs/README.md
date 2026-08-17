# dtmrs

Rust 写的分布式事务管理器，对标 [DTM](https://github.com/dtm-labs/dtm)（Go，10.9k★）。
**Apache-2.0**，商用闭源接入没有障碍。

支持 **SAGA / TCC / 二阶段消息 / XA / workflow** 五种模式，存储可跑
sqlite / postgres / mysql，对外有 **HTTP 和 gRPC** 两套等价接口。

```bash
cargo install dtmrs
DTMRS_DB=sqlite:dtmrs.db dtmrs
```

## 差异化：协调器可以嵌进你自己的进程

不需要单独部署服务，分支可以直接是进程内的函数 —— 没有网络、没有序列化：

```rust
use dtmrs::{BranchResult, Embedded};

let tc = Embedded::builder("sqlite:app.db")
    .handler("扣款",     |_ctx| async { BranchResult::Success })
    .handler("扣款撤销", |_ctx| async { BranchResult::Success })
    .start().await?;

tc.saga("order-1001")
    .step("local://扣款", "local://扣款撤销")
    .step("grpc://ship:9000/busi.Busi/Ship", "grpc://ship:9000/busi.Busi/Unship")
    .submit().await?;
```

Go 做不到这个形态：`c-shared` 会把整个运行时拖进宿主进程。所以 DTM 结构上必须独立部署。

通过 C ABI，**Python / Node / JVM / C++** 也能这么嵌（见仓库的 `bindings/`）。

## 按你的角色选 feature

这个 crate 是门面 —— 加一个依赖就够，不用挨个加五个。但两端要的东西完全不同：

```toml
# 跑协调器（或把它嵌进自己进程）
dtmrs = "0.7"

# 业务服务（RM）：只需要屏障做幂等，
# 别把整个协调器（axum、tonic 那一堆）拖进去
dtmrs = { version = "0.7", default-features = false, features = ["barrier"] }
```

| Feature | 给你什么 |
|---|---|
| `server`（默认） | 协调器本体、`Embedded`、`dtmrs` 二进制 |
| `grpc` | gRPC 服务端接口 + 调 `grpc://` 分支 |
| `barrier` | 子事务屏障 —— 任何业务服务接入都必需 |
| `xa` | 业务侧（RM）的 XA 助手 |
| `full` | 全都要 |

## 业务侧接入：屏障不是可选项

分支接口**一定会**被重复调用（TC 重试 + 崩溃恢复）。把屏障记录和业务 SQL 放进
**同一个本地事务**，一次解决幂等、空回滚、悬挂三个问题：

```rust
use dtmrs::barrier::{BranchBarrier, Decision};

let mut tx = pool.begin().await?;
if bb.decide(&mut tx).await? == Decision::Execute {
    // 业务 SQL —— 必须在这个 tx 里
}
tx.commit().await?;   // 原子性的来源
```

## 一条写错就会数据不一致的规矩

**超时 ≠ 失败。** 5xx、连接超时、gRPC 的 `UNAVAILABLE` / `DEADLINE_EXCEEDED`
都表示**结果未知** —— 对方可能已经成功了，这时候回滚就是不一致。
只有 HTTP 409、gRPC `ABORTED`、或响应体里的 `FAILURE` 才触发补偿。

---

完整文档（中英双语 README、设计说明、四种语言绑定、各数据库实测的坑）见
[github.com/jackwangfeng/dtmrs](https://github.com/jackwangfeng/dtmrs)。

Apache-2.0。
