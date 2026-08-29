# dtmrs

[English](README.md) | **简体中文**

[![CI](https://github.com/jackwangfeng/dtmrs/actions/workflows/ci.yml/badge.svg)](https://github.com/jackwangfeng/dtmrs/actions/workflows/ci.yml) [![crates.io](https://img.shields.io/crates/v/dtmrs.svg?logo=rust)](https://crates.io/crates/dtmrs) [![docs.rs](https://img.shields.io/docsrs/dtmrs?logo=rust)](https://docs.rs/dtmrs) [![Stars](https://img.shields.io/github/stars/jackwangfeng/dtmrs?style=flat&logo=github)](https://github.com/jackwangfeng/dtmrs/stargazers) [![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Rust 写的分布式事务管理器 —— SAGA、TCC、二阶段消息、XA、workflow 五种模式，
存储可跑 sqlite / Postgres / MySQL / Redis，HTTP 和 gRPC 两套等价接口。
协议对齐 [DTM](https://github.com/dtm-labs/dtm)，另外有一件 DTM 做不到的事：
**协调器可以当成一个库链进你自己的进程**。

Apache-2.0，商用闭源都没有障碍。

## 特性

* **五种事务模式**：SAGA、TCC、二阶段消息、XA、workflow
* **四种存储**：sqlite（开发）、Postgres / MySQL（生产）、Redis（应对流量尖峰）
* **两套等价接口**：HTTP 与 gRPC —— 同一个请求得到同一个结论，有测试钉着
* **可嵌入**：TC 当库链进宿主进程，分支可以是进程内函数
* **其它语言经 C ABI 接入**：Python、Node、Java、C（Rust 是原生的）
* **子事务屏障**解决幂等 / 空回滚 / 悬挂，五种语言都有
* 多实例靠数据库租约、崩溃恢复、指数退避重试、管理台
* 不做 AT 模式，这是刻意的，见[从 Seata 过来的](docs/choosing-a-mode.md#从-seata-过来的at-模式对应哪个)

## 跟 DTM 最大的不同：可嵌入的协调器

```text
DTM：    你的服务 ──HTTP──► 单独部署的 TC 进程 ──► 数据库
                            （要运维、要保活、要监控）
dtmrs： 你的服务（TC 就在里面）──► 数据库
```

分支也不必是 HTTP URL，可以就是进程内的函数 —— 不走网络，不做序列化：

```rust
let tc = Embedded::builder("sqlite:app.db")
    .handler("deduct",      |ctx| async move { BranchResult::Success })
    .handler("deduct_undo", |ctx| async move { BranchResult::Success })
    .start().await?;

tc.saga("order-1001")
    .step("local://deduct", "local://deduct_undo")
    .step("http://shipment/create", "http://shipment/cancel")   // 可以和远程调用混用
    .submit().await?;
```

**Go 在结构上做不到这件事**：`c-shared` 会把整个运行时（调度器、GC、信号处理）
拖进宿主进程，跟宿主自己的线程和信号模型打架。所以 DTM 只能独立部署。

嵌入不牺牲持久性 —— 状态照样落同一个数据库，进程重启后未完成的事务会被接着推完。
见[嵌入式示例](crates/dtmrs-server/examples/embedded.rs)。

## 快速开始

### 跑协调器

```bash
cargo install dtmrs
DTMRS_DB=sqlite:dtmrs.db dtmrs        # HTTP :36789，gRPC :36790，管理台在 /
```

### 跑一个 SAGA

一个跨行转账的例子：转出（`TransOut`）和转入（`TransIn`）。
dtmrs 保证两者要么都成功，要么都回滚。

```bash
curl -X POST localhost:36789/api/dtmsvr/submit \
  -H 'content-type: application/json' -d '{
  "gid": "transfer-1001",
  "steps": [
    {"action": "http://localhost:8081/TransOut", "compensate": "http://localhost:8081/TransOutCom"},
    {"action": "http://localhost:8081/TransIn",  "compensate": "http://localhost:8081/TransInCom"}
  ]}'
```

`TransOut`、`TransIn` 依次被调用，整个事务完成。

### 某个分支失败时

让 `TransIn` 返回 **409** —— 这是唯一表示「业务明确拒绝」的状态码。
补偿随即逆序执行：

```
[TransOut]     gid=transfer-1001 branch=01
[TransIn]      gid=transfer-1001 branch=02 → 409，要求回滚
[TransInCom]   ← 补偿
[TransOutCom]  ← 补偿
结果: Failed
```

⚠ **超时不等于失败。** 5xx、连接超时、gRPC `UNAVAILABLE` 全都是「结果未知」——
dtmrs 只重试、绝不回滚，因为对方可能其实已经成功了。只有 409 / gRPC `ABORTED` /
响应体含 `FAILURE` 才触发补偿。**这一条搞反了，是这个领域里代价最大的错误。**

能直接跑的例子：[`examples/java`](examples/java)（三个微服务的测试套件）、
[`cargo run --example embedded`](crates/dtmrs-server/examples/embedded.rs)、
[`--example workflow`](crates/dtmrs-server/examples/workflow.rs)。

## 业务侧接入：必须用屏障

分支**一定**会被重复调用，这是设计而不是缺陷。所以每个分支都得扛住三件事：
重复请求、空回滚（补偿到了但正向根本没执行过）、悬挂（补偿先执行了，
迷路的正向后到）。

屏障把这三件事一起解决 —— 判定记录写进**你自己的业务事务**里：

```rust
// gid / branch_id / op / trans_type 由 TC 传进来
let mut bb = BranchBarrier::new(be, trans_type, gid, branch_id, op)?;
let mut tx = pool.begin().await?;

if bb.decide(&mut tx).await? == Decision::Execute {
    deduct_stock(&mut tx).await?;   // 你的业务 SQL —— 必须在这个 tx 里
}

tx.commit().await?;   // 原子性的来源：屏障记录与业务变更同生共死
```

[Rust](crates/dtmrs-barrier) 和 [Go、Java、Node、Python](clients/) 都有。
完整说明见[业务侧接入](docs/integration.md)。

## 文档

README 只讲**这是什么**，**[docs/](docs/) 讲怎么用**：

| 文档 | |
|---|---|
| [快速上手](docs/quickstart.md) | 五分钟跑通第一笔 |
| [五种模式怎么选](docs/choosing-a-mode.md) | **选型** |
| [业务侧接入](docs/integration.md) | **接入指南**、屏障、TCC 的顺序 |
| [部署与运维](docs/deployment.md) | 生产部署、多实例、认证、监控 |
| [API 参考](docs/api.md) | HTTP + gRPC，含从 DTM 迁移的差异 |
| [性能实测](docs/benchmarks.md) | 数字、方法论，以及压测抓出的 4 个真 bug |
| [排错](docs/troubleshooting.md) | 排错 |
| [DESIGN.md](DESIGN.md) | 状态机与设计取舍 |

## 性能

端到端（提交 → TC 依次调两个分支 → 落终态），两万笔两步 SAGA，
12700 / 20 核上取三次中位数：

| 存储 | 吞吐 |
|---|---|
| Redis | 13105 笔/秒 |
| Postgres | 4982 笔/秒 |
| sqlite（WAL） | 1798 笔/秒 |
| MySQL | 242 笔/秒 |

MySQL 慢是它自己的默认配置导致的（每次提交两次 fsync），不是 dtmrs 的开销。
完整数字、跟 DTM 的对照，以及那些**会让压测结论变成假话的方法论陷阱**：
**[docs/benchmarks.md](docs/benchmarks.md)**。

## 安装

```toml
# 跑协调器（或者把它嵌进自己进程）
dtmrs = "0.8"

# 业务服务（RM）：只需要屏障做幂等。
# 别把整个协调器（axum、tonic 那一堆）拖进去
dtmrs = { version = "0.8", default-features = false, features = ["barrier"] }
```

编 gRPC 需要 **protoc**；关掉 `grpc` feature 就不需要。

从源码跑：

```bash
cargo build --release            # 二进制在 target/release/dtmrs
cargo test --workspace           # 204 个测试
```

⚠ 真数据库的测试全部靠环境变量开启，**没配就是没跑，不等于通过**。
见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 结构

```
crates/
  dtmrs/          门面 crate + dtmrs 二进制
  dtmrs-core/     状态机 + SQL 方言渲染，纯逻辑无 I/O
  dtmrs-store/    存储与租约抢占（SQL 走 sqlx::Any，或 Redis）
  dtmrs-server/   TC：api / http / grpc / driver / registry / workflow / embedded
  dtmrs-barrier/  子事务屏障（SQL 版和 Redis 版）
  dtmrs-xa/       业务方（RM）的 XA 助手，pg / mysql 两套语法
  dtmrs-ffi/      C ABI
clients/          Go、Java、Node、Python 的屏障库
```

## 协议出处

dtmrs 实现的是 DTM 的**协议** —— 路径、字段名、`dtm_result` 的语义。
没有抄它的代码；每条行为都对着 DTM 源码或跑起来的 DTM 实例核对过，
两边刻意不一致的地方写在 [docs/api.md](docs/api.md) 里。

## 许可证

Apache-2.0，见 [LICENSE](LICENSE)。
