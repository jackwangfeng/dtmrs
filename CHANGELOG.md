# Changelog

本项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

This project follows [Semantic Versioning](https://semver.org/).

---

## 0.2.0

### 破坏性变更 / Breaking

- **`Store::pool()` 和 `Store::backend()` 现在返回 `Option`。**
  加了 Redis 后端之后，「连接池」和「SQL 方言」对它都不存在。
  *`Store::pool()` and `Store::backend()` now return `Option` — neither exists for
  the Redis backend.*

- **`dtmrs` 二进制从 `dtmrs-server` 挪到了新的 `dtmrs` crate**，`dtmrs-server`
  变成纯库。`cargo install dtmrs-server` 改成 `cargo install dtmrs`。
  *The `dtmrs` binary moved from `dtmrs-server` to the new `dtmrs` facade crate.*

- **`dtmrs-ffi` 不再产出 `rlib`**（只有 `cdylib` + `staticlib`）。
  它的 lib 名是 `dtmrs`，留着 rlib 会跟门面 crate 撞输出文件名。
  C 用户不受影响：`libdtmrs.so` / `libdtmrs.a` 照旧。
  *`dtmrs-ffi` no longer produces an `rlib`; the `.so`/`.a` are unchanged.*

### 新增 / Added

- **`dtmrs` 门面 crate**：一个依赖顶原来五个，按角色切 feature
  （`server` / `grpc` / `barrier` / `xa` / `redis` / `full`）。
  业务侧只装 `barrier` 时依赖树里没有 axum / tonic / reqwest。
  *New `dtmrs` facade crate with role-based features.*

- **Redis 存储后端**（`redis` feature，默认关）。为秒杀那类流量尖峰而生。
  ⚠ 三条跟 SQL 后端不同的语义，用前务必读 README：持久性弱一档、
  终态事务会按 TTL 过期、`list_recent` 只留最近 1000 笔。
  *Redis storage backend. **Weaker durability than SQL** — read the README first.*

- **workflow 模式**：把事务流程写成普通函数，崩溃后靠重放 + 结果记忆化
  从断点续跑。带重放分岔检测（函数不确定时当场停下，而不是静默补偿错对象）。
  *workflow mode: write the flow as a plain function, resume after a crash.*

- **gRPC**：既能调 `grpc://` 分支（不需要业务方的 proto），也提供与 HTTP
  对等的 TC 服务端 API（`dtmrs.v1.Tc`，默认 36790 端口）。
  *gRPC in both directions.*

- **Node 和 JVM 绑定**。Node 走新增的拉取式 ABI（`dtmrs_register_pull` /
  `dtmrs_next_task` / `dtmrs_reply`）——C ABI 的回调必须同步返回 int，
  而 Node 的业务代码几乎全是异步的。JVM 用 JNA 走原有的回调式。
  *Node and JVM bindings; new pull-based dispatch in the C ABI for event-loop hosts.*

- `dtmrs-server` 新增 `api.rs`：HTTP 和 gRPC 共用一份业务判断，不会漂移。

### 修复 / Fixed

- **`protoc` 不再是硬依赖**：系统里没有就退回自带的那份，`cargo add` 的人
  不用先手动装。
  *`protoc` is no longer a hard requirement for downstream users.*

- XA 测试建表加了重试 —— Postgres 的 `CREATE TABLE IF NOT EXISTS` 在并发下
  会撞系统目录的唯一键。

---

## 0.1.0

首个版本。SAGA / TCC / 二阶段消息 / XA 四种模式，sqlite / postgres / mysql
三种存储，HTTP API，子事务屏障，嵌入式 TC，C ABI + Python 绑定。

*Initial release.*
