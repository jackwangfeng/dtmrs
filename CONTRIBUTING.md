# Contributing

[简体中文](#中文) below.

Thanks for taking a look. This project implements a protocol where mistakes cause **silent
data inconsistency**, not crashes — so a few conventions matter more than usual.

## Before you start

```bash
cargo build --release
cargo test --workspace
```

Building the gRPC layer requires `protoc`. If you don't have it, either install
`protobuf-compiler` or build with `--no-default-features` on `dtmrs-server`.

## Running the real tests

`cargo test --workspace` alone runs against sqlite only. The storage, barrier, and XA
layers exist *because* the three databases disagree — sqlite passing tells you very little
about them.

```bash
DTMRS_TEST_PG='postgres://postgres:pw@127.0.0.1:5432/dtmrs' \
DTMRS_TEST_MYSQL='mysql://root:pw@127.0.0.1:3306/dtmrs' \
DTMRS_TEST_XA_PG='postgres://postgres:pw@127.0.0.1:5432/dtmrs' \
DTMRS_TEST_XA_MYSQL='mysql://root:pw@127.0.0.1:3306/dtmrs' \
cargo test --workspace
```

Postgres needs two-phase commit turned on for the XA tests:

```bash
docker run -d --rm -p 5432:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=dtmrs \
  postgres:16-alpine -c max_prepared_transactions=32
docker run -d --rm -p 3306:3306 -e MYSQL_ROOT_PASSWORD=pw -e MYSQL_DATABASE=dtmrs mysql:8.0
```

**Not configured means not tested, not passed.** The XA tests in particular report as
`passed` when unconfigured — they print a warning and return.

If you touch `dtmrs-store`, `dtmrs-barrier`, `dtmrs-xa`, or the dialect layer, please run
against both real databases before opening a PR. CI does this too, but finding it locally
is faster.

## Conventions

**Comments and test names are in Chinese.** Test functions are Chinese assertion sentences,
e.g. `fn 超时不能触发回滚而要重试()`. These names are the specification — please follow the
style. (Code identifiers themselves are English.)

Module-level doc comments carry a lot of hard-won "we measured this" detail — tables of
per-database behavior, error codes, limits. If you change the behavior, update the table.

`cargo fmt --all` and `cargo clippy --workspace --all-targets -- -D warnings` must be clean.

## The invariants

These are the things that cause inconsistency if broken. Each is pinned by a test; read the
test before changing the behavior.

1. **A timeout is not a failure.** 5xx, connection timeouts, gRPC `UNAVAILABLE` /
   `DEADLINE_EXCEEDED` / `CANCELLED` all mean *unknown* → retry, never roll back. Only HTTP
   409, gRPC `ABORTED`, or `FAILURE` in the body triggers compensation.
2. **confirm / commit failure must never become cancel / rollback.** `tcc_advance` and
   `xa_advance` never return `Finish(Aborting)` from the Submitted phase.
3. **Rollback compensates every branch**, not just the successful ones.
4. **Duplicate submit of the same gid must succeed**, not error.
5. **Register the branch before running phase one** (TCC/XA).

## Architecture rules

- State-transition logic lives in `dtmrs-core` (`saga_advance` / `tcc_advance` /
  `msg_advance` / `xa_advance`). `driver.rs` only turns decisions into I/O. Keeping them
  apart is what makes exhaustive unit testing possible.
- HTTP (`main.rs`) and gRPC (`grpc/server.rs`) only translate protocol. All business
  judgment lives in `api.rs`. Duplicating it would eventually drift, and drift here means
  the same request is rejected over one protocol and accepted over the other.

## Writing SQL

Templates use `?` placeholders uniformly; `Backend::q` rewrites them per backend.
**String literals in templates must not contain `?`.** See the module header of
`crates/dtmrs-core/src/dialect.rs` for the full set of rules and the per-database
behavior table.

## Licensing

By contributing you agree your work is licensed under Apache-2.0.

---

## 中文

这个项目实现的协议，出错的表现是**静默的数据不一致**而不是崩溃，所以有几条约定比一般项目更要紧。

**测试和注释用中文。** 测试函数名就是中文断言句（`fn 超时不能触发回滚而要重试()`），
这些名字本身就是规格说明，请沿用这个风格。

**真库测试必须跑。** `cargo test --workspace` 只跑 sqlite；存储 / 屏障 / XA 那几层存在的
理由就是三种数据库不一致，sqlite 全绿说明不了什么。四个环境变量见上面的命令。
**没配就是没跑，不是通过** —— XA 那 6 个没配时仍然显示 passed。

**五条不能破坏的语义**（每条都有测试钉着，改之前先读对应测试）：
超时不等于失败；confirm/commit 失败绝不转 cancel/rollback；回滚要补偿所有分支；
重复提交必须幂等成功；先 registerBranch 再做一阶段。

**两条结构约束**：状态迁移逻辑只能在 `dtmrs-core`，`driver.rs` 只做 I/O；
HTTP 和 gRPC 只做协议转换，业务判断只在 `api.rs`。

提交前 `cargo fmt --all` 和 `cargo clippy --workspace --all-targets -- -D warnings` 要干净。

贡献即表示同意以 Apache-2.0 授权。
