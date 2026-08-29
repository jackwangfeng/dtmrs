# dtmrs

**English** | [简体中文](README.zh-CN.md)

[![CI](https://github.com/jackwangfeng/dtmrs/actions/workflows/ci.yml/badge.svg)](https://github.com/jackwangfeng/dtmrs/actions/workflows/ci.yml) [![crates.io](https://img.shields.io/crates/v/dtmrs.svg?logo=rust)](https://crates.io/crates/dtmrs) [![docs.rs](https://img.shields.io/docsrs/dtmrs?logo=rust)](https://docs.rs/dtmrs) [![Stars](https://img.shields.io/github/stars/jackwangfeng/dtmrs?style=flat&logo=github)](https://github.com/jackwangfeng/dtmrs/stargazers) [![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A distributed transaction manager in Rust — SAGA, TCC, two-phase messaging, XA and
workflow, over sqlite / Postgres / MySQL / Redis, with equivalent HTTP and gRPC APIs.
Protocol-compatible with [DTM](https://github.com/dtm-labs/dtm), plus one thing DTM
cannot do: **the coordinator can be a library inside your own process**.

Apache-2.0. Nothing blocks commercial or closed-source adoption.

## Features

* **Five transaction modes**: SAGA, TCC, two-phase messaging, XA, workflow
* **Four storage backends**: sqlite (dev), Postgres / MySQL (production), Redis (traffic spikes)
* **Two equivalent APIs**: HTTP and gRPC — same request, same decision, pinned by tests
* **Embeddable**: link the TC in as a library; branches can be in-process functions
* **Callable from other languages** via the C ABI: Python, Node, Java, C (Rust natively)
* **Sub-transaction barrier** for idempotence / empty rollback / suspension, in 5 languages
* Multi-instance with DB leases, crash recovery, exponential backoff, admin console
* No AT mode — by choice, see [Seata migration](docs/choosing-a-mode.md#从-seata-过来的at-模式对应哪个)

## What makes it different: the embeddable coordinator

```text
DTM:    your service ──HTTP──► separately deployed TC process ──► DB
                               (to operate, to keep available, to monitor)
dtmrs:  your service (TC lives inside it) ──► DB
```

Branches don't have to be HTTP URLs. They can be plain in-process functions — no network,
no serialization:

```rust
let tc = Embedded::builder("sqlite:app.db")
    .handler("deduct",      |ctx| async move { BranchResult::Success })
    .handler("deduct_undo", |ctx| async move { BranchResult::Success })
    .start().await?;

tc.saga("order-1001")
    .step("local://deduct", "local://deduct_undo")
    .step("http://shipment/create", "http://shipment/cancel")   // mix with remote calls
    .submit().await?;
```

**Go structurally cannot do this**: `c-shared` drags the entire runtime (scheduler, GC,
signal handling) into the host process and conflicts with the host's threading and signal
model. So DTM has to be deployed standalone.

Embedding does not weaken durability — state still goes to the same database, and a
restarted process resumes unfinished transactions. See
[the embedded example](crates/dtmrs-server/examples/embedded.rs).

## Quick start

### Run the coordinator

```bash
cargo install dtmrs
DTMRS_DB=sqlite:dtmrs.db dtmrs        # HTTP :36789, gRPC :36790, console at /
```

### Run a SAGA

A cross-bank transfer: money out (`TransOut`) and money in (`TransIn`). dtmrs guarantees
both succeed or both roll back.

```bash
curl -X POST localhost:36789/api/dtmsvr/submit \
  -H 'content-type: application/json' -d '{
  "gid": "transfer-1001",
  "steps": [
    {"action": "http://localhost:8081/TransOut", "compensate": "http://localhost:8081/TransOutCom"},
    {"action": "http://localhost:8081/TransIn",  "compensate": "http://localhost:8081/TransInCom"}
  ]}'
```

`TransOut` then `TransIn` are called in order, and the transaction completes.

### When a branch fails

Make `TransIn` return **409** — the one status that means *the business explicitly
refuses*. Compensations then run in reverse:

```
[TransOut]     gid=transfer-1001 branch=01
[TransIn]      gid=transfer-1001 branch=02 → 409, demands rollback
[TransInCom]   ← compensating
[TransOutCom]  ← compensating
result: Failed
```

⚠ **A timeout is not a failure.** 5xx, a connection timeout, gRPC `UNAVAILABLE` all mean
*unknown* — dtmrs retries and never rolls back, because the branch may in fact have
succeeded. Only 409 / gRPC `ABORTED` / a `FAILURE` body triggers compensation. Getting
this backwards is the most expensive mistake in this problem space.

Runnable examples: [`examples/java`](examples/java) (a 3-service test suite),
[`cargo run --example embedded`](crates/dtmrs-server/examples/embedded.rs),
[`--example workflow`](crates/dtmrs-server/examples/workflow.rs).

## Integrating: you must use the barrier

Branches **will** be called more than once — that is the design, not a defect. Every branch
has to survive duplicate requests, empty rollbacks (a compensation for something that never
ran), and suspension (a late forward action arriving after its compensation).

The barrier solves all three by writing a marker **inside your own business transaction**:

```rust
// gid / branch_id / op / trans_type are passed in by the TC
let mut bb = BranchBarrier::new(be, trans_type, gid, branch_id, op)?;
let mut tx = pool.begin().await?;

if bb.decide(&mut tx).await? == Decision::Execute {
    deduct_stock(&mut tx).await?;   // your business SQL — must be in this tx
}

tx.commit().await?;   // where atomicity comes from: marker and business change live or die together
```

Available for [Rust](crates/dtmrs-barrier), [Go, Java, Node, Python](clients/).
Full guide: [业务侧接入](docs/integration.md).

## Documentation

The README covers *what this is*; **[docs/](docs/) covers how to use it** (guides are in
Chinese; the [docs.rs reference](https://docs.rs/dtmrs) is language-neutral).

| Guide | |
|---|---|
| [快速上手](docs/quickstart.md) | 5-minute walkthrough |
| [五种模式怎么选](docs/choosing-a-mode.md) | **choosing a transaction mode** |
| [业务侧接入](docs/integration.md) | **integrating your services**, barrier, TCC ordering |
| [部署与运维](docs/deployment.md) | production, multi-instance, auth, monitoring |
| [API 参考](docs/api.md) | HTTP + gRPC reference, DTM migration notes |
| [性能实测](docs/benchmarks.md) | numbers, methodology, and 4 real bugs benchmarking caught |
| [排错](docs/troubleshooting.md) | troubleshooting |
| [DESIGN.md](DESIGN.md) | state machines and design rationale |

## Performance

End-to-end (submit → TC calls two branches → final state), 20k two-step SAGAs, median of
three runs on a 12700 / 20 cores:

| Storage | Throughput |
|---|---|
| Redis | 13105 tx/s |
| Postgres | 4982 tx/s |
| sqlite (WAL) | 1798 tx/s |
| MySQL | 242 tx/s |

MySQL is slow because of its own defaults (two fsyncs per commit), not dtmrs overhead.
Numbers, the DTM head-to-head, and the methodology traps that make benchmarks lie:
**[docs/benchmarks.md](docs/benchmarks.md)**.

## Installing

```toml
# running the coordinator (or embedding it in your process)
dtmrs = "0.8"

# a business service (RM): you only need the barrier for idempotence.
# Don't drag the whole coordinator (axum, tonic, ...) into it
dtmrs = { version = "0.8", default-features = false, features = ["barrier"] }
```

Building the gRPC feature needs **protoc**. Disable the `grpc` feature and you don't.

From source:

```bash
cargo build --release            # binary at target/release/dtmrs
cargo test --workspace           # 204 tests
```

⚠ Real-database tests are gated by environment variables. **Not configured means not
tested — it does not mean passed.** See [CONTRIBUTING.md](CONTRIBUTING.md).

## Layout

```
crates/
  dtmrs/          facade crate + the dtmrs binary
  dtmrs-core/     state machines and SQL dialect rendering — pure logic, no I/O
  dtmrs-store/    storage and lease claiming (SQL via sqlx::Any, or Redis)
  dtmrs-server/   the TC: api / http / grpc / driver / registry / workflow / embedded
  dtmrs-barrier/  the sub-transaction barrier (SQL and Redis)
  dtmrs-xa/       XA helpers for the RM side (Postgres and MySQL syntax)
  dtmrs-ffi/      the C ABI
clients/          barrier libraries for Go, Java, Node, Python
```

## Protocol provenance

dtmrs implements DTM's **protocol** — paths, field names, and the semantics of
`dtm_result`. No code was copied; every behaviour was verified against DTM's source or
against a running DTM instance, and the places where the two intentionally differ are
documented in [docs/api.md](docs/api.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
