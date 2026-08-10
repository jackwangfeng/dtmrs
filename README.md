# dtmrs

**English** | [简体中文](README.zh-CN.md)

A distributed transaction manager written in Rust, targeting feature parity with
[DTM](https://github.com/dtm-labs/dtm) (Go, 10.9k★).

**Why rebuild it**: the Rust ecosystem has no usable distributed transaction manager.
The only comparable project, [rseata](https://github.com/oulover/rseata), has 88 stars,
one contributor, 33 commits — **and no LICENSE file**, which legally means all rights
reserved, so it cannot be used commercially. Other Rust options (restate/obelisk) are
durable-execution engines, and are restricted by BUSL / AGPL respectively.

dtmrs is **Apache-2.0**. Nothing blocks commercial or closed-source adoption.

## What makes it different: the embeddable coordinator

Link the transaction coordinator into your own process as a library — **no separate
service to deploy**:

```text
DTM:    your service ──HTTP──► separately deployed TC process ──► DB
                               (to operate, to keep available, to monitor)
dtmrs:  your service (TC lives inside it) ──► DB
```

Branches don't have to be HTTP URLs. They can be plain in-process functions — no
network, no serialization:

```rust
let tc = Embedded::builder("sqlite:app.db")
    .handler("deduct",      |ctx| async move { /* business logic */ BranchResult::Success })
    .handler("deduct_undo", |ctx| async move { BranchResult::Success })
    .start().await?;

tc.saga("order-1001")
    .step("local://deduct", "local://deduct_undo")
    .step("http://shipment/create", "http://shipment/cancel")   // mix with remote calls
    .submit().await?;
```

Actual output of `cargo run --example embedded`:

```
② out of stock (step 2 demands rollback, compensations must run in reverse)
  [deduct]        gid=order-2 branch=01
  [out of stock]  gid=order-2 → explicitly demands rollback
  [unship]        gid=order-2 ← compensating
  [deduct_undo]   gid=order-2 ← compensating
  result: Failed
```

**Go structurally cannot do this**: `c-shared` drags the entire runtime (scheduler, GC,
signal handling) into the host process and conflicts with the host's threading and signal
model. Nobody actually ships that. So DTM has to be deployed standalone.

### Embeddable from any language (C ABI)

The build output is an ordinary `.so` with no runtime baggage:

```bash
cargo build -p dtmrs-ffi --release      # → target/release/libdtmrs.so
```

| Language | Binding | Dispatch model |
|---|---|---|
| Python | `bindings/python/` — pure ctypes, zero deps | callback |
| Node | `bindings/node/` — koffi, **async handlers** | pull |
| JVM | `bindings/java/` — JNA, Java 8+, no maven/gradle needed | callback |
| C | `include/dtmrs.h` + `examples/c/demo.c` | callback |

Python:

```python
import dtmrs
tc = dtmrs.Tc("sqlite:/tmp/app.db")

@tc.handler("transfer_out")
def transfer_out(ctx):
    move(1, 2, 100)                     # your own business SQL
    return dtmrs.SUCCESS

tc.start()
tc.submit_saga("order-1", [("local://transfer_out", "local://transfer_out_undo")])
```

Node — handlers can be `async`, so you can `await` your database normally:

```js
const dtmrs = require('./dtmrs');
const tc = new dtmrs.Tc('sqlite:/tmp/app.db');

tc.handler('transfer_out', async (ctx) => {
  await db.query('UPDATE account SET balance = balance - 100 WHERE id = 1');
  return dtmrs.SUCCESS;
});

await tc.start();
await tc.submitSaga('order-1', [['local://transfer_out', 'local://transfer_out_undo']]);
```

JVM:

```java
try (Dtmrs tc = new Dtmrs("sqlite:/tmp/app.db")) {
    tc.handler("transfer_out", ctx -> {
        jdbc.update("UPDATE account SET balance = balance - 100 WHERE id = 1");
        return Dtmrs.SUCCESS;
    });
    tc.start();
    tc.submitSaga("order-1", Dtmrs.step("local://transfer_out", "local://transfer_out_undo"));
}
```

All four examples actually run; balances really move. Actual Node output:

```
initial balances: { '1': 1000, '2': 0 }

① normal transfer
  [transfer_out] gid=node-1 branch=01 op=action
  result: succeed  balances: { '1': 900, '2': 100 }

② risk control rejects → reverse-order compensation, money comes back
  [transfer_out] gid=node-2 branch=01 op=action
  [risk_reject]  gid=node-2 branch=02 op=action
  [null_comp]    gid=node-2 branch=02 op=compensate      ← reverse order
  [transfer_undo] gid=node-2 branch=01 op=compensate
  result: failed  balances: { '1': 900, '2': 100 }   ← the transfer was undone

③ downstream timeout → retry only, never roll back
  status: submitted
```

#### Two kinds of host, two dispatch models

This split was discovered by testing, not by design:

| | Model | Why |
|---|---|---|
| Python / Java / C | **callback** (`dtmrs_register`) | the host can run handlers synchronously on any thread |
| **Node** | **pull** (`dtmrs_register_pull`) | you cannot `await` inside a synchronous callback |

The initial assumption was that JS simply cannot be called back from a foreign thread.
Testing disproved it — when the event loop is idle, koffi queues foreign-thread callbacks
onto the main thread and they run fine.

The real obstacle is different: **a C ABI callback must return an `int` synchronously**,
while essentially all Node business code is asynchronous (database clients return
Promises). No amount of documentation works around that, so the C ABI grew a second
dispatch model:

```c
int dtmrs_register_pull(DtmrsTc *tc, const char *name);
int dtmrs_next_task(DtmrsTc *tc, int timeout_ms, char *out, size_t out_len);
int dtmrs_reply(DtmrsTc *tc, unsigned long long task_id, int result);
```

The library queues pending branches; the host pulls them inside its own event loop, does
whatever async work it likes, and replies. This **does not replace** the callback model —
both can be used in one process, each owning different branch names.

Two things you must know:

- `timeout_ms = 0` means **non-blocking**. Event-loop hosts must pass 0 — blocking freezes
  the loop, and then you cannot even deliver the reply.
- If the host doesn't reply within **30 seconds**, the result is treated as **unknown**.
  It may have completed the work and simply failed to answer; treating that as failure
  would trigger a wrong rollback.

For the same reason, the Node binding's `waitFinal` polls instead of calling
`dtmrs_wait_final`: that C function blocks the calling thread, which in Node freezes the
whole event loop, stops branch dispatch, and guarantees a timeout.

**Two JVM-specific traps** (both handled inside the binding): JNA callback objects must be
strongly referenced — once garbage collected, Rust still holds a raw function pointer and
the next callback is a dangling pointer, i.e. a segfault; and exceptions must never
propagate back across the FFI boundary (undefined behavior), so they are uniformly
converted to "unknown".

JNA was chosen over FFM (`java.lang.foreign`) because FFM only became final in JDK 22
(preview in 21, incubator in 17); requiring users to be on 22+ is too steep. JNA works on
Java 8+.

#### Three cross-language hazards (all handled)

**1. Host callbacks are synchronous and may block.** A Python handler doing a database
query or an HTTP call easily takes tens of milliseconds, and CPython needs the GIL.
Calling that directly on a tokio worker would stall the runtime, so every callback goes
through `spawn_blocking` onto a dedicated blocking pool.

**2. Callbacks arrive on arbitrary threads.** The `thread=Thread-0/Thread-1` in the JVM
example output is Rust's thread calling into the JVM. Host handlers must be thread-safe.
ctypes' `CFUNCTYPE` handles the GIL automatically; JNA attaches the thread for you; raw
JNI would require `AttachCurrentThread` yourself.

**3. A host exception means "unknown", not "failure".** If a handler raises, returns
garbage, or panics, it is treated as `UNKNOWN` — retry, never roll back. You do not know
whether it actually did the work, and misjudging it as failure would destroy a
transaction that should have succeeded.

### Embedding does not sacrifice durability

The TC runs in your process, but state lives in the database. If the process dies, nothing
is lost — a new process picks up where it left off, **without the client resubmitting and
without redoing completed steps**.
(Test: `跨进程重启_事务不丢且已完成的步骤不重做`)

### One constraint you must know

A `local://` branch stores a **name**, because closures cannot be persisted. After a
restart you must register a handler under the same name.

- Missing registration → treated as "unknown result", so it **retries and never rolls
  back** (this is a deployment problem, not a business failure)
- `submit()` verifies that every name is registered — better to fail at submit time than
  to discover it after side effects have landed

## What works today

| Capability | Status |
|---|---|
| SAGA (forward commit + reverse compensation) | ✅ |
| **Embeddable TC (in-process branches, nothing to deploy)** | ✅ |
| **C ABI + Python / Node / JVM bindings** | ✅ |
| Sub-transaction barrier (idempotence / empty rollback / suspension) | ✅ |
| Crash recovery (unfinished transactions resume automatically) | ✅ |
| Multiple TC instances (DB lease prevents double-driving) | ✅ |
| Exponential backoff retry | ✅ |
| HTTP API (paths aligned with DTM) | ✅ |
| SQLite storage | ✅ |
| **Postgres storage (multi-instance production deployment)** | ✅ |
| **MySQL storage** | ✅ |
| **TCC (try / confirm / cancel)** | ✅ |
| **Two-phase messaging (replaces transactional MQ)** | ✅ |
| **XA (native two-phase commit on Postgres + MySQL)** | ✅ |
| **gRPC (branch calls + TC server API)** | ✅ |

**105 tests, all green**: 27 state-machine/dialect unit tests + 7 storage + 10 barrier
+ 6 XA helper + 8 C ABI + 9 server unit + 6 SAGA e2e + 12 TCC/msg + 5 embedded
+ 6 XA e2e + 8 gRPC e2e.
The 17 storage and barrier tests each run against **sqlite, real Postgres and real MySQL**;
the 6 XA tests require a real Postgres or MySQL (both configured → both run).
The Python, Node, Java and C examples all actually run.

Real-database tests are gated by environment variables. **Not configured means not
tested — it does not mean passed**:

```bash
DTMRS_TEST_PG='postgres://postgres:pw@127.0.0.1:5432/dtmrs' \
DTMRS_TEST_MYSQL='mysql://root:pw@127.0.0.1:3306/dtmrs' \
DTMRS_TEST_XA_PG='postgres://postgres:pw@127.0.0.1:5432/dtmrs' \
DTMRS_TEST_XA_MYSQL='mysql://root:pw@127.0.0.1:3306/dtmrs' \
cargo test --workspace
```

## Storage: one set of SQL for sqlite / Postgres / MySQL

```bash
DTMRS_DB='postgres://user:pass@host:5432/dtm' ./dtmrs
DTMRS_DB='mysql://user:pass@host:3306/dtm'    ./dtmrs
DTMRS_DB='sqlite:dtmrs.db'                    ./dtmrs
```

**Three databases, one implementation.** No `Store` trait, no three copies of the SQL —
just `sqlx::Any` plus a thin dialect rendering layer (`dtmrs-core/src/dialect.rs`).

With only two backends, `sqlx::Any` with `$N` placeholders was enough. **MySQL breaks
three assumptions at once**, which is why the layer exists. All measured
(sqlx 0.8 / pg16 / mysql 8.0.44):

| | sqlite | postgres | mysql |
|---|---|---|---|
| `$1` placeholders | ✅ | ✅ | ❌ `Unknown column '$1'` |
| `?` placeholders | ✅ | ❌ syntax error | ✅ |
| `ON CONFLICT DO NOTHING` | ✅ | ✅ | ❌ 1064 syntax error |
| `INSERT IGNORE` | ❌ | ❌ | ✅ |
| `TEXT PRIMARY KEY` | ✅ | ✅ | ❌ 1170 needs key length |
| `CREATE INDEX IF NOT EXISTS` | ✅ | ✅ | ❌ 1064 syntax error |

Templates therefore use `?` uniformly (matching MySQL), and `Backend::q` rewrites them to
`$1..$n` for the others. The cost: **string literals in templates must not contain `?`**,
or they will be treated as placeholders.

### Three MySQL traps you must know

**1. `ON DUPLICATE KEY UPDATE` returns `rows_affected = 1` on a duplicate, not 0.**
Using it for idempotence checks misreads "already existed" as "just inserted". MySQL must
use `INSERT IGNORE` (which returns 0 on duplicates, consistent with the other two).

**2. Reading MySQL `TEXT` through `sqlx::Any` always fails as a type mismatch.**

```
mismatched types; Rust type String is not compatible with SQL type BLOB
```

`LONGTEXT`, `MEDIUMTEXT`, and explicit `CHARACTER SET utf8mb4` all behave the same. Of the
five spellings tried, only `VARCHAR` decodes into `String`. So free-text columns are
`VARCHAR(n)` on MySQL, with `n` large enough for the longest content (note the 65535-byte
per-row limit, and utf8mb4 counts 4 bytes per character).

**3. Index syntax is mutually exclusive.** MySQL has no
`CREATE INDEX IF NOT EXISTS` and requires inline `KEY` at table creation; the other two are
the opposite. The dialect layer switches via a `create_index()` / `inline_index()` pair
where one of the two returns empty.

Integer columns are always `BIGINT` — Postgres `INTEGER` is only 4 bytes and cannot hold a
timestamp.

Values that are too long are rejected in Rust (`check_len`) rather than at the database:
MySQL's `INSERT IGNORE` **silently truncates** oversized values (1406 downgraded to a 1265
warning), and a truncated `gid` would make two unrelated transactions collide on the same
barrier row.

### Measured: two instances, no double-driving

This is what Postgres is for (sqlite cannot take multi-instance writes). 20 transactions,
two instances, with the business side sleeping 50 ms on purpose to widen the race window:

```
branches invoked: 40 (20 × 2 steps)
call count distribution: {1: 40}     ← each branch exactly once
duplicate calls: none ✓

ownership: succeed|tc-1|10
           succeed|tc-2|10           ← each instance drove half
```

### A trap we hit: Postgres `CREATE TABLE IF NOT EXISTS` is not concurrency-safe

Start two instances simultaneously and instance 1 crashes outright:

```
duplicate key value violates unique constraint "pg_type_typname_nsp_index"
```

In Postgres, `CREATE TABLE IF NOT EXISTS` can collide on a unique key in the system
catalog. A single-writer sqlite setup will never expose this — **only actually running two
instances will**. Table creation now retries (by the time the loser retries, the table
exists and `IF NOT EXISTS` skips normally).

## gRPC: supported in both directions

The branch address prefix selects the protocol, and **one transaction can mix all three**:

```json
{"action": "grpc://ship:9000/busi.Busi/Ship", "compensate": "grpc://ship:9000/busi.Busi/Unship"}
{"action": "http://pay/deduct",               "compensate": "http://pay/deduct-undo"}
{"action": "local://deduct",                  "compensate": "local://deduct_undo"}
```

The TC also serves a gRPC API equivalent to the HTTP one (`dtmrs.v1.Tc`, port 36790 by
default):

```bash
DTMRS_ADDR=0.0.0.0:36789 DTMRS_GRPC_ADDR=0.0.0.0:36790 ./dtmrs
```

Both surfaces share one implementation (`api.rs`), so a transaction submitted over gRPC
can be queried over HTTP and vice versa.

### Your services don't need to change for dtmrs

Calling a gRPC branch **does not require the callee's proto**. A custom byte-shuffling
codec replaces tonic's default prost codec: the request body is raw bytes, the response is
raw bytes, and the method path is a runtime string anyway.

So **any existing gRPC method can serve as a branch**. The request body is empty bytes (an
empty protobuf message is valid for *any* message type), and branch identity travels in
metadata:

| metadata key | contents |
|---|---|
| `dtm-gid` | global transaction id |
| `dtm-trans_type` | saga / tcc / msg / xa |
| `dtm-branch_id` | branch number (`01`, `02`, …) |
| `dtm-op` | action / compensate / confirm / cancel / … |

Those four are exactly what the sub-transaction barrier needs — the same information HTTP
passes as query parameters.

### Status code mapping: the one place that can cause inconsistency

HTTP uses 409 / 425 to express "explicit failure" and "still working". gRPC has neither, so
each had to be mapped onto a standard code that infrastructure won't produce by accident:

| gRPC code | meaning | HTTP equivalent |
|---|---|---|
| `OK`(0) | success | 200 |
| `ABORTED`(10) | business **explicitly** demands rollback | 409 |
| `FAILED_PRECONDITION`(9) | still working, not a failure | 425 |
| **everything else** | result **unknown** — retry, never roll back | 5xx / timeout |

The last row is the critical one. `UNAVAILABLE`(14), `DEADLINE_EXCEEDED`(4) and
`INTERNAL`(13) are precisely the codes produced by network flakiness and timeouts — and on
a timeout the callee may well have succeeded, so compensating would create inconsistency.

`CANCELLED`(1) is the easiest to get wrong: that is **us** giving up, not the callee
refusing.

(Tests: `grpc只有aborted才算失败` exhausts all 16 codes; `grpc与http的判定语义一致`
pins that both protocols reach the same verdict for the same intent.)

## TCC

The try phase is driven by the **client** (register the branch first, then call try); the
TC only handles confirm/cancel:

```bash
curl -XPOST :36789/api/dtmsvr/prepare -d '{"gid":"tcc-A","trans_type":"tcc"}'
curl -XPOST :36789/api/dtmsvr/registerBranch -d '{"gid":"tcc-A","branch_id":"01",
  "try":"http://b/try1","confirm":"http://b/confirm1","cancel":"http://b/cancel1"}'
# ← the client calls try here. All succeeded → submit; any failed → abort
curl -XPOST :36789/api/dtmsvr/submit -d '{"gid":"tcc-A","trans_type":"tcc"}'
```

**Register before calling try.** The other order means try succeeds but registration fails,
the TC never learns the branch exists, and it will not cancel it on rollback — the reserved
resources leak permanently.

### The critical semantic difference from SAGA

A SAGA action returning FAILURE triggers reverse compensation.
**A TCC confirm returning FAILURE must never trigger cancel** — try already succeeded,
resources are reserved, and the global decision to commit is made; cancelling now would
undo a confirmed transaction. The only correct handling is infinite retry plus alerting.

`tcc_advance` **never** returns `Finish(Aborting)` from the Submitted phase, and the test
exhausts all 3×3 branch-state combinations to pin it down.
(Tests: `confirm失败绝不能触发cancel`, `tcc_confirm失败只重试绝不转cancel`)

## Two-phase messaging: guaranteed delivery without an MQ

The flow: `prepare` persists → the business commits its local transaction → `submit`.
**If the process dies between those two steps**, the TC asks the business side whether that
local transaction actually committed:

| callback answer | TC action |
|---|---|
| SUCCESS | local transaction committed → keep driving forward branches |
| FAILURE | not committed → discard the whole transaction |
| ONGOING / timeout | **must not be read as "not committed"** → back off and retry |

Measured (client calls prepare and never submits):

```
prepare → SUCCESS
(no submit; waiting for the TC to check back…)
status: succeed  branches: [('action', 'succeed')]
business-side calls: {'/query': 1, '/notify': 1}     ← queried once, then the message went out
```

A `prepare` without `query_prepared` is **rejected outright** — with no callback address,
nobody can adjudicate the transaction if the client dies; guessing "committed" double-charges,
guessing "not committed" loses the order.

msg has no compensation branches. It only guarantees eventual delivery, so branches must be
idempotent and retryable forever.

## XA: native two-phase commit

Instead of compensation, this relies on the database's own two-phase commit. A branch runs
its business SQL and then prepares; the changes are **durable but invisible** until the TC
decides to commit or roll back. The upside is strong consistency (no visible intermediate
state); the cost is **locks held for the entire duration**.

**Both Postgres and MySQL are supported** (the same 6 e2e tests run against each):

```rust
// business side (RM), phase one
let xa = Xa::from_url(&db_url)?;           // detects the database, picks the syntax
let mut br = xa.begin(&pool, gid, "01").await?;
sqlx::query("UPDATE acct SET bal = bal - ? WHERE id = ?")   // placeholders per your DB
    .bind(100i64).bind(1i32).execute(br.conn()).await?;
let xid = br.prepare().await?;             // phase one done
// then register (xid, commit/rollback callback addresses) with the TC
```

### The two databases have completely different syntax (both measured)

| | Postgres 16 | MySQL 8.0 |
|---|---|---|
| begin | `BEGIN` | `XA START 'xid'` |
| phase one | `PREPARE TRANSACTION 'xid'` | `XA END 'xid'` + `XA PREPARE 'xid'` |
| commit | `COMMIT PREPARED 'xid'` | `XA COMMIT 'xid'` |
| rollback | `ROLLBACK PREPARED 'xid'` | `XA ROLLBACK 'xid'` |
| list dangling | `pg_prepared_xacts` | `XA RECOVER` |
| already-resolved error | `42704` | `XAE04` |
| enabled by default | ❌ `max_prepared_transactions=0` | ✅ |
| exposes prepare age | ✅ | ❌ `XA RECOVER` has no timestamp |
| xid length limit | 200 bytes | gtrid **64 bytes** |

SQLite has no two-phase commit at all; `Xa::from_url` rejects it.

Measured (real Postgres, two branches simulating a cross-database transfer; the same suite
is green on MySQL):

```
balances after prepare: [1000, 0]     ← durable but invisible
dangling prepared transactions: 2
submit → COMMIT PREPARED
balances after commit:  [900, 100]    ← both branches take effect together
dangling prepared transactions: 0
```

### ⚠ Three hard XA constraints (all hit in practice, each pinned by a test)

**1. Unresolved prepared transactions hold locks forever** (and block VACUUM on Postgres).
While writing this suite, one test failed midway and left a prepared transaction behind —
**a completely unrelated UPDATE then blocked indefinitely** and the test process hung for
two minutes. In production this scales up to an unwritable database. Both engines behave
the same: `Xa::list_prepared` must be monitored.
(Test: `xa_没解决的prepared事务会阻塞无关写入`, run against pg and mysql)

⚠ **`age_secs` is always 0 on MySQL** — `XA RECOVER` does not report prepare time, so
"how long has it been hanging" is unavailable there; you can only alert on "this xid still
exists".

**2. XA branches must touch disjoint data.** The first version had both branches writing
the same rows, and branch 02 deadlocked (`55P03 lock timeout`) — branch 01 had already
prepared and was holding row locks. That is not a bug, it is the nature of XA:
**one branch corresponds to one resource manager**, and in real deployments they naturally
live in different databases.
(On MySQL the equivalent is `innodb_lock_wait_timeout`; the tests set it to 5 s to turn
"hangs" into "errors".)

**3. The availability check differs per engine, but both must run at startup.**
`ensure_enabled()` dispatches:

| | checks | consequence if unmet |
|---|---|---|
| Postgres | `max_prepared_transactions` > 0 | defaults to **0**, XA is entirely unusable |
| MySQL | version ≥ **5.7.7** | XA works, but prepared transactions **are lost on restart** — no durability |

Do not wait for the first transaction to discover this (by then another branch may already
have prepared successfully).

```bash
postgres -c max_prepared_transactions=32     # MySQL 8.0 needs no configuration
```

### Two MySQL-specific traps

**1. XA statements cannot use the prepared-statement protocol.**

```
1295 This command is not supported in the prepared statement protocol yet
```

`sqlx::query()` uses prepared statements by default, so all two-phase statements use
`sqlx::raw_sql` (text protocol). **Consequence**: the xid cannot be bound as a parameter
and must be interpolated into the SQL — injection protection relies entirely on the
character allowlist in `xid_for()` (alphanumerics plus `_-`).

**2. The xid limit is 64 bytes, far stricter than Postgres' 200.** A slightly long gid
overflows it. Plain truncation would be **catastrophic** — two unrelated long gids would
collide into the same xid and then commit each other's transactions. Oversized values are
truncated and suffixed with a 16-bit FNV-1a digest.
(Test: `mysql的xid上限更严且截断不撞车`)

### Second-phase idempotence comes from the state machine

Resolving the same xid twice errors: Postgres `42704 undefined_object`, MySQL
`XAE04 XAER_NOTA`. The TC will definitely retry, so both are treated as `AlreadyResolved`
(success).

**That "not found means success" is safe** because of a guarantee from `xa_advance`: the
Submitted phase never returns `Finish(Aborting)`, so "not found" can only mean it was
already committed, never that it was rolled back. The check uses SQLSTATE rather than
error-message matching — messages change across versions and locales.
(Test: `错误码按方言区分`)

## Running it

```bash
cargo build --release
DTMRS_DB=sqlite:dtmrs.db DTMRS_ADDR=127.0.0.1:36789 ./target/release/dtmrs
```

Submit a two-step SAGA:

```bash
curl -XPOST localhost:36789/api/dtmsvr/submit -H 'content-type: application/json' -d '{
  "gid": "order-1001",
  "steps": [
    {"action": "http://busi/deduct",  "compensate": "http://busi/deduct-undo"},
    {"action": "http://busi/shipment","compensate": "http://busi/shipment-undo"}
  ]}'

curl 'localhost:36789/api/dtmsvr/query?gid=order-1001'
```

Measured rollback path (`/a2fail` returns 409):

```
status: failed   rollback reason: branch 02 returned FAILURE
  01 action      succeed
  01 compensate  succeed     ← reverse order
  02 action      failed
  02 compensate  succeed
```

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `DTMRS_DB` | `sqlite:dtmrs.db` | storage DSN (sqlite / postgres / mysql) |
| `DTMRS_ADDR` | `0.0.0.0:36789` | HTTP listen address |
| `DTMRS_GRPC_ADDR` | `0.0.0.0:36790` | gRPC listen address |
| `DTMRS_OWNER` | `tc-<pid>` | instance identity for lease ownership |

## Integrating: you must use the barrier

Branch endpoints **will** be called more than once (TC retries plus crash recovery), so the
business side must be idempotent. Use `dtmrs-barrier` and put the barrier record and your
business SQL **in the same local transaction**:

```rust
use dtmrs_barrier::{BranchBarrier, Decision};
use dtmrs_core::Backend;

let be = Backend::from_url(&db_url);        // barrier DDL and SQL render per backend
BranchBarrier::migrate(&pool, be).await?;   // create the table once at startup

// gid / branch_id / op / trans_type arrive from the TC as query params (HTTP) or metadata (gRPC)
let mut bb = BranchBarrier::new(be, trans_type, gid, branch_id, op)?;
let mut tx = pool.begin().await?;

if bb.decide(&mut tx).await? == Decision::Execute {
    // business SQL — must be inside this tx
    sqlx::query("UPDATE account SET balance = balance - ? WHERE id = ?")
        .bind(amount).bind(uid).execute(&mut *tx).await?;
}
tx.commit().await?;   // atomicity: the barrier record and the business change live or die together
```

`decide` returns one of three verdicts:

| verdict | meaning |
|---|---|
| `Execute` | do the work |
| `NullCompensation` | **empty rollback** — the forward branch never ran, compensation is a no-op |
| `Duplicated` | **duplicate or suspended** — this call was already handled, skip it |

**Prerequisite: the barrier table must live in the same database instance as your business
tables**, otherwise they cannot share a local transaction. This is not an implementation
limitation; it is the condition that makes the whole approach work.

## Three things that are easy to get wrong (each guarded by a test)

**1. A timeout is not a failure.** A 500 or a connection timeout means the result is
**unknown** — the callee may have succeeded. Rolling back here creates inconsistency; the
correct action is backoff and retry until you get an explicit SUCCESS or FAILURE. Only
HTTP 409, gRPC `ABORTED`, or `FAILURE` in the response body triggers compensation.
(Test: `超时不能触发回滚而要重试`)

**2. Compensate every branch, not just the successful ones.** An action may have timed out
yet actually succeeded — skipping its compensation because "it didn't succeed" leaks money.
So rollback compensates **all** branches in reverse order, and the barrier turns the
unnecessary ones into no-ops. Better to over-compensate than to under-compensate.
(Tests: `没跑过的分支也要补偿`, `主动中止会触发补偿`)

**3. Duplicate submits must succeed, not error.** A client retrying the same gid after a
network hiccup will assume the request was not accepted if you return an error.
`INSERT OR IGNORE` plus SUCCESS.
(Test: `重复提交同一个gid是幂等的`)

## Layout

```
crates/
  dtmrs-core/     state machine, pure logic, no I/O — where state-transition bugs get tested
                  dialect.rs: SQL dialect rendering for sqlite / postgres / mysql
  dtmrs-store/    storage (one SQL set, three databases) + lease acquisition
  dtmrs-server/   TC: api.rs (protocol-agnostic operations)
                      main.rs (axum HTTP) / grpc/ (tonic, both directions)
                      driver.rs (resident driver) / registry.rs + embedded.rs
  dtmrs-barrier/  client-side sub-transaction barrier
  dtmrs-xa/       XA helper for the business side (RM): pg PREPARE TRANSACTION / mysql XA
  dtmrs-ffi/      C ABI (cdylib + staticlib), callback and pull dispatch
include/dtmrs.h   C header
bindings/python/  Python binding (pure ctypes, zero deps)
bindings/node/    Node binding (koffi, async handlers)
bindings/java/    JVM binding (JNA, Java 8+, no maven/gradle needed)
examples/c/       C example
```

Separating the state machine from I/O is deliberate: most distributed-transaction bugs live
in state transitions, and isolating them makes exhaustive testing possible without any
network. HTTP and gRPC are both thin wrappers over `api.rs` — writing the logic twice would
eventually drift, and drift here means "the same request is rejected over HTTP but accepted
over gRPC".

## Protocol provenance

The state machine, schema, and barrier algorithm were derived by **checking DTM's source
line by line**, not from secondary sources:

- `sqls/dtmsvr.storage.mysql.sql` — global/branch states and fields
- `sqls/dtmcli.barrier.mysql.sql` — barrier table and unique key
- `client/dtmcli/barrier.go`, `BranchBarrier.Call` — barrier algorithm
- `dtmsvr/trans_type_{saga,tcc,msg,xa}.go` — per-mode implementations

Only the protocol is reimplemented; no code is copied. DTM is BSD-3, which is compatible
with Apache-2.0.

Design notes: [DESIGN.md](DESIGN.md) (Chinese).

## License

Apache-2.0. See [LICENSE](LICENSE).
