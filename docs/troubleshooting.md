# 排错

## 先看这里：怎么定位一笔卡住的事务

```bash
curl 'localhost:36789/api/dtmsvr/query?gid=order-1001'
```

返回里最有信息量的三个字段：

| 字段 | 看什么 |
|---|---|
| `status` | `submitted` = 还在推正向；`aborting` = 正在补偿；`succeed`/`failed` = 已终结 |
| `rollback_reason` | 为什么转的回滚。空的说明不是业务主动要求的 |
| `branches[].status` | 哪一步卡住了：`prepared` = 还没成功 |

看 TC 日志（`RUST_LOG=info`）里那个 gid，会有每次分支调用的结果。

---

## 事务一直停在 `submitted`

**这是最常见的现象。** 它表示「某个正向分支还没返回明确成功」，TC 在按指数退避重试。

按可能性排查：

### 1. 分支返回了「结果未知」

看日志里那个分支的返回码。5xx、连接超时、gRPC 的 `UNAVAILABLE` / `DEADLINE_EXCEEDED` / `CANCELLED` 都算未知，TC 只会重试**不会回滚**——这是刻意的。

真要让它回滚，业务必须**明确**返回 HTTP 409 / gRPC `ABORTED` / 响应体带 `FAILURE`。

### 2. 分支耗时超过 10 秒

TC 的分支调用超时是 10 秒。分支处理慢的话，让它立刻返回 **HTTP 425**（`ONGOING`）而不是干等——TC 会下轮再来。

### 3. `local://` 分支没注册（嵌入式/绑定）

日志里会有：

```
本地分支未注册，按结果未知处理（会重试，不回滚）
```

新版本删了 handler 或改了名字就会这样。这是**部署问题不是业务失败**，所以只重试。把 handler 注册回来，事务会自己继续。

### 4. msg 事务停在 `prepared`

msg 的 `prepared` 表示 TC 还没确认这单该不该执行，在等回查。看：

- `query_prepared` 地址填了吗（没填的话 prepare 会被拒，不会走到这）
- 回查接口通不通？它返回什么？
- 回查返回 `ONGOING` 或超时的话，TC 会一直退避重试——**不能当成「没提交」**

### 5. workflow 重放走岔了

日志里会有：

```
workflow 重放走岔了，已停止推进，需要人工介入
```

说明你的 workflow 函数不是确定性的，同一个位置这次跑了不同的分支。这时候 TC **既不落成功也不回滚**——那时已经不知道真实进度，硬回滚更危险。

改回确定性的代码（副作用全部放进 `branch().run()` 里），重启就能接着推。

---

## 事务变成 `failed`，但业务上不该失败

看 `rollback_reason`。如果它像 `分支 02 返回 FAILURE`，说明**你的分支主动要求了回滚**。

最常见的原因：**业务代码把「超时/异常」也返回成了 409 或 `FAILURE`**。

```rust
// ✗ 常见错误
match call_downstream().await {
    Ok(_) => StatusCode::OK,
    Err(_) => StatusCode::CONFLICT,   // 超时也变成 409 了！
}

// ✓ 应该这样
match call_downstream().await {
    Ok(_) => StatusCode::OK,
    Err(e) if e.is_business_rejection() => StatusCode::CONFLICT,
    Err(_) => StatusCode::INTERNAL_SERVER_ERROR,   // 未知 → 让 TC 重试
}
```

详见 [接入指南的返回值语义](integration.md#三返回值语义写错就会数据不一致)。

---

## 补偿被调用了，但正向分支根本没执行过

这是**正常的**，不是 bug。

TC 回滚时会补偿**所有**分支，不只是成功的那些——因为某个 action 可能超时了但实际执行成功了。宁可多发补偿，不可漏发。

多余的补偿应该由[子事务屏障](integration.md#二子事务屏障一张表解决三个问题)空转掉（返回 `NullCompensation`）。**如果你的补偿真的执行了业务逻辑，说明屏障没接或者接错了。**

---

## 同一个分支被调用了多次

也是正常的。TC 重试 + 崩溃恢复必然导致重复调用。

**分支必须幂等**——靠子事务屏障保证。如果你观察到重复扣款，检查：

1. 屏障表和业务表在同一个数据库实例吗？
2. 业务 SQL 在 `decide` 拿到的那个事务里吗？
3. `Duplicated` 时你返回的是成功还是失败？（必须成功）

---

## XA 相关

### 无关的 UPDATE 突然无限期阻塞

**几乎可以肯定是有未解决的 prepared 事务在持锁。**

```sql
SELECT gid, prepared, age(now(), prepared) FROM pg_prepared_xacts;   -- Postgres
XA RECOVER;                                                          -- MySQL
```

找到之后，确认对应的全局事务状态再决定 commit 还是 rollback。**别盲目 rollback**——如果全局事务已经决定提交、别的分支已经 commit 了，回滚这个分支就是一半提交一半回滚。

应急（确认过之后）：

```sql
ROLLBACK PREPARED 'xid';   -- 或 COMMIT PREPARED 'xid'
```

### `55P03 lock timeout` / `innodb_lock_wait_timeout`

多半是**几个 XA 分支操作了相同的数据**。这不是 bug，是 XA 的本质：一个分支对应一个资源管理器，真实场景里天然分布在不同库。

把分支拆成操作不相交的数据。

### `max_prepared_transactions = 0`

Postgres 的两阶段提交**默认是关的**：

```bash
postgres -c max_prepared_transactions=32
```

### xid 相关的报错

MySQL 的 gtrid 只有 64 字节（Postgres 是 200）。dtmrs 会对超长的 gid 做「截断 + 拼哈希摘要」，不会撞车。如果你看到 xid 相关的报错，先确认 gid 长度。

---

## 存储相关

### `database is locked`（sqlite）

sqlite 撑不住并发写。**别用它跑多实例**，换 Postgres。

### 提交时报「超长」

`gid` 上限 128 字符，`payload` 8192，url / 回查地址 1024。

这个校验是**故意在应用层做的**——MySQL 的 `INSERT IGNORE` 遇到超长值会静默截断，而截断的 gid 会让两笔不相关的事务在屏障表里撞成同一行。宁可提交失败。

### Postgres 启动时 `duplicate key ... pg_type_typname_nsp_index`

多个实例同时建表撞上了 Postgres 的系统目录竞态。dtmrs 的建表带重试，正常不会看到这个错误逃出来。如果看到了，说明重试次数不够——重启一次通常就好。

### Redis：事务莫名消失

两个可能：

1. **到了终态被 TTL 回收了**（默认 7 天）。这是设计如此
2. **Redis 崩溃丢了数据**。默认 `appendfsync everysec` 会丢最后一秒的写入——这是 Redis 后端的固有取舍，见 [部署文档](deployment.md#redis-的三条语义差异)

---

## 构建/安装相关

### 编译时报 protoc 相关错误

正常情况下不需要装 protoc——找不到会自动用自带的那份。如果仍然报错，可以：

```bash
export PROTOC=$(which protoc)        # 指定系统的
# 或者关掉 gRPC
cargo build --no-default-features --features server
```

### `cargo install dtmrs-server` 装不出二进制

从 0.2.0 起二进制在 `dtmrs` 这个包里：

```bash
cargo install dtmrs
```

---

## 还是没解决

带上这些信息开 [issue](https://github.com/jackwangfeng/dtmrs/issues)：

- `curl 'localhost:36789/api/dtmsvr/query?gid=xxx'` 的完整输出
- TC 日志里该 gid 相关的行（`RUST_LOG=info`）
- 存储类型和版本、dtmrs 版本、用的哪种模式
