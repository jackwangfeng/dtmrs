# dtmrs_close 返回时 sqlite 连接还没关完：先析构运行时、后丢连接池

- 报告方：keel（通过 FFI 嵌入 dtmrs，Go 侧 `internal/dtm`）
- 版本：dtmrs v0.11.0（c6dc313），sqlx / sqlx-sqlite 0.8.6
- 严重程度：中。不丢数据，但 `dtmrs_close` 返回后宿主**无法确认存储已经释放**，
  任何「关闭后立刻删目录 / 挪文件 / 重新打开同一个库」的宿主都会偶发出错。

## 现象

keel 的 `TestCloseIsIdempotent` 偶发失败：

```
TempDir RemoveAll cleanup: unlinkat /tmp/TestCloseIsIdempotent.../001: directory not empty
```

测试做的事情很简单：在 `t.TempDir()` 里用 `sqlite:<dir>/dtm.db` 启动嵌入式协调器，
调两次 `dtmrs_close`，测试结束时 Go 删掉这个临时目录。

实测数据（keel 侧，改之前）：

- 4 个进程各跑 150 次，600 次里失败 6 次。
- `dtmrs_close` 刚返回的那一刻：tokio 线程已经是 0，**sqlx-sqlite 的线程还剩 16 个**，
  要再过约 300 ms 才全部退出。
- 30 次里有 8 次，`dtmrs_close` 返回之后目录里的内容（`dtm.db-wal` / `dtm.db-shm`）
  还在变化。

## 根因（三层）

1. **`dtmrs_close` 只是 drop 掉 `Box<DtmrsTc>`**（`crates/dtmrs-ffi/src/lib.rs:1139`）。
   Rust 按声明顺序析构字段，而 `DtmrsTc`（`lib.rs:150`）里 `rt: tokio::runtime::Runtime`
   排在第一个，装着 sqlx 连接池的 `tc: Option<Embedded>` 排在后面。
   结果是：**运行时先没了，连接池后被丢弃，而且从来没有被 `close().await` 过**。
   `impl Drop for Embedded`（`crates/dtmrs-server/src/embedded.rs:324`）只中止了推进器任务，
   没有碰连接池。
2. **sqlx-sqlite 每条连接占一个专属 OS 线程**，spawn 之后就把 JoinHandle 丢了。
   连接被 drop 时只是关掉命令通道，线程自己随后执行 `sqlite3_close`，没有任何人等它结束。
   所以 drop 返回 ≠ 连接已关闭。
3. **WAL 模式下，最后一条连接关闭时会做 checkpoint 并删除 `-wal` / `-shm`**；而关闭那一刻
   还在建立中的连接，又会把这两个文件重新建出来。宿主删目录的动作正好和这些文件的
   删除、重建交错在一起，就撞上了 `ENOTEMPTY`。

注释 `// Embedded 的 Drop 会停掉推进器；Runtime 的 Drop 等待任务收尾` 的后半句
只对 tokio 任务成立；sqlx-sqlite 的连接线程不属于 tokio，Runtime 的 Drop 等不到它们。

宿主这一侧拿到的只是一个已经返回的 C 函数，**没有任何可以等待的句柄**，
所以正经的修法只能在 dtmrs 里做。

## 建议修法

在 `dtmrs_close` 里先在运行时上把存储关干净，再析构运行时：

```rust
pub extern "C" fn dtmrs_close(tc: *mut DtmrsTc) {
    if tc.is_null() { return; }
    let mut tc = unsafe { Box::from_raw(tc) };
    if let Some(embedded) = tc.tc.take() {
        // 停推进器，并 await 连接池的 close()：
        // sqlx 的 Pool::close() 会等所有连接真正关闭（sqlite 的每连接线程执行完 sqlite3_close）
        tc.rt.block_on(embedded.shutdown());
    }
    drop(tc); // 这时再析构运行时
}
```

需要配套做的：

- 给 `Embedded` 加一个 `pub async fn shutdown(self)`（或 `close(&self)`）：
  中止并 await 推进器任务，然后 `pool.close().await`。`Drop` 保留现在的行为，作为兜底。
- 另外两种保险可以二选一，也可以都做：
  - 调整 `DtmrsTc` 的字段顺序，把 `rt` 放到最后，这样即使走 Drop 路径也是先丢池、后丢运行时
    （但不能替代 `close().await`：drop 池子仍然不会等连接线程）。
  - 在 `Embedded` 的其他使用方（server、Node / Python 绑定）里也走 `shutdown`。
- **幂等性**：keel 会对同一个句柄调两次 close 吗？不会，Go 侧已经保证只调一次；
  但 `take()` 让重复关闭天然安全，建议保持。

## 验收标准

- `dtmrs_close` 返回后，本进程内 sqlx-sqlite 的线程数立刻回落到启动前的数量
  （Linux 上可以数 `/proc/self/task/*/comm`，keel 就是这么测的）。
- `dtmrs_close` 返回后立刻 `rm -rf` 数据目录，反复跑几百次不再出现 `ENOTEMPTY`。
- 建议在 dtmrs-ffi 里加一条测试：start → close → 立刻删除目录，循环 N 次。

## keel 侧目前的绕法（修好后可以删）

keel 新增了 `internal/dtm/dtmtest.SQLiteDSN`：在 `t.TempDir` 删除目录之前，
等本进程的 sqlx-sqlite 线程数回落到 Start 之前的数量，最多等 10 秒，超时就报错并说明在等什么。
刻意没有做「忽略 ENOTEMPTY」或「删不掉就重试」，因为那样会把真正的连接泄漏也一起掩盖掉。

改完之后：4 个进程各 150 次、8 个进程各 150 次，一共 1800 次，失败 0 次。

dtmrs 修好并发版后，keel 升级依赖，就可以把这个等待逻辑删掉
（或者改成只做断言：`Close` 返回时线程数必须已经回落）。

参考：keel 提交 2c1ed6c 的提交信息里有完整的排查过程。
