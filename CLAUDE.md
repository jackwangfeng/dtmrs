# CLAUDE.md

给 Claude Code 用的项目指引。详细设计见 `DESIGN.md`，对外说明见 `README.md`。

## 这是什么

dtmrs：Rust 写的分布式事务管理器（对标 Go 的 [DTM](https://github.com/dtm-labs/dtm)）。
支持 SAGA / TCC / 二阶段消息 / XA / workflow 五种模式，存储可跑 sqlite / postgres / mysql / redis。
对外有 HTTP 和 gRPC 两套等价接口。
除了独立部署的 TC 二进制，还有 DTM 没有的**嵌入式形态**：TC 当库链进宿主进程，
分支可以是进程内函数；再往外通过 C ABI 给任何语言用。

Apache-2.0。只实现 DTM 的协议，不抄它的代码。

## 常用命令

```bash
cargo build --release                 # 二进制在 target/release/dtmrs
cargo test --workspace                # 156 个测试（真库那部分会被跳过，见下）
cargo run --example embedded -p dtmrs-server   # 嵌入式模式的可运行示例
cargo run --example workflow -p dtmrs-server   # workflow 模式（重放/断点续跑）

# 起 TC（HTTP 和 gRPC 各占一个端口）
DTMRS_DB=sqlite:dtmrs.db DTMRS_ADDR=127.0.0.1:36789 ./target/release/dtmrs
```

环境变量：`DTMRS_DB`（默认 `sqlite:dtmrs.db`）、`DTMRS_ADDR`（默认 `0.0.0.0:36789`）、
`DTMRS_GRPC_ADDR`（默认 `0.0.0.0:36790`）、`DTMRS_OWNER`（默认 `tc-<pid>`，
多实例部署时用来区分租约持有者）。

编 gRPC 需要 **protoc**；关掉 `grpc` feature 就不需要（`dtmrs-ffi` 就是这么做的）。

四个语言绑定：

```bash
cargo build -p dtmrs-ffi --release    # → target/release/libdtmrs.so（所有绑定都靠它）
python bindings/python/example.py     # 纯 ctypes，DTMRS_LIB 可指定 .so 路径
cd bindings/node && npm install && node example.js
cd bindings/java && ./run.sh          # 自动下载 jna jar，不需要 maven/gradle
gcc -I include examples/c/demo.c -L target/release -ldtmrs -o demo && ./demo
```

## 测试：跳过 ≠ 通过

真数据库的测试全部靠环境变量开启，**没配就是没跑**（XA 那 6 个在没配时会打印醒目
提示后直接返回，仍然显示为 passed —— 别把它当成验过了）：

```bash
DTMRS_TEST_PG='postgres://postgres:pw@127.0.0.1:5432/dtmrs' \
DTMRS_TEST_MYSQL='mysql://root:pw@127.0.0.1:3306/dtmrs' \
DTMRS_TEST_XA_PG='postgres://postgres:pw@127.0.0.1:5432/dtmrs' \
DTMRS_TEST_XA_MYSQL='mysql://root:pw@127.0.0.1:3306/dtmrs' \
DTMRS_TEST_REDIS='redis://127.0.0.1:6379/0' \
cargo test --workspace --features dtmrs/redis,dtmrs/barrier-redis,dtmrs-server/redis,dtmrs-store/redis,dtmrs-barrier/redis
```

改动 `dtmrs-store` / `dtmrs-barrier` / `dtmrs-xa` / dialect 层的话，**必须对着真
Postgres 和真 MySQL 跑一遍**，否则等于没测 —— 三家的行为差异正是这几个 crate 存在的理由。
Postgres 的 2PC 默认关着，跑 XA 测试要 `postgres -c max_prepared_transactions=32`。

其它约定：
- store 的单测用 `PG_LOCK` 串行化 —— `lock_one_due` / `list_recent` 是全局查询，
  并行跑会互相看见对方的事务。加新测试时沿用 `backends()` 拿到的守卫。
- XA 测试每个用例必须用**专属的行**：留在库里的 prepared 事务会永久持锁，
  串味会让不相关的测试无限期挂住。连接上都设了 5 秒锁超时把「卡住」变成「报错」。
- e2e 测试的套路是起一个 axum 假业务服务 + 原子计数器，断言**具体的失效模式**
  （补偿顺序、调用次数），不是「跑通了就行」。

## 代码约定

**注释和测试名都用中文。** 测试函数名就是中文断言句，例如
`fn 超时不能触发回滚而要重试()`、`fn confirm失败绝不能触发cancel()`。
写新测试沿用这个风格 —— 这些名字本身就是规格说明。

模块头的文档注释承载了大量「实测撞出来的坑」，改相关行为时同步更新那些表格。

## 架构

```
crates/
  dtmrs/          门面 crate：重新导出各层 + dtmrs 二进制（main.rs 在这儿）。
                  feature 切分：server(默认) / grpc / barrier / xa / full
  dtmrs-core/     状态机 + 类型，纯逻辑无 I/O。dialect.rs 是 SQL 方言渲染
  dtmrs-store/    存储 + 租约抢占。Store 是个 enum 分发器：
                  SQL 后端(sqlx::Any 跑三种库) / Redis 后端(redis_store.rs，
                  要开 redis feature)
  dtmrs-server/   TC：api.rs(协议无关的操作层，HTTP 与 gRPC 共用)
                  main.rs(axum HTTP) / grpc/(tonic，调分支 + 提供 API)
                  driver.rs(推进器) / registry.rs(进程内分支表)
                  workflow.rs(workflow 模式) / embedded.rs(嵌入式门面)
  dtmrs-barrier/  客户端子事务屏障。SQL 版靠「加入业务的本地事务」，
                  Redis 版（redis feature）靠「屏障判定和业务操作同一个 Lua 脚本」，
                  判定语义两边必须逐条一致，测试用例名一一对应
  dtmrs-xa/       业务方(RM)的 XA 助手，pg / mysql 两套语法
  dtmrs-ffi/      C ABI（cdylib + staticlib，产物名 libdtmrs）
```

**两条结构约束，都别破坏：**

1. `dtmrs-core` 里的 `saga_advance` / `tcc_advance` / `msg_advance` / `xa_advance` /
   `workflow_advance` 决定「下一步做什么」，`driver.rs` 只负责把决策落成网络调用
   和状态更新。**状态迁移逻辑不要往 driver 里写** —— 放在 core 才能穷举单测，
   分布式事务的 bug 绝大多数就在状态迁移上。
   （workflow 是唯一的例外，而且是**受控的例外**：正向走向由用户函数决定，
   core 仍然拥有「何时跑函数、何时补偿、按什么顺序补」，见 `workflow_advance` 的文档。）
2. HTTP（`main.rs`）和 gRPC（`grpc/server.rs`）都只做协议转换，业务判断只在
   `api.rs`。**别在任一协议层里加判断** —— 两边漂移的后果是「同一个请求走 HTTP
   被拒、走 gRPC 却受理了」。

二进制在 `crates/dtmrs`（门面 crate）里，`dtmrs-server` 是纯库 —— 这样
`cargo install dtmrs` 和 `cargo add dtmrs` 都是那个显而易见的名字。

⚠ `dtmrs-ffi` 的 crate-type **刻意不带 rlib**：它的 lib 名是 `dtmrs`（为了产出
`libdtmrs.so`），带上 rlib 会跟门面 crate 的 rlib 撞同一个输出文件名。

## 绝对不能破坏的语义

这几条每条都有测试钉着，改动前先读对应测试：

1. **超时 ≠ 失败。** 5xx / 连接超时是 `BranchResult::Unknown`，只重试不回滚 ——
   对方可能已经成功了。只有 HTTP 409、gRPC `ABORTED`(10)、或响应体含 `FAILURE`
   才触发补偿。gRPC 侧尤其注意：`UNAVAILABLE`(14) / `DEADLINE_EXCEEDED`(4) /
   `CANCELLED`(1) 全是 Unknown（`CANCELLED` 是我们自己放弃，不是对方拒绝）。
   FFI 层同理：宿主返回野值、抛异常、panic、拉取式超时未回填，一律按 UNKNOWN 处理。

2. **confirm / commit 失败绝不能转成 cancel / rollback。** `tcc_advance` 和
   `xa_advance` 在 `Submitted` 阶段**永远不返回** `Finish(Aborting)`，测试里穷举了
   3×3 种分支状态组合来钉这条。唯一正确处理是无限重试 + 报警。

3. **回滚时补偿所有分支，不只是成功的那些。** action 超时但实际执行成功的情况
   必须靠补偿兜住，多余的补偿由屏障空转掉。宁可多发，不可漏发。

4. **重复提交同一个 gid 必须成功而不是报错**（`INSERT OR IGNORE` + 返回 SUCCESS），
   否则客户端会以为没受理。

5. **TCC / XA 必须先 registerBranch 再做一阶段。** 反过来会导致一阶段成功但 TC 不
   知道有这个分支：TCC 是资源永久泄漏，XA 更糟 —— 留下永久持锁的 prepared 事务。

## 写 SQL 的规矩（dialect 层）

模板统一写 `?` 占位符（跟 MySQL 一致），非 MySQL 后端由 `Backend::q` 转成 `$1..$n`。

- ⚠ **模板的字符串字面量里不能出现 `?`**，会被当成占位符
- 冲突忽略写 `{INS} ... {NOCONFLICT}`，由 `q()` 展开成 `INSERT IGNORE`（MySQL）
  或 `INSERT INTO ... ON CONFLICT DO NOTHING`（其它）。
  **MySQL 上不能用 `ON DUPLICATE KEY UPDATE`** —— 重复时 `rows_affected` 返回 1
  而不是 0，幂等判断会全错
- 列类型走 `Backend::id_text()` / `id_short()` / `text(n)`。MySQL 上自由文本列
  只能是 `VARCHAR` —— 经 `sqlx::Any` 读 MySQL 的 `TEXT` 一律报 BLOB 类型不匹配
- 索引是二选一：`create_index()`（非 MySQL）/ `inline_index()`（MySQL 建表时内联），
  另一边返回空值
- 整数列一律 `BIGINT`（postgres 的 `INTEGER` 只有 4 字节，装不下时间戳）；
  时间统一存 unix 秒（i64），不用数据库的 datetime 类型
- 写库前必须用 `check_len` 挡超长值。MySQL 的 `INSERT IGNORE` 遇到超长会**静默截断**，
  gid 被截断会让两笔不相关的事务在屏障表里撞成同一行

- 抢占待办的那条 SELECT（`lock_one_due`）有两条**性能正确性**约束，都有测试钉着
  （`并发抢占要各拿各的不能全挤在同一笔上`）：必须带 `Backend::skip_locked()`，
  且**不能加 `ORDER BY next_cron_time`**。少了前者所有 worker 抢同一行，
  加了后者 MySQL 会 filesort、把全部待办行锁光。理由写在那个函数的注释里

新增后端或改这层，先读 `crates/dtmrs-core/src/dialect.rs` 的模块头注释（有三家行为对照表）。

## 几个容易踩的实现细节

- `Store::open` 里的建表带重试：Postgres 的 `CREATE TABLE IF NOT EXISTS` 在并发下
  会撞 `pg_type` 唯一键，只有真起两个实例才能暴露。别把重试去掉。
- 分支号是 `format!("{:02}", index + 1)`（`branch_id()`），SAGA 一步产生
  两行（action + compensate）共用同一个 branch_id，靠 op 区分。
- 嵌入式的 `local://名字` 存的是**名字**（闭包没法持久化），重启后必须注册同名
  handler。`submit()` 时就检查名字是否都注册了，漏注册按「结果未知」处理只重试。
- FFI 的回调一律走 `spawn_blocking` —— 宿主 handler 是同步的还可能抢 GIL，
  直接在 tokio worker 上调会卡死运行时。
- **FFI 有两套分发，别混淆**：回调式（`dtmrs_register`，Python/Java/C 用）和
  拉取式（`dtmrs_register_pull` + `next_task` + `reply`，Node 用）。
  Node 必须用拉取式不是因为跨线程回调不行（实测可以），而是因为 C ABI 的回调
  必须同步返回 int，而 Node 的业务代码全是 async。改这块前先读
  `bindings/node/dtmrs.js` 的文件头。
- **workflow 模式靠重放**：函数会被从头跑多次，已成功的分支走记忆化。改
  `workflow.rs` 时记住两条不变量：补偿必须**先于**正向动作登记（否则动作超时
  或崩溃会漏掉补偿）；分岔检测发现名字对不上时**既不能成功也不能回滚**，
  只能停下等人。
- **Redis 后端的行为必须跟 SQL 后端一致**。`schedulable()` 在 Rust 和 Lua 里
  各有一份（后者防索引腐烂），改调度条件要**两处同时改**，否则同一笔事务
  在两种后端上的推进行为会不一样。Redis 测试必须串行（`REDIS_LOCK`）——
  `lock_one_due` 是全局扫描，且每个测试开头会清前缀。
- gRPC 分支调用用自定义 `BytesCodec` 做动态转发，**不需要业务方的 proto**。
  改 `grpc/client.rs` 时注意：请求体发空字节是刻意的（空 protobuf 消息对任何
  message 类型都合法），分支身份走 metadata。
- MySQL 的 XA 语句不能走预处理协议（错误 1295），所以两阶段相关语句用
  `sqlx::raw_sql`；xid 因此只能拼进 SQL，注入防护靠 `xid_for()` 的字符白名单。
  MySQL 的 gtrid 只有 64 字节，超长时截断 + 拼 FNV-1a 摘要，**不能直接截断**。
