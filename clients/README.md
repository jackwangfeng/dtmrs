# 业务侧客户端（子事务屏障）

给**业务服务（RM）**接入 dtmrs 用。Java / Go / Python / Node / Rust 五个语言。

## 为什么只有屏障，没有「提交事务」的 SDK

调 TC 的 `submit` / `prepare` / `registerBranch` 就是几个 HTTP POST，任何语言用
自带的 HTTP 客户端三行就写完了——为它封装 SDK 是纯样板代码。

**真正需要库的是分支侧**，而且它跟网络无关，是**操作数据库**的：

分支接口一定会被重复调用（TC 重试 + 崩溃恢复），而且可能乱序到达。
要正确处理这个，必须在业务库里记「这个分支执行过没有」，并且**和业务写入
共享同一个本地事务**。这就是子事务屏障。

算法本身只有十几行，但外面裹着一圈坑（见下），写错了不报错，是**静默的重复扣款**。

## 安装

| 语言 | 安装 | 依赖 |
|---|---|---|
| Java | `io.github.jackwangfeng:dtmrs-barrier:0.3.0` | 只要 JDBC |
| Go | `go get github.com/jackwangfeng/dtmrs/clients/go` | 只要 `database/sql` |
| Python | `pip install dtmrs-barrier` | 只要 DB-API 2.0 游标 |
| Node | `npm install dtmrs-barrier` | 无（自带 TS 类型） |
| Rust | `cargo add dtmrs --features barrier` | sqlx |

都是**零运行时依赖**的小库，也可以直接把单个源文件复制进项目。

---

## 完整例子

下面每段都是从 [`<语言>/example/`](.) 里**实际跑通的服务**摘出来的，不是手写的伪代码。
`./verify-examples.sh` 会起真的 dtmrs TC + 真 MySQL，对四个语言各跑三个场景
（正常提交 / 逆序补偿 / 幂等），**断言账户余额**——CI 里每次都跑。

用的是各语言的标准库 HTTP 服务，没有框架依赖。换成 Spring / Gin / Flask / Express
时，屏障那部分代码一个字都不用改，只是取参数的方式不同。

### 共同结构

```
取出 TC 传来的 gid / trans_type / branch_id / op
  ↓
开事务
  ↓
barrier.decide(tx)
  ├─ Execute          → 执行业务 SQL（必须用同一个 tx）
  ├─ NullCompensation → 什么都不做
  └─ Duplicated       → 什么都不做
  ↓
提交 → 200 SUCCESS
```

### Go

完整文件：[`go/example/main.go`](go/example/main.go)

```go
func branch(sign int) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        q := r.URL.Query()

        tx, err := db.Begin()
        if err != nil {
            w.WriteHeader(500) // 结果未知 → TC 重试
            return
        }
        defer tx.Rollback() // commit 之后再 rollback 是空操作，安全

        b := dtmrs.NewBarrier(dtmrs.MySQL, q.Get("trans_type"),
            q.Get("gid"), q.Get("branch_id"), q.Get("op"))

        dec, err := b.Decide(tx)
        if err != nil {
            w.WriteHeader(500) // 屏障本身出错也算「未知」
            return
        }

        if dec == dtmrs.Execute && sign != 0 {
            res, err := tx.Exec(
                "UPDATE ex_account SET balance = balance + ? WHERE id = 1 AND balance + ? >= 0",
                int64(sign)*amount, int64(sign)*amount)
            if err != nil {
                w.WriteHeader(500)
                return
            }
            if n, _ := res.RowsAffected(); n == 0 {
                // 余额不足 = 业务**明确**拒绝 → 409，TC 会逆序补偿
                w.WriteHeader(409)
                fmt.Fprint(w, `{"dtm_result":"FAILURE"}`)
                return
            }
        }
        // 空回滚 / 重复请求走到这里，什么都没做，同样返回成功
        if err := tx.Commit(); err != nil {
            w.WriteHeader(500)
            return
        }
        fmt.Fprint(w, `{"dtm_result":"SUCCESS"}`)
    }
}
```

### Java

完整文件：[`java/example/Service.java`](java/example/Service.java)

```java
static void branch(HttpExchange e, int sign) {
    Map<String, String> q = query(e);

    try (Connection c = DriverManager.getConnection(JDBC)) {
        c.setAutoCommit(false);
        try {
            Barrier b = new Barrier(Dialect.MYSQL,
                    q.get("trans_type"), q.get("gid"), q.get("branch_id"), q.get("op"));

            if (b.decide(c) == Decision.EXECUTE && sign != 0) {
                try (PreparedStatement ps = c.prepareStatement(
                        "UPDATE ex_account SET balance = balance + ? "
                      + "WHERE id = 1 AND balance + ? >= 0")) {
                    ps.setLong(1, sign * amount);
                    ps.setLong(2, sign * amount);
                    if (ps.executeUpdate() == 0) {
                        c.rollback();
                        reply(e, 409, "FAILURE");   // 余额不足 = 业务明确拒绝
                        return;
                    }
                }
            }
            // 空回滚 / 重复请求走到这里，什么都没做，同样返回成功
            c.commit();
            reply(e, 200, "SUCCESS");
        } catch (Exception ex) {
            c.rollback();
            throw ex;
        }
    } catch (Exception ex) {
        // 异常 = 结果**未知** → 5xx 让 TC 重试。绝不能返回 409
        reply(e, 500, "ONGOING");
    }
}
```

Spring Boot 里就是把 `HttpExchange` 换成 `@RequestParam`、`DriverManager` 换成
注入的 `DataSource`，中间那段一模一样。

### Python

完整文件：[`python/example/service.py`](python/example/service.py)

```python
conn = pymysql.connect(**CONN)
try:
    b = Barrier(MYSQL, q["trans_type"], q["gid"], q["branch_id"], q["op"])
    cur = conn.cursor()
    if b.decide(cur) == Decision.EXECUTE and sign != 0:
        n = cur.execute(
            "UPDATE ex_account SET balance = balance + %s "
            "WHERE id = 1 AND balance + %s >= 0",
            (sign * amount, sign * amount))
        if n == 0:
            conn.rollback()
            return self.reply(409, "FAILURE")   # 余额不足 = 业务明确拒绝
    # 空回滚 / 重复请求走到这里，什么都没做，同样返回成功
    conn.commit()
    self.reply(200, "SUCCESS")
except Exception:
    conn.rollback()
    # 异常 = 结果**未知** → 5xx 让 TC 重试。绝不能返回 409
    self.reply(500, "ONGOING")
finally:
    conn.close()
```

### Node

完整文件：[`node/example/service.js`](node/example/service.js)

```js
// ⚠ 必须拿独占连接：事务要在同一个连接上跑完。
//   直接用 pool 的话每条语句可能落到不同连接，事务就散了
const conn = await pool.getConnection();
try {
  await conn.query('BEGIN');
  const b = new Barrier(MYSQL, q.trans_type, q.gid, q.branch_id, q.op);

  if (await b.decide(conn) === Decision.EXECUTE && sign !== 0) {
    const [r] = await conn.query(
      'UPDATE ex_account SET balance = balance + ? WHERE id = 1 AND balance + ? >= 0',
      [sign * amount, sign * amount]);
    if (r.affectedRows === 0) {
      await conn.query('ROLLBACK');
      return reply(res, 409, 'FAILURE');   // 余额不足 = 业务明确拒绝
    }
  }
  // 空回滚 / 重复请求走到这里，什么都没做，同样返回成功
  await conn.query('COMMIT');
  reply(res, 200, 'SUCCESS');
} catch (e) {
  await conn.query('ROLLBACK').catch(() => {});
  // 异常 = 结果**未知** → 5xx 让 TC 重试。绝不能返回 409
  reply(res, 500, 'ONGOING');
} finally {
  conn.release();
}
```

### Rust

Rust 侧的屏障用法见 [docs/integration.md](../docs/integration.md#二子事务屏障一张表解决三个问题)，
或者直接用嵌入式形态（分支就是进程内函数，连 HTTP 都不需要）：
[`cargo run --example embedded -p dtmrs-server`](../crates/dtmrs-server/examples/embedded.rs)。

### gRPC 分支怎么取参数

上面都是 HTTP（参数在 query string）。gRPC 分支的四个值在 **metadata** 里：

```
dtm-gid  /  dtm-trans_type  /  dtm-branch_id  /  dtm-op
```

请求体是**空字节**，所以你**不需要为 dtmrs 改接口**——任何已有的 gRPC 方法都能
直接当分支。屏障部分的代码完全一样，只是参数来源不同。

---

## 自己跑一遍例子

```bash
# 需要真 MySQL（默认连 127.0.0.1:33306）和编译好的 dtmrs 二进制
cargo build --release -p dtmrs
cd clients && ./verify-examples.sh all      # 或 go / python / node / java
```

输出长这样：

```
--- go ---
  ✓ 正常提交后事务成功（succeed）
  ✓ 扣款生效（900）
  ✓ 第二步拒绝后事务失败（failed）
  ✓ 补偿把钱退回来了（900）
  ✓ 重复调用没有扣第二次（幂等）（900）
```

---

## 三种判定，两种要返回成功

| 判定 | 你该做什么 |
|---|---|
| `Execute` | 执行业务逻辑 |
| `NullCompensation` | **空回滚**——正向从没跑过，补偿空转，**返回成功** |
| `Duplicated` | **重复或悬挂**——已处理过，跳过，**返回成功** |

后两种是**正常路径**，返回失败会让 TC 以为分支出错了、一直重试。

## 返回值语义（写错就会数据不一致）

| 你的返回 | HTTP | gRPC | TC 的动作 |
|---|---|---|---|
| 成功 | 200 | `OK` | 推进下一个分支 |
| **业务明确拒绝** | **409** | **`ABORTED`** | **逆序补偿** |
| 还在处理中 | 425 | `FAILED_PRECONDITION` | 下轮再来 |
| **结果未知** | 5xx / 超时 | 其它任何码 | **重试，绝不回滚** |

**只有你确定业务规则不允许时**才返回 409。库存不足、余额不够、风控拒绝 → 409；
数据库超时、调下游超时、自己抛异常 → **5xx**。

因为**超时的时候你可能已经执行成功了**。返回 409 会让 TC 去补偿，而如果那笔
操作其实没执行，你就凭空退了一笔钱出去。

## ⚠ 三条不能违反的

**1. 屏障表必须和业务表在同一个数据库实例。** 不同实例没法共用一个本地事务，
方案直接失效。这不是实现限制，是它成立的根本条件。

**2. 业务 SQL 必须用传给 `decide` 的那个连接/事务。** 用另一个连接执行等于白做——
崩在中间会出现「业务改了但屏障没记」，重试就再执行一遍。

**3. MySQL 上绝不能用 `ON DUPLICATE KEY UPDATE`。** 整个算法依赖「冲突时影响行数
必须是 0」，而它在重复时返回的是 **1**。必须用 `INSERT IGNORE`。

第 3 条在 Java 测试里有一条专门的实测对照：

```
✓ MySQL 的 INSERT IGNORE 重复时必须返回 0（实测 1 / 0）
✓ 对照：ON DUPLICATE KEY UPDATE 重复时返回 1（≠0，所以绝不能用它做幂等判断）
```

## 算法是怎么回事

一句话：**补偿方先去正向分支的位置上占个坑。占成功了说明正向从没来过（空回滚）；
占失败了说明正向真跑过（是真补偿）。而这个坑一旦被占，迟到的正向分支就再也
插不进来（悬挂被丢弃）。**

一个 `INSERT IGNORE` 的返回值同时回答了「对方来过没有」和「我处理过没有」
两个问题——这就是它只要十几行的原因。完整推演见
[docs/integration.md](../docs/integration.md)。

## 业务数据在 Redis 里（秒杀）

上面那套屏障要求「屏障记录和业务 SQL 在同一个本地事务里提交」。
**秒杀的库存通常就在 Redis 里，没有 SQL 事务可以加入** —— 那就用 Redis 版：
原子性来源换成「屏障判定和业务操作在同一个 Lua 脚本里」。

| 语言 | 文件 | 入口 |
|---|---|---|
| Go | `go/barrier_redis.go` | `NewRedisBarrier(...).CheckAdjustAmount(eval, key, -1)` |
| Python | `python/dtmrs_barrier_redis.py` | `RedisBarrier(...).check_adjust_amount(r, key, -1)` |
| Node | `node/barrier-redis.js` | `new RedisBarrier(...).checkAdjustAmount(evalFn, key, -1)` |
| Java | `java/src/main/java/dtmrs/RedisBarrier.java` | `new RedisBarrier(...).checkAdjustAmount(eval, key, -1)` |

**四个实现都不引入任何 Redis 库**：只要你给一个「执行 Lua」的回调
（Go 是 `RedisEval` 接口，Java 是 `Eval` 接口，Python 要 `.eval()` 方法，
Node 要 `(script, keys, args) => Promise`）。用 go-redis / Jedis / redis-py /
ioredis 都行，自己包一层即可 —— 各语言文件头都给了一行的适配示例。

不是加减法的业务用 `call()` 传自己的 Lua。判定语义和 Rust 版逐条一致。

⚠ **两处跟 SQL 版的行为差异**（介质决定的）：屏障键会过期（默认 7 天，
**必须长于事务的最大生命周期**，短了会漏补偿）；业务失败要由脚本
`return 'FAILURE'` 表达。详见 [docs/integration.md](../docs/integration.md)。

## 跑测试

每个实现都有一套**相同的**五场景测试：首次执行、幂等、空回滚、悬挂、真补偿。
四个语言 × Postgres / MySQL 都实跑通过（CI 里每次都跑）。

Redis 屏障另有一套 7 条的测试，四个语言**用例名一一对应**（跟 Rust 版也对应）——
名字对不上就说明有一边跑偏了。加环境变量即可：

```bash
DTMRS_TEST_REDIS_GO='127.0.0.1:6379'    # go/    ：go test -run TestRedis
DTMRS_TEST_REDIS_PY='127.0.0.1:6379'    # python/：python3 test_barrier_redis.py
DTMRS_TEST_REDIS_NODE='127.0.0.1:6379'  # node/  ：node test-redis.js
DTMRS_TEST_REDIS_JAVA='127.0.0.1:6379'  # java/  ：./run-test.sh
```

```bash
# Java
cd java && DTMRS_TEST_PG='jdbc:postgresql://127.0.0.1:5432/dtmrs?user=postgres&password=pw' \
           DTMRS_TEST_MYSQL='jdbc:mysql://127.0.0.1:3306/dtmrs?user=root&password=pw' ./run-test.sh

# Go
cd go && DTMRS_TEST_PG_GO='postgres://postgres:pw@127.0.0.1:5432/dtmrs?sslmode=disable' \
         DTMRS_TEST_MYSQL_GO='root:pw@tcp(127.0.0.1:3306)/dtmrs' go test -v

# Python
cd python && pip install psycopg2-binary pymysql
DTMRS_TEST_PG_PY='postgresql://postgres:pw@127.0.0.1:5432/dtmrs' \
DTMRS_TEST_MYSQL_PY='127.0.0.1|3306|root|pw|dtmrs' python3 test_barrier.py

# Node
cd node && npm install
DTMRS_TEST_PG_NODE='postgresql://postgres:pw@127.0.0.1:5432/dtmrs' \
DTMRS_TEST_MYSQL_NODE='mysql://root:pw@127.0.0.1:3306/dtmrs' node test.js
```

**没配环境变量就是没测，不是通过**——每个测试在跳过时都会打印醒目提示。

## 发布

见 [PUBLISHING.md](PUBLISHING.md)——npm 和 PyPI 走 OIDC 零 token，Maven Central
还得靠 GPG + token。踩过的坑都记在那儿了。
