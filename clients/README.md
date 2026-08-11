# 业务侧客户端（子事务屏障）

给**业务服务（RM）**接入 dtmrs 用。Java / Go / Python / Node 各一份。

## 为什么只有屏障，没有「提交事务」的 SDK

调 TC 的 `submit` / `prepare` / `registerBranch` 就是几个 HTTP POST，任何语言用
自带的 HTTP 客户端三行就写完了——为它封装 SDK 是纯样板代码。

**真正需要库的是分支侧**，而且它跟网络无关，是**操作数据库**的：

分支接口一定会被重复调用（TC 重试 + 崩溃恢复），而且可能乱序到达。
要正确处理这个，必须在业务库里记「这个分支执行过没有」，并且**和业务写入
共享同一个本地事务**。这就是子事务屏障。

算法本身只有十几行，但外面裹着一圈坑（见下），写错了不报错，是**静默的重复扣款**。
所以这里给出各语言的参考实现。

## 用哪个

| 语言 | 文件 | 依赖 |
|---|---|---|
| Java | [`java/Barrier.java`](java/Barrier.java) | 只要 JDBC，无第三方依赖 |
| Go | [`go/barrier.go`](go/barrier.go) | 只要 `database/sql`，无第三方依赖 |
| Python | [`python/dtmrs_barrier.py`](python/dtmrs_barrier.py) | 只要 DB-API 2.0 游标 |
| Node | [`node/barrier.js`](node/barrier.js) | 无（适配 pg / mysql2 的返回格式） |
| Rust | [`dtmrs-barrier`](https://crates.io/crates/dtmrs-barrier) | 已发布到 crates.io |

**都是单文件、零框架依赖，直接复制进你的项目就行**（Rust 那个除外，走 cargo）。
目前没有发布到 Maven / npm / PyPI——等有人真的用起来、反馈说抄文件麻烦了再说。

## 用法

四个语言形状一致，以 Go 为例：

```go
// 启动时建表一次
dtmrs.Migrate(db, dtmrs.MySQL)

// 每次处理分支请求（gid / branchID / op / transType 由 TC 传进来）
tx, _ := db.Begin()
defer tx.Rollback()

b := dtmrs.NewBarrier(dtmrs.MySQL, transType, gid, branchID, op)
dec, err := b.Decide(tx)
if dec == dtmrs.Execute {
    // 业务 SQL —— 必须用这个 tx
    tx.Exec("UPDATE account SET balance = balance - ? WHERE id = ?", amt, uid)
}
tx.Commit()   // 原子性的来源：屏障记录与业务变更同生共死
```

三种判定：

| 判定 | 你该做什么 |
|---|---|
| `Execute` | 执行业务逻辑 |
| `NullCompensation` | **空回滚**——正向从没跑过，补偿空转，**接口返回成功** |
| `Duplicated` | **重复或悬挂**——已处理过，跳过，**接口返回成功** |

后两种是正常路径，返回失败会让 TC 以为分支出错了。

## ⚠ 三条不能违反的

**1. 屏障表必须和业务表在同一个数据库实例。** 不同实例没法共用一个本地事务，
方案直接失效。这不是实现限制，是它成立的根本条件。

**2. 业务 SQL 必须用传给 `decide` 的那个连接/事务。** 用另一个连接执行等于白做——
崩在中间会出现「业务改了但屏障没记」。

**3. MySQL 上绝不能用 `ON DUPLICATE KEY UPDATE`。** 整个算法依赖「冲突时影响行数
必须是 0」，而它在重复时返回的是 **1**。必须用 `INSERT IGNORE`。

第 3 条在 Java 测试里有一条专门的实测对照：

```
✓ MySQL 的 INSERT IGNORE 重复时必须返回 0（实测 1 / 0）
✓ 对照：ON DUPLICATE KEY UPDATE 重复时返回 1（≠0，所以绝不能用它做幂等判断）
```

## 跑测试

每个实现都有一套**相同的**五场景测试：首次执行、幂等、空回滚、悬挂、真补偿。
四个语言 × Postgres / MySQL 都实跑通过。

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

## 算法是怎么回事

一句话：**补偿方先去正向分支的位置上占个坑。占成功了说明正向从没来过（空回滚）；
占失败了说明正向真跑过（是真补偿）。而这个坑一旦被占，迟到的正向分支就再也
插不进来（悬挂被丢弃）。**

完整推演见 [docs/integration.md](../docs/integration.md)。
