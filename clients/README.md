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
| Java | [`java/src/main/java/dtmrs/Barrier.java`](java/src/main/java/dtmrs/Barrier.java) | 只要 JDBC，无第三方依赖 |
| Go | [`go/barrier.go`](go/barrier.go) | 只要 `database/sql`，无第三方依赖 |
| Python | [`python/dtmrs_barrier.py`](python/dtmrs_barrier.py) | 只要 DB-API 2.0 游标 |
| Node | [`node/barrier.js`](node/barrier.js) | 无（适配 pg / mysql2 的返回格式） |
| Rust | [`dtmrs-barrier`](https://crates.io/crates/dtmrs-barrier) | 已发布到 crates.io |

都是**零框架依赖**的小库，也可以直接把单个文件复制进项目。

```xml
<!-- Maven -->
<dependency>
  <groupId>io.github.jackwangfeng</groupId>
  <artifactId>dtmrs-barrier</artifactId>
  <version>0.2.0</version>
</dependency>
```

```bash
pip install dtmrs-barrier
npm install dtmrs-barrier
cargo add dtmrs --features barrier
```

Go 走 `go get github.com/jackwangfeng/dtmrs/clients/go`（Go 的模块机制不需要中心
仓库，打了 tag 就能拉）。

发布用 [`publish.sh`](publish.sh)。

### SSH 环境怎么认证（没法弹浏览器）

`npm login` 在 npm 9+ 默认走 web 流程要开浏览器。SSH 上三选一：

**① 用 token（推荐）** —— 在浏览器（你本机）打开
[npmjs.com Access Tokens](https://www.npmjs.com/settings/~/tokens) 生成一个
**Granular Access Token**（权限选 Read and write，范围可以只给这一个包），然后：

```bash
echo '//registry.npmjs.org/:_authToken=npm_你的token' >> ~/.npmrc
chmod 600 ~/.npmrc
npm whoami --registry https://registry.npmjs.org/   # 验证
```

token 可以限定权限、随时吊销，比存登录态安全。

**② 终端登录**（不开浏览器，走用户名密码 + OTP）：

```bash
npm login --auth-type=legacy --registry https://registry.npmjs.org/
```

**③ web 流程 —— SSH 下走不通，别试了**

`npm login` 会打印登录 URL，但 **npm 把 URL 里的会话 ID 当密钥脱敏成了 `***`**
（调试日志里也是 `***`，捞不出来），所以那条链接复制出去必然 404。
加 `--browser false` 能避免 `xdg-open` 报错，但 URL 照样是脱敏的 —— 这条路
在无浏览器环境下等于自己把自己堵死。**用方式 ① 的 token。**

### 开了 2FA 怎么发布

token 认证通过不代表能发布。如果账号开了 2FA 而 token 没勾
**Bypass two-factor authentication**，`npm publish` 会报：

```
403 Two-factor authentication or granular access token with bypass 2fa enabled is required
```

两条路：

```bash
# ① 发布时补一个动态码（推荐，token 权限更小）
NPM_OTP=123456 ./publish.sh node

# ② 或者重新生成 token 时勾上 "Bypass two-factor authentication"
#    ⚠ npm 正在收紧这类 token，长期看方式 ① 更稳
```

### 推荐：Trusted Publishing（OIDC），不存任何 token

仓库里有现成的工作流 [`.github/workflows/publish-clients.yml`](../.github/workflows/publish-clients.yml)，
GitHub Actions 用 OIDC 直接向 registry 证明身份，**不需要任何长期 token，也不需要 2FA 交互**。

一次性配置：

**npm** —— 到 npmjs.com 的**包设置页** → Trusted Publisher → 填：

| 字段 | 值 |
|---|---|
| Organization or user | `jackwangfeng` |
| Repository | `dtmrs` |
| Workflow filename | `publish-clients.yml` |
| Allowed actions | 勾 `npm publish` |

> ⚠ npm 的信任发布是在**包的设置页**里配的，所以包**得先存在**。全新的包需要
> 先手动发一次（`NPM_OTP=xxxxxx ./publish.sh node`），之后就能全走 OIDC。
>
> ⚠ 工作流**文件名**是配置的一部分，改名会导致发布失败。
>
> ⚠ **Environment name 必须留空**（除非工作流的 npm job 里也设了同名 environment）——
> 填了但对不上会一直报 `OIDC token exchange error - package not found`，
> 而这个报错措辞有误导性，看着像包不存在。
>
> ⚠ setup-node 的 `registry-url` **必须保留**。去掉会直接 ENEEDAUTH ——
> npm 靠它知道跟哪个 registry 做 OIDC 交换。它写的 NODE_AUTH_TOKEN 占位符无害。

**PyPI** —— PyPI 支持 *pending publisher*，**全新项目也能直接走 OIDC**，不用先手动发：
[Publishing → Add a new pending publisher](https://pypi.org/manage/account/publishing/)，填项目名
`dtmrs-barrier`、owner `jackwangfeng`、repo `dtmrs`、workflow `publish-clients.yml`、
environment `pypi`。

配好之后在 Actions 页手动触发这个工作流，选 target 和是否 dry_run。
**默认是 dry_run=true**，确认输出没问题再关掉重跑。

> ⚠ **如果你的 npm registry 指向淘宝等镜像**（`npm config get registry` 看一下），
> 那是**只读镜像，发布会失败**。认证和发布都必须对着 `registry.npmjs.org`。
> `package.json` 里的 `publishConfig` 已经把发布目标钉死了，但**认证仍需
> 显式带 `--registry`**，否则 `npm whoami` 查的是镜像。

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
