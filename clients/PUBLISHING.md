# 发布客户端库

三个 registry，认证方式差别很大。

| Registry | 认证 | 现状 |
|---|---|---|
| **PyPI** | OIDC（trusted publishing） | 零 token，全自动 |
| **npm** | OIDC（trusted publishing） | 零 token，自带 provenance |
| **Maven Central** | GPG 签名 + user token | 没有 OIDC，只能这样 |

## 日常发版（npm / PyPI）

改版本号 → 推 → GitHub Actions 里手动触发
[`publish-clients.yml`](../.github/workflows/publish-clients.yml)，选 target 和
是否 dry_run。**默认 dry_run=true**，确认输出没问题再关掉重跑。

仓库里和本机都**不需要任何 token**。

## 一次性配置

### PyPI

支持 *pending publisher*，全新项目也能直接走 OIDC。
[Publishing → Add a new pending publisher](https://pypi.org/manage/account/publishing/)：

| 字段 | 值 |
|---|---|
| PyPI Project Name | `dtmrs-barrier` |
| Owner / Repository | `jackwangfeng` / `dtmrs` |
| Workflow name | `publish-clients.yml` |
| Environment name | `pypi` |

GitHub 那边还要建一个叫 `pypi` 的 Environment（Settings → Environments）。

### npm

**包必须先存在**——信任发布是在包的设置页里配的。全新包要先手动发一次：

```bash
NPM_OTP=123456 ./publish.sh node     # 账号开了 2FA 就要带动态码
```

然后在 `https://www.npmjs.com/package/<包名>/access` → Trusted Publisher：

| 字段 | 值 |
|---|---|
| Organization or user | `jackwangfeng` |
| Repository | `dtmrs` |
| Workflow filename | `publish-clients.yml` |
| Environment name | **留空** |
| Allowed actions | 勾 `npm publish` |

配好后本机的 token 就可以吊销了。

## ⚠ 踩过的坑

**npm**

- **Environment name 填了但对不上** → 一直报 `OIDC token exchange error - package not found`。
  这个报错措辞极具误导性，看着像包不存在，实际是信任配置匹配不上
- **`setup-node` 的 `registry-url` 必须保留** → 去掉直接 `ENEEDAUTH`。
  npm 靠它知道跟哪个 registry 做 OIDC 交换；它写的 `NODE_AUTH_TOKEN` 占位符无害
- **别用 `npm install -g npm@latest`** → 新版 npm 会抬高 Node 要求，把自己搞挂
  （撞过：npm@12 要 Node ≥22.22，而 runner 是 22.14）。钉大版本 `npm@11`
- **SSH 下 `npm login` 走不通** → npm 把登录 URL 里的会话 ID 脱敏成了 `***`，
  复制出去必然 404。用 token 或 OIDC

**Maven Central**

- **SSH 下签名报 `Inappropriate ioctl for device`** → 默认 pinentry 是
  `pinentry-gnome3`，要图形界面。改 `~/.gnupg/gpg-agent.conf`：
  `pinentry-program /usr/bin/pinentry-curses`，并且 `export GPG_TTY=$(tty)`
- **插件报 `Unrecognized field "warnings"`** → 这时候**构件其实已经上传成功了**，
  崩的是插件轮询状态时解析 JSON。升级 `central-publishing-maven-plugin` 到 0.11+。
  遇到这个别重发，先用 API 查真实状态：

  ```bash
  AUTH=$(printf 'user:pass' | base64 -w0)
  curl -s -X POST -H "Authorization: Bearer $AUTH" \
    "https://central.sonatype.com/api/v1/publisher/status?id=<deploymentId>"
  ```

- `settings.xml` 里 `<id>` 必须是 `central`（pom 的 `publishingServerId` 写死了）。
  Portal 给的模板里是 `${server}`，**那是占位符**，照抄会连不上
- `autoPublish=false`：上传后停在 Portal 等人确认。
  **Maven Central 发布后永远删不掉**，别跳过这步

## 手动发布（不走 CI）

```bash
cd clients && ./publish.sh            # 三个都发
./publish.sh npm                      # 或单发一个
NPM_OTP=123456 ./publish.sh node      # 账号开了 2FA
```

Maven 那条要先 `export JAVA_HOME` 且 gpg-agent 里有缓存的 passphrase。
