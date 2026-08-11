# dtmrs 文档

> These guides are in Chinese. For an English overview see the [main README](../README.md);
> the Rust API reference on [docs.rs](https://docs.rs/dtmrs) is language-neutral.

## 按你现在要做什么找

| 我想… | 看这篇 |
|---|---|
| 先跑起来看看是什么效果 | [五分钟快速上手](quickstart.md) |
| **搞清楚该用哪种模式** | [五种模式怎么选](choosing-a-mode.md) |
| **把我的服务接进去** | [业务侧接入指南](integration.md) |
| 上生产 / 多实例 / 配监控 | [部署与运维](deployment.md) |
| 查接口字段和返回码 | [API 参考](api.md) |
| 出问题了 | [排错](troubleshooting.md) |
| 了解协议和内部设计 | [DESIGN.md](../DESIGN.md) |
| 查 Rust 库 API | [docs.rs/dtmrs](https://docs.rs/dtmrs) |
| 非 Rust 服务要接入 | [各语言屏障实现](../clients/) |

## 如果只读两篇

**[五种模式怎么选](choosing-a-mode.md)** 和 **[业务侧接入指南](integration.md)**。

这两件事做错的后果都不是报错，而是**静默的数据不一致**——等你发现时已经对不上账了。其它的都可以边用边查。

## 三条最容易写错的语义

赶时间的话至少记住这三条，每条都有测试钉着：

**1. 超时 ≠ 失败。** 5xx、连接超时、gRPC 的 `UNAVAILABLE` / `DEADLINE_EXCEEDED` / `CANCELLED` 都表示**结果未知**——对方可能已经成功了。这时候回滚会造成不一致，正确做法是重试到拿到明确结论。只有 HTTP 409、gRPC `ABORTED`、或响应体里的 `FAILURE` 才触发补偿。

**2. 分支一定会被重复调用。** TC 重试 + 崩溃恢复必然导致重复。业务侧**必须**接子事务屏障，把屏障记录和业务 SQL 放进同一个本地事务。

**3. 回滚会补偿所有分支，不只是成功的那些。** 因为某个 action 可能超时了但实际执行成功了。宁可多发补偿（多余的由屏障空转掉），不可漏发。

## 其它

- [CHANGELOG](../CHANGELOG.md) —— 版本变更，含破坏性变更说明
- [CONTRIBUTING](../CONTRIBUTING.md) —— 参与开发（含「没配环境变量就是没测」那套规矩）
