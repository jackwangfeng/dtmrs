# dtmrs-barrier

子事务屏障：**一张表 + 一条 `INSERT IGNORE`**，同时解决三个经典难题。

| 难题 | 场景 |
|---|---|
| 幂等 | TC 重试导致同一分支被调两次 |
| 空回滚 | action 还没跑就来了 compensate，补偿必须空转 |
| 悬挂 | compensate 先到、action 后到，晚到的必须被丢弃 |

接入 [dtmrs](https://github.com/jackwangfeng/dtmrs) 的业务方**必须用它** ——
分支接口一定会被重复调用（重试 + 崩溃恢复）。

**前提**：屏障表要和业务表在同一个数据库实例，才能共用一个本地事务。

完整文档（含中英双语 README、设计说明、各语言绑定）见
[github.com/jackwangfeng/dtmrs](https://github.com/jackwangfeng/dtmrs)。

Apache-2.0。
