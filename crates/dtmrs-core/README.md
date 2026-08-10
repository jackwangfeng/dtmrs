# dtmrs-core

[dtmrs](https://github.com/jackwangfeng/dtmrs) 的状态机与类型层 —— **纯逻辑，不碰 I/O**。

分布式事务的 bug 绝大多数出在状态迁移上，所以这一层被刻意隔离出来，
可以不起网络、不连数据库地穷举单测。

- `saga_advance` / `tcc_advance` / `msg_advance` / `xa_advance` / `workflow_advance`
- `BranchResult::from_http` / `from_grpc` —— 「超时 ≠ 失败」这条命门就在这里
- `dialect` —— sqlite / postgres / mysql 的 SQL 方言渲染

完整文档（含中英双语 README、设计说明、各语言绑定）见
[github.com/jackwangfeng/dtmrs](https://github.com/jackwangfeng/dtmrs)。

Apache-2.0。
