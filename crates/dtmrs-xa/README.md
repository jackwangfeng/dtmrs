# dtmrs-xa

XA 两阶段提交的**业务方（RM）**助手，Postgres 和 MySQL 两套语法自动分流。

不靠补偿，靠数据库原生的两阶段提交：业务 SQL 跑完就 prepare，
改动已持久化但对外不可见，等 TC 统一决定 commit 还是 rollback。

⚠ **没解决的 prepared 事务会永久持锁**（Postgres 上还阻塞 VACUUM）。
`Xa::list_prepared` 必须上监控 —— 这比补偿没跑成严重得多。

配 [dtmrs](https://github.com/jackwangfeng/dtmrs) 使用。

完整文档（含中英双语 README、设计说明、各语言绑定）见
[github.com/jackwangfeng/dtmrs](https://github.com/jackwangfeng/dtmrs)。

Apache-2.0。
