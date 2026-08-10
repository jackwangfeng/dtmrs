# dtmrs-store

[dtmrs](https://github.com/jackwangfeng/dtmrs) 的存储层：**一套 SQL 跑三种数据库**
（sqlite / postgres / mysql），靠 `sqlx::Any` 加一层薄薄的方言渲染，
没有抽 `Store` trait、没有三份 SQL。

也包含多实例部署要用的 `owner` 租约抢占（`lock_one_due`）。

完整文档（含中英双语 README、设计说明、各语言绑定）见
[github.com/jackwangfeng/dtmrs](https://github.com/jackwangfeng/dtmrs)。

Apache-2.0。
