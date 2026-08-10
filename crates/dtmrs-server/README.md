# dtmrs-server

[dtmrs](https://github.com/jackwangfeng/dtmrs) 的事务协调器（TC）。
对标 [DTM](https://github.com/dtm-labs/dtm)，Apache-2.0。

支持 **SAGA / TCC / 二阶段消息 / XA / workflow** 五种模式，
对外有 **HTTP 和 gRPC** 两套等价接口。

```bash
cargo install dtmrs-server        # 装出来的二进制叫 dtmrs
DTMRS_DB=sqlite:dtmrs.db dtmrs
```

**也可以当库嵌进你自己的进程**（DTM 做不到的形态）—— 不用单独部署服务，
分支可以直接是进程内的函数：

```rust
let tc = Embedded::builder("sqlite:app.db")
    .handler("扣款", |_| async { BranchResult::Success })
    .start().await?;
tc.saga("order-1").step("local://扣款", "local://扣款撤销").submit().await?;
```

完整文档（含中英双语 README、设计说明、各语言绑定）见
[github.com/jackwangfeng/dtmrs](https://github.com/jackwangfeng/dtmrs)。

Apache-2.0。
