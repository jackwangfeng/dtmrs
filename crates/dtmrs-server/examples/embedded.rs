//! 嵌入式 TC 最小可运行示例：`cargo run --example embedded`
//!
//! 演示的是「不部署任何额外服务」跑一个分布式事务 —— TC 就在这个进程里。
//! 第一笔成功，第二笔第 2 步要求回滚，看补偿是否逆序执行。

use dtmrs_core::BranchResult;
use dtmrs_server::embedded::Embedded;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = std::env::temp_dir().join("dtmrs_example.db");
    let _ = std::fs::remove_file(&db);

    let tc = Embedded::builder(&format!("sqlite:{}", db.display()))
        .handler("扣款", |c| async move {
            println!("  [扣款]      gid={} branch={}", c.gid, c.branch_id);
            BranchResult::Success
        })
        .handler("扣款撤销", |c| async move {
            println!("  [扣款撤销]  gid={} ← 补偿", c.gid);
            BranchResult::Success
        })
        .handler("发货", |c| async move {
            println!("  [发货]      gid={}", c.gid);
            BranchResult::Success
        })
        .handler("发货撤销", |c| async move {
            println!("  [发货撤销]  gid={} ← 补偿", c.gid);
            BranchResult::Success
        })
        .handler("库存不足", |c| async move {
            println!("  [库存不足]  gid={} → 明确要求回滚", c.gid);
            BranchResult::Failure
        })
        .tick(Duration::from_millis(20))
        .start()
        .await?;

    println!("\n① 正常下单（两步都成功）");
    tc.saga("order-1")
        .step("local://扣款", "local://扣款撤销")
        .step("local://发货", "local://发货撤销")
        .submit()
        .await?;
    println!("  结果: {:?}", tc.wait_final("order-1", Duration::from_secs(5)).await?);

    println!("\n② 库存不足（第 2 步要求回滚，应逆序补偿）");
    tc.saga("order-2")
        .step("local://扣款", "local://扣款撤销")
        .step("local://库存不足", "local://发货撤销")
        .submit()
        .await?;
    println!("  结果: {:?}", tc.wait_final("order-2", Duration::from_secs(5)).await?);

    println!("\n③ 漏注册的 handler 会在提交时就被拦住");
    match tc
        .saga("order-3")
        .step("local://扣款", "local://还没写的补偿")
        .submit()
        .await
    {
        Err(e) => println!("  提交被拒: {e}"),
        Ok(()) => println!("  不该走到这儿"),
    }

    let _ = std::fs::remove_file(&db);
    Ok(())
}
