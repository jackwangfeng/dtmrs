//! workflow 模式：把事务流程写成一个普通函数，崩溃后从断点续跑。
//!
//! ```bash
//! cargo run --example workflow -p dtmrs-server
//! ```
//!
//! 演示三件 saga 做不到的事：
//!   ① 控制流依赖上一步的返回值
//!   ② 崩溃重启后不重做已完成的步骤
//!   ③ 中途要求回滚时，只补偿**已经跑到**的分支

use dtmrs_core::{BranchResult, GlobalStatus};
use dtmrs_server::embedded::Embedded;
use dtmrs_server::workflow::{WorkflowCtx, WorkflowError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = "sqlite:/tmp/dtmrs_workflow_demo.db";
    let _ = std::fs::remove_file(db.trim_start_matches("sqlite:"));

    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let 扣款次数 = Arc::new(AtomicUsize::new(0));
    let 入账尝试 = Arc::new(AtomicUsize::new(0));

    macro_rules! say {
        ($log:expr, $($t:tt)*) => { $log.lock().unwrap().push(format!($($t)*)) };
    }
    macro_rules! flush {
        ($log:expr) => {
            for s in $log.lock().unwrap().drain(..) {
                println!("  {s}");
            }
        };
    }

    // ---------- ① 控制流依赖返回值 ----------
    println!("① 控制流依赖上一步的返回值（saga 做不到：步骤得提前声明）");
    {
        let l = log.clone();
        let tc = Embedded::builder(db)
            .tick(Duration::from_millis(20))
            .handler("退货", |_| async { BranchResult::Success })
            .workflow("按需发货", move |mut wf: WorkflowCtx| {
                let l = l.clone();
                async move {
                    // 第一步查出订单类型，第二步做不做取决于它
                    let kind = wf
                        .branch("查订单类型")
                        .run_with(|| {
                            let l = l.clone();
                            async move {
                                say!(l, "[查订单类型] → 虚拟商品");
                                (BranchResult::Success, "虚拟商品".to_string())
                            }
                        })
                        .await?;

                    if kind == "实物" {
                        wf.branch("发货")
                            .on_rollback("local://退货")
                            .run(|| async { BranchResult::Success })
                            .await?;
                    } else {
                        say!(l, "[跳过发货] 虚拟商品不需要物流");
                    }
                    Ok::<(), WorkflowError>(())
                }
            })
            .start()
            .await?;

        tc.submit_workflow("wf-1", "按需发货", "").await?;
        let s = tc.wait_final("wf-1", Duration::from_secs(10)).await?;
        flush!(log);
        println!("  结果: {s:?}\n");
    }

    // ---------- ② 崩溃重启，已完成的步骤不重做 ----------
    println!("② 崩溃重启 → 已完成的步骤不重做（这是 workflow 模式存在的理由）");
    macro_rules! 转账workflow {
        () => {{
            let (l, k, r) = (log.clone(), 扣款次数.clone(), 入账尝试.clone());
            move |mut wf: WorkflowCtx| {
                let (l, k, r) = (l.clone(), k.clone(), r.clone());
                async move {
                    wf.branch("扣款")
                        .on_rollback("local://退款")
                        .run(|| {
                            let (l, k) = (l.clone(), k.clone());
                            async move {
                                let n = k.fetch_add(1, Ordering::SeqCst) + 1;
                                say!(l, "[扣款] 真的执行了（累计第 {n} 次）");
                                BranchResult::Success
                            }
                        })
                        .await?;

                    wf.branch("入账")
                        .on_rollback("local://冲正")
                        .run(|| {
                            let (l, r) = (l.clone(), r.clone());
                            async move {
                                if r.fetch_add(1, Ordering::SeqCst) == 0 {
                                    say!(l, "[入账] 超时，结果未知 → 只重试，不回滚");
                                    BranchResult::Unknown
                                } else {
                                    say!(l, "[入账] 成功");
                                    BranchResult::Success
                                }
                            }
                        })
                        .await?;
                    Ok::<(), WorkflowError>(())
                }
            }
        }};
    }

    {
        println!("  --- 进程 A ---");
        let tc = Embedded::builder(db)
            .tick(Duration::from_millis(20))
            .handler("退款", |_| async { BranchResult::Success })
            .handler("冲正", |_| async { BranchResult::Success })
            .workflow("转账", 转账workflow!())
            .start()
            .await?;
        tc.submit_workflow("wf-2", "转账", "").await?;
        for _ in 0..100 {
            if 入账尝试.load(Ordering::SeqCst) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        flush!(log);
        println!("  状态: {:?}（进程 A 到此被杀）", tc.status("wf-2").await?);
        // tc drop = 进程没了
    }

    println!("  --- 进程 B（同一个库，全新的 TC，客户端没有重新提交）---");
    let tc = Embedded::builder(db)
        .tick(Duration::from_millis(20))
        .handler("退款", |_| async { BranchResult::Success })
        .handler("冲正", |_| async { BranchResult::Success })
        .workflow("转账", 转账workflow!())
        .start()
        .await?;
    let s = tc.wait_final("wf-2", Duration::from_secs(15)).await?;
    flush!(log);
    println!(
        "  结果: {s:?}   扣款总共执行了 {} 次 ← 重启没有重做它",
        扣款次数.load(Ordering::SeqCst)
    );
    drop(tc);

    // ---------- ③ 中途回滚，只补已经跑到的分支 ----------
    println!("\n③ 第三步要求回滚 → 只逆序补偿已经跑到的分支");
    {
        let l = log.clone();
        let mk = |name: &'static str, l: Arc<Mutex<Vec<String>>>| {
            move |_ctx: dtmrs_server::registry::BranchCtx| {
                let l = l.clone();
                async move {
                    say!(l, "[{name}] ← 补偿");
                    BranchResult::Success
                }
            }
        };
        let tc = Embedded::builder(db)
            .tick(Duration::from_millis(20))
            .handler("退款", mk("退款", log.clone()))
            .handler("退货", mk("退货", log.clone()))
            .workflow("会被风控拒绝", move |mut wf: WorkflowCtx| {
                let l = l.clone();
                async move {
                    for (名字, 补偿) in [("扣款", "local://退款"), ("发货", "local://退货")]
                    {
                        let l2 = l.clone();
                        wf.branch(名字)
                            .on_rollback(补偿)
                            .run(move || {
                                let l2 = l2.clone();
                                async move {
                                    say!(l2, "[{名字}]");
                                    BranchResult::Success
                                }
                            })
                            .await?;
                    }
                    // 没给 on_rollback：这一步没有副作用，不需要补偿
                    wf.branch("风控")
                        .run(|| {
                            let l = l.clone();
                            async move {
                                say!(l, "[风控] → 明确要求回滚");
                                BranchResult::Failure
                            }
                        })
                        .await?;
                    Ok::<(), WorkflowError>(())
                }
            })
            .start()
            .await?;

        tc.submit_workflow("wf-3", "会被风控拒绝", "").await?;
        let s = tc.wait_final("wf-3", Duration::from_secs(10)).await?;
        flush!(log);
        println!("  结果: {s:?}");
        assert_eq!(s, GlobalStatus::Failed);
    }

    let _ = std::fs::remove_file(db.trim_start_matches("sqlite:"));
    Ok(())
}
