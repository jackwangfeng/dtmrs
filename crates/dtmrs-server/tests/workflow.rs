//! workflow 模式端到端。
//!
//! 重点验三件在别的模式里不存在的事：
//!
//! 1. **重放不重做**已完成的步骤（这是这个模式存在的理由）
//! 2. 分支返回值被记忆化，重放时原样还回来
//! 3. 函数不确定时能**当场发现**并停下，而不是静默补偿错对象
//!
//! 外加照例的那条命门：超时只重试，绝不回滚。

use dtmrs_core::{BranchResult, BranchStatus, GlobalStatus, TransType};
use dtmrs_server::driver::Driver;
use dtmrs_server::workflow::{WorkflowCtx, WorkflowError, WorkflowRegistry};
use dtmrs_store::Store;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

async fn store() -> Store {
    Store::open("sqlite::memory:").await.unwrap()
}

/// 建一个挂了 workflow 注册表的推进器
fn driver(st: &Store, wf: WorkflowRegistry) -> Driver {
    Driver::new(st.clone(), "tc-1".into()).with_workflows(Arc::new(wf))
}

/// 提交一个 workflow 事务（绕开 Embedded，直接写库，测试里更好控制）
async fn submit(st: &Store, gid: &str, name: &str, input: &str) {
    let mut g = dtmrs_server::tcc_rows(gid);
    g.trans_type = TransType::Workflow;
    g.status = GlobalStatus::Submitted;
    g.payload = serde_json::json!({"name": name, "input": input}).to_string();
    st.create_global(&g, &[]).await.unwrap();
}

async fn global(st: &Store, gid: &str) -> dtmrs_store::GlobalRow {
    st.get_global(gid).await.unwrap().unwrap()
}

#[tokio::test]
async fn workflow_正常跑完() {
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let mut wf = WorkflowRegistry::new();
    wf.register("下单", move |mut ctx: WorkflowCtx| {
        let h = h.clone();
        async move {
            ctx.branch("建订单")
                .on_rollback("local://取消订单")
                .run(|| {
                    let h = h.clone();
                    async move {
                        h.fetch_add(1, Ordering::SeqCst);
                        BranchResult::Success
                    }
                })
                .await?;
            ctx.branch("扣款")
                .on_rollback("local://退款")
                .run(|| {
                    let h = h.clone();
                    async move {
                        h.fetch_add(1, Ordering::SeqCst);
                        BranchResult::Success
                    }
                })
                .await?;
            Ok(())
        }
    });

    let st = store().await;
    let d = driver(&st, wf);
    submit(&st, "wf-ok", "下单", "").await;
    let g = global(&st, "wf-ok").await;
    d.process(&g).await.unwrap();

    assert_eq!(global(&st, "wf-ok").await.status, GlobalStatus::Succeed);
    assert_eq!(hits.load(Ordering::SeqCst), 2, "两步各跑一次");

    // 分支行按序登记了，补偿也在（虽然没用上）
    let rows = st.list_branches("wf-ok").await.unwrap();
    let actions: Vec<_> = rows
        .iter()
        .filter(|r| r.op == dtmrs_core::BranchOp::Action)
        .map(|r| (r.branch_id.as_str(), r.url.as_str(), r.status))
        .collect();
    assert_eq!(
        actions,
        vec![
            ("01", "建订单", BranchStatus::Succeed),
            ("02", "扣款", BranchStatus::Succeed)
        ]
    );
}

#[tokio::test]
async fn 重放不重做已完成的步骤() {
    // **这是 workflow 模式存在的理由。**
    // 第一次跑到第二步卡住（结果未知），重试时第一步绝不能再跑一遍 ——
    // 那就是重复扣款。
    let step1 = Arc::new(AtomicUsize::new(0));
    let step2 = Arc::new(AtomicUsize::new(0));
    // 第二步头一次返回「结果未知」，之后返回成功
    let flaky = Arc::new(AtomicUsize::new(0));

    let (s1, s2, fl) = (step1.clone(), step2.clone(), flaky.clone());
    let mut wf = WorkflowRegistry::new();
    wf.register("转账", move |mut ctx: WorkflowCtx| {
        let (s1, s2, fl) = (s1.clone(), s2.clone(), fl.clone());
        async move {
            ctx.branch("扣款")
                .on_rollback("local://退款")
                .run(|| {
                    let s1 = s1.clone();
                    async move {
                        s1.fetch_add(1, Ordering::SeqCst);
                        BranchResult::Success
                    }
                })
                .await?;

            ctx.branch("入账")
                .on_rollback("local://冲正")
                .run(|| {
                    let (s2, fl) = (s2.clone(), fl.clone());
                    async move {
                        s2.fetch_add(1, Ordering::SeqCst);
                        if fl.fetch_add(1, Ordering::SeqCst) == 0 {
                            BranchResult::Unknown // 第一次：超时
                        } else {
                            BranchResult::Success
                        }
                    }
                })
                .await?;
            Ok(())
        }
    });

    let st = store().await;
    let d = driver(&st, wf);
    submit(&st, "wf-replay", "转账", "").await;

    // 第一轮：第一步成功，第二步结果未知 → 整单等重试
    let g = global(&st, "wf-replay").await;
    d.process(&g).await.unwrap();
    assert_eq!(
        global(&st, "wf-replay").await.status,
        GlobalStatus::Submitted,
        "结果未知只能重试，绝不能转 aborting"
    );
    assert_eq!(step1.load(Ordering::SeqCst), 1);
    assert_eq!(step2.load(Ordering::SeqCst), 1);

    // 第二轮（模拟 cron 重新捞起 / 进程重启后续推）
    let g = global(&st, "wf-replay").await;
    d.process(&g).await.unwrap();

    assert_eq!(global(&st, "wf-replay").await.status, GlobalStatus::Succeed);
    assert_eq!(
        step1.load(Ordering::SeqCst),
        1,
        "第一步已经成功过，重放时必须跳过 —— 再跑一次就是重复扣款"
    );
    assert_eq!(step2.load(Ordering::SeqCst), 2, "第二步上次没成，该重跑");
}

#[tokio::test]
async fn 分支返回值被记忆化() {
    // 重放时把上次的返回值原样还回来，后续步骤才能沿着同一条路走
    let gen_count = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let flaky = Arc::new(AtomicUsize::new(0));

    let (gc, sn, fl) = (gen_count.clone(), seen.clone(), flaky.clone());
    let mut wf = WorkflowRegistry::new();
    wf.register("带返回值", move |mut ctx: WorkflowCtx| {
        let (gc, sn, fl) = (gc.clone(), sn.clone(), fl.clone());
        async move {
            // 第一步生成一个 id。**每次真跑都会生成不同的值** ——
            // 如果记忆化没生效，第二轮就会拿到不一样的 id
            let oid = ctx
                .branch("建订单")
                .run_with(|| {
                    let gc = gc.clone();
                    async move {
                        let n = gc.fetch_add(1, Ordering::SeqCst);
                        (BranchResult::Success, format!("order-{n}"))
                    }
                })
                .await?;

            ctx.branch("用这个id")
                .run(|| {
                    let (sn, fl, oid) = (sn.clone(), fl.clone(), oid.clone());
                    async move {
                        sn.lock().unwrap().push(oid);
                        if fl.fetch_add(1, Ordering::SeqCst) == 0 {
                            BranchResult::Unknown
                        } else {
                            BranchResult::Success
                        }
                    }
                })
                .await?;
            Ok(())
        }
    });

    let st = store().await;
    let d = driver(&st, wf);
    submit(&st, "wf-memo", "带返回值", "").await;

    let g = global(&st, "wf-memo").await;
    d.process(&g).await.unwrap();
    let g = global(&st, "wf-memo").await;
    d.process(&g).await.unwrap();

    assert_eq!(global(&st, "wf-memo").await.status, GlobalStatus::Succeed);
    assert_eq!(
        gen_count.load(Ordering::SeqCst),
        1,
        "生成函数只该真跑一次，第二轮走记忆化"
    );
    let s = seen.lock().unwrap().clone();
    assert_eq!(s.len(), 2);
    assert_eq!(s[0], s[1], "两轮拿到的 id 必须一样，否则后续步骤会走岔");
    assert_eq!(s[0], "order-0");
}

#[tokio::test]
async fn 失败时逆序补偿已登记的分支() {
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let l = log.clone();
    let mut wf = WorkflowRegistry::new();
    wf.register("会失败", move |mut ctx: WorkflowCtx| {
        let l = l.clone();
        async move {
            ctx.branch("扣款")
                .on_rollback("local://退款")
                .run(|| async { BranchResult::Success })
                .await?;
            ctx.branch("发货")
                .on_rollback("local://退货")
                .run(|| async { BranchResult::Success })
                .await?;
            // 第三步业务明确要求回滚
            ctx.branch("风控")
                .on_rollback("local://空补偿")
                .run(|| {
                    let l = l.clone();
                    async move {
                        l.lock().unwrap().push("风控拒绝".into());
                        BranchResult::Failure
                    }
                })
                .await?;
            Ok(())
        }
    });

    // 补偿是普通的进程内分支
    let mut reg = dtmrs_server::registry::Registry::new();
    for name in ["退款", "退货", "空补偿"] {
        let l = log.clone();
        let n = name.to_string();
        reg.register(name, move |_ctx| {
            let (l, n) = (l.clone(), n.clone());
            async move {
                l.lock().unwrap().push(n);
                BranchResult::Success
            }
        });
    }

    let st = store().await;
    let d = Driver::new(st.clone(), "tc-1".into())
        .with_registry(Arc::new(reg))
        .with_workflows(Arc::new(wf));
    submit(&st, "wf-fail", "会失败", "").await;
    let g = global(&st, "wf-fail").await;
    d.process(&g).await.unwrap();

    let g = global(&st, "wf-fail").await;
    assert_eq!(g.status, GlobalStatus::Failed);
    assert!(
        g.rollback_reason.contains("FAILURE"),
        "回滚原因要说清是哪一步"
    );

    let l = log.lock().unwrap().clone();
    assert_eq!(
        l,
        vec!["风控拒绝", "空补偿", "退货", "退款"],
        "必须逆序补偿：后执行的先回滚"
    );
}

#[tokio::test]
async fn 没登记补偿的分支不会被补偿() {
    // 纯查询之类的步骤没有副作用，不给 on_rollback 就不该被补
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let l = log.clone();
    let mut wf = WorkflowRegistry::new();
    wf.register("混合", move |mut ctx: WorkflowCtx| {
        let l = l.clone();
        async move {
            // 第一步：只查不改，没有补偿
            ctx.branch("查库存")
                .run(|| async { BranchResult::Success })
                .await?;
            // 第二步：有副作用，有补偿
            ctx.branch("扣款")
                .on_rollback("local://退款")
                .run(|| async { BranchResult::Success })
                .await?;
            ctx.branch("失败")
                .run(|| {
                    let l = l.clone();
                    async move {
                        l.lock().unwrap().push("失败".into());
                        BranchResult::Failure
                    }
                })
                .await?;
            Ok(())
        }
    });

    let mut reg = dtmrs_server::registry::Registry::new();
    let l2 = log.clone();
    reg.register("退款", move |_| {
        let l = l2.clone();
        async move {
            l.lock().unwrap().push("退款".into());
            BranchResult::Success
        }
    });

    let st = store().await;
    let d = Driver::new(st.clone(), "tc-1".into())
        .with_registry(Arc::new(reg))
        .with_workflows(Arc::new(wf));
    submit(&st, "wf-partial", "混合", "").await;
    let g = global(&st, "wf-partial").await;
    d.process(&g).await.unwrap();

    assert_eq!(global(&st, "wf-partial").await.status, GlobalStatus::Failed);
    assert_eq!(
        log.lock().unwrap().clone(),
        vec!["失败", "退款"],
        "只补偿登记过补偿的那个分支"
    );
}

#[tokio::test]
async fn 控制流可以依赖上一步的返回值() {
    // 这是 workflow 相对 saga 的全部价值：步骤不是提前声明的
    let shipped = Arc::new(AtomicUsize::new(0));
    let sh = shipped.clone();
    let mut wf = WorkflowRegistry::new();
    wf.register("按需发货", move |mut ctx: WorkflowCtx| {
        let sh = sh.clone();
        async move {
            let kind = ctx
                .branch("查订单类型")
                .run_with(|| async { (BranchResult::Success, "虚拟商品".to_string()) })
                .await?;
            // 只有实物才发货
            if kind == "实物" {
                ctx.branch("发货")
                    .on_rollback("local://退货")
                    .run(|| {
                        let sh = sh.clone();
                        async move {
                            sh.fetch_add(1, Ordering::SeqCst);
                            BranchResult::Success
                        }
                    })
                    .await?;
            }
            Ok(())
        }
    });

    let st = store().await;
    let d = driver(&st, wf);
    submit(&st, "wf-cond", "按需发货", "").await;
    let g = global(&st, "wf-cond").await;
    d.process(&g).await.unwrap();

    assert_eq!(global(&st, "wf-cond").await.status, GlobalStatus::Succeed);
    assert_eq!(shipped.load(Ordering::SeqCst), 0, "虚拟商品不该走发货分支");
    // 只登记了一个分支
    let rows = st.list_branches("wf-cond").await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn 重放走岔了会被当场发现() {
    // 函数不确定 → 第二轮在同一个位置跑了不同的分支。
    // 这时候**必须停下**：按位置记忆化会张冠李戴，回滚也会补错对象。
    let round = Arc::new(AtomicUsize::new(0));
    let caught = Arc::new(Mutex::new(None::<WorkflowError>));

    let (r, c) = (round.clone(), caught.clone());
    let mut wf = WorkflowRegistry::new();
    wf.register("不确定的", move |mut ctx: WorkflowCtx| {
        let (r, c) = (r.clone(), c.clone());
        async move {
            let n = r.fetch_add(1, Ordering::SeqCst);
            // 第一轮走「路线A」，第二轮走「路线B」—— 典型的非确定性
            let name = if n == 0 { "路线A" } else { "路线B" };
            let res = ctx
                .branch(name)
                .run(|| async move {
                    if n == 0 {
                        BranchResult::Unknown // 逼出第二轮
                    } else {
                        BranchResult::Success
                    }
                })
                .await;
            if let Err(e) = &res {
                if matches!(e, WorkflowError::Diverged { .. }) {
                    *c.lock().unwrap() = Some(e.clone());
                }
            }
            res?;
            Ok(())
        }
    });

    let st = store().await;
    let d = driver(&st, wf);
    submit(&st, "wf-diverge", "不确定的", "").await;

    let g = global(&st, "wf-diverge").await;
    d.process(&g).await.unwrap();
    let g = global(&st, "wf-diverge").await;
    d.process(&g).await.unwrap();

    let e = caught.lock().unwrap().clone().expect("第二轮必须报分岔");
    match e {
        WorkflowError::Diverged {
            branch_id,
            recorded,
            got,
        } => {
            assert_eq!(branch_id, "01");
            assert_eq!(recorded, "路线A");
            assert_eq!(got, "路线B");
        }
        other => panic!("错误类型不对: {other:?}"),
    }

    // **不能因为分岔就回滚** —— 这时已经不知道真实进度了，硬回滚更危险。
    // 停在 submitted 等人改代码，重启后能接着推
    assert_eq!(
        global(&st, "wf-diverge").await.status,
        GlobalStatus::Submitted,
        "分岔要停下等人，既不能落成功也不能回滚"
    );
}

#[tokio::test]
async fn workflow没注册时只重试不回滚() {
    // 新版本删了这个 workflow、或者改了名字 —— 这是部署问题，不是业务失败。
    // 判失败会白白触发回滚
    let st = store().await;
    let d = driver(&st, WorkflowRegistry::new());
    submit(&st, "wf-missing", "根本没注册的", "").await;
    let g = global(&st, "wf-missing").await;
    d.process(&g).await.unwrap();

    assert_eq!(
        global(&st, "wf-missing").await.status,
        GlobalStatus::Submitted,
        "漏注册只能重试，绝不能回滚"
    );
}

#[tokio::test]
async fn 输入数据能透传给函数() {
    let got = Arc::new(Mutex::new(String::new()));
    let g2 = got.clone();
    let mut wf = WorkflowRegistry::new();
    wf.register("读输入", move |ctx: WorkflowCtx| {
        let g2 = g2.clone();
        async move {
            *g2.lock().unwrap() = format!("{}|{}", ctx.gid, ctx.input);
            Ok(())
        }
    });

    let st = store().await;
    let d = driver(&st, wf);
    submit(&st, "wf-input", "读输入", r#"{"amount":100}"#).await;
    let g = global(&st, "wf-input").await;
    d.process(&g).await.unwrap();

    assert_eq!(got.lock().unwrap().clone(), r#"wf-input|{"amount":100}"#);
    assert_eq!(global(&st, "wf-input").await.status, GlobalStatus::Succeed);
}

#[tokio::test]
async fn 空函数直接成功() {
    let mut wf = WorkflowRegistry::new();
    wf.register("啥也不干", |_ctx: WorkflowCtx| async { Ok(()) });
    let st = store().await;
    let d = driver(&st, wf);
    submit(&st, "wf-empty", "啥也不干", "").await;
    let g = global(&st, "wf-empty").await;
    d.process(&g).await.unwrap();
    assert_eq!(global(&st, "wf-empty").await.status, GlobalStatus::Succeed);
}

/// 真正的崩溃恢复：**换一个进程**（新的 Embedded + 新的推进器）接着推。
///
/// 上面那个 `重放不重做已完成的步骤` 用的是同一个 driver 重试，这里把整个
/// TC 都换掉，只有数据库是同一个 —— 这才是线上重启的样子。
#[tokio::test]
async fn 跨进程重启_workflow从断点续跑() {
    use dtmrs_server::embedded::Embedded;
    use std::time::Duration;

    let db = format!("sqlite:/tmp/dtmrs_wf_restart_{}.db", std::process::id());
    let _ = std::fs::remove_file(db.trim_start_matches("sqlite:"));

    let step1 = Arc::new(AtomicUsize::new(0));
    let step2 = Arc::new(AtomicUsize::new(0));
    // 用文件记「第一个进程已经试过第二步了」，新进程据此改变行为 ——
    // 内存计数器也行，这里用它顺便证明状态确实是从库里恢复的
    let attempted = Arc::new(AtomicUsize::new(0));

    // 造一份 workflow 函数，两个「进程」注册同名的同一份逻辑
    macro_rules! make_wf {
        () => {{
            let (s1, s2, at) = (step1.clone(), step2.clone(), attempted.clone());
            move |mut ctx: WorkflowCtx| {
                let (s1, s2, at) = (s1.clone(), s2.clone(), at.clone());
                async move {
                    ctx.branch("扣款")
                        .on_rollback("local://退款")
                        .run(|| {
                            let s1 = s1.clone();
                            async move {
                                s1.fetch_add(1, Ordering::SeqCst);
                                BranchResult::Success
                            }
                        })
                        .await?;
                    ctx.branch("入账")
                        .on_rollback("local://冲正")
                        .run(|| {
                            let (s2, at) = (s2.clone(), at.clone());
                            async move {
                                s2.fetch_add(1, Ordering::SeqCst);
                                // 第一个进程里返回未知（模拟卡住然后进程被杀）
                                if at.fetch_add(1, Ordering::SeqCst) == 0 {
                                    BranchResult::Unknown
                                } else {
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

    // ---- 第一个进程 ----
    {
        let tc = Embedded::builder(&db)
            .tick(Duration::from_millis(20))
            .workflow("转账", make_wf!())
            .start()
            .await
            .unwrap();
        tc.submit_workflow("wf-restart", "转账", "").await.unwrap();
        // 等它把第一步跑完、第二步卡住
        for _ in 0..100 {
            if step2.load(Ordering::SeqCst) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(step1.load(Ordering::SeqCst), 1);
        assert_eq!(
            tc.status("wf-restart").await.unwrap(),
            Some(GlobalStatus::Submitted)
        );
        // tc 在这里 drop —— 推进器停掉，等价于进程没了
    }

    // ---- 第二个进程：同一个库，全新的 TC ----
    let tc2 = Embedded::builder(&db)
        .tick(Duration::from_millis(20))
        .workflow("转账", make_wf!())
        .start()
        .await
        .unwrap();

    // **不需要客户端重新提交**，新进程会自己把未终结的事务捞起来接着推
    let final_status = tc2
        .wait_final("wf-restart", Duration::from_secs(15))
        .await
        .unwrap();

    assert_eq!(final_status, GlobalStatus::Succeed);
    assert_eq!(
        step1.load(Ordering::SeqCst),
        1,
        "重启后第一步绝不能再跑一遍 —— 那就是重复扣款"
    );
    assert!(
        step2.load(Ordering::SeqCst) >= 2,
        "第二步上次没成，重启后要接着试"
    );

    drop(tc2);
    let _ = std::fs::remove_file(db.trim_start_matches("sqlite:"));
}

#[tokio::test]
async fn 第一步就要求回滚时没有补偿可做() {
    let mut wf = WorkflowRegistry::new();
    wf.register("立刻失败", |mut ctx: WorkflowCtx| async move {
        ctx.branch("风控")
            .run(|| async { BranchResult::Failure })
            .await?;
        Ok::<(), WorkflowError>(())
    });
    let st = store().await;
    let d = driver(&st, wf);
    submit(&st, "wf-first-fail", "立刻失败", "").await;
    let g = global(&st, "wf-first-fail").await;
    d.process(&g).await.unwrap();
    assert_eq!(
        global(&st, "wf-first-fail").await.status,
        GlobalStatus::Failed
    );
}
