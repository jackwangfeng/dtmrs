//! 嵌入式 TC 的端到端测试。
//!
//! 重点是最后那个「跨进程重启恢复」—— 它证明的是嵌入式模式**没有牺牲持久性**：
//! TC 在你的进程里，但事务状态在 DB 里，进程死了事务不丢。
//! 这是嵌入式方案能不能当真的分水岭。

use dtmrs_core::{BranchResult, GlobalStatus};
use dtmrs_server::embedded::Embedded;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// 每个测试用独立的库文件，避免互相干扰
fn tmp_db(name: &str) -> (String, std::path::PathBuf) {
    let p = std::env::temp_dir().join(format!("dtmrs_test_{}_{}.db", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    (format!("sqlite:{}", p.display()), p)
}

#[derive(Default)]
struct Calls {
    a1: AtomicUsize,
    c1: AtomicUsize,
    a2: AtomicUsize,
    c2: AtomicUsize,
}

impl Calls {
    fn get(&self) -> (usize, usize, usize, usize) {
        (
            self.a1.load(Ordering::SeqCst),
            self.c1.load(Ordering::SeqCst),
            self.a2.load(Ordering::SeqCst),
            self.c2.load(Ordering::SeqCst),
        )
    }
}

#[tokio::test]
async fn 进程内分支全成功() {
    let (db, path) = tmp_db("happy");
    let c = Arc::new(Calls::default());
    let (c1, c2, c3, c4) = (c.clone(), c.clone(), c.clone(), c.clone());

    let tc = Embedded::builder(&db)
        .handler("a1", move |_| {
            let c = c1.clone();
            async move {
                c.a1.fetch_add(1, Ordering::SeqCst);
                BranchResult::Success
            }
        })
        .handler("c1", move |_| {
            let c = c2.clone();
            async move {
                c.c1.fetch_add(1, Ordering::SeqCst);
                BranchResult::Success
            }
        })
        .handler("a2", move |_| {
            let c = c3.clone();
            async move {
                c.a2.fetch_add(1, Ordering::SeqCst);
                BranchResult::Success
            }
        })
        .handler("c2", move |_| {
            let c = c4.clone();
            async move {
                c.c2.fetch_add(1, Ordering::SeqCst);
                BranchResult::Success
            }
        })
        .tick(Duration::from_millis(10))
        .start()
        .await
        .unwrap();

    tc.saga("emb-happy")
        .step("local://a1", "local://c1")
        .step("local://a2", "local://c2")
        .submit()
        .await
        .unwrap();

    let s = tc
        .wait_final("emb-happy", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(s, GlobalStatus::Succeed);
    let (a1, cc1, a2, cc2) = c.get();
    assert_eq!((a1, a2), (1, 1), "两步各调一次");
    assert_eq!((cc1, cc2), (0, 0), "成功路径不该有补偿");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn 进程内分支失败触发逆序补偿() {
    let (db, path) = tmp_db("rollback");
    let c = Arc::new(Calls::default());
    let (x1, x2, x3) = (c.clone(), c.clone(), c.clone());

    let tc = Embedded::builder(&db)
        .handler("a1", move |_| {
            let c = x1.clone();
            async move {
                c.a1.fetch_add(1, Ordering::SeqCst);
                BranchResult::Success
            }
        })
        .handler("c1", move |_| {
            let c = x2.clone();
            async move {
                c.c1.fetch_add(1, Ordering::SeqCst);
                BranchResult::Success
            }
        })
        // 第二步业务明确要求回滚
        .handler("a2", |_| async { BranchResult::Failure })
        .handler("c2", move |_| {
            let c = x3.clone();
            async move {
                c.c2.fetch_add(1, Ordering::SeqCst);
                BranchResult::Success
            }
        })
        .tick(Duration::from_millis(10))
        .start()
        .await
        .unwrap();

    tc.saga("emb-rb")
        .step("local://a1", "local://c1")
        .step("local://a2", "local://c2")
        .submit()
        .await
        .unwrap();

    let s = tc
        .wait_final("emb-rb", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(s, GlobalStatus::Failed);
    let (_, cc1, _, cc2) = c.get();
    assert_eq!((cc1, cc2), (1, 1), "两步都要补偿");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn 跨进程重启_事务不丢且已完成的步骤不重做() {
    // 这是嵌入式方案的关键证明：TC 在进程里，但状态在 DB 里。
    // 进程死了，事务照样能被新进程接着推完。
    let (db, path) = tmp_db("restart");
    let c = Arc::new(Calls::default());

    // ---------- 第一个「进程」：第 2 步还没准备好，返回 Unknown ----------
    {
        let (x1, x2) = (c.clone(), c.clone());
        let tc = Embedded::builder(&db)
            .owner("proc-1")
            .handler("a1", move |_| {
                let c = x1.clone();
                async move {
                    c.a1.fetch_add(1, Ordering::SeqCst);
                    BranchResult::Success
                }
            })
            .handler("c1", |_| async { BranchResult::Success })
            .handler("a2", move |_| {
                let c = x2.clone();
                async move {
                    c.a2.fetch_add(1, Ordering::SeqCst);
                    // 下游还没好 —— 结果未知，不是失败
                    BranchResult::Unknown
                }
            })
            .handler("c2", |_| async { BranchResult::Success })
            .tick(Duration::from_millis(10))
            .start()
            .await
            .unwrap();

        tc.saga("emb-restart")
            .step("local://a1", "local://c1")
            .step("local://a2", "local://c2")
            .submit()
            .await
            .unwrap();

        // 等它把第 1 步做完、第 2 步卡住
        tokio::time::sleep(Duration::from_millis(400)).await;
        let s = tc.status("emb-restart").await.unwrap().unwrap();
        assert_eq!(s, GlobalStatus::Submitted, "Unknown 不能让事务回滚或终结");
        let (a1, cc1, a2, cc2) = c.get();
        assert_eq!(a1, 1, "第 1 步已完成");
        assert!(a2 >= 1, "第 2 步试过了");
        assert_eq!((cc1, cc2), (0, 0), "结果未知时绝不能补偿");
        // tc 出作用域 → Drop → 推进器停掉，等于进程退出
    }

    // ---------- 第二个「进程」：同一个库，第 2 步这次会成功 ----------
    let a1_before = c.a1.load(Ordering::SeqCst);
    {
        let x1 = c.clone();
        let tc = Embedded::builder(&db)
            .owner("proc-2")
            .handler("a1", move |_| {
                let c = x1.clone();
                async move {
                    c.a1.fetch_add(1, Ordering::SeqCst);
                    BranchResult::Success
                }
            })
            .handler("c1", |_| async { BranchResult::Success })
            .handler("a2", |_| async { BranchResult::Success })
            .handler("c2", |_| async { BranchResult::Success })
            .tick(Duration::from_millis(10))
            .start()
            .await
            .unwrap();

        // 新进程不需要客户端重新提交，自己就把事务捞起来推完了
        let s = tc
            .wait_final("emb-restart", Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(s, GlobalStatus::Succeed, "新进程应该把事务推完");
    }

    assert_eq!(
        c.a1.load(Ordering::SeqCst),
        a1_before,
        "第 1 步已经成功了，重启后绝不能再做一次"
    );
    let (_, cc1, _, cc2) = c.get();
    assert_eq!((cc1, cc2), (0, 0), "最终成功，一次补偿都不该发");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn 提交时就拦住漏注册的分支() {
    // 等推到一半才发现 handler 不存在就晚了 —— 前几步副作用已经落地
    let (db, path) = tmp_db("missing");
    let tc = Embedded::builder(&db)
        .handler("a1", |_| async { BranchResult::Success })
        .start()
        .await
        .unwrap();

    let err = tc
        .saga("emb-missing")
        .step("local://a1", "local://c1_忘了注册")
        .submit()
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("c1_忘了注册"),
        "错误信息要点出是哪个 handler: {err}"
    );
    // 提交被拦住了，库里不该有这笔事务
    assert!(tc.status("emb-missing").await.unwrap().is_none());
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn 本地分支与远端http可以混用() {
    // 迁移路径：老服务继续走 HTTP，新逻辑内联进来，同一个事务里
    use axum::routing::post;
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let app = axum::Router::new().route(
        "/a2",
        post(move || {
            let h = h.clone();
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                "SUCCESS"
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let (db, path) = tmp_db("mixed");
    let local_hits = Arc::new(AtomicUsize::new(0));
    let lh = local_hits.clone();
    let tc = Embedded::builder(&db)
        .handler("a1", move |_| {
            let c = lh.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                BranchResult::Success
            }
        })
        .handler("c1", |_| async { BranchResult::Success })
        .tick(Duration::from_millis(10))
        .start()
        .await
        .unwrap();

    tc.saga("emb-mixed")
        .step("local://a1", "local://c1")
        .step(&format!("http://{addr}/a2"), &format!("http://{addr}/c2"))
        .submit()
        .await
        .unwrap();

    let s = tc
        .wait_final("emb-mixed", Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(s, GlobalStatus::Succeed);
    assert_eq!(local_hits.load(Ordering::SeqCst), 1, "本地分支被调用");
    assert_eq!(hits.load(Ordering::SeqCst), 1, "远端分支被调用");
    let _ = std::fs::remove_file(path);
}
