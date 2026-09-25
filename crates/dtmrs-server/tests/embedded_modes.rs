//! 嵌入式形态下的 TCC / XA / 二阶段消息。
//!
//! 这三种模式原先只有 HTTP / gRPC 能用（`api.rs`），嵌入式只有 saga 和 workflow，
//! FFI 跟着也只有 saga。这里钉的是各模式在嵌入式下**最容易写错的那条规则**：
//!
//! - TCC / XA：先登记再做一阶段；二阶段失败绝不转方向；回滚时每个分支都要撤
//! - msg：本地事务结果未知时不能猜，只能靠回查决断
//!
//! 每个用例在 sqlite 和 Redis 上**各跑一遍**（见 `common::for_each_backend`）。
//! 状态机在 core 里只有一份，但「什么时候被调度」不是：Redis 的 `schedulable()`
//! 在 Lua 里另有一份，prepared 的 msg 能不能被捞起来回查就取决于它。

mod common;

use dtmrs_core::{BranchResult, GlobalStatus};
use dtmrs_server::embedded::Embedded;
use dtmrs_server::registry::BranchCtx;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// 所有 handler 的调用记录，形如 `"confirm@01"`，按调用顺序
type Log = Arc<Mutex<Vec<String>>>;

fn recorder(
    log: &Log,
    tag: &'static str,
    r: BranchResult,
) -> impl Fn(BranchCtx) -> std::future::Ready<BranchResult> + Send + Sync + 'static {
    let log = log.clone();
    move |ctx: BranchCtx| {
        log.lock().unwrap().push(format!("{tag}@{}", ctx.branch_id));
        std::future::ready(r)
    }
}

fn count(log: &Log, tag: &str) -> usize {
    log.lock()
        .unwrap()
        .iter()
        .filter(|s| s.starts_with(&format!("{tag}@")))
        .count()
}

fn calls(log: &Log, tag: &str) -> Vec<String> {
    log.lock()
        .unwrap()
        .iter()
        .filter(|s| s.starts_with(&format!("{tag}@")))
        .cloned()
        .collect()
}

async fn start(db: &str, log: &Log, confirm: BranchResult) -> Embedded {
    Embedded::builder(db)
        .handler("confirm", recorder(log, "confirm", confirm))
        .handler("cancel", recorder(log, "cancel", BranchResult::Success))
        .handler("commit", recorder(log, "commit", confirm))
        .handler("rollback", recorder(log, "rollback", BranchResult::Success))
        .handler("action", recorder(log, "action", BranchResult::Success))
        .handler("query_yes", recorder(log, "query", BranchResult::Success))
        .handler("query_no", recorder(log, "query", BranchResult::Failure))
        .tick(Duration::from_millis(10))
        .start()
        .await
        .unwrap()
}

// ======================= TCC =======================

#[tokio::test]
async fn 嵌入式tcc_try全成功后confirm每个分支() {
    common::for_each_backend("tcc_ok", |be| async move {
        let db = be.url;
        let log = Log::default();
        let tc = start(&db, &log, BranchResult::Success).await;

        let mut t = tc.tcc("tcc-ok").await.unwrap();
        for _ in 0..2 {
            let r = t
                .try_branch("local://confirm", "local://cancel", |_bid| async {
                    BranchResult::Success
                })
                .await
                .unwrap();
            assert_eq!(r, BranchResult::Success);
        }
        // try 阶段 TC 不插手：还没 submit，一次 confirm 都不该发
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(count(&log, "confirm"), 0, "submit 之前不该 confirm");
        t.submit().await.unwrap();

        let s = tc
            .wait_final("tcc-ok", Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(s, GlobalStatus::Succeed);
        assert_eq!(calls(&log, "confirm"), ["confirm@01", "confirm@02"]);
        assert_eq!(count(&log, "cancel"), 0);
    })
    .await;
}

#[tokio::test]
async fn 嵌入式tcc_try失败则逆序cancel所有分支_包括失败的那个() {
    // try 失败的那个分支也要 cancel：它可能冻结了一半资源（或者 try 其实超时成功了）。
    // 多出来的 cancel 由屏障空转掉 —— 宁可多发，不可漏发
    common::for_each_backend("tcc_rb", |be| async move {
        let db = be.url;
        let log = Log::default();
        let tc = start(&db, &log, BranchResult::Success).await;

        let mut t = tc.tcc("tcc-rb").await.unwrap();
        t.try_branch("local://confirm", "local://cancel", |_| async {
            BranchResult::Success
        })
        .await
        .unwrap();
        let r = t
            .try_branch("local://confirm", "local://cancel", |_| async {
                BranchResult::Failure
            })
            .await
            .unwrap();
        assert_eq!(r, BranchResult::Failure);
        t.abort().await.unwrap();

        let s = tc
            .wait_final("tcc-rb", Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(s, GlobalStatus::Failed);
        assert_eq!(
            calls(&log, "cancel"),
            ["cancel@02", "cancel@01"],
            "逆序、全部"
        );
        assert_eq!(count(&log, "confirm"), 0);
    })
    .await;
}

#[tokio::test]
async fn 嵌入式tcc_confirm失败绝不能触发cancel() {
    common::for_each_backend("tcc_cf", |be| async move {
        let db = be.url;
        let log = Log::default();
        let tc = start(&db, &log, BranchResult::Failure).await;

        let mut t = tc.tcc("tcc-cf").await.unwrap();
        t.try_branch("local://confirm", "local://cancel", |_| async {
            BranchResult::Success
        })
        .await
        .unwrap();
        t.submit().await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            tc.status("tcc-cf").await.unwrap(),
            Some(GlobalStatus::Submitted)
        );
        assert!(count(&log, "confirm") >= 1);
        // 默认退避 10 秒起，这里等不到第二次 confirm；看它确实排上了重试
        let g = tc.store().get_global("tcc-cf").await.unwrap().unwrap();
        assert!(g.next_cron_interval > 0, "confirm 失败要退避重试");
        assert_eq!(count(&log, "cancel"), 0, "confirm 失败绝不能转 cancel");
        // 方向已定，调用方想「救一下」而去 abort 也必须被拒
        assert!(
            tc.abort("tcc-cf").await.is_err(),
            "已 submit 的 tcc 不能 abort"
        );
    })
    .await;
}

#[tokio::test]
async fn 嵌入式tcc_登记失败时绝不能跑try() {
    // 先登记再 try 的另一半：登记没成功就不能 try，否则 try 冻结的资源 TC 不知道，
    // 永远没人 cancel。这里用「cancel 漏注册」让登记失败
    common::for_each_backend("tcc_noreg", |be| async move {
        let db = be.url;
        let log = Log::default();
        let tc = start(&db, &log, BranchResult::Success).await;

        let mut t = tc.tcc("tcc-noreg").await.unwrap();
        let ran = Arc::new(Mutex::new(false));
        let r2 = ran.clone();
        let err = t
            .try_branch(
                "local://confirm",
                "local://没注册的cancel",
                move |_| async move {
                    *r2.lock().unwrap() = true;
                    BranchResult::Success
                },
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("没注册的cancel"),
            "要点名是哪个: {err}"
        );
        assert!(!*ran.lock().unwrap(), "登记失败了，try 一次都不能跑");
    })
    .await;
}

#[tokio::test]
async fn 嵌入式tcc_显式分支号重号且地址不同必须拒绝() {
    // FFI / 跨进程的调用方自己编分支号。两个分支撞了同一个号，
    // 第二个的地址不会被写进去 —— 必须报错，而不是返回成功让它去 try
    common::for_each_backend("tcc_dup", |be| async move {
        let db = be.url;
        let log = Log::default();
        let tc = start(&db, &log, BranchResult::Success).await;
        tc.tcc("tcc-dup").await.unwrap();
        tc.register_tcc_branch("tcc-dup", "01", "local://confirm", "local://cancel")
            .await
            .unwrap();
        // 同一个分支重试登记：幂等放行
        tc.register_tcc_branch("tcc-dup", "01", "local://confirm", "local://cancel")
            .await
            .expect("原样重试登记必须成功");
        let err = tc
            .register_tcc_branch("tcc-dup", "01", "local://commit", "local://cancel")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("01"), "{err}");
    })
    .await;
}

// ======================= XA =======================

#[tokio::test]
async fn 嵌入式xa_一阶段全成功后commit每个分支() {
    common::for_each_backend("xa_ok", |be| async move {
        let db = be.url;
        let log = Log::default();
        let tc = start(&db, &log, BranchResult::Success).await;

        let mut x = tc.xa("xa-ok").await.unwrap();
        for _ in 0..2 {
            x.prepare_branch("local://commit", "local://rollback", |_| async {
                BranchResult::Success
            })
            .await
            .unwrap();
        }
        x.submit().await.unwrap();
        let s = tc
            .wait_final("xa-ok", Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(s, GlobalStatus::Succeed);
        assert_eq!(calls(&log, "commit"), ["commit@01", "commit@02"]);
        assert_eq!(count(&log, "rollback"), 0);
    })
    .await;
}

#[tokio::test]
async fn 嵌入式xa_一阶段失败则rollback每个分支() {
    // 不 rollback 的分支会留下永久持锁的 prepared 事务
    common::for_each_backend("xa_rb", |be| async move {
        let db = be.url;
        let log = Log::default();
        let tc = start(&db, &log, BranchResult::Success).await;

        let mut x = tc.xa("xa-rb").await.unwrap();
        x.prepare_branch("local://commit", "local://rollback", |_| async {
            BranchResult::Success
        })
        .await
        .unwrap();
        // 一阶段超时：结果未知。XA 还没 submit，回滚是安全的（rollback 对没 prepare
        // 的 xid 是空操作），所以调用方照样 abort
        let r = x
            .prepare_branch("local://commit", "local://rollback", |_| async {
                BranchResult::Unknown
            })
            .await
            .unwrap();
        assert_eq!(r, BranchResult::Unknown);
        x.abort().await.unwrap();

        let s = tc
            .wait_final("xa-rb", Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(s, GlobalStatus::Failed);
        assert_eq!(calls(&log, "rollback"), ["rollback@02", "rollback@01"]);
        assert_eq!(count(&log, "commit"), 0);
    })
    .await;
}

#[tokio::test]
async fn 嵌入式xa_commit失败绝不能触发rollback() {
    common::for_each_backend("xa_cf", |be| async move {
        let db = be.url;
        let log = Log::default();
        let tc = start(&db, &log, BranchResult::Failure).await;
        let mut x = tc.xa("xa-cf").await.unwrap();
        x.prepare_branch("local://commit", "local://rollback", |_| async {
            BranchResult::Success
        })
        .await
        .unwrap();
        x.submit().await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            tc.status("xa-cf").await.unwrap(),
            Some(GlobalStatus::Submitted)
        );
        assert!(count(&log, "commit") >= 1);
        let g = tc.store().get_global("xa-cf").await.unwrap().unwrap();
        assert!(g.next_cron_interval > 0, "commit 失败要退避重试");
        assert_eq!(count(&log, "rollback"), 0);
    })
    .await;
}

// ======================= msg =======================

#[tokio::test]
async fn 嵌入式msg_本地事务成功后送达每条消息() {
    common::for_each_backend("msg_ok", |be| async move {
        let db = be.url;
        let log = Log::default();
        let tc = start(&db, &log, BranchResult::Success).await;

        let r = tc
            .msg("msg-ok")
            .action("local://action")
            .action("local://action")
            .query_prepared("local://query_yes")
            .do_and_submit(|| async { BranchResult::Success })
            .await
            .unwrap();
        assert_eq!(r, BranchResult::Success);
        let s = tc
            .wait_final("msg-ok", Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(s, GlobalStatus::Succeed);
        assert_eq!(calls(&log, "action"), ["action@01", "action@02"]);
        assert_eq!(count(&log, "query"), 0, "正常 submit 了就不该回查");
    })
    .await;
}

#[tokio::test]
async fn 嵌入式msg_本地事务明确失败则整单作废_一条消息都不发() {
    common::for_each_backend("msg_fail", |be| async move {
        let db = be.url;
        let log = Log::default();
        let tc = start(&db, &log, BranchResult::Success).await;

        tc.msg("msg-fail")
            .action("local://action")
            .query_prepared("local://query_yes")
            .do_and_submit(|| async { BranchResult::Failure })
            .await
            .unwrap();
        let s = tc
            .wait_final("msg-fail", Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(s, GlobalStatus::Failed);
        assert_eq!(count(&log, "action"), 0);
    })
    .await;
}

#[tokio::test]
async fn 嵌入式msg_本地事务结果未知时不猜_靠回查决断() {
    // 本地事务「超时了」—— 可能提交了也可能没有。猜「提交了」会重复发，
    // 猜「没提交」会丢单。唯一对的是停在 prepared 等回查
    for (gid, query, want, actions) in [
        ("msg-q-yes", "local://query_yes", GlobalStatus::Succeed, 1),
        ("msg-q-no", "local://query_no", GlobalStatus::Failed, 0),
    ] {
        common::for_each_backend(gid, |be| async move {
            let db = be.url;
            let log = Log::default();
            let tc = start(&db, &log, BranchResult::Success).await;
            let r = tc
                .msg(gid)
                .action("local://action")
                .query_prepared(query)
                .grace_secs(0)
                .do_and_submit(|| async { BranchResult::Unknown })
                .await
                .unwrap();
            assert_eq!(r, BranchResult::Unknown);
            let s = tc.wait_final(gid, Duration::from_secs(10)).await.unwrap();
            assert_eq!(s, want, "{gid}");
            assert!(count(&log, "query") >= 1, "{gid} 必须回查过");
            assert_eq!(count(&log, "action"), actions, "{gid}");
        })
        .await;
    }
}

#[tokio::test]
async fn 嵌入式msg_本地事务不能在prepare之前跑() {
    // prepare 失败（这里是回查地址漏注册）时本地事务不能跑：
    // 跑了就是本地已提交、TC 却不知道有这笔消息 —— 永远不会发
    common::for_each_backend("msg_noprep", |be| async move {
        let db = be.url;
        let log = Log::default();
        let tc = start(&db, &log, BranchResult::Success).await;
        let ran = Arc::new(Mutex::new(false));
        let r2 = ran.clone();
        let err = tc
            .msg("msg-noprep")
            .action("local://action")
            .query_prepared("local://没注册的回查")
            .do_and_submit(move || async move {
                *r2.lock().unwrap() = true;
                BranchResult::Success
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("没注册的回查"), "{err}");
        assert!(!*ran.lock().unwrap());
        assert!(tc.status("msg-noprep").await.unwrap().is_none());
    })
    .await;
}
