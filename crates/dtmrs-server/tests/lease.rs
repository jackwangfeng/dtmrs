//! 租约和「推的过程中状态被外部改掉」的竞态。
//!
//! 原先租约就是「抢到时把 next_cron_time 推到租约之后」，并且推进器落状态是无条件写。
//! 于是有三个洞，这里每个都有一条测试钉着：
//!
//! 1. **租约被冲掉**：abort / 管理台「立刻重试」调 `schedule_now`，正在推的那笔被
//!    第二个 worker 抢走，两个 worker 并发推同一笔
//! 2. **abort 丢失**：saga 推到一半被 abort，持租约的 worker 手上还是 submitted，
//!    推完照样写 succeed，把 aborting 盖掉 —— 调用方拿到的却是「中止成功」
//! 3. **abort 被推迟**：持租约的 worker 这一轮以退避收尾，把 abort 要求的「现在」
//!    推到 10 秒之后
//!
//! 做法：分支 handler 在一道闸门前停住，测试在它停住的那一刻去 abort / 重试，
//! 再放它走。每个 handler 都记录并发度，断言**任何时刻最多一个 worker 在推这笔**。

mod common;

use dtmrs_core::{BranchResult, GlobalStatus, SagaStep, TransType};
use dtmrs_server::api::Api;
use dtmrs_server::driver::Driver;
use dtmrs_server::registry::{BranchCtx, Registry};
use dtmrs_server::workflow::{WorkflowCtx, WorkflowRegistry};
use dtmrs_store::Store;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

/// 分支调用记录 + 并发度
#[derive(Default)]
struct Probe {
    log: Mutex<Vec<String>>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    /// handler 停在闸门前时通知测试
    entered: Notify,
    /// 测试放行
    release: Notify,
}

impl Probe {
    fn calls(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }
    fn count(&self, tag: &str) -> usize {
        self.calls().iter().filter(|s| s.starts_with(tag)).count()
    }
}

/// 注册一个记录型 handler。`gated` 的话第一次调用会停在闸门前，等测试放行；
/// `results` 依次作为每次调用的返回值，用完了就一直返回最后一个
fn handler(
    r: &mut Registry,
    p: &Arc<Probe>,
    name: &'static str,
    gated: bool,
    results: Vec<BranchResult>,
) {
    let p = p.clone();
    let n = Arc::new(AtomicUsize::new(0));
    r.register(name, move |ctx: BranchCtx| {
        let p = p.clone();
        let n = n.clone();
        let results = results.clone();
        async move {
            let now = p.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            p.max_in_flight.fetch_max(now, Ordering::SeqCst);
            let i = n.fetch_add(1, Ordering::SeqCst);
            p.log
                .lock()
                .unwrap()
                .push(format!("{name}@{}", ctx.branch_id));
            if gated && i == 0 {
                p.entered.notify_one();
                p.release.notified().await;
            }
            p.in_flight.fetch_sub(1, Ordering::SeqCst);
            *results.get(i).or(results.last()).unwrap()
        }
    });
}

/// 起推进器（多个 worker、轮询很快 —— 并发推同一笔的机会给足）
fn spawn_driver(st: &Store, reg: Registry, wf: WorkflowRegistry) -> tokio::task::JoinHandle<()> {
    let d = Driver::new(st.clone(), "tc-lease".into())
        .with_registry(Arc::new(reg))
        .with_workflows(Arc::new(wf));
    tokio::spawn(d.run_forever(Duration::from_millis(5)))
}

async fn wait_final(st: &Store, gid: &str, secs: u64) -> GlobalStatus {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let s = st.get_global(gid).await.unwrap().unwrap().status;
        if s.is_final() || std::time::Instant::now() > deadline {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn entered(p: &Probe) {
    tokio::time::timeout(Duration::from_secs(10), p.entered.notified())
        .await
        .expect("分支一直没被调到");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saga推到一半被abort_abort不能被推完的succeed盖掉() {
    common::for_each_backend("lease_saga_abort", |be| async move {
        let st = Store::open(&be.url).await.unwrap();
        let p = Arc::new(Probe::default());
        let mut reg = Registry::new();
        handler(&mut reg, &p, "a1", true, vec![BranchResult::Success]);
        handler(&mut reg, &p, "a2", false, vec![BranchResult::Success]);
        handler(&mut reg, &p, "c", false, vec![BranchResult::Success]);
        let task = spawn_driver(&st, reg, WorkflowRegistry::new());
        let api = Api::new(st.clone());

        let steps = vec![
            SagaStep::new("local://a1", "local://c"),
            SagaStep::new("local://a2", "local://c"),
        ];
        api.submit("s1", "saga", &steps).await.unwrap();
        entered(&p).await;
        // 第一步正在执行，这时 abort
        api.abort("s1").await.expect("推到一半的 saga 允许 abort");
        p.release.notify_one();

        let s = wait_final(&st, "s1", 20).await;
        assert_eq!(
            s,
            GlobalStatus::Failed,
            "[{}] abort 被推完的 succeed 盖掉了",
            be.label
        );
        // 补偿所有分支（包括 abort 之后才推完的那些），逆序
        let comps: Vec<String> = p
            .calls()
            .into_iter()
            .filter(|c| c.starts_with("c@"))
            .collect();
        assert_eq!(comps, ["c@02", "c@01"], "[{}] {:?}", be.label, p.calls());
        assert_eq!(
            p.max_in_flight.load(Ordering::SeqCst),
            1,
            "[{}] 同一笔被两个 worker 并发推了: {:?}",
            be.label,
            p.calls()
        );
        task.abort();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn 持租约的worker以退避收尾时_abort的补偿不能被推迟() {
    // 第一步在 abort 之后返回「结果未知」→ 持有者退避 10 秒。
    // 但 abort 要求的是「现在」，补偿必须马上开始
    common::for_each_backend("lease_abort_kick", |be| async move {
        let st = Store::open(&be.url).await.unwrap();
        let p = Arc::new(Probe::default());
        let mut reg = Registry::new();
        handler(&mut reg, &p, "a1", true, vec![BranchResult::Unknown]);
        handler(&mut reg, &p, "c", false, vec![BranchResult::Success]);
        let task = spawn_driver(&st, reg, WorkflowRegistry::new());
        let api = Api::new(st.clone());

        api.submit("s2", "saga", &[SagaStep::new("local://a1", "local://c")])
            .await
            .unwrap();
        entered(&p).await;
        api.abort("s2").await.unwrap();
        let t0 = std::time::Instant::now();
        p.release.notify_one();

        let s = wait_final(&st, "s2", 20).await;
        assert_eq!(s, GlobalStatus::Failed, "[{}]", be.label);
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "[{}] abort 的补偿被持有者的退避推迟了 {:?}",
            be.label,
            t0.elapsed()
        );
        assert_eq!(p.count("c@"), 1, "[{}] {:?}", be.label, p.calls());
        assert_eq!(p.max_in_flight.load(Ordering::SeqCst), 1, "[{}]", be.label);
        task.abort();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn 立刻重试打在正在推的事务上_不能出现第二个worker() {
    // 管理台的「立刻重试」原先就是 schedule_now —— 正在推的那笔会被第二个 worker 抢走
    common::for_each_backend("lease_retry", |be| async move {
        let st = Store::open(&be.url).await.unwrap();
        let p = Arc::new(Probe::default());
        let mut reg = Registry::new();
        handler(&mut reg, &p, "a1", true, vec![BranchResult::Success]);
        handler(&mut reg, &p, "c", false, vec![BranchResult::Success]);
        let task = spawn_driver(&st, reg, WorkflowRegistry::new());
        let api = Api::new(st.clone());

        api.submit("s3", "saga", &[SagaStep::new("local://a1", "local://c")])
            .await
            .unwrap();
        entered(&p).await;
        for _ in 0..3 {
            api.retry("s3").await.unwrap();
        }
        // 给「第二个 worker」足够的时间去抢（它要是能抢到的话）
        tokio::time::sleep(Duration::from_millis(300)).await;
        p.release.notify_one();

        assert_eq!(
            wait_final(&st, "s3", 20).await,
            GlobalStatus::Succeed,
            "[{}]",
            be.label
        );
        assert_eq!(
            p.calls(),
            ["a1@01"],
            "[{}] 正在推的分支又被调了一次",
            be.label
        );
        assert_eq!(p.max_in_flight.load(Ordering::SeqCst), 1, "[{}]", be.label);
        task.abort();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn msg回查的同时被abort_abort要生效_消息一条都不发() {
    // prepared 的 msg 允许 abort（本地事务失败了）。回查恰好同时在进行、并且回查说
    // 「已提交」—— 推进器那一写要是覆盖掉 aborting，调用方以为作废了，消息照发
    common::for_each_backend("lease_msg_abort", |be| async move {
        let st = Store::open(&be.url).await.unwrap();
        let p = Arc::new(Probe::default());
        let mut reg = Registry::new();
        handler(&mut reg, &p, "q", true, vec![BranchResult::Success]);
        handler(&mut reg, &p, "act", false, vec![BranchResult::Success]);
        let task = spawn_driver(&st, reg, WorkflowRegistry::new());
        let api = Api::new(st.clone());

        api.prepare("m1", "msg", &["local://act".into()], "local://q", Some(0))
            .await
            .unwrap();
        entered(&p).await;
        api.abort("m1").await.expect("prepared 的 msg 允许 abort");
        p.release.notify_one();

        assert_eq!(
            wait_final(&st, "m1", 20).await,
            GlobalStatus::Failed,
            "[{}]",
            be.label
        );
        assert_eq!(
            p.count("act@"),
            0,
            "[{}] 作废了的消息被发出去了: {:?}",
            be.label,
            p.calls()
        );
        task.abort();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow跑到一半被abort_已登记的分支要补偿() {
    common::for_each_backend("lease_wf_abort", |be| async move {
        let st = Store::open(&be.url).await.unwrap();
        let p = Arc::new(Probe::default());
        let mut reg = Registry::new();
        handler(&mut reg, &p, "undo", false, vec![BranchResult::Success]);
        let mut wf = WorkflowRegistry::new();
        let pw = p.clone();
        wf.register("w", move |mut ctx: WorkflowCtx| {
            let p = pw.clone();
            async move {
                ctx.branch("第一步")
                    .on_rollback("local://undo")
                    .run(|| async {
                        p.entered.notify_one();
                        p.release.notified().await;
                        BranchResult::Success
                    })
                    .await?;
                Ok(())
            }
        });
        let task = spawn_driver(&st, reg, wf);
        let api = Api::new(st.clone());

        let mut g = dtmrs_server::tcc_rows("w1");
        g.trans_type = TransType::Workflow;
        g.status = GlobalStatus::Submitted;
        g.payload = serde_json::json!({"name": "w", "input": ""}).to_string();
        st.create_global(&g, &[]).await.unwrap();
        entered(&p).await;
        api.abort("w1")
            .await
            .expect("跑到一半的 workflow 允许 abort");
        p.release.notify_one();

        assert_eq!(
            wait_final(&st, "w1", 20).await,
            GlobalStatus::Failed,
            "[{}] abort 被函数跑完之后的 succeed 盖掉了",
            be.label
        );
        assert_eq!(p.calls(), ["undo@01"], "[{}]", be.label);
        task.abort();
    })
    .await;
}

/// `dtmrs_core::status_contested` 决定 Redis 上哪些落状态要真比较（要走 Lua）、
/// 哪些可以直接写。它漏掉一个 abort / submit 能改的状态，那个状态上的并发 abort
/// 就会被推进器盖掉 —— 所以把它跟 Api 的实际放行规则逐格钉在一起
#[tokio::test]
async fn 外部能改的状态必须跟api的放行规则一致() {
    let st = Store::open("sqlite::memory:").await.unwrap();
    let api = Api::new(st.clone());
    let statuses = [
        GlobalStatus::Prepared,
        GlobalStatus::Submitted,
        GlobalStatus::Aborting,
        GlobalStatus::Succeed,
        GlobalStatus::Failed,
    ];
    let types = [
        TransType::Saga,
        TransType::Tcc,
        TransType::Msg,
        TransType::Xa,
        TransType::Workflow,
    ];
    let mut n = 0;
    for status in statuses {
        for tt in types {
            let mut changed = false;
            for op in ["abort", "submit"] {
                n += 1;
                let gid = format!("c{n}");
                let mut g = dtmrs_server::tcc_rows(&gid);
                g.trans_type = tt;
                g.status = status;
                st.create_global(&g, &[]).await.unwrap();
                let _ = match op {
                    "abort" => api.abort(&gid).await,
                    _ => api.submit(&gid, &tt.to_string(), &[]).await,
                };
                changed |= st.get_global(&gid).await.unwrap().unwrap().status != status;
            }
            assert_eq!(
                dtmrs_core::status_contested(status, tt),
                changed,
                "({}, {tt})：Api {}能改它，status_contested 却说{}",
                status.as_str(),
                if changed { "" } else { "不" },
                if changed { "不能" } else { "能" },
            );
        }
    }
}
