//! XA 模式的压测支持。
//!
//! # 跟别的模式不一样在哪
//!
//! 其它模式的业务分支是**零操作** HTTP，所以测出来基本是 TC + 存储的开销。
//! XA 做不到这一点：它的一阶段就是**在业务库上真的做两阶段提交**
//! （`XA START` → 业务 SQL → `XA END` → `XA PREPARE`），二阶段才是
//! `XA COMMIT`。把这部分掏空，测出来的就不是 XA。
//!
//! 所以这里起一个真的资源管理器（RM），连真的 Postgres/MySQL。
//! **结论也因此不同**：这个数字的上限在数据库，不在 TC。
//!
//! # 三条必须知道的约束（前两条是 XA 本身的，第三条是压测的）
//!
//! 1. **Postgres 的 `max_prepared_transactions` 默认是 0** —— 不改配置根本
//!    用不了 XA。而且它直接**限死了在途并发**：设成 32 就最多 32 笔同时
//!    处于 prepared。压测要放宽（我们用 2000），但生产上这个值通常很小。
//! 2. **一笔事务的各分支必须操作不相交的数据**，否则互相锁死 —— 这不是 bug，
//!    是 XA 的本质（一个分支 = 一个资源管理器）。所以这里每笔事务
//!    **INSERT 自己的一行**，天然无争用。测的是吞吐上限，不是争用行为。
//! 3. **没解决的 prepared 事务会永久持锁**。压测中途崩了要手动清，
//!    否则后面所有测试都会被无关地阻塞（`XA RECOVER` / `pg_prepared_xacts`）。
//!
//! # ⚠ 为什么 XA 操作要挪到单独的线程里
//!
//! sqlx 的 `Any` 驱动有个已知的 HRTB 限制：
//!
//! ```text
//! error: implementation of `Executor` is not general enough
//!   `Executor<'_>` would have to be implemented for `&'0 mut AnyConnection`,
//!   for any lifetime '0 ...but is actually implemented for some specific lifetime '1
//! ```
//!
//! 结果就是 `Xa::begin` 的 future **编译器证明不了 `Send`**，而 axum 的 handler
//! 必须 `Send`。（`#[tokio::test]` 里没事，因为那不要求 Send。）
//!
//! 所以这里把 XA 操作放进一个跑 `LocalSet` 的专用线程，HTTP handler 只通过
//! channel 收发 —— 消息都是 String 和 bool，天然 Send。

use tokio::sync::{mpsc, oneshot};

/// 交给 XA 线程干的一件事
enum Job {
    /// 一阶段：begin → 业务 SQL → prepare
    First {
        gid: String,
        bid: String,
    },
    Commit {
        gid: String,
        bid: String,
    },
    Rollback {
        gid: String,
        bid: String,
    },
}

use axum::extract::State;
use axum::routing::post;
use axum::Router;
use dtmrs_core::Backend;
use dtmrs_xa::Xa;
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// handler 侧的状态：**不碰 sqlx**，只有 channel 和 HTTP 客户端。
/// 这样 handler 的 future 才是 Send（见模块头）
#[derive(Clone)]
pub struct RmState {
    jobs: mpsc::Sender<(Job, oneshot::Sender<bool>)>,
    tc: String,
    http: reqwest::Client,
    base: String,
}

/// 起一个真的 XA 资源管理器。返回它的基址。
///
/// 结构：HTTP 服务（Send）→ channel → XA 工作线程（非 Send，跑 LocalSet）
pub async fn spawn_rm(
    dsn: &str,
    tc: &str,
    port: u16,
    done: Arc<AtomicU64>,
) -> Result<String, String> {
    let (tx, rx) = mpsc::channel::<(Job, oneshot::Sender<bool>)>(4096);
    let (ready_tx, ready_rx) = oneshot::channel::<Result<(), String>>();

    let (dsn2, done2) = (dsn.to_string(), done.clone());
    // 专用线程 + current_thread 运行时：这里的 future 不需要 Send
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("建运行时");
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, xa_worker(dsn2, rx, done2, ready_tx));
    });
    ready_rx.await.map_err(|_| "XA 线程没起来".to_string())??;

    let base = format!("http://127.0.0.1:{port}");
    let st = RmState {
        jobs: tx,
        tc: tc.to_string(),
        http: reqwest::Client::new(),
        base: base.clone(),
    };
    let app = Router::new()
        .route("/xa1", post(first_phase))
        .route("/commit", post(commit))
        .route("/rollback", post(rollback))
        .with_state(st);
    let l = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .map_err(|e| format!("RM 绑不上端口: {e}"))?;
    tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    Ok(base)
}

/// XA 工作线程：所有碰 sqlx 的活都在这里，用 spawn_local 保持并发
async fn xa_worker(
    dsn: String,
    mut rx: mpsc::Receiver<(Job, oneshot::Sender<bool>)>,
    done: Arc<AtomicU64>,
    ready: oneshot::Sender<Result<(), String>>,
) {
    sqlx::any::install_default_drivers();
    let setup = async {
        let pool = AnyPoolOptions::new()
            .max_connections(300)
            .connect(&dsn)
            .await
            .map_err(|e| format!("连不上业务库: {e}"))?;
        let xa = Xa::from_url(&dsn).map_err(|e| format!("{e}"))?;
        // ⚠ 占位符必须过方言层：模板统一写 `?`，Postgres 上要转成 `$1`。
        // 漏了这步的话 INSERT 会静默失败，而 commit_prepared 对不存在的 xid
        // 是「找不到就算成功」—— 于是压测显示 100% 成功，业务表里一行都没有。
        // （踩过，靠查行数才发现）
        let be = Backend::from_url(&dsn);
        let ins = be.q("INSERT INTO xa_bench (gid, val) VALUES (?, ?)");
        xa.ensure_enabled(&pool)
            .await
            .map_err(|e| format!("业务库没开两阶段: {e}"))?;
        // 每笔事务插自己的一行，天然无争用
        sqlx::query("CREATE TABLE IF NOT EXISTS xa_bench (gid VARCHAR(128), val BIGINT)")
            .execute(&pool)
            .await
            .map_err(|e| format!("建表失败: {e}"))?;
        let _ = sqlx::query("DELETE FROM xa_bench").execute(&pool).await;
        Ok::<_, String>((pool, xa, ins))
    };
    let (pool, xa, ins) = match setup.await {
        Ok(v) => {
            let _ = ready.send(Ok(()));
            v
        }
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };

    while let Some((job, reply)) = rx.recv().await {
        let (pool, xa, done, ins) = (pool.clone(), xa, done.clone(), ins.clone());
        // spawn_local：不要求 Send，但仍然并发
        tokio::task::spawn_local(async move {
            let ok = match job {
                Job::First { gid, bid } => match xa.begin(&pool, &gid, &bid).await {
                    Ok(mut br) => {
                        let q = sqlx::query(&ins)
                            .bind(&gid)
                            .bind(1i64)
                            .execute(br.conn())
                            .await;
                        q.is_ok() && br.prepare().await.is_ok()
                    }
                    Err(_) => false,
                },
                Job::Commit { gid, bid } => {
                    let xid = xa.xid(&gid, &bid);
                    let r = xa.commit_prepared(&pool, &xid).await.is_ok();
                    if r {
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                    r
                }
                Job::Rollback { gid, bid } => {
                    let xid = xa.xid(&gid, &bid);
                    xa.rollback_prepared(&pool, &xid).await.is_ok()
                }
            };
            let _ = reply.send(ok);
        });
    }
}

/// 把一件事交给 XA 线程并等结果
async fn run_job(s: &RmState, job: Job) -> bool {
    let (tx, rx) = oneshot::channel();
    if s.jobs.send((job, tx)).await.is_err() {
        return false;
    }
    rx.await.unwrap_or(false)
}

/// 从 URL 里取 gid 和 branch_id —— TC 调分支时是拼在 query 上的
fn gid_branch(uri: &axum::http::Uri) -> (String, String) {
    let (mut gid, mut bid) = (String::new(), "01".to_string());
    for kv in uri.query().unwrap_or("").split('&') {
        match kv.split_once('=') {
            Some(("gid", v)) => gid = v.to_string(),
            Some(("branch_id", v)) => bid = v.to_string(),
            _ => {}
        }
    }
    (gid, bid)
}

/// 一阶段：**先登记再 prepare**。
///
/// ⚠ 顺序不能反 —— 反过来的话 prepare 成功但 TC 不知道有这个分支，
/// 就留下一个**永久持锁的 prepared 事务**，比 SAGA 漏补偿严重得多。
async fn first_phase(State(s): State<RmState>, uri: axum::http::Uri) -> String {
    let (gid, bid) = gid_branch(&uri);
    // ⚠ 先登记再 prepare —— 反过来的话 prepare 成功但 TC 不知道有这个分支，
    // 就留下一个永久持锁的 prepared 事务
    let r = s
        .http
        .post(format!("{}/api/dtmsvr/registerBranch", s.tc))
        .json(&serde_json::json!({
            "gid": gid, "branch_id": bid,
            "commit": format!("{}/commit", s.base),
            "rollback": format!("{}/rollback", s.base),
        }))
        .send()
        .await;
    if r.is_err() {
        return FAIL.into();
    }
    if run_job(&s, Job::First { gid, bid }).await {
        "{}".into()
    } else {
        FAIL.into()
    }
}

async fn commit(State(s): State<RmState>, uri: axum::http::Uri) -> String {
    let (gid, bid) = gid_branch(&uri);
    if run_job(&s, Job::Commit { gid, bid }).await {
        "{}".into()
    } else {
        // 二阶段失败绝不能让 TC 以为失败了去回滚 —— 返回未知，让它重试
        "{\"dtm_result\":\"ONGOING\"}".into()
    }
}

async fn rollback(State(s): State<RmState>, uri: axum::http::Uri) -> String {
    let (gid, bid) = gid_branch(&uri);
    run_job(&s, Job::Rollback { gid, bid }).await;
    "{}".into()
}

const FAIL: &str = "{\"dtm_result\":\"FAILURE\"}";

/// 跑完之后核对业务库里真的写进去了多少行。
///
/// ⚠ **这个检查不能省。** XA 的 `commit_prepared` 对不存在的 xid 是
/// 「找不到就算成功」（那是状态机保证下的正确行为），所以**一阶段全军覆没时
/// 压测照样显示 100% 成功**。第一版就是这样：占位符没过方言层，
/// Postgres 上 INSERT 全部语法错误，而压测报告 5000/5000 落终态，
/// 业务表里一行都没有。
pub async fn verify_rows(dsn: &str) -> (i64, i64) {
    sqlx::any::install_default_drivers();
    let Ok(pool) = AnyPoolOptions::new().max_connections(2).connect(dsn).await else {
        return (-1, -1);
    };
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM xa_bench")
        .fetch_one(&pool)
        .await
        .unwrap_or(-1);
    let stuck = match Xa::from_url(dsn) {
        Ok(xa) => xa
            .list_prepared(&pool)
            .await
            .map(|v| v.len() as i64)
            .unwrap_or(-1),
        Err(_) => -1,
    };
    (rows, stuck)
}

/// 清掉上一轮压测崩溃时留下的 prepared 事务。
///
/// **不清的话后面所有事务都会被无关地阻塞** —— 那些事务永久持着行锁。
pub async fn clear_prepared(dsn: &str) -> String {
    sqlx::any::install_default_drivers();
    let Ok(pool) = AnyPoolOptions::new().max_connections(2).connect(dsn).await else {
        return "连不上业务库".into();
    };
    let Ok(xa) = Xa::from_url(dsn) else {
        return "不认识的 DSN".into();
    };
    let Ok(list) = xa.list_prepared(&pool).await else {
        return "查不到 prepared 事务".into();
    };
    let n = list.len();
    for p in list {
        let _ = xa.rollback_prepared(&pool, &p.xid).await;
    }
    format!("清掉 {n} 笔残留的 prepared 事务")
}
