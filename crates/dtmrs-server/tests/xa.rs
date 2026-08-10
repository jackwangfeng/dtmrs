//! XA 端到端：Postgres 和 MySQL 的原生两阶段提交，各跑一遍全套。
//!
//! ```bash
//! # Postgres（2PC 默认关着，必须显式打开）
//! docker run -d --rm -p 55434:5432 -e POSTGRES_PASSWORD=dtmrs -e POSTGRES_DB=dtmrs \
//!   postgres:16-alpine -c max_prepared_transactions=32
//! export DTMRS_TEST_XA_PG='postgres://postgres:dtmrs@127.0.0.1:55434/dtmrs'
//!
//! # MySQL（XA 默认开着）
//! docker run -d -p 33306:3306 -e MYSQL_ROOT_PASSWORD=dtmrs -e MYSQL_DATABASE=dtmrs mysql:8.0
//! export DTMRS_TEST_XA_MYSQL='mysql://root:dtmrs@127.0.0.1:33306/dtmrs'
//! ```
//!
//! 没配环境变量就跳过 —— **跳过不等于通过**，会打印醒目提示。
//!
//! # 写这套测试时撞上的两条 XA 约束
//!
//! **1. 没解决的 prepared 事务会阻塞无关写入。**
//! 第一版所有测试共用同几行数据，某个测试中途失败留下一个 prepared 事务，
//! 后面完全不相关的 UPDATE 就无限期阻塞了，整个测试进程卡死两分钟。
//! 这正是 XA 在生产上最危险的失效模式。对策：每个测试用专属行、连接设
//! `lock_timeout`、开池先清残留。最后一个测试专门把它钉成断言。
//!
//! **2. XA 的分支必须操作不相交的数据。**
//! 第一版两个分支改同几行，分支 02 被锁死。这不是 bug，是 XA 的本质：
//! **一个分支对应一个资源管理器**，真实场景里天然分布在不同库。
//! 所以下面按"一个分支扣款、另一个分支入账"来写。

use dtmrs_core::{Backend, BranchOp, GlobalStatus, TransType};
use dtmrs_server::driver::Driver;
use dtmrs_server::tcc_rows;
use dtmrs_store::Store;
use dtmrs_xa::{PreparedXact, Resolved, Xa};
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;

/// 一个可测的后端
struct Target {
    name: &'static str,
    pool: AnyPool,
    be: Backend,
    xa: Xa,
}

/// 取所有配了环境变量的后端。**一个都没配就打印醒目提示。**
async fn targets(ids: &[(i32, i64)]) -> Vec<Target> {
    sqlx::any::install_default_drivers();
    let mut out = Vec::new();
    for (name, env) in [
        ("postgres", "DTMRS_TEST_XA_PG"),
        ("mysql", "DTMRS_TEST_XA_MYSQL"),
    ] {
        let Ok(url) = std::env::var(env) else {
            continue;
        };
        let be = Backend::from_url(&url);
        let xa = Xa::from_url(&url).expect("这两种库都支持 XA");
        let pool = open(&url, be).await;
        reset(&pool, be, &xa, ids).await;
        out.push(Target { name, pool, be, xa });
    }
    if out.is_empty() {
        eprintln!(
            "\n⚠ 跳过 XA 测试：DTMRS_TEST_XA_PG / DTMRS_TEST_XA_MYSQL 都没配。\n  \
             这不等于 XA 通过 —— XA 只有对着真数据库才能验。\n"
        );
    }
    out
}

async fn open(url: &str, be: Backend) -> AnyPool {
    let lock_sql = match be {
        // 等锁超过 5 秒就报错。XA 的锁问题一旦出现就是无限期挂着，
        // 有这个上限才能把"卡住"变成"报错"
        Backend::Postgres => "SET lock_timeout = '5s'",
        Backend::MySql => "SET SESSION innodb_lock_wait_timeout = 5",
        _ => "SELECT 1",
    };
    AnyPoolOptions::new()
        .max_connections(6)
        .after_connect(move |conn, _| {
            Box::pin(async move {
                sqlx::query(lock_sql).execute(&mut *conn).await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .unwrap()
}

/// 建业务表，**带重试**。
///
/// Postgres 的 `CREATE TABLE IF NOT EXISTS` 不是并发安全的：多个测试同时建
/// 同一张表会在系统目录上撞唯一键（`23505 pg_type_typname_nsp_index`），
/// 而不是老老实实跳过。这跟 `dtmrs-store::migrate_racy` 处理的是同一个问题。
///
/// 这条一度只在 CI 上炸、本地全绿 —— 因为本地的库里表早就建好了，
/// `IF NOT EXISTS` 直接短路，压根没进到会竞态的那条路径。**全新的库才撞得出来。**
async fn create_acct_racy(pool: &AnyPool) {
    const SQL: &str = "CREATE TABLE IF NOT EXISTS xa_acct(id INT PRIMARY KEY, bal BIGINT)";
    let mut last = None;
    for _ in 0..5 {
        match sqlx::query(SQL).execute(pool).await {
            Ok(_) => return,
            Err(e) => {
                // 输的那个重试时表已经建好了，IF NOT EXISTS 正常跳过
                last = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(80)).await;
            }
        }
    }
    panic!("建 xa_acct 失败: {}", last.unwrap());
}

async fn reset(pool: &AnyPool, be: Backend, xa: &Xa, ids: &[(i32, i64)]) {
    // 残留的 prepared 事务会锁住行，先全清掉
    for x in xa.list_prepared(pool).await.unwrap() {
        let _ = xa.rollback_prepared(pool, &x.xid).await;
    }
    create_acct_racy(pool).await;
    for (id, bal) in ids {
        // 三种库的"插入或覆盖"写法不同，这里只有两种，手写更清楚
        let sql = match be {
            Backend::MySql => {
                "INSERT INTO xa_acct (id,bal) VALUES (?,?) ON DUPLICATE KEY UPDATE bal=VALUES(bal)"
            }
            _ => "INSERT INTO xa_acct (id,bal) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET bal = EXCLUDED.bal",
        };
        sqlx::query(sql)
            .bind(id)
            .bind(bal)
            .execute(pool)
            .await
            .unwrap();
    }
}

async fn bal(t: &Target, ids: &[i32]) -> Vec<i64> {
    let mut v = Vec::new();
    for id in ids {
        let b: i64 = sqlx::query_scalar(&t.be.q("SELECT bal FROM xa_acct WHERE id=?"))
            .bind(id)
            .fetch_one(&t.pool)
            .await
            .unwrap();
        v.push(b);
    }
    v
}

/// 只看本测试自己的 xid，别被别的测试的残留干扰
async fn hanging(t: &Target, gid: &str) -> Vec<PreparedXact> {
    let key = format!("_{gid}_");
    t.xa.list_prepared(&t.pool)
        .await
        .unwrap()
        .into_iter()
        .filter(|x| x.xid.contains(&key))
        .collect()
}

/// 假的 RM 回调服务：TC 调 /commit 或 /rollback，这里执行真正的二阶段
async fn spawn_rm(pool: AnyPool, xa: Xa) -> String {
    use axum::extract::{Query, State};
    use axum::routing::post;
    use axum::Router;
    use std::collections::HashMap;

    async fn handler(
        State((pool, xa, commit)): State<(AnyPool, Xa, bool)>,
        Query(q): Query<HashMap<String, String>>,
    ) -> &'static str {
        let gid = q.get("gid").cloned().unwrap_or_default();
        let bid = q.get("branch_id").cloned().unwrap_or_default();
        let x = xa.xid(&gid, &bid);
        let r = if commit {
            xa.commit_prepared(&pool, &x).await
        } else {
            xa.rollback_prepared(&pool, &x).await
        };
        match r {
            // AlreadyResolved 也算成功 —— TC 重试时一定撞上
            Ok(_) => "SUCCESS",
            Err(e) => {
                eprintln!("[RM] 解决 {x} 失败: {e}");
                // 结果未知，让 TC 重试。绝不返回 FAILURE ——
                // XA 的方向定了就不能改
                "ONGOING"
            }
        }
    }

    let app = Router::new()
        .route(
            "/commit",
            post(handler).with_state((pool.clone(), xa, true)),
        )
        .route("/rollback", post(handler).with_state((pool, xa, false)));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    format!("http://{addr}")
}

async fn new_xa_trans(store: &Store, gid: &str) {
    let mut g = tcc_rows(gid);
    g.trans_type = TransType::Xa;
    store.create_global(&g, &[]).await.unwrap();
}

/// 客户端一阶段：登记分支 → 业务 SQL → prepare
///
/// `id` 是这个分支**专属**的账户行。不同分支必须改不同的行 ——
/// 见文件头「XA 的分支必须操作不相交的数据」。
async fn prepare_branch(
    t: &Target,
    store: &Store,
    base: &str,
    gid: &str,
    bid: &str,
    id: i32,
    delta: i64,
) {
    // **先登记再做一阶段** —— 反过来的话 prepare 成功但 TC 不知道这个分支，
    // 就留下一个永久持锁的 prepared 事务
    store
        .register_branch(
            gid,
            bid,
            &[
                (BranchOp::Commit, format!("{base}/commit")),
                (BranchOp::Rollback, format!("{base}/rollback")),
            ],
        )
        .await
        .unwrap();

    let mut br = t.xa.begin(&t.pool, gid, bid).await.unwrap();
    let sql = t.be.q("UPDATE xa_acct SET bal = bal + ? WHERE id = ?");
    sqlx::query(&sql)
        .bind(delta)
        .bind(id)
        .execute(br.conn())
        .await
        .unwrap();
    br.prepare().await.unwrap();
}

#[tokio::test]
async fn xa_提交前改动不可见_提交后一起生效() {
    for t in targets(&[(11, 1000), (12, 0)]).await {
        let base = spawn_rm(t.pool.clone(), t.xa).await;
        let store = Store::open("sqlite::memory:").await.unwrap();
        let d = Driver::new(store.clone(), "tc".into());
        let gid = "xaok";

        new_xa_trans(&store, gid).await;
        // 模拟跨库转账：分支 01 在"付款方库"扣钱，分支 02 在"收款方库"入账
        prepare_branch(&t, &store, &base, gid, "01", 11, -100).await;
        prepare_branch(&t, &store, &base, gid, "02", 12, 100).await;

        // 一阶段完成：改动已持久化，但**对外不可见** —— XA 相对 SAGA 的核心优势
        assert_eq!(
            bal(&t, &[11, 12]).await,
            vec![1000, 0],
            "{}: prepare 后不该可见",
            t.name
        );
        assert_eq!(hanging(&t, gid).await.len(), 2, "{}: 应有 2 个挂着", t.name);

        store
            .set_global_status(gid, GlobalStatus::Submitted, "")
            .await
            .unwrap();
        let g = store.get_global(gid).await.unwrap().unwrap();
        d.process(&g).await.unwrap();

        assert_eq!(
            store.get_global(gid).await.unwrap().unwrap().status,
            GlobalStatus::Succeed,
            "{}",
            t.name
        );
        assert_eq!(
            bal(&t, &[11, 12]).await,
            vec![900, 100],
            "{}: 两个分支的改动一起生效",
            t.name
        );
        assert!(
            hanging(&t, gid).await.is_empty(),
            "{}: 必须全部解决",
            t.name
        );
    }
}

#[tokio::test]
async fn xa_中止则全部回滚_余额不动() {
    for t in targets(&[(21, 1000), (22, 0)]).await {
        let base = spawn_rm(t.pool.clone(), t.xa).await;
        let store = Store::open("sqlite::memory:").await.unwrap();
        let d = Driver::new(store.clone(), "tc".into());
        let gid = "xarb";

        new_xa_trans(&store, gid).await;
        prepare_branch(&t, &store, &base, gid, "01", 21, -100).await;
        prepare_branch(&t, &store, &base, gid, "02", 22, 100).await;

        store
            .set_global_status(gid, GlobalStatus::Aborting, "某分支一阶段失败")
            .await
            .unwrap();
        let g = store.get_global(gid).await.unwrap().unwrap();
        d.process(&g).await.unwrap();

        assert_eq!(
            store.get_global(gid).await.unwrap().unwrap().status,
            GlobalStatus::Failed,
            "{}",
            t.name
        );
        assert_eq!(
            bal(&t, &[21, 22]).await,
            vec![1000, 0],
            "{}: 余额一点没动",
            t.name
        );
        assert!(hanging(&t, gid).await.is_empty(), "{}", t.name);
    }
}

#[tokio::test]
async fn xa_二阶段幂等_重复提交不报错() {
    // TC 一定会重试。重复的二阶段在两种库上都会报错，但错误码不同
    // （Postgres `42704` / MySQL `XAE04`），都必须被当成"已解决过"。
    for t in targets(&[(31, 1000)]).await {
        let base = spawn_rm(t.pool.clone(), t.xa).await;
        let store = Store::open("sqlite::memory:").await.unwrap();
        let gid = "xaidem";

        new_xa_trans(&store, gid).await;
        prepare_branch(&t, &store, &base, gid, "01", 31, -10).await;

        let x = t.xa.xid(gid, "01");
        assert_eq!(
            t.xa.commit_prepared(&t.pool, &x).await.unwrap(),
            Resolved::Done,
            "{}",
            t.name
        );
        assert_eq!(
            t.xa.commit_prepared(&t.pool, &x).await.unwrap(),
            Resolved::AlreadyResolved,
            "{}: 重复提交必须是 AlreadyResolved 而不是报错",
            t.name
        );
        // 对一个已提交的做回滚也一样 —— 找不到就是已解决
        assert_eq!(
            t.xa.rollback_prepared(&t.pool, &x).await.unwrap(),
            Resolved::AlreadyResolved,
            "{}",
            t.name
        );
        assert_eq!(bal(&t, &[31]).await, vec![990], "{}: 只生效一次", t.name);
    }
}

#[tokio::test]
async fn xa_分支忘了收尾也不会污染连接池() {
    // XaBranch 开了事务就被丢弃（比如中间 `?` 提前返回）。
    // 那条连接还带着未完事务，直接还回池里会让下一个使用者跑在别人的事务里。
    for t in targets(&[(41, 1000)]).await {
        {
            let mut br = t.xa.begin(&t.pool, "xadrop", "01").await.unwrap();
            let sql = t.be.q("UPDATE xa_acct SET bal = ? WHERE id = 41");
            sqlx::query(&sql)
                .bind(12345i64)
                .execute(br.conn())
                .await
                .unwrap();
            // 既不 prepare 也不 discard，直接丢
        }
        // Drop 里起的回滚任务需要一点时间
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        assert!(
            hanging(&t, "xadrop").await.is_empty(),
            "{}: 没 prepare 的分支不该留下 prepared 事务",
            t.name
        );
        assert_eq!(
            bal(&t, &[41]).await,
            vec![1000],
            "{}: 未收尾的改动必须回滚",
            t.name
        );
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM xa_acct")
            .fetch_one(&t.pool)
            .await
            .unwrap();
        assert!(n >= 1, "{}: 连接池没被污染", t.name);
    }
}

#[tokio::test]
async fn xa_启动自检能确认两阶段可用() {
    for t in targets(&[(51, 0)]).await {
        let n = t.xa.ensure_enabled(&t.pool).await.unwrap_or_else(|e| {
            panic!("{}: 自检失败 {e}", t.name);
        });
        assert!(n > 0, "{}: 自检应该通过，实际 {n}", t.name);
    }
}

#[tokio::test]
async fn xa_没解决的prepared事务会阻塞无关写入() {
    // **把 XA 最危险的失效模式钉成断言。**
    //
    // 一个 prepare 了但没解决的事务会一直持有行锁，别的会话再改那些行就
    // 无限期等待（这里靠 lock_timeout / innodb_lock_wait_timeout 变成快速失败）。
    // 生产上这会连锁放大成整库不可写。
    //
    // 这就是为什么 list_prepared 必须上监控，也是为什么拿不准就别用 XA。
    for t in targets(&[(61, 1000)]).await {
        let mut br = t.xa.begin(&t.pool, "xablock", "01").await.unwrap();
        let sql = t.be.q("UPDATE xa_acct SET bal = bal - ? WHERE id = 61");
        sqlx::query(&sql)
            .bind(1i64)
            .execute(br.conn())
            .await
            .unwrap();
        let x = br.prepare().await.unwrap();

        // 监控视角：能看到它挂在那儿
        let h = t.xa.list_prepared(&t.pool).await.unwrap();
        assert!(
            h.iter().any(|p| p.xid == x),
            "{}: list_prepared 要能看到",
            t.name
        );

        // 另一个会话改同一行 —— 会被锁住，超时后报错
        let blocked = sqlx::query(&t.be.q("UPDATE xa_acct SET bal = 999 WHERE id = 61"))
            .execute(&t.pool)
            .await;
        assert!(
            blocked.is_err(),
            "{}: 没解决的 prepared 事务必须阻塞无关写入 —— 这是 XA 的固有代价",
            t.name
        );

        // 解决掉之后立刻恢复正常
        t.xa.rollback_prepared(&t.pool, &x).await.unwrap();
        sqlx::query(&t.be.q("UPDATE xa_acct SET bal = 999 WHERE id = 61"))
            .execute(&t.pool)
            .await
            .unwrap_or_else(|e| panic!("{}: 解决之后应该能正常写: {e}", t.name));
        assert_eq!(bal(&t, &[61]).await, vec![999], "{}", t.name);
    }
}
