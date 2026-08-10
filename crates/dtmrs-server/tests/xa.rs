//! XA 端到端：真 Postgres 的两阶段提交。
//!
//! 需要一个开了 2PC 的 Postgres：
//!
//! ```bash
//! docker run -d --rm -p 55433:5432 -e POSTGRES_PASSWORD=dtmrs -e POSTGRES_DB=dtmrs \
//!   postgres:16-alpine -c max_prepared_transactions=32
//! export DTMRS_TEST_XA_PG='postgres://postgres:dtmrs@127.0.0.1:55433/dtmrs'
//! ```
//!
//! 没配环境变量就跳过 —— **跳过不等于通过**，会打印醒目提示。
//!
//! # 写这套测试时自己撞上的 XA 陷阱
//!
//! 第一版所有测试共用同几行数据。某个测试中途失败留下一个没解决的
//! prepared 事务，**后面完全不相关的 UPDATE 就无限期阻塞了** ——
//! 整个测试进程卡死两分钟才被超时杀掉。
//!
//! 这正是 XA 在生产上最危险的失效模式，只不过在测试里提前撞到了。所以：
//!
//! 1. 每个测试用**自己专属的账户行**，互不干扰
//! 2. 连接一律设 `lock_timeout` —— 宁可快速失败，也别无限期挂着
//! 3. 每次开池先把残留的 prepared 事务全部回滚
//!
//! 最后一个测试专门把这个失效模式钉成断言。
//!
//! # 第二条实测撞出来的约束：XA 的分支必须操作不相交的数据
//!
//! 第一版把两个分支写成都改同几行，结果分支 02 直接被锁死
//! （`55P03 canceling statement due to lock timeout`）——
//! 因为分支 01 已经 `PREPARE TRANSACTION` 了，行锁一直持着。
//!
//! 这不是 bug，是 XA 的本质：**一个分支对应一个资源管理器**。
//! 真实场景里分支 01 在服务 A 的库、分支 02 在服务 B 的库，天然不相交。
//! 下面的测试照这个模型写：一个分支扣款、另一个分支入账，各改自己的行。

use dtmrs_core::{BranchOp, GlobalStatus, TransType};
use dtmrs_server::driver::Driver;
use dtmrs_server::tcc_rows;
use dtmrs_store::Store;
use dtmrs_xa::{commit_prepared, list_prepared, rollback_prepared, xid, Resolved, XaBranch};
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;

fn pg_url() -> Option<String> {
    match std::env::var("DTMRS_TEST_XA_PG") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!(
                "\n⚠ 跳过 XA 测试：没配 DTMRS_TEST_XA_PG。\n  \
                 这不等于 XA 通过 —— XA 只有对着真 Postgres 才能验。\n"
            );
            None
        }
    }
}

/// 开业务库连接池。
///
/// `ids` 是这个测试专属的账户行，每个测试用不同的，避免互相锁。
async fn biz_pool(url: &str, ids: &[(i32, i64)]) -> AnyPool {
    sqlx::any::install_default_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(6)
        // 任何等锁超过 5 秒就报错。XA 的锁问题一旦出现就是无限期挂着，
        // 有这个上限才能把"卡住"变成"报错"，不然只能等超时杀进程
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("SET lock_timeout = '5s'").execute(&mut *conn).await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .unwrap();

    // 残留的 prepared 事务会锁住行，先全清掉
    for x in list_prepared(&pool).await.unwrap() {
        let _ = rollback_prepared(&pool, &x.xid).await;
    }
    sqlx::query("CREATE TABLE IF NOT EXISTS xa_acct(id INT PRIMARY KEY, bal BIGINT)")
        .execute(&pool)
        .await
        .unwrap();
    for (id, bal) in ids {
        sqlx::query(
            "INSERT INTO xa_acct (id,bal) VALUES ($1,$2)
             ON CONFLICT (id) DO UPDATE SET bal = $3",
        )
        .bind(id)
        .bind(bal)
        .bind(bal)
        .execute(&pool)
        .await
        .unwrap();
    }
    pool
}

async fn bal(pool: &AnyPool, ids: &[i32]) -> Vec<i64> {
    let mut v = Vec::new();
    for id in ids {
        let b: i64 = sqlx::query_scalar("SELECT bal FROM xa_acct WHERE id=$1")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap();
        v.push(b);
    }
    v
}

/// 只看本测试自己那几个 xid，别被别的测试的残留干扰
async fn hanging(pool: &AnyPool, gid: &str) -> Vec<String> {
    list_prepared(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|x| x.xid)
        .filter(|x| x.contains(&gid.replace('-', "-")))
        .collect()
}

/// 假的 RM 回调服务：TC 调 /commit 或 /rollback，这里执行真正的二阶段
async fn spawn_rm(pool: AnyPool) -> String {
    use axum::extract::{Query, State};
    use axum::routing::post;
    use axum::Router;
    use std::collections::HashMap;

    async fn handler(
        State((pool, commit)): State<(AnyPool, bool)>,
        Query(q): Query<HashMap<String, String>>,
    ) -> &'static str {
        let gid = q.get("gid").cloned().unwrap_or_default();
        let bid = q.get("branch_id").cloned().unwrap_or_default();
        let x = xid(&gid, &bid);
        let r = if commit {
            commit_prepared(&pool, &x).await
        } else {
            rollback_prepared(&pool, &x).await
        };
        match r {
            // AlreadyResolved 也算成功 —— TC 重试时一定撞上
            Ok(_) => "SUCCESS",
            Err(e) => {
                eprintln!("[RM] 解决 {x} 失败: {e}");
                // 结果未知，让 TC 重试。绝不返回 FAILURE ——
                // 那会让 TC 以为方向该变，而 XA 的方向定了就不能改
                "ONGOING"
            }
        }
    }

    let app = Router::new()
        .route("/commit", post(handler).with_state((pool.clone(), true)))
        .route("/rollback", post(handler).with_state((pool, false)));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    format!("http://{addr}")
}

async fn new_xa(store: &Store, gid: &str) {
    let mut g = tcc_rows(gid);
    g.trans_type = TransType::Xa;
    store.create_global(&g, &[]).await.unwrap();
}

/// 客户端一阶段：登记分支 → 业务 SQL → PREPARE TRANSACTION
///
/// `id` 是这个分支**专属**的账户行。不同分支必须改不同的行 ——
/// 见文件头「XA 的分支必须操作不相交的数据」。
async fn prepare_branch(
    biz: &AnyPool,
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

    let mut br = XaBranch::begin(biz, gid, bid).await.unwrap();
    sqlx::query("UPDATE xa_acct SET bal = bal + $1 WHERE id = $2")
        .bind(delta)
        .bind(id)
        .execute(br.conn())
        .await
        .unwrap();
    br.prepare().await.unwrap();
}

#[tokio::test]
async fn xa_提交前改动不可见_提交后一起生效() {
    let Some(url) = pg_url() else { return };
    let biz = biz_pool(&url, &[(11, 1000), (12, 0)]).await;
    let base = spawn_rm(biz.clone()).await;
    let store = Store::open("sqlite::memory:").await.unwrap();
    let d = Driver::new(store.clone(), "tc".into());

    new_xa(&store, "xaok").await;
    // 模拟跨库转账：分支 01 在"付款方库"扣钱，分支 02 在"收款方库"入账
    prepare_branch(&biz, &store, &base, "xaok", "01", 11, -100).await;
    prepare_branch(&biz, &store, &base, "xaok", "02", 12, 100).await;

    // 一阶段完成：改动已持久化，但**对外不可见** —— 这是 XA 相对 SAGA 的核心优势
    assert_eq!(bal(&biz, &[11, 12]).await, vec![1000, 0], "PREPARE 后改动不该可见");
    assert_eq!(hanging(&biz, "xaok").await.len(), 2, "应有 2 个 prepared 事务挂着");

    store.set_global_status("xaok", GlobalStatus::Submitted, "").await.unwrap();
    let g = store.get_global("xaok").await.unwrap().unwrap();
    d.process(&g).await.unwrap();

    assert_eq!(
        store.get_global("xaok").await.unwrap().unwrap().status,
        GlobalStatus::Succeed
    );
    assert_eq!(
        bal(&biz, &[11, 12]).await,
        vec![900, 100],
        "COMMIT PREPARED 之后两个分支的改动一起生效"
    );
    assert!(
        hanging(&biz, "xaok").await.is_empty(),
        "prepared 事务必须全部解决，否则永久持锁"
    );
}

#[tokio::test]
async fn xa_中止则全部回滚_余额不动() {
    let Some(url) = pg_url() else { return };
    let biz = biz_pool(&url, &[(21, 1000), (22, 0)]).await;
    let base = spawn_rm(biz.clone()).await;
    let store = Store::open("sqlite::memory:").await.unwrap();
    let d = Driver::new(store.clone(), "tc".into());

    new_xa(&store, "xarb").await;
    prepare_branch(&biz, &store, &base, "xarb", "01", 21, -100).await;
    prepare_branch(&biz, &store, &base, "xarb", "02", 22, 100).await;

    // 客户端某个分支一阶段失败了 → abort
    store
        .set_global_status("xarb", GlobalStatus::Aborting, "某分支一阶段失败")
        .await
        .unwrap();
    let g = store.get_global("xarb").await.unwrap().unwrap();
    d.process(&g).await.unwrap();

    assert_eq!(
        store.get_global("xarb").await.unwrap().unwrap().status,
        GlobalStatus::Failed
    );
    assert_eq!(bal(&biz, &[21, 22]).await, vec![1000, 0], "回滚后余额一点没动");
    assert!(hanging(&biz, "xarb").await.is_empty());
}

#[tokio::test]
async fn xa_二阶段幂等_重复提交不报错() {
    // TC 一定会重试。第二次 COMMIT PREPARED 在 Postgres 里会报
    // "prepared transaction does not exist"（SQLSTATE 42704），
    // 必须被当成"已经解决过"而不是失败 —— 否则事务永远推不完。
    let Some(url) = pg_url() else { return };
    let biz = biz_pool(&url, &[(31, 1000), (32, 0)]).await;
    let base = spawn_rm(biz.clone()).await;
    let store = Store::open("sqlite::memory:").await.unwrap();

    new_xa(&store, "xaidem").await;
    prepare_branch(&biz, &store, &base, "xaidem", "01", 31, -10).await;

    let x = xid("xaidem", "01");
    assert_eq!(commit_prepared(&biz, &x).await.unwrap(), Resolved::Done);
    assert_eq!(
        commit_prepared(&biz, &x).await.unwrap(),
        Resolved::AlreadyResolved,
        "重复提交必须返回 AlreadyResolved 而不是报错"
    );
    // 对一个已提交的做回滚也一样 —— 找不到就是已解决
    assert_eq!(
        rollback_prepared(&biz, &x).await.unwrap(),
        Resolved::AlreadyResolved
    );
    assert_eq!(bal(&biz, &[31]).await, vec![990], "只生效一次");
}

#[tokio::test]
async fn xa_分支忘了收尾也不会污染连接池() {
    // XaBranch 开了 BEGIN 就被丢弃（比如中间 `?` 提前返回）。
    // 那条连接还带着未完事务，直接还回池里会让下一个使用者跑在别人的事务里。
    let Some(url) = pg_url() else { return };
    let biz = biz_pool(&url, &[(41, 1000)]).await;

    {
        let mut br = XaBranch::begin(&biz, "xadrop", "01").await.unwrap();
        sqlx::query("UPDATE xa_acct SET bal = 12345 WHERE id = 41")
            .execute(br.conn())
            .await
            .unwrap();
        // 既不 prepare 也不 discard，直接丢
    }
    // Drop 里起的 ROLLBACK 任务需要一点时间
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    assert!(
        hanging(&biz, "xadrop").await.is_empty(),
        "没 prepare 的分支不该留下 prepared 事务"
    );
    assert_eq!(bal(&biz, &[41]).await, vec![1000], "未收尾的改动必须被回滚掉");
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM xa_acct")
        .fetch_one(&biz)
        .await
        .unwrap();
    assert!(n >= 1, "连接池没被污染");
}

#[tokio::test]
async fn xa_启动自检能确认两阶段是否开启() {
    let Some(url) = pg_url() else { return };
    let biz = biz_pool(&url, &[(51, 0)]).await;
    let n = dtmrs_xa::ensure_enabled(&biz).await.unwrap();
    assert!(n > 0, "max_prepared_transactions 应该 > 0，实际 {n}");
}

#[tokio::test]
async fn xa_没解决的prepared事务会阻塞无关写入() {
    // **把 XA 最危险的失效模式钉成断言。**
    //
    // 一个 prepare 了但没 commit/rollback 的事务会一直持有行锁，
    // 别的会话再改那些行就无限期等待（这里靠 lock_timeout 变成快速失败）。
    // 生产上这会连锁放大成整库不可写，还阻塞 VACUUM。
    //
    // 这就是为什么 list_prepared 必须上监控，也是为什么拿不准就别用 XA。
    let Some(url) = pg_url() else { return };
    let biz = biz_pool(&url, &[(61, 1000)]).await;

    let mut br = XaBranch::begin(&biz, "xablock", "01").await.unwrap();
    sqlx::query("UPDATE xa_acct SET bal = bal - 1 WHERE id = 61")
        .execute(br.conn())
        .await
        .unwrap();
    let x = br.prepare().await.unwrap();

    // 监控视角：能看到它挂在那儿
    let h = list_prepared(&biz).await.unwrap();
    assert!(h.iter().any(|p| p.xid == x), "list_prepared 必须能看到它");

    // 另一个会话改同一行 —— 会被锁住，5 秒后 lock_timeout 报错
    let blocked = sqlx::query("UPDATE xa_acct SET bal = 999 WHERE id = 61")
        .execute(&biz)
        .await;
    assert!(
        blocked.is_err(),
        "没解决的 prepared 事务必须阻塞无关写入 —— 这是 XA 的固有代价"
    );

    // 解决掉之后立刻恢复正常
    rollback_prepared(&biz, &x).await.unwrap();
    sqlx::query("UPDATE xa_acct SET bal = 999 WHERE id = 61")
        .execute(&biz)
        .await
        .expect("解决之后应该能正常写");
    assert_eq!(bal(&biz, &[61]).await, vec![999]);
}
