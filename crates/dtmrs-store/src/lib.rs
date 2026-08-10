//! 存储层。TC 本身无状态，所有状态都在这里 —— 所以 TC 可以多实例、可以随时重启。
//!
//! # 一套 SQL 同时跑 sqlite 和 postgres
//!
//! 用 `sqlx::Any` 而不是抽 `Store` trait 写两份实现。实测过的边界（`sqlx 0.8`）：
//!
//! | | sqlite | postgres |
//! |---|---|---|
//! | `$1` 占位符 | ✅ | ✅ |
//! | `?` 占位符 | ✅ | ❌ 语法错误 |
//! | `ON CONFLICT DO NOTHING` | ✅ | ✅ |
//! | 冲突时 `rows_affected` | 1 → 0 | 1 → 0（一致） |
//!
//! 所以两条硬规矩：
//! 1. **只用 `$N` 占位符**，永远不用 `?`
//! 2. **同一个 `$N` 不复用**（sqlite 把 `$4` 当命名参数，复用会让位置绑定错位）
//!
//! 时间统一用 **unix 秒（i64）** 存，不用数据库的 datetime 类型 ——
//! 跨库的时间类型映射是反复踩坑的地方，整数没有这个问题。
//! 列类型用 `BIGINT`：postgres 的 `INTEGER` 只有 4 字节，装不下时间戳。

use dtmrs_core::{BranchOp, BranchStatus, GlobalStatus, TransType};
use sqlx::any::{AnyPoolOptions, AnyRow};
use sqlx::{AnyPool, Row};
use std::sync::Once;

pub type Result<T> = std::result::Result<T, sqlx::Error>;

pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
pub struct GlobalRow {
    pub gid: String,
    pub trans_type: TransType,
    pub status: GlobalStatus,
    pub payload: String,
    pub next_cron_time: i64,
    pub next_cron_interval: i64,
    pub owner: String,
    pub rollback_reason: String,
    /// 二阶段消息的回查地址。进程在 prepare 和 submit 之间崩了，
    /// TC 靠它问业务方"这单本地事务到底提交了没有"
    pub query_prepared: String,
    pub create_time: i64,
    pub finish_time: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct BranchRow {
    pub gid: String,
    pub branch_id: String,
    pub op: BranchOp,
    pub url: String,
    pub payload: String,
    pub status: BranchStatus,
}

#[derive(Clone)]
pub struct Store {
    pool: AnyPool,
}

static DRIVERS: Once = Once::new();

impl Store {
    /// `url` 可以是：
    /// - `sqlite:dtmrs.db` / `sqlite::memory:`
    /// - `postgres://user:pass@host:5432/db`
    pub async fn open(url: &str) -> Result<Self> {
        DRIVERS.call_once(sqlx::any::install_default_drivers);

        // sqlite 默认只读打开，不会建文件。AnyConnectOptions 没法像
        // SqliteConnectOptions 那样设 create_if_missing，只能走 URL 参数。
        let mut url = url.to_string();
        if url.starts_with("sqlite") && !url.contains("mode=") && !url.contains(":memory:") {
            url.push_str(if url.contains('?') { "&mode=rwc" } else { "?mode=rwc" });
        }
        // 内存库必须单连接，否则每条连接看到的是各自独立的库
        let max = if url.contains(":memory:") { 1 } else { 8 };
        let pool = AnyPoolOptions::new().max_connections(max).connect(&url).await?;
        let s = Self { pool };
        s.migrate_racy().await?;
        Ok(s)
    }

    /// 建表，容忍并发。
    ///
    /// **Postgres 的 `CREATE TABLE IF NOT EXISTS` 不是并发安全的** ——
    /// 两个 TC 实例同时启动会在系统目录上撞唯一键：
    /// `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`。
    /// 这是实测撞出来的（sqlite 单写不会暴露）。
    ///
    /// 输了的那个重试一次就好：这时表已经被对方建出来了，
    /// `IF NOT EXISTS` 会正常跳过。
    async fn migrate_racy(&self) -> Result<()> {
        let mut last = None;
        for attempt in 0..3 {
            match self.migrate().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last = Some(e);
                    // 让对方把 DDL 事务提交完
                    tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt + 1)))
                        .await;
                }
            }
        }
        Err(last.expect("循环至少失败一次"))
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS trans_global (
              gid                TEXT PRIMARY KEY,
              trans_type         TEXT   NOT NULL,
              status             TEXT   NOT NULL,
              payload            TEXT   NOT NULL DEFAULT '',
              next_cron_time     BIGINT NOT NULL DEFAULT 0,
              next_cron_interval BIGINT NOT NULL DEFAULT 0,
              owner              TEXT   NOT NULL DEFAULT '',
              rollback_reason    TEXT   NOT NULL DEFAULT '',
              query_prepared     TEXT   NOT NULL DEFAULT '',
              create_time        BIGINT NOT NULL,
              update_time        BIGINT NOT NULL,
              finish_time        BIGINT
            )"#,
        )
        .execute(&self.pool)
        .await?;
        // cron 靠这个索引扫待办，没它到量之后会全表扫
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_status_cron
             ON trans_global(status, next_cron_time)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS trans_branch_op (
              gid         TEXT   NOT NULL,
              branch_id   TEXT   NOT NULL,
              op          TEXT   NOT NULL,
              url         TEXT   NOT NULL,
              payload     TEXT   NOT NULL DEFAULT '',
              status      TEXT   NOT NULL,
              create_time BIGINT NOT NULL,
              update_time BIGINT NOT NULL,
              finish_time BIGINT,
              PRIMARY KEY (gid, branch_id, op)
            )"#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub fn pool(&self) -> &AnyPool {
        &self.pool
    }

    /// 建全局事务 + 所有分支，一个事务里做完。
    ///
    /// 返回 `false` 表示 gid 已存在 —— 这是**幂等提交**，不是错误：
    /// 客户端重试提交时必须拿到"已受理"而不是报错。
    pub async fn create_global(&self, g: &GlobalRow, branches: &[BranchRow]) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let t = now();
        let n = sqlx::query(
            "INSERT INTO trans_global
             (gid,trans_type,status,payload,next_cron_time,next_cron_interval,
              owner,rollback_reason,query_prepared,create_time,update_time)
             VALUES ($1,$2,$3,$4,$5,$6,'','',$7,$8,$9)
             ON CONFLICT DO NOTHING",
        )
        .bind(&g.gid)
        .bind(g.trans_type.to_string())
        .bind(g.status.as_str())
        .bind(&g.payload)
        .bind(g.next_cron_time)
        .bind(g.next_cron_interval)
        .bind(&g.query_prepared)
        .bind(t)
        .bind(t)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if n == 0 {
            tx.rollback().await?;
            return Ok(false);
        }
        for b in branches {
            sqlx::query(
                "INSERT INTO trans_branch_op
                 (gid,branch_id,op,url,payload,status,create_time,update_time)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                 ON CONFLICT DO NOTHING",
            )
            .bind(&b.gid)
            .bind(&b.branch_id)
            .bind(b.op.as_str())
            .bind(&b.url)
            .bind(&b.payload)
            .bind(b.status.as_str())
            .bind(t)
            .bind(t)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(true)
    }

    pub async fn get_global(&self, gid: &str) -> Result<Option<GlobalRow>> {
        let row = sqlx::query(&format!("{SELECT_GLOBAL} WHERE gid=$1"))
            .bind(gid)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(global_from_row))
    }

    pub async fn list_branches(&self, gid: &str) -> Result<Vec<BranchRow>> {
        let rows = sqlx::query(
            "SELECT gid,branch_id,op,url,payload,status FROM trans_branch_op
             WHERE gid=$1 ORDER BY branch_id, op",
        )
        .bind(gid)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| BranchRow {
                gid: r.get("gid"),
                branch_id: r.get("branch_id"),
                op: BranchOp::parse(r.get::<String, _>("op").as_str()).unwrap_or(BranchOp::Action),
                url: r.get("url"),
                payload: r.get("payload"),
                status: BranchStatus::parse(r.get::<String, _>("status").as_str())
                    .unwrap_or(BranchStatus::Prepared),
            })
            .collect())
    }

    pub async fn set_global_status(
        &self,
        gid: &str,
        status: GlobalStatus,
        reason: &str,
    ) -> Result<()> {
        let t = now();
        let fin = if status.is_final() { Some(t) } else { None };
        // 注意 $4/$5 都绑 reason —— 不能复用同一个 $N，见文件头注释
        sqlx::query(
            "UPDATE trans_global SET status=$1, update_time=$2, finish_time=$3,
             rollback_reason = CASE WHEN $4 <> '' THEN $5 ELSE rollback_reason END
             WHERE gid=$6",
        )
        .bind(status.as_str())
        .bind(t)
        .bind(fin)
        .bind(reason)
        .bind(reason)
        .bind(gid)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_branch_status(
        &self,
        gid: &str,
        branch_id: &str,
        op: BranchOp,
        status: BranchStatus,
    ) -> Result<()> {
        let t = now();
        sqlx::query(
            "UPDATE trans_branch_op SET status=$1, update_time=$2,
             finish_time = CASE WHEN $3 <> 'prepared' THEN $4 ELSE finish_time END
             WHERE gid=$5 AND branch_id=$6 AND op=$7",
        )
        .bind(status.as_str())
        .bind(t)
        .bind(status.as_str())
        .bind(t)
        .bind(gid)
        .bind(branch_id)
        .bind(op.as_str())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 抢一个到期的待办事务，**抢占式更新，原子的**。
    ///
    /// 多个 TC 实例同时跑也不会重复推进同一个事务：谁的 UPDATE 生效谁持有租约。
    /// 持租约的实例崩了，`next_cron_time` 到期后别的实例接手 —— 这就是崩溃恢复。
    pub async fn lock_one_due(&self, owner: &str, lease: i64) -> Result<Option<GlobalRow>> {
        let mut tx = self.pool.begin().await?;
        let t = now();
        let gid: Option<String> = sqlx::query_scalar(
            // prepared 的 msg 也要捞：客户端可能在 prepare 之后就崩了，
            // 得靠 TC 回查 query_prepared 决定这单是提交还是作废。
            // 其它类型的 prepared 不碰 —— TCC 的 try 阶段由客户端驱动。
            "SELECT gid FROM trans_global
             WHERE (status IN ('submitted','aborting')
                    OR (status = 'prepared' AND trans_type = 'msg'))
               AND next_cron_time <= $1
             ORDER BY next_cron_time LIMIT 1",
        )
        .bind(t)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(gid) = gid else {
            tx.rollback().await?;
            return Ok(None);
        };
        // 立刻把 next_cron_time 推到租约之后，等于占坑
        let n = sqlx::query(
            "UPDATE trans_global SET owner=$1, next_cron_time=$2, update_time=$3
             WHERE gid=$4 AND next_cron_time <= $5",
        )
        .bind(owner)
        .bind(t + lease)
        .bind(t)
        .bind(&gid)
        .bind(t)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if n == 0 {
            tx.rollback().await?;
            return Ok(None); // 被别人抢走了
        }
        let row = sqlx::query(&format!("{SELECT_GLOBAL} WHERE gid=$1"))
            .bind(&gid)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(Some(global_from_row(row)))
    }

    /// 推进失败后设置下次重试时间（指数退避）
    pub async fn schedule_retry(&self, gid: &str, interval: i64) -> Result<()> {
        let t = now();
        sqlx::query(
            "UPDATE trans_global SET next_cron_interval=$1, next_cron_time=$2, update_time=$3
             WHERE gid=$4",
        )
        .bind(interval)
        .bind(t + interval)
        .bind(t)
        .bind(gid)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 让某个事务立刻可被调度（提交/中止之后叫一下，不用等 cron 周期）
    pub async fn schedule_now(&self, gid: &str) -> Result<()> {
        sqlx::query(
            "UPDATE trans_global SET next_cron_time=$1, next_cron_interval=0 WHERE gid=$2",
        )
        .bind(now())
        .bind(gid)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// TCC 的 try 阶段：客户端在调 try 之前先来登记这个分支的 confirm/cancel。
    ///
    /// **必须先登记再调 try**。反过来的话：try 成功了但登记失败，
    /// TC 就不知道有这个分支，回滚时不会 cancel 它 —— 资源永久泄漏。
    ///
    /// 冲突时忽略，所以重复登记是幂等的（客户端重试很常见）。
    pub async fn register_branch(
        &self,
        gid: &str,
        branch_id: &str,
        ops: &[(BranchOp, String)],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let t = now();
        for (op, url) in ops {
            sqlx::query(
                "INSERT INTO trans_branch_op
                 (gid,branch_id,op,url,payload,status,create_time,update_time)
                 VALUES ($1,$2,$3,$4,'',$5,$6,$7)
                 ON CONFLICT DO NOTHING",
            )
            .bind(gid)
            .bind(branch_id)
            .bind(op.as_str())
            .bind(url)
            .bind(BranchStatus::Prepared.as_str())
            .bind(t)
            .bind(t)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn list_recent(&self, limit: i64) -> Result<Vec<GlobalRow>> {
        let rows = sqlx::query(&format!(
            "{SELECT_GLOBAL} ORDER BY create_time DESC LIMIT $1"
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(global_from_row).collect())
    }
}

/// 列清单只写一处 —— 三个地方读 trans_global，列顺序漂移过一次就够难查了
const SELECT_GLOBAL: &str = "SELECT gid,trans_type,status,payload,next_cron_time,
    next_cron_interval,owner,rollback_reason,query_prepared,create_time,finish_time
    FROM trans_global";

fn global_from_row(r: AnyRow) -> GlobalRow {
    GlobalRow {
        gid: r.get("gid"),
        trans_type: TransType::parse(r.get::<String, _>("trans_type").as_str())
            .unwrap_or(TransType::Saga),
        status: GlobalStatus::parse(r.get::<String, _>("status").as_str())
            .unwrap_or(GlobalStatus::Prepared),
        payload: r.get("payload"),
        next_cron_time: r.get("next_cron_time"),
        next_cron_interval: r.get("next_cron_interval"),
        owner: r.get("owner"),
        rollback_reason: r.get("rollback_reason"),
        query_prepared: r.get("query_prepared"),
        create_time: r.get("create_time"),
        finish_time: r.get("finish_time"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 每个测试都在两种后端上跑一遍。
    ///
    /// postgres 靠环境变量开启（`DTMRS_TEST_PG=postgres://...`）——
    /// 没配就只跑 sqlite，这样没数据库的机器也能 `cargo test`。
    /// 但**别把这当成"postgres 也过了"** —— 没配就是没测。
    async fn backends() -> Vec<(&'static str, Store)> {
        let mut v = vec![("sqlite", Store::open("sqlite::memory:").await.unwrap())];
        if let Ok(url) = std::env::var("DTMRS_TEST_PG") {
            let s = Store::open(&url).await.expect("连不上 DTMRS_TEST_PG");
            // 同一个库反复跑测试，先清干净
            sqlx::query("DROP TABLE IF EXISTS trans_branch_op")
                .execute(s.pool())
                .await
                .unwrap();
            sqlx::query("DROP TABLE IF EXISTS trans_global")
                .execute(s.pool())
                .await
                .unwrap();
            s.migrate().await.unwrap();
            v.push(("postgres", s));
        }
        v
    }

    fn g(gid: &str) -> GlobalRow {
        GlobalRow {
            gid: gid.into(),
            trans_type: TransType::Saga,
            status: GlobalStatus::Submitted,
            payload: "{}".into(),
            next_cron_time: 0,
            next_cron_interval: 0,
            owner: String::new(),
            rollback_reason: String::new(),
            query_prepared: String::new(),
            create_time: 0,
            finish_time: None,
        }
    }

    #[tokio::test]
    async fn 重复提交同一个gid是幂等的() {
        for (name, s) in backends().await {
            assert!(s.create_global(&g("t1"), &[]).await.unwrap(), "{name}");
            // 第二次返回 false 而不是报错 —— 客户端重试不该失败
            assert!(!s.create_global(&g("t1"), &[]).await.unwrap(), "{name}");
            assert_eq!(s.list_recent(10).await.unwrap().len(), 1, "{name}");
        }
    }

    #[tokio::test]
    async fn 租约只能被抢到一次() {
        for (name, s) in backends().await {
            s.create_global(&g("t2"), &[]).await.unwrap();
            let a = s.lock_one_due("worker-a", 60).await.unwrap();
            assert!(a.is_some(), "{name}: 第一个实例应该抢到");
            // 同一个事务不能被第二个实例同时抢到，否则会重复推进
            let b = s.lock_one_due("worker-b", 60).await.unwrap();
            assert!(b.is_none(), "{name}: 租约期内不能被别人抢走");
        }
    }

    #[tokio::test]
    async fn 终态不再被调度() {
        for (name, s) in backends().await {
            s.create_global(&g("t3"), &[]).await.unwrap();
            s.set_global_status("t3", GlobalStatus::Succeed, "").await.unwrap();
            assert!(s.lock_one_due("w", 60).await.unwrap().is_none(), "{name}");
            let got = s.get_global("t3").await.unwrap().unwrap();
            assert_eq!(got.status, GlobalStatus::Succeed, "{name}");
            assert!(got.finish_time.is_some(), "{name}: 终态要落 finish_time");
        }
    }

    #[tokio::test]
    async fn 分支状态可更新() {
        for (name, s) in backends().await {
            let b = BranchRow {
                gid: "t4".into(),
                branch_id: "01".into(),
                op: BranchOp::Action,
                url: "http://x/a".into(),
                payload: "{}".into(),
                status: BranchStatus::Prepared,
            };
            s.create_global(&g("t4"), std::slice::from_ref(&b)).await.unwrap();
            s.set_branch_status("t4", "01", BranchOp::Action, BranchStatus::Succeed)
                .await
                .unwrap();
            let got = s.list_branches("t4").await.unwrap();
            assert_eq!(got.len(), 1, "{name}");
            assert_eq!(got[0].status, BranchStatus::Succeed, "{name}");
        }
    }

    #[tokio::test]
    async fn 回滚原因和回查地址能存取() {
        // 这两列是后加的，跨库的字符串/空值处理最容易在这儿出问题
        for (name, s) in backends().await {
            let mut row = g("t5");
            row.query_prepared = "http://busi/query".into();
            s.create_global(&row, &[]).await.unwrap();
            s.set_global_status("t5", GlobalStatus::Aborting, "分支 02 返回 FAILURE")
                .await
                .unwrap();
            let got = s.get_global("t5").await.unwrap().unwrap();
            assert_eq!(got.query_prepared, "http://busi/query", "{name}");
            assert_eq!(got.rollback_reason, "分支 02 返回 FAILURE", "{name}");
            assert!(got.finish_time.is_none(), "{name}: 非终态不该有 finish_time");

            // 空 reason 不能把已有的原因冲掉
            s.set_global_status("t5", GlobalStatus::Failed, "").await.unwrap();
            let got = s.get_global("t5").await.unwrap().unwrap();
            assert_eq!(got.rollback_reason, "分支 02 返回 FAILURE", "{name}: 空原因不能覆盖");
        }
    }

    #[tokio::test]
    async fn msg的prepared会被捞tcc的不会() {
        for (name, s) in backends().await {
            let mut m = g("m1");
            m.trans_type = TransType::Msg;
            m.status = GlobalStatus::Prepared;
            s.create_global(&m, &[]).await.unwrap();
            let mut t = g("c1");
            t.trans_type = TransType::Tcc;
            t.status = GlobalStatus::Prepared;
            s.create_global(&t, &[]).await.unwrap();

            let got = s.lock_one_due("w", 60).await.unwrap();
            assert_eq!(got.map(|x| x.gid), Some("m1".to_string()), "{name}: 只该捞到 msg");
            // 再捞一次应该没有了（msg 被租约占住，tcc 不该被碰）
            assert!(s.lock_one_due("w2", 60).await.unwrap().is_none(), "{name}");
        }
    }

    #[tokio::test]
    async fn 分支登记是幂等的() {
        for (name, s) in backends().await {
            let mut t = g("c2");
            t.trans_type = TransType::Tcc;
            s.create_global(&t, &[]).await.unwrap();
            let ops = [
                (BranchOp::Confirm, "http://x/c".to_string()),
                (BranchOp::Cancel, "http://x/n".to_string()),
            ];
            s.register_branch("c2", "01", &ops).await.unwrap();
            s.register_branch("c2", "01", &ops).await.unwrap(); // 客户端重试
            assert_eq!(s.list_branches("c2").await.unwrap().len(), 2, "{name}: 不该重复插入");
        }
    }
}
