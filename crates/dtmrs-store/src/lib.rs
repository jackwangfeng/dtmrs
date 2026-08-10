//! 存储层。TC 本身无状态，所有状态都在这里 —— 所以 TC 可以多实例、可以随时重启。
//!
//! 时间统一用 **unix 秒（i64）** 存，不用数据库的 datetime 类型：
//! 跨 sqlite/postgres 的时间类型映射是个反复踩坑的地方，整数没有这个问题。

use dtmrs_core::{BranchOp, BranchStatus, GlobalStatus, TransType};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

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
    pool: SqlitePool,
}

impl Store {
    /// `url` 形如 `sqlite:dtmrs.db` 或 `sqlite::memory:`
    pub async fn open(url: &str) -> Result<Self> {
        let opts: SqliteConnectOptions = url.parse::<SqliteConnectOptions>()?.create_if_missing(true);
        // 内存库必须限制成单连接，否则每条连接看到的是不同的库
        let max = if url.contains(":memory:") { 1 } else { 8 };
        let pool = SqlitePoolOptions::new().max_connections(max).connect_with(opts).await?;
        let s = Self { pool };
        s.migrate().await?;
        Ok(s)
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS trans_global (
              gid                TEXT PRIMARY KEY,
              trans_type         TEXT NOT NULL,
              status             TEXT NOT NULL,
              payload            TEXT NOT NULL DEFAULT '',
              next_cron_time     INTEGER NOT NULL DEFAULT 0,
              next_cron_interval INTEGER NOT NULL DEFAULT 0,
              owner              TEXT NOT NULL DEFAULT '',
              rollback_reason    TEXT NOT NULL DEFAULT '',
              create_time        INTEGER NOT NULL,
              update_time        INTEGER NOT NULL,
              finish_time        INTEGER
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
              gid         TEXT NOT NULL,
              branch_id   TEXT NOT NULL,
              op          TEXT NOT NULL,
              url         TEXT NOT NULL,
              payload     TEXT NOT NULL DEFAULT '',
              status      TEXT NOT NULL,
              create_time INTEGER NOT NULL,
              update_time INTEGER NOT NULL,
              finish_time INTEGER,
              PRIMARY KEY (gid, branch_id, op)
            )"#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// 建全局事务 + 所有分支，一个事务里做完。
    ///
    /// 返回 `false` 表示 gid 已存在 —— 这是**幂等提交**，不是错误：
    /// 客户端重试提交时必须拿到"已受理"而不是报错。
    pub async fn create_global(
        &self,
        g: &GlobalRow,
        branches: &[BranchRow],
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let t = now();
        let n = sqlx::query(
            "INSERT OR IGNORE INTO trans_global
             (gid,trans_type,status,payload,next_cron_time,next_cron_interval,
              owner,rollback_reason,create_time,update_time)
             VALUES (?,?,?,?,?,?,'','',?,?)",
        )
        .bind(&g.gid)
        .bind(g.trans_type.to_string())
        .bind(g.status.as_str())
        .bind(&g.payload)
        .bind(g.next_cron_time)
        .bind(g.next_cron_interval)
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
                "INSERT OR IGNORE INTO trans_branch_op
                 (gid,branch_id,op,url,payload,status,create_time,update_time)
                 VALUES (?,?,?,?,?,?,?,?)",
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
        let row = sqlx::query(
            "SELECT gid,trans_type,status,payload,next_cron_time,next_cron_interval,
                    owner,rollback_reason,create_time,finish_time
             FROM trans_global WHERE gid=?",
        )
        .bind(gid)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(global_from_row))
    }

    pub async fn list_branches(&self, gid: &str) -> Result<Vec<BranchRow>> {
        let rows = sqlx::query(
            "SELECT gid,branch_id,op,url,payload,status FROM trans_branch_op
             WHERE gid=? ORDER BY branch_id, op",
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
        sqlx::query(
            "UPDATE trans_global SET status=?, update_time=?, finish_time=?,
             rollback_reason=CASE WHEN ?<>'' THEN ? ELSE rollback_reason END
             WHERE gid=?",
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
            "UPDATE trans_branch_op SET status=?, update_time=?,
             finish_time=CASE WHEN ?<>'prepared' THEN ? ELSE finish_time END
             WHERE gid=? AND branch_id=? AND op=?",
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
            "SELECT gid FROM trans_global
             WHERE status IN ('submitted','aborting') AND next_cron_time <= ?
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
            "UPDATE trans_global SET owner=?, next_cron_time=?, update_time=?
             WHERE gid=? AND next_cron_time <= ?",
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
        let row = sqlx::query(
            "SELECT gid,trans_type,status,payload,next_cron_time,next_cron_interval,
                    owner,rollback_reason,create_time,finish_time
             FROM trans_global WHERE gid=?",
        )
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
            "UPDATE trans_global SET next_cron_interval=?, next_cron_time=?, update_time=?
             WHERE gid=?",
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
        sqlx::query("UPDATE trans_global SET next_cron_time=?, next_cron_interval=0 WHERE gid=?")
            .bind(now())
            .bind(gid)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_recent(&self, limit: i64) -> Result<Vec<GlobalRow>> {
        let rows = sqlx::query(
            "SELECT gid,trans_type,status,payload,next_cron_time,next_cron_interval,
                    owner,rollback_reason,create_time,finish_time
             FROM trans_global ORDER BY create_time DESC LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(global_from_row).collect())
    }
}

fn global_from_row(r: sqlx::sqlite::SqliteRow) -> GlobalRow {
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
        create_time: r.get("create_time"),
        finish_time: r.get("finish_time"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn mem() -> Store {
        Store::open("sqlite::memory:").await.unwrap()
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
            create_time: 0,
            finish_time: None,
        }
    }

    #[tokio::test]
    async fn 重复提交同一个gid是幂等的() {
        let s = mem().await;
        assert!(s.create_global(&g("t1"), &[]).await.unwrap());
        // 第二次返回 false 而不是报错 —— 客户端重试不该失败
        assert!(!s.create_global(&g("t1"), &[]).await.unwrap());
        assert_eq!(s.list_recent(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn 租约只能被抢到一次() {
        let s = mem().await;
        s.create_global(&g("t2"), &[]).await.unwrap();
        let a = s.lock_one_due("worker-a", 60).await.unwrap();
        assert!(a.is_some(), "第一个实例应该抢到");
        // 同一个事务不能被第二个实例同时抢到，否则会重复推进
        let b = s.lock_one_due("worker-b", 60).await.unwrap();
        assert!(b.is_none(), "租约期内不能被别人抢走");
    }

    #[tokio::test]
    async fn 终态不再被调度() {
        let s = mem().await;
        s.create_global(&g("t3"), &[]).await.unwrap();
        s.set_global_status("t3", GlobalStatus::Succeed, "").await.unwrap();
        assert!(s.lock_one_due("w", 60).await.unwrap().is_none());
        let got = s.get_global("t3").await.unwrap().unwrap();
        assert_eq!(got.status, GlobalStatus::Succeed);
        assert!(got.finish_time.is_some(), "终态要落 finish_time");
    }

    #[tokio::test]
    async fn 分支状态可更新() {
        let s = mem().await;
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
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].status, BranchStatus::Succeed);
    }
}
