//! 子事务屏障 —— 一张表 + `INSERT OR IGNORE`，同时解决三个经典难题：
//!
//! | 难题 | 场景 |
//! |---|---|
//! | **幂等** | TC 重试导致同一分支被调两次 |
//! | **空回滚** | action 还没跑（丢包）就来了 compensate，补偿必须空转 |
//! | **悬挂** | compensate 先到、action 后到，晚到的 action 必须被丢弃 |
//!
//! 算法逐条对照 DTM 的 `BranchBarrier.Call`（`client/dtmcli/barrier.go`）。
//!
//! # 跟 Go 版的一处刻意不同
//!
//! Go 版把业务逻辑当闭包传进来（`Call(tx, busiCall)`）。Rust 里让闭包借用事务
//! 并返回 Future 会撞上一堆 HRTB 生命周期问题，可读性还差。所以这里**只做判定**，
//! 业务 SQL 由调用方在同一个事务里自己执行：
//!
//! ```ignore
//! let mut tx = pool.begin().await?;
//! if barrier.decide(&mut tx).await? == Decision::Execute {
//!     // 业务 SQL —— 必须在这个 tx 里，跟屏障记录同生共死
//!     sqlx::query("UPDATE account SET balance = balance - ?").bind(amt)
//!         .execute(&mut *tx).await?;
//! }
//! tx.commit().await?;   // 原子性的来源
//! ```
//!
//! **前提：barrier 表必须和业务表在同一个数据库实例**，才能共用一个本地事务。
//! 这不是实现限制，是这个方案成立的根本条件。

use dtmrs_core::BranchOp;
use sqlx::{Any, AnyPool, Transaction};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("事务信息不完整: {0}")]
    InvalidTransInfo(String),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// 屏障给出的判定
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// 该干活。调用方在同一个事务里执行业务 SQL
    Execute,
    /// **空回滚**：正向分支从没执行过，补偿直接空转
    NullCompensation,
    /// **重复请求或悬挂**：这个 (gid, branch, op, barrier_id) 已经处理过
    Duplicated,
}

pub struct BranchBarrier {
    pub trans_type: String,
    pub gid: String,
    pub branch_id: String,
    pub op: BranchOp,
    /// 同一分支内多次调用要用不同 barrier_id，否则第二次会被当成重复
    counter: u32,
    table: String,
}

impl BranchBarrier {
    pub fn new(trans_type: &str, gid: &str, branch_id: &str, op: &str) -> Result<Self> {
        if trans_type.is_empty() || gid.is_empty() || branch_id.is_empty() || op.is_empty() {
            return Err(Error::InvalidTransInfo(format!(
                "trans_type={trans_type} gid={gid} branch_id={branch_id} op={op}"
            )));
        }
        let op = BranchOp::parse(op)
            .ok_or_else(|| Error::InvalidTransInfo(format!("未知 op: {op}")))?;
        Ok(Self {
            trans_type: trans_type.into(),
            gid: gid.into(),
            branch_id: branch_id.into(),
            op,
            counter: 0,
            table: "barrier".into(),
        })
    }

    pub fn with_table(mut self, t: &str) -> Self {
        self.table = t.into();
        self
    }

    fn next_barrier_id(&mut self) -> String {
        self.counter += 1;
        format!("{:02}", self.counter)
    }

    /// 建屏障表。sqlite 和 postgres 通用。
    ///
    /// 跟 DTM 的表比少了自增 `id` 列 —— 那是唯一的方言不可移植点
    /// （`AUTOINCREMENT` vs `BIGSERIAL`），而算法只依赖那个唯一约束，用不到 id。
    /// 干掉它换来一套 DDL 跑两种库。
    pub async fn migrate(pool: &AnyPool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS barrier (
              trans_type  TEXT   NOT NULL DEFAULT '',
              gid         TEXT   NOT NULL DEFAULT '',
              branch_id   TEXT   NOT NULL DEFAULT '',
              op          TEXT   NOT NULL DEFAULT '',
              barrier_id  TEXT   NOT NULL DEFAULT '',
              reason      TEXT   NOT NULL DEFAULT '',
              create_time BIGINT NOT NULL,
              PRIMARY KEY (gid, branch_id, op, barrier_id)
            )"#,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    /// 判定这次调用该不该执行业务逻辑。
    ///
    /// 必须传入**业务自己的**数据库事务 —— 屏障记录和业务 SQL 要一起提交。
    pub async fn decide(&mut self, tx: &mut Transaction<'_, Any>) -> Result<Decision> {
        let bid = self.next_barrier_id();
        let op = self.op;

        // 第一步：如果我是补偿类操作，先"假装自己是正向分支"插一行。
        // 插进去了说明正向分支从来没来过 → 空回滚。
        let origin_affected = match op.origin_op() {
            Some(origin) => self.insert(tx, origin.as_str(), &bid).await?,
            None => 0,
        };

        // 第二步：以自己的身份插一行。插不进去说明这次调用已经被处理过。
        let current_affected = self.insert(tx, op.as_str(), &bid).await?;

        if op.is_compensating() && origin_affected > 0 {
            // 正向分支的位置被我抢到了 ⇒ 它从没执行过 ⇒ 不需要补偿
            return Ok(Decision::NullCompensation);
        }
        if current_affected == 0 {
            // 重复请求；或者是悬挂 —— 补偿先到，把我这行位置占了
            return Ok(Decision::Duplicated);
        }
        Ok(Decision::Execute)
    }

    async fn insert(
        &self,
        tx: &mut Transaction<'_, Any>,
        op: &str,
        bid: &str,
    ) -> Result<u64> {
        // 只用 $N 占位符：postgres 不认 `?`（实测语法错误）。
        // ON CONFLICT DO NOTHING 两种库都支持，且冲突时 rows_affected 都是 0 ——
        // 整个屏障算法就靠这个返回值判空回滚和重复请求。
        let sql = format!(
            "INSERT INTO {} (trans_type,gid,branch_id,op,barrier_id,reason,create_time)
             VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING",
            self.table
        );
        let n = sqlx::query(&sql)
            .bind(&self.trans_type)
            .bind(&self.gid)
            .bind(&self.branch_id)
            .bind(op)
            .bind(bid)
            .bind(self.op.as_str()) // reason = 是哪个分支插的这行
            .bind(now())
            .execute(&mut **tx)
            .await?
            .rows_affected();
        Ok(n)
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::any::AnyPoolOptions;

    static RESET: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

    /// 屏障也要在两种后端上验 —— 业务库是 pg 的话，屏障表就在 pg 里。
    /// 配了 DTMRS_TEST_PG 就多跑一遍真 postgres。
    async fn pools() -> Vec<(&'static str, AnyPool)> {
        sqlx::any::install_default_drivers();
        let mem = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        BranchBarrier::migrate(&mem).await.unwrap();
        let mut v = vec![("sqlite", mem)];
        if let Ok(url) = std::env::var("DTMRS_TEST_PG") {
            let p = AnyPoolOptions::new().max_connections(2).connect(&url).await.unwrap();
            // 建表 + 清表只做一次：每个测试各自 DROP+CREATE 会撞上
            // Postgres 的 DDL 并发竞态。各测试的 gid 本来就不重叠。
            RESET
                .get_or_init(|| async {
                    BranchBarrier::migrate(&p).await.expect("建表");
                    sqlx::query("DELETE FROM barrier").execute(&p).await.expect("清表");
                })
                .await;
            v.push(("postgres", p));
        }
        v
    }

    /// 走一次完整判定并提交，返回结论
    async fn once(p: &AnyPool, gid: &str, branch: &str, op: &str) -> Decision {
        let mut bb = BranchBarrier::new("saga", gid, branch, op).unwrap();
        let mut tx = p.begin().await.unwrap();
        let d = bb.decide(&mut tx).await.unwrap();
        tx.commit().await.unwrap();
        d
    }

    #[tokio::test]
    async fn 幂等_同一个action调两次只执行一次() {
        for (be, p) in pools().await {
            let _ = be;
        
            assert_eq!(once(&p, "g1", "01", "action").await, Decision::Execute, "{be}");
            // TC 重试 —— 必须被挡住
            assert_eq!(once(&p, "g1", "01", "action").await, Decision::Duplicated, "{be}");
        }
    }

    #[tokio::test]
    async fn 空回滚_action没跑过时补偿要空转() {
        for (be, p) in pools().await {
            let _ = be;
        
            // action 从没来过，直接来 compensate
            assert_eq!(
                once(&p, "g2", "01", "compensate").await,
                Decision::NullCompensation, "{be}"
            );
        }
    }

    #[tokio::test]
    async fn 正常补偿_action跑过之后补偿要执行() {
        for (be, p) in pools().await {
            let _ = be;
        
            assert_eq!(once(&p, "g3", "01", "action").await, Decision::Execute, "{be}");
            assert_eq!(once(&p, "g3", "01", "compensate").await, Decision::Execute, "{be}");
        }
    }

    #[tokio::test]
    async fn 悬挂_补偿先到则晚到的action必须被丢弃() {
        for (be, p) in pools().await {
            let _ = be;
        
            // 补偿先到（网络乱序），空回滚
            assert_eq!(
                once(&p, "g4", "01", "compensate").await,
                Decision::NullCompensation, "{be}"
            );
            // 晚到的 action 如果执行了，钱就扣出去再也回不来了 —— 必须丢弃
            assert_eq!(once(&p, "g4", "01", "action").await, Decision::Duplicated, "{be}");
        }
    }

    #[tokio::test]
    async fn 补偿也要幂等() {
        for (be, p) in pools().await {
            let _ = be;
        
            once(&p, "g5", "01", "action").await;
            assert_eq!(once(&p, "g5", "01", "compensate").await, Decision::Execute, "{be}");
            assert_eq!(
                once(&p, "g5", "01", "compensate").await,
                Decision::Duplicated
            );
        }
    }

    #[tokio::test]
    async fn 不同分支互不干扰() {
        for (be, p) in pools().await {
            let _ = be;
        
            assert_eq!(once(&p, "g6", "01", "action").await, Decision::Execute, "{be}");
            assert_eq!(once(&p, "g6", "02", "action").await, Decision::Execute, "{be}");
            // 补 02 不影响 01
            assert_eq!(once(&p, "g6", "02", "compensate").await, Decision::Execute, "{be}");
            assert_eq!(once(&p, "g6", "01", "compensate").await, Decision::Execute, "{be}");
        }
    }

    #[tokio::test]
    async fn tcc的cancel对应try() {
        for (be, p) in pools().await {
            let _ = be;
        
            // try 没跑过 → cancel 空回滚
            assert_eq!(once(&p, "g7", "01", "cancel").await, Decision::NullCompensation);
            let p2 = pools().await.into_iter().next().unwrap().1;
            assert_eq!(once(&p2, "g8", "01", "try").await, Decision::Execute, "{be}");
            assert_eq!(once(&p2, "g8", "01", "cancel").await, Decision::Execute, "{be}");
        }
    }

    #[tokio::test]
    async fn 同一分支多次调用用不同barrier_id() {
        for (be, p) in pools().await {
            let _ = be;
        
            let mut bb = BranchBarrier::new("saga", "g9", "01", "action").unwrap();
            let mut tx = p.begin().await.unwrap();
            // 一个分支内连续两次判定，第二次不该被当成重复
            assert_eq!(bb.decide(&mut tx).await.unwrap(), Decision::Execute, "{be}");
            assert_eq!(bb.decide(&mut tx).await.unwrap(), Decision::Execute, "{be}");
            tx.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn 回滚的事务不留屏障记录() {
        for (be, p) in pools().await {
            let _ = be;
        
            let mut bb = BranchBarrier::new("saga", "g10", "01", "action").unwrap();
            let mut tx = p.begin().await.unwrap();
            assert_eq!(bb.decide(&mut tx).await.unwrap(), Decision::Execute, "{be}");
            tx.rollback().await.unwrap(); // 业务 SQL 失败，整个事务回滚
            // 屏障记录也跟着没了，所以重试还能再执行 —— 这才是正确的
            assert_eq!(once(&p, "g10", "01", "action").await, Decision::Execute, "{be}");
        }
    }

    #[tokio::test]
    async fn 信息不全要报错() {
        for (be, p) in pools().await {
            let _ = be;
            assert!(BranchBarrier::new("saga", "", "01", "action").is_err());
            assert!(BranchBarrier::new("saga", "g", "01", "bogus").is_err());
        }
    }
}
