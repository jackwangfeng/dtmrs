//! XA 两阶段提交的**业务方（RM）**助手。支持 Postgres 和 MySQL。
//!
//! XA 跟 SAGA/TCC 的根本区别：不靠补偿，靠数据库原生的两阶段提交。
//! 分支的业务 SQL 跑完后 prepare，改动**已持久化但还不可见**，
//! 等 TC 统一决定提交还是回滚。
//!
//! 好处是强一致（没有中间态可见）；代价是**全程持锁**。
//!
//! # 两种数据库的语法完全不同（都实测过）
//!
//! | | Postgres 16 | MySQL 8.0 |
//! |---|---|---|
//! | 开始 | `BEGIN` | `XA START 'xid'` |
//! | 一阶段 | `PREPARE TRANSACTION 'xid'` | `XA END 'xid'` + `XA PREPARE 'xid'` |
//! | 提交 | `COMMIT PREPARED 'xid'` | `XA COMMIT 'xid'` |
//! | 回滚 | `ROLLBACK PREPARED 'xid'` | `XA ROLLBACK 'xid'` |
//! | 列出悬挂的 | `pg_prepared_xacts` | `XA RECOVER` |
//! | 重复解决的错误码 | `42704` | `XAE04` |
//! | 默认是否开启 | ❌ `max_prepared_transactions=0` | ✅ 开着 |
//! | 能看到 prepare 时长 | ✅ | ❌ `XA RECOVER` 不给时间 |
//! | xid 长度上限 | 200 字节 | gtrid **64 字节** |
//!
//! SQLite **根本没有**两阶段提交，用不了 XA（[`Flavor::from_url`] 会拒绝）。
//!
//! # 为什么这里用 `raw_sql` 而不是 `query`
//!
//! MySQL 的 XA 语句**不能走预处理协议**：
//! `1295 This command is not supported in the prepared statement protocol yet`。
//! `sqlx::query()` 默认走预处理，所以两阶段相关的语句一律用 `sqlx::raw_sql`
//! （文本协议）。这也是为什么 xid 只能拼进 SQL 而不能绑定参数 ——
//! 注入防护全靠 [`xid_for`] 里的字符白名单。
//!
//! # ⚠ XA 的真正危险：悬挂的 prepared 事务
//!
//! 已 prepare 但没被解决的事务会**永久持有锁**。Postgres 里还会阻塞 VACUUM，
//! 带来事务 ID 回卷风险；MySQL 里会一直占着行锁。
//!
//! 这比 SAGA「补偿没跑成」严重得多：SAGA 顶多数据不一致，XA 能把库搞停。
//!
//! - 上线前必须监控 [`Xa::list_prepared`]，有长期不消失的就报警
//! - 二阶段必须最终送达（TC 会无限重试）
//! - 拿不准就别用 XA，用 SAGA

use dtmrs_core::Backend;
use sqlx::pool::PoolConnection;
use sqlx::{Any, AnyConnection, AnyPool, Connection, Row};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Postgres 的两阶段提交没开：max_prepared_transactions = 0。改 postgresql.conf 后重启")]
    TwoPhaseDisabled,
    #[error("{0} 不支持两阶段提交，用不了 XA")]
    Unsupported(Backend),
    #[error("xid 不合法: {0}")]
    BadXid(String),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// 支持 XA 的数据库
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    Postgres,
    MySql,
}

impl Flavor {
    /// SQLite 没有两阶段提交，会返回 [`Error::Unsupported`]
    pub fn from_url(url: &str) -> Result<Self> {
        match Backend::from_url(url) {
            Backend::Postgres => Ok(Self::Postgres),
            Backend::MySql => Ok(Self::MySql),
            be => Err(Error::Unsupported(be)),
        }
    }

    /// xid 的长度上限。MySQL 的 gtrid 只有 64 字节，比 Postgres 严得多。
    fn xid_limit(&self) -> usize {
        match self {
            Self::Postgres => 190,
            Self::MySql => 64,
        }
    }

    /// "这个 xid 已经不在了"对应的 SQLSTATE。
    ///
    /// 用错误码而不是匹配错误文本 —— 文本会随版本和语言变。
    fn already_resolved_code(&self) -> &'static str {
        match self {
            // undefined_object
            Self::Postgres => "42704",
            // XAER_NOTA: Unknown XID
            Self::MySql => "XAE04",
        }
    }
}

/// 解决一个 prepared 事务的结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolved {
    /// 这次调用真的提交/回滚了它
    Done,
    /// 它已经不在悬挂列表里了 —— 之前已经解决过
    ///
    /// **必须当成功**：TC 会重试，第二次调用一定撞上这个。
    AlreadyResolved,
}

/// FNV-1a：xid 超长被截断时拼一个短摘要，避免不同 gid 撞成同一个 xid
fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// 由 (gid, branch_id) 生成 xid。
///
/// xid 会被直接拼进 SQL（`PREPARE TRANSACTION '...'` / `XA START '...'`
/// 都不接受参数绑定），所以**必须**在这里把注入的可能性掐死：
/// 只留字母数字和 `_-`。
///
/// 超过长度上限时截断并拼 16 位十六进制摘要 —— 直接截断会让不同的长 gid
/// 撞成同一个 xid，那是灾难性的（两个不相关的事务互相提交对方）。
pub fn xid_for(flavor: Flavor, gid: &str, branch_id: &str) -> String {
    let clean = |s: &str| -> String {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    };
    let full = format!("dtmrs_{}_{}", clean(gid), clean(branch_id));
    let limit = flavor.xid_limit();
    if full.len() <= limit {
        return full;
    }
    let digest = format!("{:016x}", fnv1a(&full));
    let keep = limit - digest.len() - 1;
    format!("{}_{}", &full[..keep], digest)
}

fn check_xid(flavor: Flavor, x: &str) -> Result<()> {
    if x.is_empty() || x.len() > flavor.xid_limit() {
        return Err(Error::BadXid(format!("长度 {}", x.len())));
    }
    if !x
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        // 最后一道防线：xid 要拼进 SQL 字面量
        return Err(Error::BadXid(x.to_string()));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PreparedXact {
    pub xid: String,
    /// 已经 prepare 了多久（秒）。**这是运维要盯的指标**：长期不降说明
    /// 有分支的二阶段没送达，锁在一直持着。
    ///
    /// ⚠ **MySQL 恒为 0** —— `XA RECOVER` 不提供 prepare 时间。
    pub age_secs: i64,
}

/// XA 操作入口。按数据库类型创建。
#[derive(Debug, Clone, Copy)]
pub struct Xa {
    flavor: Flavor,
}

impl Xa {
    pub fn new(flavor: Flavor) -> Self {
        Self { flavor }
    }

    pub fn from_url(url: &str) -> Result<Self> {
        Ok(Self::new(Flavor::from_url(url)?))
    }

    pub fn flavor(&self) -> Flavor {
        self.flavor
    }

    pub fn xid(&self, gid: &str, branch_id: &str) -> String {
        xid_for(self.flavor, gid, branch_id)
    }

    /// 启动时自检：数据库支持并开启了两阶段提交没有。
    ///
    /// 别等第一笔事务失败才发现 —— 那时候可能已经有别的分支 prepare 成功了。
    ///
    /// - Postgres：查 `max_prepared_transactions`，0 就报 [`Error::TwoPhaseDisabled`]
    /// - MySQL：XA 默认可用，只校验版本（5.7.7 之前 prepared 的 XA 事务
    ///   在重启后会丢，等于没有持久性，这里直接拒绝）
    pub async fn ensure_enabled(&self, pool: &AnyPool) -> Result<i64> {
        match self.flavor {
            Flavor::Postgres => {
                let row = sqlx::query("SHOW max_prepared_transactions")
                    .fetch_one(pool)
                    .await?;
                let v: String = row.try_get(0).unwrap_or_default();
                let n: i64 = v.trim().parse().unwrap_or(0);
                if n <= 0 {
                    return Err(Error::TwoPhaseDisabled);
                }
                Ok(n)
            }
            Flavor::MySql => {
                let row = sqlx::query("SELECT VERSION() AS v").fetch_one(pool).await?;
                let v: String = row.try_get("v").unwrap_or_default();
                if !mysql_xa_durable(&v) {
                    return Err(Error::TwoPhaseDisabled);
                }
                Ok(1)
            }
        }
    }

    /// 一个 XA 分支的开始
    pub async fn begin(&self, pool: &AnyPool, gid: &str, branch_id: &str) -> Result<XaBranch> {
        let x = self.xid(gid, branch_id);
        check_xid(self.flavor, &x)?;
        let mut conn = pool.acquire().await?;
        // 手写而不是用 sqlx 的 Transaction：收尾要用 prepare 顶替 COMMIT，
        // sqlx 的事务守卫会强行发 COMMIT/ROLLBACK
        let sql = match self.flavor {
            Flavor::Postgres => "BEGIN".to_string(),
            Flavor::MySql => format!("XA START '{x}'"),
        };
        // XA 语句必须走文本协议，见文件头说明
        sqlx::raw_sql(&sql).execute(&mut *conn).await?;
        Ok(XaBranch {
            conn: Some(conn),
            xid: x,
            flavor: self.flavor,
        })
    }

    /// 二阶段：提交。**幂等** —— 已解决过会返回 [`Resolved::AlreadyResolved`]。
    ///
    /// 这个"找不到就算成功"之所以安全，靠的是状态机的保证：
    /// `xa_advance` 在 Submitted 阶段永远不转向回滚，所以"找不到"只可能是
    /// 之前已经提交过，不可能是被回滚了。
    pub async fn commit_prepared(&self, pool: &AnyPool, xid: &str) -> Result<Resolved> {
        let sql = match self.flavor {
            Flavor::Postgres => format!("COMMIT PREPARED '{xid}'"),
            Flavor::MySql => format!("XA COMMIT '{xid}'"),
        };
        self.resolve(pool, xid, &sql).await
    }

    /// 二阶段：回滚。同样幂等。
    pub async fn rollback_prepared(&self, pool: &AnyPool, xid: &str) -> Result<Resolved> {
        let sql = match self.flavor {
            Flavor::Postgres => format!("ROLLBACK PREPARED '{xid}'"),
            Flavor::MySql => format!("XA ROLLBACK '{xid}'"),
        };
        self.resolve(pool, xid, &sql).await
    }

    async fn resolve(&self, pool: &AnyPool, xid: &str, sql: &str) -> Result<Resolved> {
        check_xid(self.flavor, xid)?;
        match sqlx::raw_sql(sql).execute(pool).await {
            Ok(_) => Ok(Resolved::Done),
            Err(e) => {
                let already = e
                    .as_database_error()
                    .and_then(|d| d.code())
                    .map(|c| c == self.flavor.already_resolved_code())
                    .unwrap_or(false);
                if already {
                    Ok(Resolved::AlreadyResolved)
                } else {
                    Err(Error::Db(e))
                }
            }
        }
    }

    /// 列出所有悬挂的 prepared 事务。**上生产必须监控这个。**
    pub async fn list_prepared(&self, pool: &AnyPool) -> Result<Vec<PreparedXact>> {
        match self.flavor {
            Flavor::Postgres => {
                let rows = sqlx::raw_sql(
                    "SELECT gid, CAST(EXTRACT(EPOCH FROM (now() - prepared)) AS BIGINT) AS age
                     FROM pg_prepared_xacts ORDER BY prepared",
                )
                .fetch_all(pool)
                .await?;
                Ok(rows
                    .into_iter()
                    .map(|r| PreparedXact {
                        xid: r.get("gid"),
                        age_secs: r.try_get("age").unwrap_or(0),
                    })
                    .collect())
            }
            Flavor::MySql => {
                // XA RECOVER 的列是 formatID / gtrid_length / bqual_length / data。
                // 只有 data 是我们要的 xid；**没有 prepare 时间**，所以 age 只能给 0。
                let rows = sqlx::raw_sql("XA RECOVER").fetch_all(pool).await?;
                Ok(rows
                    .into_iter()
                    .filter_map(|r| {
                        r.try_get::<String, _>("data")
                            .ok()
                            .or_else(|| r.try_get::<String, _>(3).ok())
                    })
                    .map(|xid| PreparedXact { xid, age_secs: 0 })
                    .collect())
            }
        }
    }

    /// 建业务侧不需要的表？不需要 —— XA 不用额外的表。这里只是提醒：
    /// XA 模式**不需要**子事务屏障表，因为两阶段提交本身就保证了原子性。
    pub const fn needs_barrier_table() -> bool {
        false
    }
}

/// MySQL 5.7.7 之前，prepared 的 XA 事务在服务重启后会丢 —— 等于没有持久性，
/// XA 的意义就没了。所以低于这个版本直接拒绝。
fn mysql_xa_durable(version: &str) -> bool {
    let nums: Vec<u32> = version
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .take(3)
        .map(|s| s.parse().unwrap_or(0))
        .collect();
    match nums.as_slice() {
        [maj, ..] if *maj >= 8 => true,
        [5, 7, patch] => *patch >= 7,
        [5, 7] => false,
        _ => false,
    }
}

/// 一个 XA 分支：开始 → 业务 SQL → 一阶段 prepare。
///
/// ```ignore
/// let xa = Xa::from_url(&db_url)?;
/// let mut br = xa.begin(&pool, gid, "01").await?;
/// sqlx::query("UPDATE acct SET bal = bal - ? WHERE id = ?")   // 占位符按你的库来
///     .bind(100i64).bind(1i32)
///     .execute(br.conn()).await?;
/// let xid = br.prepare().await?;      // 改动已持久化，但还不可见
/// // 然后把 (xid, commit/rollback 回调地址) 登记给 TC
/// ```
pub struct XaBranch {
    conn: Option<PoolConnection<Any>>,
    xid: String,
    flavor: Flavor,
}

impl XaBranch {
    pub fn xid(&self) -> &str {
        &self.xid
    }

    /// 拿连接跑业务 SQL。**必须用这个连接** —— 换连接就不在同一个事务里了。
    pub fn conn(&mut self) -> &mut AnyConnection {
        self.conn.as_mut().expect("XaBranch 已收尾").as_mut()
    }

    /// 一阶段完成。返回 xid。
    ///
    /// 成功之后改动已经**持久化**（数据库崩了也不丢），但对其它会话不可见，
    /// 而且**锁一直持着**，直到二阶段。
    pub async fn prepare(mut self) -> Result<String> {
        let mut conn = self.conn.take().expect("XaBranch 已收尾");
        let x = self.xid.clone();
        match self.flavor {
            Flavor::Postgres => {
                sqlx::raw_sql(&format!("PREPARE TRANSACTION '{x}'"))
                    .execute(&mut *conn)
                    .await?;
            }
            Flavor::MySql => {
                // MySQL 必须先 END 再 PREPARE，少一步会报 XAER_RMFAIL
                sqlx::raw_sql(&format!("XA END '{x}'"))
                    .execute(&mut *conn)
                    .await?;
                sqlx::raw_sql(&format!("XA PREPARE '{x}'"))
                    .execute(&mut *conn)
                    .await?;
            }
        }
        Ok(x)
    }

    /// 一阶段就决定不干了：直接回滚，不留 prepared 事务。
    pub async fn discard(mut self) -> Result<()> {
        let mut conn = self.conn.take().expect("XaBranch 已收尾");
        let x = self.xid.clone();
        match self.flavor {
            Flavor::Postgres => {
                sqlx::raw_sql("ROLLBACK").execute(&mut *conn).await?;
            }
            Flavor::MySql => {
                sqlx::raw_sql(&format!("XA END '{x}'"))
                    .execute(&mut *conn)
                    .await?;
                sqlx::raw_sql(&format!("XA ROLLBACK '{x}'"))
                    .execute(&mut *conn)
                    .await?;
            }
        }
        Ok(())
    }
}

impl Drop for XaBranch {
    fn drop(&mut self) {
        // 既没 prepare 也没 discard 就被丢了（比如中间 `?` 提前返回）。
        // 这条连接还开着一个事务，**直接还回池里会污染下一个使用者** ——
        // 下一个人会在别人的事务里跑 SQL。
        //
        // 处理办法是把连接从池里**摘出来关掉**，而不是补发 ROLLBACK：
        //
        // 1. 连接一断，数据库自己会回滚这个未提交的事务（Postgres 和 MySQL
        //    都是这个行为），效果一样且更可靠 —— 不依赖我们还能不能发出 SQL
        // 2. `sqlx::Any` 的 `Executor` 在 `tokio::spawn` 里过不了 HRTB
        //    （`implementation of Executor is not general enough`），
        //    发 SQL 这条路在 Any 上走不通
        //
        // 代价是丢一条连接，池子会补建。错误路径上这个代价可以接受。
        if let Some(conn) = self.conn.take() {
            let xid = std::mem::take(&mut self.xid);
            tokio::spawn(async move {
                let owned = conn.detach();
                match owned.close().await {
                    Ok(()) => eprintln!(
                        "[dtmrs-xa] {xid} 的 XaBranch 没收尾就被丢弃，已断开连接（事务由数据库回滚）"
                    ),
                    Err(e) => eprintln!("[dtmrs-xa] {xid} 断开连接时出错: {e}"),
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite用不了xa() {
        assert!(matches!(
            Flavor::from_url("sqlite::memory:"),
            Err(Error::Unsupported(Backend::Sqlite))
        ));
        assert_eq!(
            Flavor::from_url("postgres://u@h/d").unwrap(),
            Flavor::Postgres
        );
        assert_eq!(Flavor::from_url("mysql://u@h/d").unwrap(), Flavor::MySql);
    }

    #[test]
    fn xid里的危险字符会被清掉() {
        // xid 要拼进 SQL 字面量，注入必须在这里掐死
        for f in [Flavor::Postgres, Flavor::MySql] {
            let x = xid_for(f, "order';DROP TABLE t;--", "01");
            assert!(!x.contains('\''), "不能留单引号: {x}");
            assert!(!x.contains(' '), "不能留空格: {x}");
            assert!(check_xid(f, &x).is_ok());
        }
        assert_eq!(
            xid_for(Flavor::Postgres, "order-1001", "01"),
            "dtmrs_order-1001_01"
        );
    }

    #[test]
    fn mysql的xid上限更严且截断不撞车() {
        // MySQL 的 gtrid 只有 64 字节
        let a = xid_for(Flavor::MySql, &"a".repeat(200), "01");
        let b = xid_for(Flavor::MySql, &format!("{}b", "a".repeat(199)), "01");
        assert!(a.len() <= 64, "实际 {}", a.len());
        assert!(b.len() <= 64);
        // 只截断的话这两个会完全一样 —— 那会让两个不相关的事务互相提交对方
        assert_ne!(a, b, "截断后必须靠摘要区分开");
        assert!(check_xid(Flavor::MySql, &a).is_ok());

        // Postgres 上限宽一些
        let p = xid_for(Flavor::Postgres, &"a".repeat(500), "01");
        assert!(p.len() <= 190);
        assert!(check_xid(Flavor::Postgres, &p).is_ok());
        // 同一个 gid 在两种库上会得到不同长度的 xid，这没问题 ——
        // 一个事务的分支只会落在一种库上
        assert_ne!(a.len(), p.len());
    }

    #[test]
    fn 非法xid被拒() {
        let f = Flavor::Postgres;
        assert!(check_xid(f, "").is_err());
        assert!(check_xid(f, "has space").is_err());
        assert!(check_xid(f, "has'quote").is_err());
        assert!(check_xid(f, &"a".repeat(300)).is_err());
        assert!(check_xid(f, "ok_xid-01").is_ok());
        // 65 字符在 MySQL 上超限，在 Postgres 上没事
        let x = "a".repeat(65);
        assert!(check_xid(Flavor::MySql, &x).is_err());
        assert!(check_xid(Flavor::Postgres, &x).is_ok());
    }

    #[test]
    fn mysql版本太低要拒掉() {
        // 5.7.7 之前 prepared 的 XA 事务重启就丢，等于没有持久性
        assert!(!mysql_xa_durable("5.6.51"));
        assert!(!mysql_xa_durable("5.7.6"));
        assert!(mysql_xa_durable("5.7.7"));
        assert!(mysql_xa_durable("5.7.44-log"));
        assert!(mysql_xa_durable("8.0.44"));
        assert!(mysql_xa_durable("8.4.0"));
    }

    #[test]
    fn 错误码按方言区分() {
        assert_eq!(Flavor::Postgres.already_resolved_code(), "42704");
        assert_eq!(Flavor::MySql.already_resolved_code(), "XAE04");
    }
}
