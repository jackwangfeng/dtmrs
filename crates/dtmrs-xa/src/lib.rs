//! XA 两阶段提交的**业务方（RM）**助手。
//!
//! XA 跟 SAGA/TCC 的根本区别：不靠补偿，靠数据库原生的两阶段提交。
//! 分支的业务 SQL 跑完后 `PREPARE TRANSACTION`，改动**已持久化但还不可见**，
//! 等 TC 统一决定 `COMMIT PREPARED` 还是 `ROLLBACK PREPARED`。
//!
//! 好处是强一致（没有中间态可见）；代价是**全程持锁**。
//!
//! # ⚠ XA 的真正危险：悬挂的 prepared 事务
//!
//! 已 prepare 但没被解决的事务会**永久持有锁**。在 Postgres 里还会阻塞 VACUUM，
//! 进而带来事务 ID 回卷的风险 —— 整个库可能被迫停写。
//!
//! 这比 SAGA「补偿没跑成」严重得多：SAGA 顶多数据不一致，XA 能把库搞停。
//! 所以：
//!
//! - 上线前必须监控 [`list_prepared`]，有长期不消失的就报警
//! - `COMMIT PREPARED` / `ROLLBACK PREPARED` 必须最终送达（TC 会无限重试）
//! - 拿不准就别用 XA，用 SAGA
//!
//! # 只支持 Postgres
//!
//! Postgres 用 `PREPARE TRANSACTION` / `COMMIT PREPARED` / `ROLLBACK PREPARED`。
//! MySQL 是另一套语法（`XA START/END/PREPARE/COMMIT`），**没实现也没测**——
//! 手上没 MySQL，不写没验过的代码。
//!
//! SQLite **根本没有**两阶段提交，用不了 XA。
//!
//! ## Postgres 默认是关着的
//!
//! `max_prepared_transactions` 默认 **0**，即 2PC 禁用。必须先改配置重启：
//!
//! ```text
//! max_prepared_transactions = 32     # postgresql.conf
//! ```
//!
//! [`ensure_enabled`] 可以在启动时检查这一项，别等第一笔事务才发现。

use sqlx::pool::PoolConnection;
use sqlx::{Any, AnyConnection, AnyPool, Row};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("数据库两阶段提交没开：max_prepared_transactions = 0。改 postgresql.conf 后重启")]
    TwoPhaseDisabled,
    #[error("xid 不合法: {0}")]
    BadXid(String),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// 解决一个 prepared 事务的结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolved {
    /// 这次调用真的提交/回滚了它
    Done,
    /// 它已经不在 `pg_prepared_xacts` 里了 —— 之前已经解决过
    ///
    /// **必须当成功**：TC 会重试，第二次调用一定撞上这个。
    AlreadyResolved,
}

/// 由 (gid, branch_id) 生成 xid。
///
/// Postgres 的事务标识符上限 200 字节，且我们只允许字母数字和 `_-`：
/// xid 会被直接拼进 SQL（`PREPARE TRANSACTION '...'` 不接受参数绑定），
/// 所以**必须**在这里把注入的可能性掐死。
pub fn xid(gid: &str, branch_id: &str) -> String {
    let clean = |s: &str| -> String {
        s.chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect()
    };
    let s = format!("dtmrs_{}_{}", clean(gid), clean(branch_id));
    s.chars().take(190).collect()
}

fn check_xid(x: &str) -> Result<()> {
    if x.is_empty() || x.len() > 200 {
        return Err(Error::BadXid(format!("长度 {}", x.len())));
    }
    if !x.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        // 这里是最后一道防线：xid 要拼进 SQL 字面量
        return Err(Error::BadXid(x.to_string()));
    }
    Ok(())
}

/// 启动时自检：数据库开了两阶段提交没有。
///
/// 别等第一笔事务失败才发现 —— 那时候可能已经有别的分支 prepare 成功了。
pub async fn ensure_enabled(pool: &AnyPool) -> Result<i64> {
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

/// 一个 XA 分支：`BEGIN` → 业务 SQL → `PREPARE TRANSACTION`。
///
/// ```ignore
/// let mut br = XaBranch::begin(&pool, gid, "01").await?;
/// sqlx::query("UPDATE acct SET bal = bal - $1 WHERE id = $2")
///     .bind(100i64).bind(1i32)
///     .execute(br.conn()).await?;
/// let xid = br.prepare().await?;      // 改动已持久化，但还不可见
/// // 然后把 (xid, commit/rollback 回调地址) 登记给 TC
/// ```
pub struct XaBranch {
    conn: Option<PoolConnection<Any>>,
    xid: String,
}

impl XaBranch {
    pub async fn begin(pool: &AnyPool, gid: &str, branch_id: &str) -> Result<Self> {
        let x = xid(gid, branch_id);
        check_xid(&x)?;
        let mut conn = pool.acquire().await?;
        // 手写 BEGIN 而不是用 sqlx 的 Transaction：因为收尾要用
        // PREPARE TRANSACTION 顶替 COMMIT，sqlx 的事务守卫会强行发 COMMIT/ROLLBACK
        sqlx::query("BEGIN").execute(&mut *conn).await?;
        Ok(Self { conn: Some(conn), xid: x })
    }

    pub fn xid(&self) -> &str {
        &self.xid
    }

    /// 拿连接跑业务 SQL。**必须用这个连接** —— 换连接就不在同一个事务里了。
    pub fn conn(&mut self) -> &mut AnyConnection {
        self.conn.as_mut().expect("XaBranch 已收尾").as_mut()
    }

    /// 一阶段完成：`PREPARE TRANSACTION`。返回 xid。
    ///
    /// 成功之后改动已经**持久化**（数据库崩了也不丢），但对其它会话不可见，
    /// 而且**锁一直持着**，直到 commit 或 rollback。
    pub async fn prepare(mut self) -> Result<String> {
        let mut conn = self.conn.take().expect("XaBranch 已收尾");
        let sql = format!("PREPARE TRANSACTION '{}'", self.xid);
        sqlx::query(&sql).execute(&mut *conn).await?;
        Ok(self.xid.clone())
    }

    /// 一阶段就决定不干了：普通 `ROLLBACK`，不留 prepared 事务。
    pub async fn discard(mut self) -> Result<()> {
        let mut conn = self.conn.take().expect("XaBranch 已收尾");
        sqlx::query("ROLLBACK").execute(&mut *conn).await?;
        Ok(())
    }
}

impl Drop for XaBranch {
    fn drop(&mut self) {
        // 既没 prepare 也没 discard 就被丢了（比如中间 `?` 提前返回）。
        // 这条连接还开着一个事务，**直接还回池里会污染下一个使用者**。
        // Drop 里不能 await，所以起个任务把 ROLLBACK 补上。
        if let Some(mut conn) = self.conn.take() {
            let xid = std::mem::take(&mut self.xid);
            tokio::spawn(async move {
                let r = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                match r {
                    Ok(_) => eprintln!(
                        "[dtmrs-xa] {xid} 的 XaBranch 没收尾就被丢弃，已自动 ROLLBACK"
                    ),
                    Err(e) => eprintln!(
                        "[dtmrs-xa] {xid} 的 XaBranch 没收尾就被丢弃，且 ROLLBACK 失败: {e}"
                    ),
                }
            });
        }
    }
}

/// 二阶段：提交。
///
/// **幂等**：如果这个 xid 已经不在 `pg_prepared_xacts` 里，返回
/// [`Resolved::AlreadyResolved`] 而不是报错 —— TC 会重试，第二次一定撞上。
///
/// 这个"不存在就算成功"之所以安全，靠的是状态机的保证：
/// `xa_advance` 在 Submitted 阶段永远不会转向回滚，所以"不存在"只可能是
/// 之前已经 commit 过，不可能是被 rollback 了。
pub async fn commit_prepared(pool: &AnyPool, xid: &str) -> Result<Resolved> {
    resolve(pool, xid, "COMMIT PREPARED").await
}

/// 二阶段：回滚。同样幂等。
pub async fn rollback_prepared(pool: &AnyPool, xid: &str) -> Result<Resolved> {
    resolve(pool, xid, "ROLLBACK PREPARED").await
}

async fn resolve(pool: &AnyPool, xid: &str, verb: &str) -> Result<Resolved> {
    check_xid(xid)?;
    let sql = format!("{verb} '{xid}'");
    match sqlx::query(&sql).execute(pool).await {
        Ok(_) => Ok(Resolved::Done),
        Err(e) => {
            // 42704 = undefined_object，也就是"没有这个 prepared 事务"。
            // 用 SQLSTATE 而不是匹配错误文本 —— 文本会随版本和语言变。
            let already = e
                .as_database_error()
                .and_then(|d| d.code())
                .map(|c| c == "42704")
                .unwrap_or(false);
            if already {
                Ok(Resolved::AlreadyResolved)
            } else {
                Err(Error::Db(e))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedXact {
    pub xid: String,
    /// 已经 prepare 了多久（秒）。**这是运维要盯的指标** ——
    /// 长期不降的说明有分支的二阶段没送达，锁在一直持着
    pub age_secs: i64,
}

/// 列出所有悬挂的 prepared 事务。**上生产必须监控这个。**
pub async fn list_prepared(pool: &AnyPool) -> Result<Vec<PreparedXact>> {
    let rows = sqlx::query(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xid里的危险字符会被清掉() {
        // xid 要拼进 SQL 字面量，注入必须在这里掐死
        let x = xid("order';DROP TABLE t;--", "01");
        assert!(!x.contains('\''), "不能留单引号: {x}");
        assert!(!x.contains(' '), "不能留空格: {x}");
        assert!(check_xid(&x).is_ok());
        // 正常情况保持可读
        assert_eq!(xid("order-1001", "01"), "dtmrs_order-1001_01");
    }

    #[test]
    fn 超长gid会被截断到限长内() {
        let x = xid(&"a".repeat(500), "01");
        assert!(x.len() <= 190, "实际 {}", x.len());
        assert!(check_xid(&x).is_ok());
    }

    #[test]
    fn 非法xid被拒() {
        assert!(check_xid("").is_err());
        assert!(check_xid("has space").is_err());
        assert!(check_xid("has'quote").is_err());
        assert!(check_xid(&"a".repeat(300)).is_err());
        assert!(check_xid("ok_xid-01").is_ok());
    }
}
