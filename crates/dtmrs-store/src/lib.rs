//! 存储层。TC 本身无状态，所有状态都在这里 —— 所以 TC 可以多实例、可以随时重启。
//!
//! # 一套 SQL 同时跑 sqlite / postgres / mysql
//!
//! 用 `sqlx::Any` + [`dtmrs_core::dialect`] 的模板渲染，而不是抽 `Store` trait
//! 写三份实现。方言差异（占位符、冲突忽略、列类型、索引写法）全在 dialect 那层，
//! 各家实测出来的坑也记在那个文件头，这里只遵守它的两条写法约定：
//!
//! 1. **模板里统一写 `?`**，由 [`Backend::q`] 渲染成各后端能吃的语句
//!    （非 MySQL 转成 `$1..$n`，MySQL 原样保留）
//! 2. **模板的字符串字面量里不能出现 `?`** —— 会被当成占位符
//!
//! 顺带一条只有 sqlite 有的老坑：它把 `$4` 当命名参数，所以同一个 `$N` 不能
//! 复用。`q()` 逐个 `?` 顺序编号，天然不会复用。
//!
//! 时间统一用 **unix 秒（i64）** 存，不用数据库的 datetime 类型 ——
//! 跨库的时间类型映射是反复踩坑的地方，整数没有这个问题。
//! 列类型用 `BIGINT`：postgres 的 `INTEGER` 只有 4 字节，装不下时间戳。

pub use dtmrs_core::Backend;

use dtmrs_core::dialect::check_len;
use dtmrs_core::{BranchOp, BranchStatus, GlobalStatus, TransType};
use sqlx::any::{AnyPoolOptions, AnyRow};
use sqlx::{AnyPool, Row};
use std::sync::Once;

pub type Result<T> = std::result::Result<T, sqlx::Error>;

/// payload 列的字符上限（`trans_global.payload`）
pub const BIG: usize = 8192;
/// url / reason 一类中等长度列的字符上限
pub const MID: usize = 1024;

/// 把超长字段变成错误。
///
/// **不能省**：MySQL 的 `INSERT IGNORE` 遇到超长值会静默截断而不是报错，
/// 详见 [`dtmrs_core::dialect::check_len`]。宁可提交时报错，也不能让一笔
/// 内容被悄悄改过的事务落库。
fn len_ok(col: &'static str, val: &str, max: usize) -> Result<()> {
    check_len(col, val, max).map_err(|e| sqlx::Error::Encode(Box::new(e)))
}

pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// [`Store::submit_prepared`] 的结果。
///
/// 之所以要区分三种情况：`submit` 既要处理「tcc/msg/xa 把 prepared 推成
/// submitted」，也要处理「saga 第一次提交，事务还不存在」，还要保证
/// **重复提交返回成功而不是报错**（见 `api::submit` 的注释）。
#[derive(Debug, Clone)]
pub enum SubmitOutcome {
    /// gid 不存在 —— 调用方该按新事务建
    Missing,
    /// 本来停在 prepared，已经推成 submitted 并排进调度队列。
    ///
    /// **带上事务体**：调用方要是顺便占了租约（见 `submit_prepared` 的
    /// `owner` 参数），可以拿它直接开推，不用再读一次。为这个多带的返回值，
    /// 两种后端都没有多付往返 —— Redis 是脚本尾巴上加一个 `HGETALL`，
    /// SQL 是把本来就要发的那条 SELECT 从「只取 status」改成取全行
    Advanced(Box<GlobalRow>),
    /// 已经提交过了。**必须当成功返回**，否则客户端会以为没受理
    Already,
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
pub struct SqlStore {
    pool: AnyPool,
    be: Backend,
}

static DRIVERS: Once = Once::new();

impl SqlStore {
    /// `url` 可以是：
    /// - `sqlite:dtmrs.db` / `sqlite::memory:`
    /// - `postgres://user:pass@host:5432/db`
    pub async fn open(url: &str) -> Result<Self> {
        DRIVERS.call_once(sqlx::any::install_default_drivers);

        // sqlite 默认只读打开，不会建文件。AnyConnectOptions 没法像
        // SqliteConnectOptions 那样设 create_if_missing，只能走 URL 参数。
        let mut url = url.to_string();
        if url.starts_with("sqlite") && !url.contains("mode=") && !url.contains(":memory:") {
            url.push_str(if url.contains('?') {
                "&mode=rwc"
            } else {
                "?mode=rwc"
            });
        }
        // 内存库必须单连接，否则每条连接看到的是各自独立的库
        //
        // 非内存库默认 32：这个池子是**推进器和 HTTP/gRPC 接口共用**的，
        // 而推进一笔事务要好几次往返。原来写死 8，比推进 worker 数还少，
        // 于是提交请求和推进器互相抢连接。
        //
        // 32 是「够用就行」：默认 16 个 worker + HTTP/gRPC 接口，实测再往上
        // 加池子已经不涨了（Postgres 64 worker 配 32 的池子 3184 笔/秒，
        // 池子开到 64 也是 3227）。
        //
        // 后端连接数吃紧（比如和业务共用一个 Postgres，默认才 100 条）
        // 就用 `DTMRS_DB_POOL` 调小
        let max = if url.contains(":memory:") {
            1
        } else {
            std::env::var("DTMRS_DB_POOL")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(32)
        };
        let be = Backend::from_url(&url);
        let is_file_sqlite = be == Backend::Sqlite && !url.contains(":memory:");
        let pool = AnyPoolOptions::new()
            .max_connections(max)
            .after_connect(move |conn, _| {
                Box::pin(async move {
                    if is_file_sqlite {
                        // ⚠ **别去掉这两条。**
                        //
                        // sqlite 默认是 rollback journal + synchronous=FULL，
                        // 每笔事务一次 fsync，而且写事务会锁住整个库 ——
                        // 实测提交吞吐只有约 13 笔/秒，并发 20 就大量
                        // `database is locked` 并把请求拖到超时。
                        //
                        // WAL 让读写不互斥、synchronous=NORMAL 把每事务 fsync
                        // 降成 checkpoint 时才 fsync。代价是**断电可能丢最后
                        // 几笔已提交事务**（进程崩溃不丢，WAL 还在）——
                        // sqlite 后端本来就只建议单机/开发用，这个取舍是划算的。
                        // 要严格持久性就用 Postgres。
                        for pragma in [
                            "PRAGMA journal_mode=WAL",
                            "PRAGMA synchronous=NORMAL",
                            // 拿不到锁时先自旋 5 秒再报错，别让偶发争用直接失败
                            "PRAGMA busy_timeout=5000",
                        ] {
                            sqlx::query(pragma).execute(&mut *conn).await?;
                        }
                    }
                    Ok(())
                })
            })
            .connect(&url)
            .await?;
        let s = Self { pool, be };
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
                    tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt + 1))).await;
                }
            }
        }
        Err(last.expect("循环至少失败一次"))
    }

    pub async fn migrate(&self) -> Result<()> {
        let idt = self.be.id_text();
        let ids = self.be.id_short();
        // payload 要装下所有步骤的 URL；MySQL 上是 VARCHAR，有长度上限。
        // 上限同时是写库前的校验依据（BIG/MID），改这里就得改那里 —— 所以是常量
        let big = self.be.text(BIG);
        let mid = self.be.text(MID);
        // 索引二选一：MySQL 只能建表时内联，其它后端用独立的
        // CREATE INDEX IF NOT EXISTS（MySQL 那个语法直接 1064）
        let inline = self
            .be
            .inline_index("idx_status_cron", "status, next_cron_time");

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS trans_global (
              gid                {idt} NOT NULL,
              trans_type         {ids} NOT NULL,
              status             {ids} NOT NULL,
              payload            {big} NOT NULL,
              next_cron_time     BIGINT NOT NULL DEFAULT 0,
              next_cron_interval BIGINT NOT NULL DEFAULT 0,
              owner              {idt} NOT NULL,
              rollback_reason    {mid} NOT NULL,
              query_prepared     {mid} NOT NULL,
              create_time        BIGINT NOT NULL,
              update_time        BIGINT NOT NULL,
              finish_time        BIGINT,
              PRIMARY KEY (gid){inline}
            )"
        ))
        .execute(&self.pool)
        .await?;
        // cron 靠这个索引扫待办，没它到量之后会全表扫
        if let Some(sql) =
            self.be
                .create_index("idx_status_cron", "trans_global", "status, next_cron_time")
        {
            sqlx::query(&sql).execute(&self.pool).await?;
        }
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS trans_branch_op (
              gid         {idt} NOT NULL,
              branch_id   {idt} NOT NULL,
              op          {ids} NOT NULL,
              url         {mid} NOT NULL,
              payload     {mid} NOT NULL,
              status      {ids} NOT NULL,
              create_time BIGINT NOT NULL,
              update_time BIGINT NOT NULL,
              finish_time BIGINT,
              PRIMARY KEY (gid, branch_id, op)
            )"
        ))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub fn backend(&self) -> Backend {
        self.be
    }

    pub fn pool(&self) -> &AnyPool {
        &self.pool
    }

    /// 建全局事务 + 所有分支，一个事务里做完。
    ///
    /// 返回 `false` 表示 gid 已存在 —— 这是**幂等提交**，不是错误：
    /// 客户端重试提交时必须拿到"已受理"而不是报错。
    pub async fn create_global(&self, g: &GlobalRow, branches: &[BranchRow]) -> Result<bool> {
        // 先校验再落库：超长的值在 MySQL 上会被 INSERT IGNORE 静默截断
        len_ok("gid", &g.gid, Backend::ID_MAX)?;
        len_ok("payload", &g.payload, BIG)?;
        len_ok("query_prepared", &g.query_prepared, MID)?;
        for b in branches {
            len_ok("branch_id", &b.branch_id, Backend::ID_MAX)?;
            len_ok("url", &b.url, MID)?;
            len_ok("payload", &b.payload, MID)?;
        }
        let mut tx = self.pool.begin().await?;
        let t = now();
        let n = sqlx::query(&self.be.q("{INS} trans_global
             (gid,trans_type,status,payload,next_cron_time,next_cron_interval,
              owner,rollback_reason,query_prepared,create_time,update_time)
             VALUES (?,?,?,?,?,?,?,'',?,?,?)
             {NOCONFLICT}"))
        .bind(&g.gid)
        .bind(g.trans_type.to_string())
        .bind(g.status.as_str())
        .bind(&g.payload)
        .bind(g.next_cron_time)
        .bind(g.next_cron_interval)
        // ⚠ owner 要真的写进去，不能像原来那样写死空串。
        // 提交方可以在建事务时就把租约占在自己手上（owner=自己、
        // next_cron_time=现在+租约），这样它能直接开推，
        // **省掉一次抢占往返** —— 见 `Api::submit`
        .bind(&g.owner)
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
            sqlx::query(&self.be.q("{INS} trans_branch_op
                 (gid,branch_id,op,url,payload,status,create_time,update_time)
                 VALUES (?,?,?,?,?,?,?,?)
                 {NOCONFLICT}"))
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
        let row = sqlx::query(&self.be.q(&format!("{SELECT_GLOBAL} WHERE gid=?")))
            .bind(gid)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(global_from_row))
    }

    pub async fn list_branches(&self, gid: &str) -> Result<Vec<BranchRow>> {
        let rows = sqlx::query(&self.be.q(
            "SELECT gid,branch_id,op,url,payload,status FROM trans_branch_op
             WHERE gid=? ORDER BY branch_id, op",
        ))
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

    /// 落全局状态。
    ///
    /// `trans_type` 这一层用不上（UPDATE 不需要它），但 Redis 后端靠它把
    /// 「落终态」这条热路径从 Lua 脚本降级成一次 MULTI —— 两边签名要一致
    pub async fn set_global_status(
        &self,
        gid: &str,
        status: GlobalStatus,
        _trans_type: TransType,
        reason: &str,
    ) -> Result<()> {
        let t = now();
        let fin = if status.is_final() { Some(t) } else { None };
        // reason 是诊断信息，**截断而不是报错**：这条 UPDATE 是状态机的收尾，
        // 让它因为一句话太长而失败，事务就永远推不到终态了（MySQL strict mode
        // 下超长 UPDATE 直接报 1406，不像 INSERT IGNORE 那样只是截断）。
        let reason: String = reason.chars().take(MID).collect();
        let reason = reason.as_str();
        // 注意 $4/$5 都绑 reason —— 不能复用同一个 $N，见文件头注释
        sqlx::query(&self.be.q(
            "UPDATE trans_global SET status=?, update_time=?, finish_time=?,
             rollback_reason = CASE WHEN ? <> '' THEN ? ELSE rollback_reason END
             WHERE gid=?",
        ))
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

    /// 落一个分支的状态**和结果数据**。
    ///
    /// workflow 模式的重放靠这个：函数崩溃后会从头再跑一遍，已完成的分支
    /// 不重新执行，而是把上次存的 `payload` 原样还给它。所以这个值必须跟
    /// 「分支已成功」在**同一条 UPDATE 里**落盘 —— 分两步写的话，中间崩了
    /// 就会出现「标了成功但结果丢了」，重放时拿不到返回值。
    pub async fn set_branch_result(
        &self,
        gid: &str,
        branch_id: &str,
        op: BranchOp,
        status: BranchStatus,
        payload: &str,
    ) -> Result<()> {
        len_ok("payload", payload, MID)?;
        let t = now();
        sqlx::query(&self.be.q(
            "UPDATE trans_branch_op SET status=?, payload=?, update_time=?,
             finish_time = CASE WHEN ? <> 'prepared' THEN ? ELSE finish_time END
             WHERE gid=? AND branch_id=? AND op=?",
        ))
        .bind(status.as_str())
        .bind(payload)
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

    pub async fn set_branch_status(
        &self,
        gid: &str,
        branch_id: &str,
        op: BranchOp,
        status: BranchStatus,
    ) -> Result<()> {
        let t = now();
        sqlx::query(
            &self
                .be
                .q("UPDATE trans_branch_op SET status=?, update_time=?,
             finish_time = CASE WHEN ? <> 'prepared' THEN ? ELSE finish_time END
             WHERE gid=? AND branch_id=? AND op=?"),
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
        // ⚠ 两个细节都不能改，改了并行推进就退化成串行：
        //
        // 1. 结尾的 `FOR UPDATE SKIP LOCKED`（sqlite 上是空串）。没有它的话
        //    每个 worker 都选中同一行，然后挤在下面那条 UPDATE 上排队，
        //    只有一个能成。见 `Backend::skip_locked`
        //
        // 2. **不能加 `ORDER BY next_cron_time`。** 索引是
        //    (status, next_cron_time)，而 WHERE 里 status 是个 IN 范围，
        //    所以按 next_cron_time 排序用不上索引 —— MySQL 的执行计划里会
        //    出现 `Using filesort`，意味着它要**把所有命中的行都读出来并加锁**
        //    才能排序，然后才 LIMIT 1。于是第一个 worker 锁光全部待办，
        //    其余 worker 全部 SKIP 掉、一笔都抢不到（实测 6 并发只成 1 笔）。
        //
        //    去掉 ORDER BY 后走索引范围扫描，天然就是按 (status, next_cron_time)
        //    顺序取第一条：同一状态内仍然是**最早到期的先跑**，只是不再跨状态
        //    全局排序。不会饿死 —— 抢到的行会把 next_cron_time 推到租约之后，
        //    自动排到队尾。
        //    （Redis 那边是 ZRANGEBYSCORE，严格按到期时间。可调度的**集合**
        //    两边完全一致，只是取用顺序不同，这个差异是可以接受的。）
        let gid: Option<String> = sqlx::query_scalar(&self.be.q(&format!(
            "SELECT gid FROM trans_global
             WHERE (status IN ('submitted','aborting')
                    OR (status = 'prepared' AND trans_type = 'msg'))
               AND next_cron_time <= ?
             LIMIT 1{}",
            self.be.skip_locked()
        )))
        .bind(t)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(gid) = gid else {
            tx.rollback().await?;
            return Ok(None);
        };
        // 立刻把 next_cron_time 推到租约之后，等于占坑
        let n = sqlx::query(&self.be.q(
            "UPDATE trans_global SET owner=?, next_cron_time=?, update_time=?
             WHERE gid=? AND next_cron_time <= ?",
        ))
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
        let row = sqlx::query(&self.be.q(&format!("{SELECT_GLOBAL} WHERE gid=?")))
            .bind(&gid)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(Some(global_from_row(row)))
    }

    /// 推进失败后设置下次重试时间（指数退避）
    pub async fn schedule_retry(&self, gid: &str, interval: i64) -> Result<()> {
        let t = now();
        sqlx::query(&self.be.q(
            "UPDATE trans_global SET next_cron_interval=?, next_cron_time=?, update_time=?
             WHERE gid=?",
        ))
        .bind(interval)
        .bind(t + interval)
        .bind(t)
        .bind(gid)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 把停在 prepared 的事务推成 submitted，并立刻排进调度队列。
    ///
    /// **一次调用做完原来三次的活**（`get_global` + `set_global_status` +
    /// `schedule_now`）。Redis 后端上这是一个 Lua 脚本，11 条命令降到 3 条 ——
    /// 那边是单线程 CPU 瓶颈，命令数直接决定吞吐。
    ///
    /// `owner` / `next_cron_time` 让提交方**顺便把租约占下来**：传自己的
    /// owner 和「现在 + 租约」，这一条 UPDATE 之后事务就归调用方推了，
    /// 不用再走一次抢占。不想占就传空 owner 和 `now()`。
    pub async fn submit_prepared(
        &self,
        gid: &str,
        owner: &str,
        next_cron_time: i64,
    ) -> Result<SubmitOutcome> {
        // 先查一次。saga 第一次提交时事务还不存在，这是最常见的路径，
        // 一次 SELECT 就该返回，不值得为它先空跑一条 UPDATE。
        // 取全行而不只是 status —— 反正这条 SELECT 免不了，顺手把事务体带回去，
        // 调用方就能直接开推（见 `SubmitOutcome::Advanced`）
        let row = sqlx::query(&self.be.q(&format!("{SELECT_GLOBAL} WHERE gid=?")))
            .bind(gid)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(SubmitOutcome::Missing);
        };
        let mut g = global_from_row(row);
        if g.status != GlobalStatus::Prepared {
            return Ok(SubmitOutcome::Already);
        }
        let t = now();
        // 状态、退避、排队一条 UPDATE 落完。
        // ⚠ `AND status=?`（prepared）不能省：并发重复提交时，
        // 别把已经在推进的事务硬拽回队首
        sqlx::query(&self.be.q("UPDATE trans_global SET status=?, update_time=?,
             next_cron_time=?, next_cron_interval=0, owner=?
             WHERE gid=? AND status=?"))
        .bind(GlobalStatus::Submitted.as_str())
        .bind(t)
        .bind(next_cron_time)
        .bind(owner)
        .bind(gid)
        .bind(GlobalStatus::Prepared.as_str())
        .execute(&self.pool)
        .await?;
        // 把刚写下去的三个字段补到返回的事务体上，省掉一次回读
        g.status = GlobalStatus::Submitted;
        g.next_cron_time = next_cron_time;
        g.owner = owner.to_string();
        Ok(SubmitOutcome::Advanced(Box::new(g)))
    }

    /// 让某个事务立刻可被调度（提交/中止之后叫一下，不用等 cron 周期）
    pub async fn schedule_now(&self, gid: &str) -> Result<()> {
        sqlx::query(
            &self
                .be
                .q("UPDATE trans_global SET next_cron_time=?, next_cron_interval=0 WHERE gid=?"),
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
        len_ok("gid", gid, Backend::ID_MAX)?;
        len_ok("branch_id", branch_id, Backend::ID_MAX)?;
        for (_, url) in ops {
            len_ok("url", url, MID)?;
        }
        let mut tx = self.pool.begin().await?;
        let t = now();
        for (op, url) in ops {
            sqlx::query(&self.be.q("{INS} trans_branch_op
                 (gid,branch_id,op,url,payload,status,create_time,update_time)
                 VALUES (?,?,?,?,'',?,?,?)
                 {NOCONFLICT}"))
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
        let rows = sqlx::query(&self.be.q(&format!(
            "{SELECT_GLOBAL} ORDER BY create_time DESC LIMIT ?"
        )))
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

    /// 每个测试都在**所有可用后端**上跑一遍：sqlite / postgres / mysql / redis。
    ///
    /// 真库靠环境变量开启（`DTMRS_TEST_PG` / `DTMRS_TEST_MYSQL` / `DTMRS_TEST_REDIS`）——
    /// 没配就只跑 sqlite，这样没数据库的机器也能 `cargo test`。
    /// 但**别把这当成"它们也过了"** —— 没配就是没测。
    ///
    /// Redis 跟另外三个不是同一类东西（不是 SQL），能共用这一套断言恰恰是
    /// 我们要的证据：两种后端的**行为**必须一致，哪怕实现天差地别。
    /// Postgres 测试必须串行 —— `lock_one_due` 和 `list_recent` 是**全局查询**，
    /// 并行跑会互相看见对方的事务，断言就没意义了。
    /// （光给各测试不同的 gid 不够：捞待办是不按 gid 过滤的。）
    static PG_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// 返回 (串行锁, 各后端)。锁要持到测试结束，所以由调用方接着。
    ///
    /// 每次进来把 Postgres 的表清空（只 DELETE 不 DDL —— 并发 DDL 会撞上
    /// Postgres 的 `pg_type` 竞态，见 `migrate_racy`）。
    /// 「跳过 ≠ 通过」的闸门。
    ///
    /// 这些测试没配环境变量时直接返回，**仍然显示为 passed**。所以只要 CI 里
    /// 某个数据库容器没起来、或者环境变量名打错一个字母，那个 job 会
    /// **安安静静地全绿** —— 而真库那部分其实一行没跑。
    ///
    /// CI 的真库 job 里设 `DTMRS_TEST_REQUIRE_REAL_DB=1`，把「悄悄没测」
    /// 变成「响亮地失败」。本地开发不设这个变量，跳过行为不变。
    fn require_real_db(缺的变量: &str) {
        if std::env::var("DTMRS_TEST_REQUIRE_REAL_DB").is_ok() {
            panic!(
                "设了 DTMRS_TEST_REQUIRE_REAL_DB，却没有 {缺的变量} —— \
                 这是 CI 配置坏了（容器没起来？变量名打错？），不是可以跳过的情况"
            );
        }
    }

    async fn backends() -> (
        tokio::sync::MutexGuard<'static, ()>,
        Vec<(&'static str, Store)>,
    ) {
        let guard = PG_LOCK.lock().await;
        let mut v = vec![("sqlite", Store::open("sqlite::memory:").await.unwrap())];
        // 每种真数据库都配一个环境变量。**没配就是没测**，不是"通过"。
        for (name, env) in [("postgres", "DTMRS_TEST_PG"), ("mysql", "DTMRS_TEST_MYSQL")] {
            if std::env::var(env).is_err() {
                require_real_db(env);
                continue;
            }
            if let Ok(url) = std::env::var(env) {
                let s = Store::open(&url)
                    .await
                    .unwrap_or_else(|e| panic!("连不上 {env}: {e}"));
                for t in ["trans_branch_op", "trans_global"] {
                    sqlx::query(&format!("DELETE FROM {t}"))
                        .execute(s.pool().expect("SQL 后端才有连接池"))
                        .await
                        .expect("清表");
                }
                v.push((name, s));
            }
        }
        #[cfg(feature = "redis")]
        if std::env::var("DTMRS_TEST_REDIS").is_err() {
            require_real_db("DTMRS_TEST_REDIS");
        }
        #[cfg(feature = "redis")]
        if let Ok(url) = std::env::var("DTMRS_TEST_REDIS") {
            let s = Store::open(&url)
                .await
                .unwrap_or_else(|e| panic!("连不上 DTMRS_TEST_REDIS: {e}"));
            // 每次进来清干净。Redis 没有"表"，按前缀删
            s.as_redis()
                .unwrap()
                .flush_prefix()
                .await
                .expect("清 redis");
            v.push(("redis", s));
        }
        (guard, v)
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
        let (_g, bes) = backends().await;
        for (name, s) in bes {
            assert!(s.create_global(&g("t1"), &[]).await.unwrap(), "{name}");
            // 第二次返回 false 而不是报错 —— 客户端重试不该失败
            assert!(!s.create_global(&g("t1"), &[]).await.unwrap(), "{name}");
            assert_eq!(s.list_recent(10).await.unwrap().len(), 1, "{name}");
        }
    }

    #[tokio::test]
    async fn 租约只能被抢到一次() {
        let (_g, bes) = backends().await;
        for (name, s) in bes {
            s.create_global(&g("t2"), &[]).await.unwrap();
            let a = s.lock_one_due("worker-a", 60).await.unwrap();
            assert!(a.is_some(), "{name}: 第一个实例应该抢到");
            // 同一个事务不能被第二个实例同时抢到，否则会重复推进
            let b = s.lock_one_due("worker-b", 60).await.unwrap();
            assert!(b.is_none(), "{name}: 租约期内不能被别人抢走");
        }
    }

    /// 并发抢占要抢到**不同的**事务，而不是全挤在同一笔上。
    ///
    /// 这条钉的是 `FOR UPDATE SKIP LOCKED`（见 `Backend::skip_locked`）。
    /// 少了它，N 个 worker 的 SELECT 会同时选中队首那一行，然后在 UPDATE
    /// 上排队，最后只有一个成功 —— 不会算错，但并行推进等于白做：
    /// 实测 Postgres 上 8 个 worker 只跑出 1 个 worker 的 1.8 倍。
    ///
    /// sqlite 例外：它没有行锁，写本来就是全库串行的。所以那边只要求
    /// 「不重复」（安全性），不要求「都能抢到」（并行度）。
    #[tokio::test]
    async fn 并发抢占要各拿各的不能全挤在同一笔上() {
        const K: usize = 6;
        let (_g, bes) = backends().await;
        for (name, s) in bes {
            for i in 0..K {
                s.create_global(&g(&format!("par-{i}")), &[]).await.unwrap();
            }

            let mut hs = Vec::new();
            for i in 0..K {
                let s = s.clone();
                hs.push(tokio::spawn(async move {
                    s.lock_one_due(&format!("w-{i}"), 60).await.unwrap()
                }));
            }
            let mut got: Vec<String> = Vec::new();
            for h in hs {
                if let Some(row) = h.await.unwrap() {
                    got.push(row.gid);
                }
            }

            // 安全性：所有后端都不能把同一笔交给两个 owner
            let uniq: std::collections::HashSet<_> = got.iter().collect();
            assert_eq!(uniq.len(), got.len(), "{name}: 同一笔被抢到了两次");

            // 并行度：有行锁的后端应该 K 个各拿各的
            if name != "sqlite" {
                assert_eq!(
                    got.len(),
                    K,
                    "{name}: 并发抢占退化成串行了（SKIP LOCKED 没生效？）"
                );
            }
        }
    }

    #[tokio::test]
    async fn 终态不再被调度() {
        let (_g, bes) = backends().await;
        for (name, s) in bes {
            s.create_global(&g("t3"), &[]).await.unwrap();
            s.set_global_status("t3", GlobalStatus::Succeed, TransType::Saga, "")
                .await
                .unwrap();
            assert!(s.lock_one_due("w", 60).await.unwrap().is_none(), "{name}");
            let got = s.get_global("t3").await.unwrap().unwrap();
            assert_eq!(got.status, GlobalStatus::Succeed, "{name}");
            assert!(got.finish_time.is_some(), "{name}: 终态要落 finish_time");
        }
    }

    #[tokio::test]
    async fn 分支状态可更新() {
        let (_g, bes) = backends().await;
        for (name, s) in bes {
            let b = BranchRow {
                gid: "t4".into(),
                branch_id: "01".into(),
                op: BranchOp::Action,
                url: "http://x/a".into(),
                payload: "{}".into(),
                status: BranchStatus::Prepared,
            };
            s.create_global(&g("t4"), std::slice::from_ref(&b))
                .await
                .unwrap();
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
        let (_g, bes) = backends().await;
        for (name, s) in bes {
            let mut row = g("t5");
            row.query_prepared = "http://busi/query".into();
            s.create_global(&row, &[]).await.unwrap();
            s.set_global_status(
                "t5",
                GlobalStatus::Aborting,
                TransType::Saga,
                "分支 02 返回 FAILURE",
            )
            .await
            .unwrap();
            let got = s.get_global("t5").await.unwrap().unwrap();
            assert_eq!(got.query_prepared, "http://busi/query", "{name}");
            assert_eq!(got.rollback_reason, "分支 02 返回 FAILURE", "{name}");
            assert!(
                got.finish_time.is_none(),
                "{name}: 非终态不该有 finish_time"
            );

            // 空 reason 不能把已有的原因冲掉
            s.set_global_status("t5", GlobalStatus::Failed, TransType::Saga, "")
                .await
                .unwrap();
            let got = s.get_global("t5").await.unwrap().unwrap();
            assert_eq!(
                got.rollback_reason, "分支 02 返回 FAILURE",
                "{name}: 空原因不能覆盖"
            );
        }
    }

    #[tokio::test]
    async fn msg的prepared会被捞tcc的不会() {
        let (_g, bes) = backends().await;
        for (name, s) in bes {
            let mut m = g("m1");
            m.trans_type = TransType::Msg;
            m.status = GlobalStatus::Prepared;
            s.create_global(&m, &[]).await.unwrap();
            let mut t = g("c1");
            t.trans_type = TransType::Tcc;
            t.status = GlobalStatus::Prepared;
            s.create_global(&t, &[]).await.unwrap();

            let got = s.lock_one_due("w", 60).await.unwrap();
            assert_eq!(
                got.map(|x| x.gid),
                Some("m1".to_string()),
                "{name}: 只该捞到 msg"
            );
            // 再捞一次应该没有了（msg 被租约占住，tcc 不该被碰）
            assert!(s.lock_one_due("w2", 60).await.unwrap().is_none(), "{name}");
        }
    }

    #[tokio::test]
    async fn 分支登记是幂等的() {
        let (_g, bes) = backends().await;
        for (name, s) in bes {
            let mut t = g("c2");
            t.trans_type = TransType::Tcc;
            s.create_global(&t, &[]).await.unwrap();
            let ops = [
                (BranchOp::Confirm, "http://x/c".to_string()),
                (BranchOp::Cancel, "http://x/n".to_string()),
            ];
            s.register_branch("c2", "01", &ops).await.unwrap();
            s.register_branch("c2", "01", &ops).await.unwrap(); // 客户端重试
            assert_eq!(
                s.list_branches("c2").await.unwrap().len(),
                2,
                "{name}: 不该重复插入"
            );
        }
    }
}

// ==================== 后端分发 ====================

#[cfg(feature = "redis")]
pub mod redis_store;
#[cfg(feature = "redis")]
pub use redis_store::RedisStore;

/// 存储后端。
///
/// # 为什么现在才抽这一层
///
/// 这个项目原本**刻意没有抽 `Store` trait**，理由写在 DESIGN.md 里：
/// sqlite / postgres / mysql 的差异小到一层 SQL 模板就能吸收，抽象是过早的。
/// 那个判断在当时是对的。
///
/// **Redis 让前提不成立了** —— 它根本不是 SQL，没有表、没有事务、没有 WHERE，
/// 模板吸收不了。所以这里加了一层分发。
///
/// 用 enum 而不是 trait：调用方拿到的还是同一个 `Store` 具体类型，
/// 四十多个调用点一行都不用改，也不用到处写泛型或 `dyn`。
#[derive(Clone)]
enum Inner {
    Sql(SqlStore),
    #[cfg(feature = "redis")]
    Redis(RedisStore),
}

/// 存储层的统一入口。按 URL 前缀自动选后端：
///
/// ```text
/// sqlite:...     / postgres://...  / mysql://...   → SQL 后端
/// redis://...    / rediss://...                    → Redis 后端（要开 redis feature）
/// ```
///
/// ⚠ Redis 后端跟 SQL 后端有**实打实的语义差异**（持久性更弱、终态会过期），
/// 用之前务必读 [`redis_store`] 的模块说明。
#[derive(Clone)]
pub struct Store {
    inner: Inner,
}

/// 存储层的错误。
///
/// 两种后端的原生错误类型不同，统一收口到这里；`sqlx::Error` 仍然直接透出，
/// 免得改动现有调用方对错误的处理。
pub type StoreError = sqlx::Error;

#[cfg(feature = "redis")]
fn redis_err(e: redis::RedisError) -> sqlx::Error {
    sqlx::Error::Configuration(Box::new(e))
}

/// 这个 URL 是不是要走 Redis
pub fn is_redis_url(url: &str) -> bool {
    let u = url.trim().to_ascii_lowercase();
    u.starts_with("redis://") || u.starts_with("rediss://") || u.starts_with("redis+unix:")
}

impl Store {
    /// 按 URL 选后端并连上。
    pub async fn open(url: &str) -> Result<Self> {
        if is_redis_url(url) {
            #[cfg(feature = "redis")]
            {
                let r = RedisStore::open(url).await.map_err(redis_err)?;
                return Ok(Self {
                    inner: Inner::Redis(r),
                });
            }
            #[cfg(not(feature = "redis"))]
            {
                // 明确报错，而不是把 redis:// 当成 sqlite 文件名去建库 ——
                // 那会静默跑起来然后数据全落在一个叫 "redis:" 的文件里
                return Err(sqlx::Error::Configuration(
                    "这个 URL 要 Redis 后端，但构建时没开 dtmrs-store 的 `redis` feature".into(),
                ));
            }
        }
        Ok(Self {
            inner: Inner::Sql(SqlStore::open(url).await?),
        })
    }

    /// 底层是不是 Redis
    pub fn is_redis(&self) -> bool {
        match &self.inner {
            Inner::Sql(_) => false,
            #[cfg(feature = "redis")]
            Inner::Redis(_) => true,
        }
    }

    /// SQL 后端的连接池。Redis 后端返回 `None` ——
    /// 调用方（主要是测试和屏障）要自己处理这种情况
    pub fn pool(&self) -> Option<&AnyPool> {
        match &self.inner {
            Inner::Sql(s) => Some(s.pool()),
            #[cfg(feature = "redis")]
            Inner::Redis(_) => None,
        }
    }

    /// SQL 方言。Redis 后端没有方言可言，返回 `None`
    pub fn backend(&self) -> Option<Backend> {
        match &self.inner {
            Inner::Sql(s) => Some(s.backend()),
            #[cfg(feature = "redis")]
            Inner::Redis(_) => None,
        }
    }

    /// 拿底层的 Redis store（比如为了调 `with_ttl`）
    #[cfg(feature = "redis")]
    pub fn as_redis(&self) -> Option<&RedisStore> {
        match &self.inner {
            Inner::Redis(r) => Some(r),
            _ => None,
        }
    }
}

/// 把 13 个方法逐个手写分发太啰嗦，而且漏一个编译器不会提醒 ——
/// 用宏保证两边签名严格一致
macro_rules! dispatch {
    ($( $(#[$m:meta])* fn $name:ident (&self $(, $arg:ident : $ty:ty)* ) -> $ret:ty; )*) => {
        impl Store {
            $(
                $(#[$m])*
                pub async fn $name(&self $(, $arg: $ty)*) -> Result<$ret> {
                    match &self.inner {
                        Inner::Sql(s) => s.$name($($arg),*).await,
                        #[cfg(feature = "redis")]
                        Inner::Redis(r) => r.$name($($arg),*).await.map_err(redis_err),
                    }
                }
            )*
        }
    };
}

dispatch! {
    /// 建表（Redis 后端是空操作）
    fn migrate(&self) -> ();
    /// 建全局事务 + 分支。已存在返回 `false`，**不覆盖**
    fn create_global(&self, g: &GlobalRow, branches: &[BranchRow]) -> bool;
    fn get_global(&self, gid: &str) -> Option<GlobalRow>;
    fn list_branches(&self, gid: &str) -> Vec<BranchRow>;
    /// 抢一个到期事务。多实例不重复推进就靠它的原子性
    fn lock_one_due(&self, owner: &str, lease: i64) -> Option<GlobalRow>;
    fn set_global_status(&self, gid: &str, status: GlobalStatus, trans_type: TransType, reason: &str) -> ();
    /// 把 prepared 推成 submitted 并排进调度队列，一次调用做完。见 [`SubmitOutcome`]
    fn submit_prepared(&self, gid: &str, owner: &str, next_cron_time: i64) -> SubmitOutcome;
    fn set_branch_result(&self, gid: &str, branch_id: &str, op: BranchOp, status: BranchStatus, payload: &str) -> ();
    fn set_branch_status(&self, gid: &str, branch_id: &str, op: BranchOp, status: BranchStatus) -> ();
    fn schedule_retry(&self, gid: &str, interval: i64) -> ();
    fn schedule_now(&self, gid: &str) -> ();
    fn register_branch(&self, gid: &str, branch_id: &str, ops: &[(BranchOp, String)]) -> ();
    fn list_recent(&self, limit: i64) -> Vec<GlobalRow>;
}
