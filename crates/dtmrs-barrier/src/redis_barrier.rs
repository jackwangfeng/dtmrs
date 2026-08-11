//! 业务数据在 Redis 里时用的子事务屏障。
//!
//! # 为什么不能复用 SQL 那套
//!
//! SQL 屏障的原子性来自「屏障记录和业务 SQL 在同一个本地事务里提交」——
//! 调用方把自己的 `Transaction` 传进来，我们只做判定。
//!
//! Redis 没有可以让外部加入的多语句事务（`MULTI` 不能在中途根据读到的值
//! 决定后面做什么）。所以这里换一个原子性来源：**屏障判定和业务操作写在
//! 同一个 Lua 脚本里**，Redis 执行脚本单线程且不可打断。
//!
//! 代价是**业务逻辑必须能用 Lua 表达**。扣库存、扣余额这类计数操作正好可以，
//! 而秒杀场景要的也就是这个。复杂业务请继续用 SQL 屏障。
//!
//! # 判定语义跟 SQL 版逐条一致
//!
//! | 情况 | SQL 版 | 这里 |
//! |---|---|---|
//! | 重复请求 | 插不进去（`rows_affected = 0`） | `SET NX` 返回 nil |
//! | 空回滚 | 补偿抢到了正向分支的行 | 补偿抢到了正向分支的键 |
//! | 悬挂 | 补偿先占了位，晚到的正向插不进去 | 同上 |
//!
//! 同一套不变量在 `tests/` 里对着真 Redis 跑，用例名跟 SQL 版一一对应。
//!
//! # 跟 SQL 版的两处**行为差异**（不是 bug，是介质决定的）
//!
//! 1. **屏障键会过期。** SQL 版的屏障行永久保留，这里必须挂 TTL，否则秒杀
//!    几千万笔之后内存就没了。TTL 必须**长于事务可能的最大生命周期**
//!    （含重试退避），否则一笔事务的正向和补偿可能落在 TTL 两侧，
//!    屏障就失效了。默认 7 天。
//! 2. **业务失败要由脚本自己表达。** SQL 版里业务失败是调用方自己的事
//!    （不提交就行）；这里业务在我们的脚本内，所以约定：脚本
//!    `return 'FAILURE'` 表示业务拒绝，我们翻译成 [`RedisOutcome::Failure`]。

use crate::{Error, Result};
use dtmrs_core::BranchOp;
use redis::aio::ConnectionLike;

/// 屏障键的默认存活时间（秒）。**必须长于事务的最大生命周期**，见模块头注释
pub const DEFAULT_BARRIER_TTL: i64 = 7 * 24 * 3600;

/// Redis 屏障的判定结果。
///
/// 比 SQL 版的 [`crate::Decision`] 多一个 `Failure`：业务逻辑跑在我们的脚本
/// 里，它的拒绝只能从这里带出来（SQL 版里业务失败是调用方自己处理的）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisOutcome {
    /// 业务操作已经执行
    Executed,
    /// **空回滚**：正向分支从没执行过，补偿空转了，业务操作没跑
    NullCompensation,
    /// **重复请求或悬挂**：这次调用之前处理过，业务操作没跑
    Duplicated,
    /// 业务逻辑自己拒绝了（脚本 `return 'FAILURE'`），比如库存不足。
    /// 屏障键仍然会留下 —— 这次调用**确实被处理过了**，重试不该再跑一遍
    Failure,
}

/// 业务数据在 Redis 里时的分支屏障。
///
/// 用法见 [`Self::check_adjust_amount`]（秒杀扣库存）和 [`Self::call`]（通用）。
#[derive(Debug, Clone)]
pub struct RedisBarrier {
    pub trans_type: String,
    pub gid: String,
    pub branch_id: String,
    pub op: BranchOp,
    prefix: String,
    ttl: i64,
    counter: u32,
}

impl RedisBarrier {
    /// 从 TC 传来的分支信息构造。四个字段缺一不可 —— 缺了就没法定位屏障键
    pub fn new(trans_type: &str, gid: &str, branch_id: &str, op: &str) -> Result<Self> {
        let Some(op) = BranchOp::parse(op) else {
            return Err(Error::InvalidTransInfo(format!("未知 op: {op}")));
        };
        if trans_type.is_empty() || gid.is_empty() || branch_id.is_empty() {
            return Err(Error::InvalidTransInfo(
                "trans_type / gid / branch_id 都不能为空".into(),
            ));
        }
        Ok(Self {
            trans_type: trans_type.into(),
            gid: gid.into(),
            branch_id: branch_id.into(),
            op,
            prefix: "dtmrs:bar:".into(),
            ttl: DEFAULT_BARRIER_TTL,
            counter: 0,
        })
    }

    /// 改键前缀，多个环境共用一个 Redis 时用
    pub fn with_prefix(mut self, p: &str) -> Self {
        self.prefix = p.into();
        self
    }

    /// 改屏障键的存活时间（秒）。
    ///
    /// ⚠ **必须长于事务可能的最大生命周期**（含重试退避）。短了的话，
    /// 正向分支的屏障键先过期、补偿再来时会以为「正向没跑过」而空转，
    /// 副作用就漏补了。
    pub fn with_ttl(mut self, secs: i64) -> Self {
        self.ttl = secs;
        self
    }

    fn next_barrier_id(&mut self) -> String {
        self.counter += 1;
        format!("{:02}", self.counter)
    }

    fn bkey(&self, op: &str, bid: &str) -> String {
        format!(
            "{}{}-{}-{}-{}",
            self.prefix, self.gid, self.branch_id, op, bid
        )
    }

    /// 带屏障保护地跑一段业务 Lua。
    ///
    /// `busi` 里可以用 `KEYS[1..]` 和 `ARGV[1..]`，编号就是你传进来的顺序 ——
    /// 屏障自己用的键和参数追加在**后面**，不会打乱你的编号。
    ///
    /// 业务想拒绝（库存不足之类）就 `return 'FAILURE'`。
    ///
    /// ```ignore
    /// let mut b = RedisBarrier::new("saga", gid, "01", "action")?;
    /// let r = b.call(&mut conn,
    ///     "local v = redis.call('GET', KEYS[1])
    ///      if not v or tonumber(v) < tonumber(ARGV[1]) then return 'FAILURE' end
    ///      redis.call('DECRBY', KEYS[1], ARGV[1])",
    ///     &["stock:1001".into()], &["1".into()]).await?;
    /// ```
    pub async fn call<C: ConnectionLike>(
        &mut self,
        conn: &mut C,
        busi: &str,
        keys: &[String],
        args: &[String],
    ) -> Result<RedisOutcome> {
        let bid = self.next_barrier_id();
        let nk = keys.len();
        let na = args.len();
        // 屏障键接在用户的键后面，这样用户脚本里的 KEYS[1..n] 编号不变
        let k_cur = nk + 1;
        let k_org = nk + 2;
        let a_ttl = na + 1;
        let a_has_origin = na + 2;

        // ⚠ 这段的结构必须跟 SQL 版的 `decide` 一一对应，改一处就要改两处：
        //   1. 补偿类操作先「假装自己是正向分支」占位，占到了 = 正向没来过 = 空回滚
        //   2. 再以自己的身份占位，占不到 = 重复请求或悬挂
        // `SET NX` 占不到时返回 false，对应 SQL 那边的 rows_affected = 0
        let script = format!(
            r#"
            local origin_got = false
            if ARGV[{a_has_origin}] == '1' then
                origin_got = redis.call('SET', KEYS[{k_org}], 'origin',
                                        'NX', 'EX', ARGV[{a_ttl}])
            end
            local cur_got = redis.call('SET', KEYS[{k_cur}], 'cur',
                                       'NX', 'EX', ARGV[{a_ttl}])
            if ARGV[{a_has_origin}] == '1' and origin_got then
                return 'NULL_COMPENSATION'
            end
            if not cur_got then
                return 'DUPLICATED'
            end
            local function busi()
            {busi}
            end
            local r = busi()
            if r == 'FAILURE' then return 'FAILURE' end
            return 'EXECUTED'
            "#
        );

        let script = redis::Script::new(&script);
        let mut inv = script.prepare_invoke();
        for k in keys {
            inv.key(k);
        }
        inv.key(self.bkey(self.op.as_str(), &bid));
        // 非补偿操作用不到这个键，但 KEYS 的个数要固定，随便塞一个同前缀的占位
        let origin = self.op.origin_op().map(|o| o.as_str()).unwrap_or("none");
        inv.key(self.bkey(origin, &bid));
        for a in args {
            inv.arg(a);
        }
        inv.arg(self.ttl);
        inv.arg(if self.op.is_compensating() { "1" } else { "0" });

        let r: String = inv
            .invoke_async(conn)
            .await
            .map_err(|e| Error::Redis(e.to_string()))?;
        Ok(match r.as_str() {
            "EXECUTED" => RedisOutcome::Executed,
            "NULL_COMPENSATION" => RedisOutcome::NullCompensation,
            "DUPLICATED" => RedisOutcome::Duplicated,
            _ => RedisOutcome::Failure,
        })
    }

    /// 秒杀那个形状：**检查余额够不够，够就调整**，屏障保护和调整在同一个脚本里。
    ///
    /// `amount` 传负数表示扣减。余额不足（会变成负数）返回
    /// [`RedisOutcome::Failure`] —— 调用方该把它翻译成「业务明确失败」
    /// 让 TC 回滚，而不是当成未知去重试。
    ///
    /// 键不存在也算失败（不会凭空创建库存）。
    pub async fn check_adjust_amount<C: ConnectionLike>(
        &mut self,
        conn: &mut C,
        key: &str,
        amount: i64,
    ) -> Result<RedisOutcome> {
        self.call(
            conn,
            // 键不存在 → 失败；扣完变负 → 失败。两者都不能悄悄放过
            r#"
            local v = redis.call('GET', KEYS[1])
            if v == false or tonumber(v) + tonumber(ARGV[1]) < 0 then
                return 'FAILURE'
            end
            redis.call('INCRBY', KEYS[1], ARGV[1])
            "#,
            &[key.to_string()],
            &[amount.to_string()],
        )
        .await
    }

    /// 二阶段消息的回查（`query_prepared`）。
    ///
    /// 业务在本地事务里没留下痕迹时，TC 会来问「这单到底提交了没有」。
    /// 这里的答复规则是：**没见过就地写一个 rollback 标记**，让后来的正向
    /// 分支看到标记后放弃 —— 也就是把「不知道」固化成「没提交」，
    /// 避免两边各自猜出不同的结论。
    ///
    /// 返回 [`RedisOutcome::Failure`] 表示「这单没提交，TC 该回滚」。
    pub async fn query_prepared<C: ConnectionLike>(
        &mut self,
        conn: &mut C,
    ) -> Result<RedisOutcome> {
        // msg 的正向分支固定是 01/action/01，回查要看的就是它那个键
        let key = format!("{}{}-{}-{}-{}", self.prefix, self.gid, "01", "action", "01");
        let script = r#"
            local v = redis.call('GET', KEYS[1])
            if v == false then
                redis.call('SET', KEYS[1], 'rollback', 'EX', ARGV[1])
                v = 'rollback'
            end
            if v == 'rollback' then return 'FAILURE' end
            return 'EXECUTED'
            "#;
        let r: String = redis::Script::new(script)
            .key(&key)
            .arg(self.ttl)
            .invoke_async(conn)
            .await
            .map_err(|e| Error::Redis(e.to_string()))?;
        Ok(if r == "FAILURE" {
            RedisOutcome::Failure
        } else {
            RedisOutcome::Executed
        })
    }
}
