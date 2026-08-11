//! Redis 后端 —— 为吞吐而生，代价是持久性。
//!
//! # 什么时候该用它
//!
//! 秒杀那类**流量尖峰**场景：短时间涌进海量事务，SQL 库的写入和行锁扛不住。
//! DTM 支持 Redis 也是这个动机。
//!
//! # ⚠ 三条跟 SQL 后端不一样的语义，用之前必须知道
//!
//! **1. 持久性弱一档。**
//! Redis 默认 `appendfsync everysec`，崩溃可能丢掉最后一秒的写入。对事务协调器
//! 来说，丢的是**事务状态**：可能出现「业务侧已经扣了款，但 TC 这边没有这笔事务」
//! 的悬挂。SQL 后端不会这样（提交即落盘）。
//!
//! 要么接受它（秒杀场景下业务常常能接受，且有对账兜底），要么把 Redis 配成
//! `appendfsync always` —— 那样吞吐会掉，但仍然比 SQL 快。
//! **别在默认配置下拿它跑资金类强一致场景。**
//!
//! **2. 终态事务会过期消失。**
//! SQL 后端里已完成的事务永久留着；这里到终态后会挂 TTL（默认 7 天，见
//! [`RedisStore::with_ttl`]）。不然秒杀几千万笔之后内存就没了。
//! 需要长期审计记录的话，自己往别处归档。
//!
//! **3. `list_recent` 的索引有上限。**
//! 只保留最近 [`RECENT_CAP`] 笔用于管理接口，不是全量历史。
//!
//! # 原子性怎么保证
//!
//! 关键操作全走 Lua 脚本。Redis 执行脚本是**单线程且不可打断**的，
//! 所以「查一个到期事务 + 抢占它」这种复合操作天然原子 ——
//! 比 SQL 那边「先 SELECT 再带条件 UPDATE」的两步法反而更直接。
//!
//! 多个 TC 实例抢同一笔事务时，Redis 侧不需要行锁也不会重复推进。

use crate::{BranchRow, GlobalRow, MID};
use dtmrs_core::dialect::check_len;
use dtmrs_core::{Backend, BranchOp, BranchStatus, GlobalStatus, TransType};
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;

/// `list_recent` 的索引上限。管理接口只看最近的，没必要留全量
pub const RECENT_CAP: isize = 1000;

/// 终态事务的默认存活时间（秒）。7 天够排查问题了
pub const DEFAULT_FINAL_TTL: i64 = 7 * 24 * 3600;

/// 一次最多扫多少个候选去找可调度的事务。
///
/// 索引里可能混进已经不该调度的（比如 tcc 还停在 prepared），
/// 扫一小段就够，扫不到就当没有 —— 下一轮 cron 还会再来。
const SCAN_LIMIT: usize = 20;

type Result<T> = std::result::Result<T, redis::RedisError>;

fn err(msg: &str) -> redis::RedisError {
    redis::RedisError::from((redis::ErrorKind::Client, "dtmrs", msg.to_string()))
}

/// 判断一个事务现在该不该被 cron 调度。
///
/// **必须跟 SQL 后端的 `lock_one_due` 那个 WHERE 完全一致**，否则两种后端
/// 会有不同的推进行为。Lua 脚本里也有一份同样的判断（防索引漂移）。
fn schedulable(status: GlobalStatus, tt: TransType) -> bool {
    matches!(status, GlobalStatus::Submitted | GlobalStatus::Aborting)
        || (status == GlobalStatus::Prepared && tt == TransType::Msg)
}

/// 每个脚本都要用的那段判断，避免抄三遍抄漏
const LUA_SCHEDULABLE: &str = r#"
local function schedulable(status, tt)
  if status == 'submitted' or status == 'aborting' then return true end
  if status == 'prepared' and tt == 'msg' then return true end
  return false
end
"#;

#[derive(Clone)]
pub struct RedisStore {
    conn: MultiplexedConnection,
    prefix: String,
    final_ttl: i64,
}

impl RedisStore {
    pub async fn open(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(Self {
            conn,
            prefix: "dtmrs:".to_string(),
            final_ttl: DEFAULT_FINAL_TTL,
        })
    }

    /// 改 key 前缀，多个环境共用一个 Redis 时用
    pub fn with_prefix(mut self, p: &str) -> Self {
        self.prefix = p.to_string();
        self
    }

    /// 改终态事务的存活时间。传 0 表示**不过期**（内存会一直涨，自己盯着）
    pub fn with_ttl(mut self, secs: i64) -> Self {
        self.final_ttl = secs;
        self
    }

    fn gkey(&self, gid: &str) -> String {
        format!("{}g:{}", self.prefix, gid)
    }
    fn bkey(&self, gid: &str) -> String {
        format!("{}b:{}", self.prefix, gid)
    }
    /// 可调度事务的索引，score = next_cron_time
    fn ikey(&self) -> String {
        format!("{}idx", self.prefix)
    }
    /// 最近事务的索引，score = create_time
    fn akey(&self) -> String {
        format!("{}all", self.prefix)
    }

    /// 分支在 hash 里的字段名。用 `\x1f`（单元分隔符）拼，
    /// 因为它不可能出现在 branch_id 或 op 里
    fn bfield(branch_id: &str, op: BranchOp) -> String {
        format!("{}\x1f{}", branch_id, op.as_str())
    }

    fn parse_bfield(f: &str) -> Option<(String, BranchOp)> {
        let (b, o) = f.split_once('\x1f')?;
        Some((b.to_string(), BranchOp::parse(o)?))
    }

    /// 长度校验。Redis 本身没有列宽限制，但**仍然要挡** ——
    /// 换到 SQL 后端时同一批数据必须还能存下，不然迁移会炸。
    /// 保持两种后端接受的输入完全一致。
    fn check_global(g: &GlobalRow) -> Result<()> {
        check_len("gid", &g.gid, Backend::ID_MAX).map_err(|e| err(&e.to_string()))?;
        check_len("payload", &g.payload, crate::BIG).map_err(|e| err(&e.to_string()))?;
        check_len("query_prepared", &g.query_prepared, MID).map_err(|e| err(&e.to_string()))?;
        Ok(())
    }

    fn check_branch(b: &BranchRow) -> Result<()> {
        check_len("branch_id", &b.branch_id, Backend::ID_MAX).map_err(|e| err(&e.to_string()))?;
        check_len("url", &b.url, MID).map_err(|e| err(&e.to_string()))?;
        check_len("payload", &b.payload, MID).map_err(|e| err(&e.to_string()))?;
        Ok(())
    }

    fn branch_value(b: &BranchRow) -> String {
        serde_json::json!({
            "url": b.url,
            "payload": b.payload,
            "status": b.status.as_str(),
        })
        .to_string()
    }

    fn branch_from(gid: &str, field: &str, val: &str) -> Option<BranchRow> {
        let (branch_id, op) = Self::parse_bfield(field)?;
        let v: serde_json::Value = serde_json::from_str(val).ok()?;
        Some(BranchRow {
            gid: gid.to_string(),
            branch_id,
            op,
            url: v
                .get("url")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            payload: v
                .get("payload")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            status: v
                .get("status")
                .and_then(|x| x.as_str())
                .and_then(BranchStatus::parse)
                .unwrap_or(BranchStatus::Prepared),
        })
    }

    fn global_from(map: &std::collections::HashMap<String, String>) -> Option<GlobalRow> {
        let get = |k: &str| map.get(k).cloned().unwrap_or_default();
        let num = |k: &str| map.get(k).and_then(|v| v.parse::<i64>().ok()).unwrap_or(0);
        if map.is_empty() {
            return None;
        }
        Some(GlobalRow {
            gid: get("gid"),
            trans_type: TransType::parse(&get("trans_type")).unwrap_or(TransType::Saga),
            status: GlobalStatus::parse(&get("status")).unwrap_or(GlobalStatus::Prepared),
            payload: get("payload"),
            next_cron_time: num("next_cron_time"),
            next_cron_interval: num("next_cron_interval"),
            owner: get("owner"),
            rollback_reason: get("rollback_reason"),
            query_prepared: get("query_prepared"),
            create_time: num("create_time"),
            // 没到终态时这个字段压根不存在，不能当成 0
            finish_time: map.get("finish_time").and_then(|v| v.parse::<i64>().ok()),
        })
    }

    // ---------------- 对外接口（跟 SQL 后端一一对应）----------------

    /// Redis 不需要建表。留这个方法只为两种后端的调用方式一致
    pub async fn migrate(&self) -> Result<()> {
        Ok(())
    }

    /// 建全局事务 + 分支。**已存在则原样返回 false，不覆盖** ——
    /// 客户端重试提交同一个 gid 是很常见的，覆盖会把已经推进的状态抹掉。
    pub async fn create_global(&self, g: &GlobalRow, branches: &[BranchRow]) -> Result<bool> {
        Self::check_global(g)?;
        for b in branches {
            Self::check_branch(b)?;
        }
        let t = crate::now();

        let script = redis::Script::new(&format!(
            r#"
            if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
            redis.call('HSET', KEYS[1],
                'gid', ARGV[1], 'trans_type', ARGV[2], 'status', ARGV[3],
                'payload', ARGV[4], 'next_cron_time', ARGV[5],
                'next_cron_interval', ARGV[6], 'owner', '', 'rollback_reason', '',
                'query_prepared', ARGV[7], 'create_time', ARGV[8], 'update_time', ARGV[8])
            -- 分支从第 10 个 ARGV 开始，每两个一组（field, value）
            for i = 10, #ARGV, 2 do
                redis.call('HSETNX', KEYS[2], ARGV[i], ARGV[i + 1])
            end
            if ARGV[9] == '1' then
                redis.call('ZADD', KEYS[3], ARGV[5], ARGV[1])
            end
            redis.call('ZADD', KEYS[4], ARGV[8], ARGV[1])
            -- 管理视图只看最近的，索引留个上限免得内存无限涨
            redis.call('ZREMRANGEBYRANK', KEYS[4], 0, -{cap})
            return 1
            "#,
            cap = RECENT_CAP + 1
        ));

        let mut inv = script.prepare_invoke();
        inv.key(self.gkey(&g.gid))
            .key(self.bkey(&g.gid))
            .key(self.ikey())
            .key(self.akey())
            .arg(&g.gid)
            .arg(g.trans_type.to_string())
            .arg(g.status.as_str())
            .arg(&g.payload)
            .arg(g.next_cron_time)
            .arg(g.next_cron_interval)
            .arg(&g.query_prepared)
            .arg(t)
            .arg(if schedulable(g.status, g.trans_type) {
                "1"
            } else {
                "0"
            });
        for b in branches {
            inv.arg(Self::bfield(&b.branch_id, b.op))
                .arg(Self::branch_value(b));
        }

        let mut c = self.conn.clone();
        let created: i64 = inv.invoke_async(&mut c).await?;
        Ok(created == 1)
    }

    pub async fn get_global(&self, gid: &str) -> Result<Option<GlobalRow>> {
        let mut c = self.conn.clone();
        let map: std::collections::HashMap<String, String> = c.hgetall(self.gkey(gid)).await?;
        Ok(Self::global_from(&map))
    }

    pub async fn list_branches(&self, gid: &str) -> Result<Vec<BranchRow>> {
        let mut c = self.conn.clone();
        let map: std::collections::HashMap<String, String> = c.hgetall(self.bkey(gid)).await?;
        let mut v: Vec<BranchRow> = map
            .iter()
            .filter_map(|(f, val)| Self::branch_from(gid, f, val))
            .collect();
        // SQL 那边没写 ORDER BY，顺序本来就没保证；这里显式排一下更可预期
        v.sort_by(|a, b| {
            a.branch_id
                .cmp(&b.branch_id)
                .then(a.op.as_str().cmp(b.op.as_str()))
        });
        Ok(v)
    }

    /// 抢一个到期的事务。**这是多实例不重复推进的关键。**
    ///
    /// 整段逻辑在一个 Lua 脚本里跑完，Redis 执行脚本单线程且不可打断，
    /// 所以「找到 + 抢占」是原子的，两个实例不可能抢到同一笔。
    ///
    /// 脚本里会顺手清掉索引中已经不该调度的成员（终态、或者 tcc 还停在
    /// prepared），这样索引不会因为别处漏维护而慢慢腐烂。
    pub async fn lock_one_due(&self, owner: &str, lease: i64) -> Result<Option<GlobalRow>> {
        let t = crate::now();
        let script = redis::Script::new(&format!(
            r#"
            {sched}
            local cands = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1],
                                     'LIMIT', 0, {scan})
            for i = 1, #cands do
                local gid = cands[i]
                local gkey = ARGV[4] .. gid
                local st = redis.call('HGET', gkey, 'status')
                local tt = redis.call('HGET', gkey, 'trans_type')
                if not st then
                    -- 事务本体没了（过期了），索引里的残留清掉
                    redis.call('ZREM', KEYS[1], gid)
                elseif not schedulable(st, tt) then
                    -- 状态已经不该被调度，从索引摘掉
                    redis.call('ZREM', KEYS[1], gid)
                else
                    -- 抢到了：立刻把下次调度时间推到租约之后，等于占坑
                    redis.call('HSET', gkey, 'owner', ARGV[2],
                               'next_cron_time', ARGV[3], 'update_time', ARGV[1])
                    redis.call('ZADD', KEYS[1], ARGV[3], gid)
                    return redis.call('HGETALL', gkey)
                end
            end
            return nil
            "#,
            sched = LUA_SCHEDULABLE,
            scan = SCAN_LIMIT
        ));

        let mut c = self.conn.clone();
        let flat: Option<Vec<String>> = script
            .key(self.ikey())
            .arg(t)
            .arg(owner)
            .arg(t + lease)
            .arg(format!("{}g:", self.prefix))
            .invoke_async(&mut c)
            .await?;

        let Some(flat) = flat else { return Ok(None) };
        let map: std::collections::HashMap<String, String> = flat
            .chunks(2)
            .filter(|c| c.len() == 2)
            .map(|c| (c[0].clone(), c[1].clone()))
            .collect();
        Ok(Self::global_from(&map))
    }

    /// 落全局状态。到终态时挂 TTL 并从调度索引里摘掉。
    pub async fn set_global_status(
        &self,
        gid: &str,
        status: GlobalStatus,
        reason: &str,
    ) -> Result<()> {
        let t = crate::now();
        // 跟 SQL 后端一致：reason 是诊断信息，**截断而不是报错** ——
        // 让状态机的收尾因为一句话太长而失败，事务就永远推不到终态了
        let reason: String = reason.chars().take(MID).collect();

        let script = redis::Script::new(&format!(
            r#"
            {sched}
            if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
            local tt = redis.call('HGET', KEYS[1], 'trans_type')
            redis.call('HSET', KEYS[1], 'status', ARGV[1], 'update_time', ARGV[2])
            if ARGV[3] == '1' then
                redis.call('HSET', KEYS[1], 'finish_time', ARGV[2])
            end
            if ARGV[4] ~= '' then
                redis.call('HSET', KEYS[1], 'rollback_reason', ARGV[4])
            end
            if schedulable(ARGV[1], tt) then
                local nct = redis.call('HGET', KEYS[1], 'next_cron_time')
                redis.call('ZADD', KEYS[2], nct, ARGV[5])
            else
                redis.call('ZREM', KEYS[2], ARGV[5])
            end
            -- 终态挂 TTL：秒杀跑几千万笔之后，不回收内存会撑爆
            if ARGV[3] == '1' and tonumber(ARGV[6]) > 0 then
                redis.call('EXPIRE', KEYS[1], ARGV[6])
                redis.call('EXPIRE', KEYS[3], ARGV[6])
            end
            return 1
            "#,
            sched = LUA_SCHEDULABLE
        ));

        let mut c = self.conn.clone();
        let _: i64 = script
            .key(self.gkey(gid))
            .key(self.ikey())
            .key(self.bkey(gid))
            .arg(status.as_str())
            .arg(t)
            .arg(if status.is_final() { "1" } else { "0" })
            .arg(reason)
            .arg(gid)
            .arg(self.final_ttl)
            .invoke_async(&mut c)
            .await?;
        Ok(())
    }

    /// 落分支状态和结果数据。
    ///
    /// 分支是一个 JSON 值，改一个字段得读-改-写 —— 所以走脚本，
    /// 保证「状态」和「结果」在同一次写里落盘。分两步写的话中间崩了，
    /// 会出现「标了成功但结果丢了」，workflow 重放时就拿不到返回值。
    pub async fn set_branch_result(
        &self,
        gid: &str,
        branch_id: &str,
        op: BranchOp,
        status: BranchStatus,
        payload: &str,
    ) -> Result<()> {
        check_len("payload", payload, MID).map_err(|e| err(&e.to_string()))?;
        let script = redis::Script::new(
            r#"
            local cur = redis.call('HGET', KEYS[1], ARGV[1])
            if not cur then return 0 end
            local v = cjson.decode(cur)
            v['status'] = ARGV[2]
            if ARGV[4] == '1' then v['payload'] = ARGV[3] end
            redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(v))
            return 1
            "#,
        );
        let mut c = self.conn.clone();
        let _: i64 = script
            .key(self.bkey(gid))
            .arg(Self::bfield(branch_id, op))
            .arg(status.as_str())
            .arg(payload)
            .arg("1")
            .invoke_async(&mut c)
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
        let script = redis::Script::new(
            r#"
            local cur = redis.call('HGET', KEYS[1], ARGV[1])
            if not cur then return 0 end
            local v = cjson.decode(cur)
            v['status'] = ARGV[2]
            redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(v))
            return 1
            "#,
        );
        let mut c = self.conn.clone();
        let _: i64 = script
            .key(self.bkey(gid))
            .arg(Self::bfield(branch_id, op))
            .arg(status.as_str())
            .invoke_async(&mut c)
            .await?;
        Ok(())
    }

    pub async fn schedule_retry(&self, gid: &str, interval: i64) -> Result<()> {
        let t = crate::now();
        let script = redis::Script::new(
            r#"
            if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
            redis.call('HSET', KEYS[1], 'next_cron_interval', ARGV[1],
                       'next_cron_time', ARGV[2], 'update_time', ARGV[3])
            -- 只更新已经在索引里的，别把不该调度的塞回去
            if redis.call('ZSCORE', KEYS[2], ARGV[4]) then
                redis.call('ZADD', KEYS[2], ARGV[2], ARGV[4])
            end
            return 1
            "#,
        );
        let mut c = self.conn.clone();
        let _: i64 = script
            .key(self.gkey(gid))
            .key(self.ikey())
            .arg(interval)
            .arg(t + interval)
            .arg(t)
            .arg(gid)
            .invoke_async(&mut c)
            .await?;
        Ok(())
    }

    /// 让某个事务立刻可被调度（提交/中止之后叫一下，不用等 cron 周期）
    pub async fn schedule_now(&self, gid: &str) -> Result<()> {
        let t = crate::now();
        let script = redis::Script::new(&format!(
            r#"
            {sched}
            if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
            redis.call('HSET', KEYS[1], 'next_cron_time', ARGV[1], 'next_cron_interval', 0)
            local st = redis.call('HGET', KEYS[1], 'status')
            local tt = redis.call('HGET', KEYS[1], 'trans_type')
            -- 这里跟 schedule_retry 不同：submit / abort 之后事务**变得**可调度了，
            -- 索引里没有就得加进去
            if schedulable(st, tt) then
                redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2])
            end
            return 1
            "#,
            sched = LUA_SCHEDULABLE
        ));
        let mut c = self.conn.clone();
        let _: i64 = script
            .key(self.gkey(gid))
            .key(self.ikey())
            .arg(t)
            .arg(gid)
            .invoke_async(&mut c)
            .await?;
        Ok(())
    }

    /// 登记分支。冲突时忽略，所以重复登记是幂等的（客户端重试很常见）
    pub async fn register_branch(
        &self,
        gid: &str,
        branch_id: &str,
        ops: &[(BranchOp, String)],
    ) -> Result<()> {
        check_len("gid", gid, Backend::ID_MAX).map_err(|e| err(&e.to_string()))?;
        check_len("branch_id", branch_id, Backend::ID_MAX).map_err(|e| err(&e.to_string()))?;
        for (_, url) in ops {
            check_len("url", url, MID).map_err(|e| err(&e.to_string()))?;
        }
        let mut c = self.conn.clone();
        for (op, url) in ops {
            let row = BranchRow {
                gid: gid.to_string(),
                branch_id: branch_id.to_string(),
                op: *op,
                url: url.clone(),
                payload: String::new(),
                status: BranchStatus::Prepared,
            };
            let _: i64 = c
                .hset_nx(
                    self.bkey(gid),
                    Self::bfield(branch_id, *op),
                    Self::branch_value(&row),
                )
                .await?;
        }
        Ok(())
    }

    /// 最近的事务，按创建时间倒序。**只有最近 [`RECENT_CAP`] 笔**，不是全量历史
    pub async fn list_recent(&self, limit: i64) -> Result<Vec<GlobalRow>> {
        let mut c = self.conn.clone();
        let gids: Vec<String> = c
            .zrevrange(self.akey(), 0, (limit.max(1) - 1) as isize)
            .await?;
        let mut out = Vec::with_capacity(gids.len());
        for gid in gids {
            let map: std::collections::HashMap<String, String> = c.hgetall(self.gkey(&gid)).await?;
            // 已经过期消失的就跳过 —— 索引里留了残影是正常的
            if let Some(g) = Self::global_from(&map) {
                out.push(g);
            }
        }
        Ok(out)
    }

    /// 清空这个前缀下的全部数据。**只给测试用。**
    pub async fn flush_prefix(&self) -> Result<()> {
        let mut c = self.conn.clone();
        let keys: Vec<String> = c.keys(format!("{}*", self.prefix)).await?;
        if !keys.is_empty() {
            let _: i64 = c.del(keys).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 可调度判断跟sql后端的where一致() {
        // 这两处漂移的话，同一笔事务在两种后端上的推进行为就不一样了
        assert!(schedulable(GlobalStatus::Submitted, TransType::Saga));
        assert!(schedulable(GlobalStatus::Aborting, TransType::Saga));
        // 只有 msg 的 prepared 要被捞起来回查
        assert!(schedulable(GlobalStatus::Prepared, TransType::Msg));
        assert!(!schedulable(GlobalStatus::Prepared, TransType::Tcc));
        assert!(!schedulable(GlobalStatus::Prepared, TransType::Xa));
        // 终态一律不调度
        for tt in [TransType::Saga, TransType::Msg, TransType::Workflow] {
            assert!(!schedulable(GlobalStatus::Succeed, tt));
            assert!(!schedulable(GlobalStatus::Failed, tt));
        }
    }

    #[test]
    fn 分支字段名能往返() {
        let f = RedisStore::bfield("01", BranchOp::Compensate);
        assert_eq!(
            RedisStore::parse_bfield(&f),
            Some(("01".to_string(), BranchOp::Compensate))
        );
        // 分隔符用 \x1f，正常内容里不会出现
        assert!(f.contains('\x1f'));
        assert_eq!(RedisStore::parse_bfield("没有分隔符"), None);
    }

    #[test]
    fn 没终结的事务不该有完成时间() {
        // finish_time 缺字段要解成 None 而不是 0 —— 0 会被当成 1970 年完成
        let mut m = std::collections::HashMap::new();
        m.insert("gid".to_string(), "g1".to_string());
        m.insert("status".to_string(), "submitted".to_string());
        let g = RedisStore::global_from(&m).unwrap();
        assert_eq!(g.finish_time, None);
        assert_eq!(g.status, GlobalStatus::Submitted);

        m.insert("finish_time".to_string(), "123".to_string());
        assert_eq!(RedisStore::global_from(&m).unwrap().finish_time, Some(123));
    }

    #[test]
    fn 空哈希解成none() {
        // key 过期消失时会拿到空 map，不能当成一个字段全空的事务
        assert!(RedisStore::global_from(&std::collections::HashMap::new()).is_none());
    }
}
