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

use crate::{TokenRow, BranchRow, GlobalRow, SubmitOutcome, MID};
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
    /// 一个访问令牌一个 hash：`tok:{sha256}`
    fn tkey(&self, hash: &str) -> String {
        format!("{}tok:{}", self.prefix, hash)
    }
    /// 令牌索引（set），用来列举 —— Redis 上不能像 SQL 那样 SELECT *
    fn tidx(&self) -> String {
        format!("{}tokens", self.prefix)
    }

    /// 分支在 hash 里的字段名。用 `\x1f`（单元分隔符）拼，
    /// 因为它不可能出现在 branch_id 或 op 里
    /// 分支的**不变部分**（url + payload）在 `b:{gid}` 里的字段名。
    ///
    /// # 为什么可变的 status 要单独一个字段（见 [`Self::sfield`]）
    ///
    /// 早先 url / payload / status 一起塞在一个 JSON 值里，于是改一次状态要
    /// `HGET` → `cjson.decode` → 改 → `cjson.encode` → `HSET`：两条命令外加
    /// 两次 JSON 编解码，全花在 Redis 那个唯一的线程上。
    ///
    /// 拆开之后改状态就是**一条 `HSET`，不读、不解析**。读那边不变 ——
    /// `list_branches` still 一次 `HGETALL` 把两种字段一起取回来。
    fn bfield(branch_id: &str, op: BranchOp) -> String {
        format!("{}\x1f{}", branch_id, op.as_str())
    }

    /// 分支状态字段。跟 [`Self::bfield`] 同一个哈希，多一个 `\x1fs` 后缀
    fn sfield(branch_id: &str, op: BranchOp) -> String {
        format!("{}\x1f{}\x1fs", branch_id, op.as_str())
    }

    /// 状态字段的后缀。`branch_id` 和 op 里都不会出现 `\x1f`，所以拿它区分
    /// 两类字段是安全的
    const SUFFIX: &'static str = "\x1fs";

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

    /// 分支的不变部分。**status 不在里面** —— 见 [`Self::bfield`]
    fn branch_value(b: &BranchRow) -> String {
        serde_json::json!({
            "url": b.url,
            "payload": b.payload,
        })
        .to_string()
    }

    /// `status` 由调用方从同一个 HGETALL 结果里按 [`Self::sfield`] 取出来传进来。
    /// 取不到按 `prepared` 算：老数据、或者写了定义还没写状态的中间态，
    /// 都该当成「还没跑」而不是「跑过了」
    fn branch_from(gid: &str, field: &str, val: &str, status: Option<&str>) -> Option<BranchRow> {
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
            status: status
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
                'next_cron_interval', ARGV[6], 'owner', ARGV[10], 'rollback_reason', '',
                'query_prepared', ARGV[7], 'create_time', ARGV[8], 'update_time', ARGV[8])
            -- 分支从第 11 个 ARGV 开始，每两个一组（field, value），
            -- 定义和状态各占一组。**一条 HSET 全写完** —— 原来是每个字段
            -- 一条 HSETNX，分支多的时候命令数线性涨。
            -- 上面已经确认过全局键不存在，所以不需要 NX 语义
            if #ARGV >= 11 then
                redis.call('HSET', KEYS[2], unpack(ARGV, 11))
            end
            if ARGV[9] == '1' then
                redis.call('ZADD', KEYS[3], ARGV[5], ARGV[1])
            end
            redis.call('ZADD', KEYS[4], ARGV[8], ARGV[1])
            -- ⚠ 裁剪**必须留在写路径上**。
            --
            -- 曾经为了省一条命令把它挪去 list_recent，结果是：没人打开管理台
            -- 就永远不裁剪。实测跑 5000 笔之后索引里就是 5000 个成员（上限本该
            -- 是 1000）—— 每笔事务留一个成员且永不回收，跑久了必然撑爆内存。
            -- 而它本身极便宜：索引已经在上限内时删 0 个成员，只是一次 O(log N)。
            -- 省 4% 的命令数换一个无界增长，不划算
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
            })
            // ⚠ owner 要真的写进去（原来写死空串）。提交方可以在建事务时
            // 就把租约占在自己手上，直接开推，省掉一次抢占往返 —— 见 `Api::submit`
            .arg(&g.owner);
        for b in branches {
            inv.arg(Self::bfield(&b.branch_id, b.op))
                .arg(Self::branch_value(b));
            // 状态字段缺失就按 prepared 读（见 branch_from），所以新建的分支
            // **根本不用写状态**。这也让 register_branch 的 HSETNX 保持幂等：
            // 重复登记不会把已经成功的分支状态抹回去
            if b.status != BranchStatus::Prepared {
                inv.arg(Self::sfield(&b.branch_id, b.op))
                    .arg(b.status.as_str());
            }
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
        // 一次 HGETALL 拿回两类字段：定义（`01\x1faction`）和状态
        // （`01\x1faction\x1fs`）。命令数没变，但改状态那边省掉了读+JSON
        let mut v: Vec<BranchRow> = map
            .iter()
            .filter(|(f, _)| !f.ends_with(Self::SUFFIX))
            .filter_map(|(f, val)| {
                Self::branch_from(
                    gid,
                    f,
                    val,
                    map.get(&format!("{f}{}", Self::SUFFIX)).map(|s| s.as_str()),
                )
            })
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
                -- 一条 HMGET 拿两个字段。索引自愈保留着（下面两个 ZREM），
                -- 只是把两次 HGET 合成一次
                local v = redis.call('HMGET', gkey, 'status', 'trans_type')
                local st, tt = v[1], v[2]
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
    /// 落全局状态。
    ///
    /// # 为什么要传 `trans_type`
    ///
    /// 它决定这笔事务落到新状态之后**还该不该被调度**
    /// （`schedulable`）。原来是在脚本里现读的，但它**建完就不再变**，
    /// 调用方手上一定有——传进来就能省掉那次读，更重要的是：
    /// 不可调度的那条路（也就是**落终态**，每笔成功事务的必经之路）
    /// 因此不再需要读任何字段，可以彻底不用 Lua。
    ///
    /// 实测 Redis 那颗核有 **66% 花在 evalsha 上**（12.1µs/次，是普通命令的
    /// 10 倍），而每笔事务 3 次 evalsha 里就有这一次。
    ///
    /// 收益：evalsha 3 → 2 次/笔，吞吐 **26858 → 30863 笔/秒（+15%）**
    /// （20 万笔，交替 A/B 各四轮取中位数）。
    ///
    /// ⚠ 这个数字第一次测成了 +31%，因为 A/B 做成了「先连跑新的、再连跑旧的」，
    /// 中间几分钟里有个忘删的 Dragonfly 容器在空转吃 CPU，噪声全算给了旧版。
    /// **跨版本对比必须交替跑**，而且先确认两个二进制的 md5 真的不同 ——
    /// 改动已提交时 `git stash` 是空操作，会让你拿同一个二进制自己跟自己比。
    pub async fn set_global_status(
        &self,
        gid: &str,
        status: GlobalStatus,
        trans_type: TransType,
        reason: &str,
    ) -> Result<()> {
        let t = crate::now();
        // 跟 SQL 后端一致：reason 是诊断信息，**截断而不是报错** ——
        // 让状态机的收尾因为一句话太长而失败，事务就永远推不到终态了
        let reason: String = reason.chars().take(MID).collect();

        // 快路径：新状态不可调度（终态，或非 msg 的 prepared）。
        // 这时候要做的事全都是**无条件写**：改字段、从调度索引摘掉、挂 TTL。
        // 一个字段都不用读 ⇒ 不需要 Lua，一次 MULTI 就够。
        //
        // ⚠ 仍然用 MULTI 而不是裸 pipeline：改字段和摘索引之间如果被别人看见
        // 中间态，会出现「已终结但还在索引里」，抢占方白抢一次。
        // 索引自愈能兜住（抢占时发现不可调度就摘掉），但那是补救不是保证
        if !schedulable(status, trans_type) {
            let mut c = self.conn.clone();
            let mut pipe = redis::pipe();
            pipe.atomic();
            let mut fields: Vec<(&str, String)> = vec![
                ("status", status.as_str().to_string()),
                ("update_time", t.to_string()),
            ];
            if status.is_final() {
                fields.push(("finish_time", t.to_string()));
            }
            if !reason.is_empty() {
                fields.push(("rollback_reason", reason.clone()));
            }
            pipe.hset_multiple(self.gkey(gid), &fields).ignore();
            pipe.zrem(self.ikey(), gid).ignore();
            // 终态挂 TTL：秒杀跑几千万笔之后，不回收内存会撑爆
            if status.is_final() && self.final_ttl > 0 {
                pipe.expire(self.gkey(gid), self.final_ttl).ignore();
                pipe.expire(self.bkey(gid), self.final_ttl).ignore();
            }
            let _: () = pipe.query_async(&mut c).await?;
            return Ok(());
        }

        let script = redis::Script::new(
            r#"
            -- 走到这里说明新状态是**可调度**的（submitted / aborting /
            -- msg 的 prepared），得把它按原来的到期时间放回索引，
            -- 所以还是要读一次 next_cron_time。
            -- trans_type 由调用方传进来了，不用再读（见函数头注释）
            local nct = redis.call('HGET', KEYS[1], 'next_cron_time')
            if not nct then return 0 end
            -- 要写的字段先攒起来，最后一条 HSET 落完 —— 原来状态、finish_time、
            -- rollback_reason 是分三条写的。
            -- ⚠ 这里是普通字符串字面量不是 format!，花括号**不要写成 {{}}**
            local f = {'status', ARGV[1], 'update_time', ARGV[2]}
            if ARGV[3] == '1' then
                f[#f + 1] = 'finish_time'
                f[#f + 1] = ARGV[2]
            end
            if ARGV[4] ~= '' then
                f[#f + 1] = 'rollback_reason'
                f[#f + 1] = ARGV[4]
            end
            redis.call('HSET', KEYS[1], unpack(f))
            redis.call('ZADD', KEYS[2], nct, ARGV[5])
            return 1
            "#,
        );

        let mut c = self.conn.clone();
        let _: i64 = script
            .key(self.gkey(gid))
            .key(self.ikey())
            .arg(status.as_str())
            .arg(t)
            .arg(if status.is_final() { "1" } else { "0" })
            .arg(reason)
            .arg(gid)
            .invoke_async(&mut c)
            .await?;
        Ok(())
    }

    /// 落分支状态和结果数据。
    ///
    /// 结果数据（`payload`）和状态**必须在同一次写里落盘**：分两步写的话
    /// 中间崩了会出现「标了成功但结果丢了」，workflow 重放时就拿不到返回值。
    /// 一条 `HSET` 同时写两个字段天然满足这一点，不用脚本。
    pub async fn set_branch_result(
        &self,
        gid: &str,
        branch_id: &str,
        op: BranchOp,
        status: BranchStatus,
        payload: &str,
    ) -> Result<()> {
        check_len("payload", payload, MID).map_err(|e| err(&e.to_string()))?;
        // ⚠ 必须走脚本。payload 在「定义」那一格里，改它是**读-改-写**，
        // 拆成 HGET + HSET 两次往返的话中间会有窗口。曾经这么写过 ——
        // 实际用它的只有 workflow（持租约，单写者），撞不上，但那是「碰巧安全」，
        // 不是结构上安全，不值得为一条不在热路上的语句冒这个险。
        //
        // 状态和 payload 在同一条 HSET 里落盘也是必须的：分两步写，中间崩了
        // 会出现「标了成功但结果丢了」，workflow 重放时就拿不到返回值。
        let script = redis::Script::new(
            r#"
            local cur = redis.call('HGET', KEYS[1], ARGV[1])
            if not cur then return 0 end
            local v = cjson.decode(cur)
            v['payload'] = ARGV[3]
            redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(v), ARGV[4], ARGV[2])
            return 1
            "#,
        );
        let mut c = self.conn.clone();
        let _: i64 = script
            .key(self.bkey(gid))
            .arg(Self::bfield(branch_id, op))
            .arg(status.as_str())
            .arg(payload)
            .arg(Self::sfield(branch_id, op))
            .invoke_async(&mut c)
            .await?;
        Ok(())
    }

    /// 只落状态 —— **推进热路上唯一会改分支的操作**。
    ///
    /// 状态拆成独立字段之后，这里就是一条 `HSET`：不读、不解析 JSON。
    /// 原来是 `HGET` + `cjson.decode` + `cjson.encode` + `HSET`，
    /// 两条命令加两次编解码，全压在 Redis 那唯一的线程上。
    pub async fn set_branch_status(
        &self,
        gid: &str,
        branch_id: &str,
        op: BranchOp,
        status: BranchStatus,
    ) -> Result<()> {
        let mut c = self.conn.clone();
        let _: () = c
            .hset(self.bkey(gid), Self::sfield(branch_id, op), status.as_str())
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
    /// 把停在 prepared 的事务推成 submitted 并排进调度队列，**一个脚本做完**。
    ///
    /// 原来是 `get_global`(HGETALL) + `set_global_status` + `schedule_now`
    /// 三次往返、11 条 Redis 命令；现在 3 条。Redis 是单线程 CPU 瓶颈，
    /// 命令数就是吞吐。
    ///
    /// `HMGET` 读不到就说明键不存在 —— **不用再单发一次 `EXISTS`**，
    /// 这个套路下面几个脚本里也都用上了。
    pub async fn submit_prepared(
        &self,
        gid: &str,
        owner: &str,
        next_cron_time: i64,
    ) -> Result<SubmitOutcome> {
        let t = crate::now();
        let script = redis::Script::new(&format!(
            r#"
            {sched}
            local v = redis.call('HMGET', KEYS[1], 'status', 'trans_type')
            if not v[1] then return {{'MISSING'}} end
            if v[1] ~= ARGV[3] then return {{'ALREADY'}} end
            redis.call('HSET', KEYS[1], 'status', ARGV[2], 'update_time', ARGV[1],
                       'next_cron_time', ARGV[5], 'next_cron_interval', 0,
                       'owner', ARGV[6])
            if schedulable(ARGV[2], v[2]) then
                redis.call('ZADD', KEYS[2], ARGV[5], ARGV[4])
            end
            -- 尾巴上带回事务体，调用方占了租约就能直接开推，不用再读一次。
            -- 首元素是标记，后面是 HGETALL 的 field/value 对
            local r = redis.call('HGETALL', KEYS[1])
            table.insert(r, 1, 'ADVANCED')
            return r
            "#,
            sched = LUA_SCHEDULABLE
        ));
        let mut c = self.conn.clone();
        let r: Vec<String> = script
            .key(self.gkey(gid))
            .key(self.ikey())
            .arg(t)
            .arg(GlobalStatus::Submitted.as_str())
            .arg(GlobalStatus::Prepared.as_str())
            .arg(gid)
            .arg(next_cron_time)
            .arg(owner)
            .invoke_async(&mut c)
            .await?;
        Ok(match r.first().map(String::as_str) {
            Some("ADVANCED") => {
                let map: std::collections::HashMap<String, String> = r[1..]
                    .chunks(2)
                    .filter(|c| c.len() == 2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();
                match Self::global_from(&map) {
                    Some(g) => SubmitOutcome::Advanced(Box::new(g)),
                    // 理论上不会发生（刚写完就读不出来），保守当成已提交
                    None => SubmitOutcome::Already,
                }
            }
            Some("MISSING") => SubmitOutcome::Missing,
            _ => SubmitOutcome::Already,
        })
    }

    pub async fn schedule_now(&self, gid: &str) -> Result<()> {
        let t = crate::now();
        let script = redis::Script::new(&format!(
            r#"
            {sched}
            -- 一条 HMGET 顶掉原来的 EXISTS + HGET(status) + HGET(trans_type)
            local v = redis.call('HMGET', KEYS[1], 'status', 'trans_type')
            local st, tt = v[1], v[2]
            if not st then return 0 end
            redis.call('HSET', KEYS[1], 'next_cron_time', ARGV[1], 'next_cron_interval', 0)
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
    ) -> Result<crate::RegisterOutcome> {
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
            // hset_nx 返回 1=写进去了 / 0=这个字段已经有值了。
            // ⚠ 这个返回值原先被丢掉了，跟 SQL 后端一样的坑：重号的第二个分支
            //   静默不写入，客户端却拿到 SUCCESS，那份资源永久泄漏。
            //   两个后端的判定语义必须逐条一致（见 CLAUDE.md）。
            let set: i64 = c
                .hset_nx(
                    self.bkey(gid),
                    Self::bfield(branch_id, *op),
                    Self::branch_value(&row),
                )
                .await?;
            if set == 0 {
                // 已经有了 —— 得看清楚里面躺的是不是同一个 URL。
                // 一致就是客户端重试（幂等，放行）；不一致就是两个分支编了同一个号
                let raw: Option<String> = c
                    .hget(self.bkey(gid), Self::bfield(branch_id, *op))
                    .await?;
                let existing = raw
                    .as_deref()
                    .and_then(|v| serde_json::from_str::<serde_json::Value>(v).ok())
                    .and_then(|v| v.get("url").and_then(|x| x.as_str()).map(String::from));
                if let Some(existing) = existing {
                    if existing != *url {
                        return Ok(crate::RegisterOutcome::Conflict { op: *op, existing });
                    }
                }
            }
        }
        Ok(crate::RegisterOutcome::Registered)
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
    // ---------------- 访问令牌 ----------------
    //
    // 语义必须跟 SQL 后端逐条一致：作废是**打标记不删**（管理台要能看到
    // 「什么时候被作废的」），列举按创建时间倒序。
    //
    // ⚠ 令牌 key **不设 TTL**。事务记录会过期是因为它们是流水；
    // 令牌是配置，过期消失等于凭据莫名失效。

    pub async fn create_token(&self, hash: &str, name: &str, secret: &str) -> Result<()> {
        let mut c = self.conn.clone();
        let mut pipe = redis::pipe();
        pipe.atomic()
            .hset_multiple(
                self.tkey(hash),
                &[
                    ("name", name.to_string()),
                    ("create_time", crate::now().to_string()),
                    ("last_used", "0".into()),
                    ("use_count", "0".into()),
                    ("last_ip", String::new()),
                    ("revoked", "0".into()),
                    ("secret", secret.to_string()),
                ],
            )
            .ignore()
            .sadd(self.tidx(), hash)
            .ignore();
        pipe.query_async::<()>(&mut c).await?;
        Ok(())
    }

    pub async fn list_tokens(&self) -> Result<Vec<TokenRow>> {
        let mut c = self.conn.clone();
        let hashes: Vec<String> = redis::cmd("SMEMBERS")
            .arg(self.tidx())
            .query_async(&mut c)
            .await?;
        let mut out = Vec::with_capacity(hashes.len());
        for h in hashes {
            let m: std::collections::HashMap<String, String> = redis::cmd("HGETALL")
                .arg(self.tkey(&h))
                .query_async(&mut c)
                .await?;
            if m.is_empty() {
                continue;
            }
            let g = |k: &str| m.get(k).cloned().unwrap_or_default();
            let n = |k: &str| g(k).parse::<i64>().unwrap_or(0);
            out.push(TokenRow {
                token_hash: h,
                name: g("name"),
                create_time: n("create_time"),
                last_used: n("last_used"),
                use_count: n("use_count"),
                last_ip: g("last_ip"),
                revoked: n("revoked"),
                secret: g("secret"),
            });
        }
        // 跟 SQL 后端一致：创建时间倒序
        out.sort_by(|a, b| b.create_time.cmp(&a.create_time));
        Ok(out)
    }

    pub async fn revoke_token(&self, hash: &str) -> Result<bool> {
        let mut c = self.conn.clone();
        // 只有当前是有效状态才算作废成功 —— 跟 SQL 的 `AND revoked=0` 对齐
        let script = redis::Script::new(
            r#"
            if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
            if redis.call('HGET', KEYS[1], 'revoked') ~= '0' then return 0 end
            redis.call('HSET', KEYS[1], 'revoked', ARGV[1])
            return 1
            "#,
        );
        let n: i64 = script
            .key(self.tkey(hash))
            .arg(crate::now())
            .invoke_async(&mut c)
            .await?;
        Ok(n == 1)
    }

    pub async fn active_token_hashes(&self) -> Result<Vec<String>> {
        Ok(self
            .list_tokens()
            .await?
            .into_iter()
            .filter(|t| t.revoked == 0)
            .map(|t| t.token_hash)
            .collect())
    }

    pub async fn touch_token(&self, hash: &str, ip: &str) -> Result<()> {
        let mut c = self.conn.clone();
        let mut pipe = redis::pipe();
        pipe.atomic()
            .hset(self.tkey(hash), "last_used", crate::now())
            .ignore()
            .hincr(self.tkey(hash), "use_count", 1)
            .ignore()
            .hset(self.tkey(hash), "last_ip", ip)
            .ignore();
        pipe.query_async::<()>(&mut c).await?;
        Ok(())
    }

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
