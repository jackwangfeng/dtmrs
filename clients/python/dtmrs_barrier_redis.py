"""业务数据在 Redis 里时的子事务屏障。

# 为什么不能用 dtmrs_barrier.Barrier

那个的原子性来自「屏障记录和业务 SQL 在同一个本地事务里提交」。秒杀那类
场景库存本身就在 Redis，没有 SQL 事务可以加入。

这里换一个原子性来源：**屏障判定和业务操作写在同一个 Lua 脚本里**，
Redis 执行脚本单线程且不可打断。

代价是**业务逻辑必须能用 Lua 表达**。扣库存、扣余额这类计数操作正好可以，
复杂业务请继续用 SQL 屏障。

# 用法

    import redis
    from dtmrs_barrier_redis import RedisBarrier, RedisOutcome

    r = redis.Redis()
    b = RedisBarrier(trans_type, gid, branch_id, op)
    out = b.check_adjust_amount(r, "stock:1001", -1)

    if out is RedisOutcome.FAILURE:
        return 失败            # 库存不足 → 让 TC 回滚
    return 成功                # 其余三种都是正常路径

**不依赖 redis-py**：只要求传进来的对象有 `.eval(script, numkeys, *keys_and_args)`
方法（redis-py 就是这个签名）。别的库自己包一层。

# 跟 SQL 版的两处行为差异（介质决定的，不是 bug）

1. **屏障键会过期**（默认 7 天）。SQL 版的屏障行永久保留，Redis 不挂 TTL
   内存会撑爆。⚠ TTL 必须**长于事务可能的最大生命周期**（含重试退避），
   否则正向的键先过期、补偿再来会以为「正向没跑过」而空转，副作用就漏补了。
2. **业务失败要由脚本自己表达**（``return 'FAILURE'``）。SQL 版里业务失败是
   调用方自己的事（不提交就行），这里业务跑在屏障的脚本内。
"""

from enum import Enum

#: 屏障键的默认存活时间（秒）
DEFAULT_BARRIER_TTL = 7 * 24 * 3600

_ORIGIN = {"compensate": "action", "cancel": "try", "rollback": "commit"}
_KNOWN = {"action", "compensate", "try", "confirm", "cancel", "commit", "rollback"}


class RedisOutcome(Enum):
    """Redis 屏障的判定结果。

    比 SQL 版的 Decision 多一个 FAILURE：业务逻辑跑在屏障的脚本里，
    它的拒绝只能从这里带出来。
    """

    #: 业务操作已经执行
    EXECUTED = "EXECUTED"
    #: 空回滚：正向分支从没执行过，业务操作没跑。接口应返回成功
    NULL_COMPENSATION = "NULL_COMPENSATION"
    #: 重复或悬挂：这次调用之前处理过，业务操作没跑。接口应返回成功
    DUPLICATED = "DUPLICATED"
    #: 业务逻辑自己拒绝了（比如库存不足）。屏障键仍然留下 ——
    #: 这次调用确实被处理过了，重试不该再跑一遍
    FAILURE = "FAILURE"


class RedisBarrier:
    """业务数据在 Redis 里时的分支屏障"""

    def __init__(self, trans_type, gid, branch_id, op,
                 prefix="dtmrs:bar:", ttl=DEFAULT_BARRIER_TTL):
        if not (trans_type and gid and branch_id and op):
            raise ValueError("trans_type / gid / branch_id / op 都不能为空")
        if op not in _KNOWN:
            raise ValueError(f"未知 op: {op}")
        self.trans_type = trans_type
        self.gid = gid
        self.branch_id = branch_id
        self.op = op
        self.prefix = prefix
        self.ttl = ttl
        self._counter = 0

    def _next_barrier_id(self):
        self._counter += 1
        return f"{self._counter:02d}"

    def _bkey(self, op, bid):
        return f"{self.prefix}{self.gid}-{self.branch_id}-{op}-{bid}"

    def call(self, rd, busi, keys, args):
        """带屏障保护地跑一段业务 Lua。

        `busi` 里可以用 ``KEYS[1..]`` / ``ARGV[1..]``，编号就是你传进来的顺序 ——
        屏障自己的键和参数追加在**后面**，不会打乱你的编号。

        业务想拒绝就 ``return 'FAILURE'``。
        """
        bid = self._next_barrier_id()
        nk, na = len(keys), len(args)
        # 屏障键接在用户的键后面，用户脚本里的编号才不会被打乱
        k_cur, k_org = nk + 1, nk + 2
        a_ttl, a_has_origin = na + 1, na + 2

        # ⚠ 这段的结构必须跟 SQL 版的 decide 一一对应，也必须跟 Rust / Go /
        # Node / Java 各版逐字一致 —— 五份实现的判定语义不能漂。
        #  1. 补偿类操作先「假装自己是正向分支」占位，占到了 = 正向没来过 = 空回滚
        #  2. 再以自己的身份占位，占不到 = 重复请求或悬挂
        # SET NX 占不到时返回 false，对应 SQL 那边的 rows_affected = 0
        script = f"""
local origin_got = false
if ARGV[{a_has_origin}] == '1' then
    origin_got = redis.call('SET', KEYS[{k_org}], 'origin', 'NX', 'EX', ARGV[{a_ttl}])
end
local cur_got = redis.call('SET', KEYS[{k_cur}], 'cur', 'NX', 'EX', ARGV[{a_ttl}])
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
"""
        origin = _ORIGIN.get(self.op)
        all_keys = list(keys) + [
            self._bkey(self.op, bid),
            # 非补偿操作用不到这个键，但 KEYS 个数要固定，塞个同前缀的占位
            self._bkey(origin or "none", bid),
        ]
        all_args = list(args) + [str(self.ttl), "1" if origin else "0"]

        v = rd.eval(script, len(all_keys), *all_keys, *all_args)
        if isinstance(v, bytes):
            v = v.decode()
        try:
            return RedisOutcome(v)
        except ValueError:
            return RedisOutcome.FAILURE

    def check_adjust_amount(self, rd, key, amount):
        """秒杀那个形状：检查余额够不够，够就调整。

        `amount` 传负数表示扣减。余额不足（会变成负数）或键不存在都返回
        FAILURE —— 调用方该把它翻译成「业务明确失败」让 TC 回滚，
        而不是当成未知去重试。
        """
        # 键不存在 → 失败（INCRBY 会把不存在的键当 0，不挡就凭空创建库存了）
        return self.call(rd, """
local v = redis.call('GET', KEYS[1])
if v == false or tonumber(v) + tonumber(ARGV[1]) < 0 then
    return 'FAILURE'
end
redis.call('INCRBY', KEYS[1], ARGV[1])
""", [key], [str(amount)])

    def query_prepared(self, rd):
        """二阶段消息的回查。

        规则：**没见过就地写一个 rollback 标记**，让后来的正向分支看到标记后
        放弃 —— 把「不知道」固化成「没提交」，避免两边各自猜出不同的结论。

        返回 FAILURE 表示「这单没提交，TC 该回滚」。
        """
        # msg 的正向分支固定是 01/action/01，回查看的就是它那个键
        key = f"{self.prefix}{self.gid}-01-action-01"
        v = rd.eval("""
local v = redis.call('GET', KEYS[1])
if v == false then
    redis.call('SET', KEYS[1], 'rollback', 'EX', ARGV[1])
    v = 'rollback'
end
if v == 'rollback' then return 'FAILURE' end
return 'EXECUTED'
""", 1, key, str(self.ttl))
        if isinstance(v, bytes):
            v = v.decode()
        return RedisOutcome.FAILURE if v == "FAILURE" else RedisOutcome.EXECUTED
