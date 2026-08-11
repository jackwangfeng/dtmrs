package dtmrs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 业务数据在 Redis 里时的子事务屏障。
 *
 * <h2>为什么不能用 Barrier</h2>
 *
 * {@link Barrier} 的原子性来自「屏障记录和业务 SQL 在同一个本地事务里提交」。
 * 秒杀那类场景库存本身就在 Redis，没有 SQL 事务可以加入。
 *
 * <p>这里换一个原子性来源：<b>屏障判定和业务操作写在同一个 Lua 脚本里</b>，
 * Redis 执行脚本单线程且不可打断。
 *
 * <p>代价是<b>业务逻辑必须能用 Lua 表达</b>。扣库存、扣余额这类计数操作正好可以，
 * 复杂业务请继续用 SQL 屏障。
 *
 * <h2>用法</h2>
 *
 * <pre>{@code
 * RedisBarrier b = new RedisBarrier(transType, gid, branchId, op);
 * RedisOutcome out = b.checkAdjustAmount(eval, "stock:1001", -1);
 * if (out == RedisOutcome.FAILURE) return 失败;   // 库存不足 → 让 TC 回滚
 * return 成功;                                     // 其余三种都是正常路径
 * }</pre>
 *
 * <h2>⚠ eval 要你自己给</h2>
 *
 * <b>不依赖任何 Redis 库</b>（Jedis / Lettuce 都行），实现 {@link Eval} 包一层：
 *
 * <pre>{@code
 * // Jedis
 * RedisBarrier.Eval eval = (script, keys, args) -> jedis.eval(script, keys, args);
 * }</pre>
 *
 * <h2>跟 SQL 版的两处行为差异（介质决定的，不是 bug）</h2>
 *
 * <ol>
 * <li><b>屏障键会过期</b>（默认 7 天）。SQL 版的屏障行永久保留，Redis 不挂 TTL
 *     内存会撑爆。⚠ TTL 必须<b>长于事务可能的最大生命周期</b>（含重试退避），
 *     否则正向的键先过期、补偿再来会以为「正向没跑过」而空转，副作用就漏补了。
 * <li><b>业务失败要由脚本自己表达</b>（{@code return 'FAILURE'}）。SQL 版里
 *     业务失败是调用方自己的事（不提交就行），这里业务跑在屏障的脚本内。
 * </ol>
 */
public class RedisBarrier {

    /** 屏障键的默认存活时间（秒） */
    public static final int DEFAULT_BARRIER_TTL = 7 * 24 * 3600;

    /**
     * Redis 屏障的判定结果。
     *
     * <p>比 SQL 版的 {@link Barrier.Decision} 多一个 FAILURE：业务逻辑跑在屏障
     * 的脚本里，它的拒绝只能从这里带出来。
     */
    public enum RedisOutcome {
        /** 业务操作已经执行 */
        EXECUTED,
        /** 空回滚：正向分支从没执行过，业务操作没跑。接口应返回成功 */
        NULL_COMPENSATION,
        /** 重复或悬挂：这次调用之前处理过，业务操作没跑。接口应返回成功 */
        DUPLICATED,
        /** 业务逻辑自己拒绝了（比如库存不足）。屏障键仍然留下 ——
         *  这次调用确实被处理过了，重试不该再跑一遍 */
        FAILURE
    }

    /** 这个类对 Redis 客户端的<b>全部</b>要求 */
    public interface Eval {
        Object eval(String script, List<String> keys, List<String> args) throws Exception;
    }

    private static final Map<String, String> ORIGIN =
            Map.of("compensate", "action", "cancel", "try", "rollback", "commit");
    private static final Set<String> KNOWN = new HashSet<>(Arrays.asList(
            "action", "compensate", "try", "confirm", "cancel", "commit", "rollback"));

    private final String transType;
    private final String gid;
    private final String branchId;
    private final String op;
    private String prefix = "dtmrs:bar:";
    private int ttl = DEFAULT_BARRIER_TTL;
    private int counter = 0;

    public RedisBarrier(String transType, String gid, String branchId, String op) {
        if (transType == null || transType.isEmpty() || gid == null || gid.isEmpty()
                || branchId == null || branchId.isEmpty() || op == null || op.isEmpty()) {
            throw new IllegalArgumentException("trans_type / gid / branch_id / op 都不能为空");
        }
        if (!KNOWN.contains(op)) {
            throw new IllegalArgumentException("未知 op: " + op);
        }
        this.transType = transType;
        this.gid = gid;
        this.branchId = branchId;
        this.op = op;
    }

    /** 改键前缀，多个环境共用一个 Redis 时用 */
    public RedisBarrier withPrefix(String p) {
        this.prefix = p;
        return this;
    }

    /**
     * 改屏障键的存活时间（秒）。
     *
     * <p>⚠ 必须长于事务可能的最大生命周期（含重试退避）。
     */
    public RedisBarrier withTtl(int secs) {
        this.ttl = secs;
        return this;
    }

    private String nextBarrierId() {
        counter++;
        return String.format("%02d", counter);
    }

    private String bkey(String op, String bid) {
        return prefix + gid + "-" + branchId + "-" + op + "-" + bid;
    }

    /**
     * 带屏障保护地跑一段业务 Lua。
     *
     * <p>busi 里可以用 KEYS[1..] / ARGV[1..]，编号就是你传进来的顺序 ——
     * 屏障自己的键和参数追加在<b>后面</b>，不会打乱你的编号。
     *
     * <p>业务想拒绝就 {@code return 'FAILURE'}。
     */
    public RedisOutcome call(Eval e, String busi, List<String> keys, List<String> args)
            throws Exception {
        String bid = nextBarrierId();
        int nk = keys.size();
        int na = args.size();
        // 屏障键接在用户的键后面，用户脚本里的编号才不会被打乱
        int kCur = nk + 1;
        int kOrg = nk + 2;
        int aTtl = na + 1;
        int aHasOrigin = na + 2;

        // ⚠ 这段的结构必须跟 SQL 版的 decide 一一对应，也必须跟 Rust / Go /
        // Python / Node 各版逐字一致 —— 五份实现的判定语义不能漂。
        //  1. 补偿类操作先「假装自己是正向分支」占位，占到了 = 正向没来过 = 空回滚
        //  2. 再以自己的身份占位，占不到 = 重复请求或悬挂
        // SET NX 占不到时返回 false，对应 SQL 那边的 rows_affected = 0
        String script = String.format(
                "%nlocal origin_got = false%n"
                + "if ARGV[%d] == '1' then%n"
                + "    origin_got = redis.call('SET', KEYS[%d], 'origin', 'NX', 'EX', ARGV[%d])%n"
                + "end%n"
                + "local cur_got = redis.call('SET', KEYS[%d], 'cur', 'NX', 'EX', ARGV[%d])%n"
                + "if ARGV[%d] == '1' and origin_got then%n"
                + "    return 'NULL_COMPENSATION'%n"
                + "end%n"
                + "if not cur_got then%n"
                + "    return 'DUPLICATED'%n"
                + "end%n"
                + "local function busi()%n%s%nend%n"
                + "local r = busi()%n"
                + "if r == 'FAILURE' then return 'FAILURE' end%n"
                + "return 'EXECUTED'%n",
                aHasOrigin, kOrg, aTtl, kCur, aTtl, aHasOrigin, busi);

        String origin = ORIGIN.get(op);
        List<String> allKeys = new ArrayList<>(keys);
        allKeys.add(bkey(op, bid));
        // 非补偿操作用不到这个键，但 KEYS 个数要固定，塞个同前缀的占位
        allKeys.add(bkey(origin == null ? "none" : origin, bid));
        List<String> allArgs = new ArrayList<>(args);
        allArgs.add(String.valueOf(ttl));
        allArgs.add(origin == null ? "0" : "1");

        Object v = e.eval(script, allKeys, allArgs);
        String s = v instanceof byte[] ? new String((byte[]) v) : String.valueOf(v);
        switch (s) {
            case "EXECUTED":
                return RedisOutcome.EXECUTED;
            case "NULL_COMPENSATION":
                return RedisOutcome.NULL_COMPENSATION;
            case "DUPLICATED":
                return RedisOutcome.DUPLICATED;
            default:
                // 认不出来的一律当失败 —— 宁可让 TC 回滚，也不能把未知当成功
                return RedisOutcome.FAILURE;
        }
    }

    /**
     * 秒杀那个形状：检查余额够不够，够就调整。
     *
     * <p>amount 传负数表示扣减。余额不足（会变成负数）或键不存在都返回 FAILURE ——
     * 调用方该把它翻译成「业务明确失败」让 TC 回滚，而不是当成未知去重试。
     */
    public RedisOutcome checkAdjustAmount(Eval e, String key, long amount) throws Exception {
        // 键不存在 → 失败（INCRBY 会把不存在的键当 0，不挡就凭空创建库存了）
        return call(e,
                "\nlocal v = redis.call('GET', KEYS[1])\n"
                + "if v == false or tonumber(v) + tonumber(ARGV[1]) < 0 then\n"
                + "    return 'FAILURE'\n"
                + "end\n"
                + "redis.call('INCRBY', KEYS[1], ARGV[1])\n",
                List.of(key), List.of(String.valueOf(amount)));
    }

    /**
     * 二阶段消息的回查。
     *
     * <p>规则：<b>没见过就地写一个 rollback 标记</b>，让后来的正向分支看到标记后
     * 放弃 —— 把「不知道」固化成「没提交」，避免两边各自猜出不同的结论。
     *
     * <p>返回 FAILURE 表示「这单没提交，TC 该回滚」。
     */
    public RedisOutcome queryPrepared(Eval e) throws Exception {
        // msg 的正向分支固定是 01/action/01，回查看的就是它那个键
        String key = prefix + gid + "-01-action-01";
        Object v = e.eval(
                "\nlocal v = redis.call('GET', KEYS[1])\n"
                + "if v == false then\n"
                + "    redis.call('SET', KEYS[1], 'rollback', 'EX', ARGV[1])\n"
                + "    v = 'rollback'\n"
                + "end\n"
                + "if v == 'rollback' then return 'FAILURE' end\n"
                + "return 'EXECUTED'\n",
                List.of(key), List.of(String.valueOf(ttl)));
        String s = v instanceof byte[] ? new String((byte[]) v) : String.valueOf(v);
        return "FAILURE".equals(s) ? RedisOutcome.FAILURE : RedisOutcome.EXECUTED;
    }
}
