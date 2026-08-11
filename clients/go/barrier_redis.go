package dtmrs

import (
	"fmt"
	"strconv"
)

// 业务数据在 Redis 里时的子事务屏障。
//
// # 为什么不能用 Barrier
//
// Barrier 的原子性来自「屏障记录和业务 SQL 在同一个本地事务里提交」。
// 秒杀那类场景库存本身就在 Redis，没有 SQL 事务可以加入。
//
// 这里换一个原子性来源：**屏障判定和业务操作写在同一个 Lua 脚本里**，
// Redis 执行脚本单线程且不可打断。
//
// 代价是**业务逻辑必须能用 Lua 表达**。扣库存、扣余额这类计数操作正好可以。
// 复杂业务请继续用 SQL 屏障。
//
// # 用法
//
//	b := dtmrs.NewRedisBarrier(transType, gid, branchID, op)
//	out, err := b.CheckAdjustAmount(eval, "stock:1001", -1)
//	switch out {
//	case dtmrs.RedisExecuted:         // 扣成功
//	case dtmrs.RedisFailure:          // 库存不足 → 让 TC 回滚（返回失败）
//	case dtmrs.RedisNullCompensation: // 空回滚，正常路径 → 返回成功
//	case dtmrs.RedisDuplicated:       // 重复/悬挂，正常路径 → 返回成功
//	}
//
// # 跟 SQL 版的两处行为差异（介质决定的，不是 bug）
//
//  1. **屏障键会过期**（默认 7 天）。SQL 版的屏障行永久保留，Redis 里不挂
//     TTL 内存会撑爆。⚠ TTL 必须**长于事务可能的最大生命周期**（含重试
//     退避），否则正向的键先过期、补偿再来会以为「正向没跑过」而空转，
//     副作用就漏补了。
//  2. **业务失败要由脚本自己表达**（`return 'FAILURE'`）。SQL 版里业务失败
//     是调用方自己的事（不提交就行），这里业务跑在屏障的脚本内。

// RedisBarrierTTL 屏障键的默认存活时间（秒）
const RedisBarrierTTL = 7 * 24 * 3600

// RedisOutcome 是 Redis 屏障的判定结果。
//
// 比 SQL 版的 Decision 多一个 RedisFailure：业务逻辑跑在屏障的脚本里，
// 它的拒绝只能从这里带出来。
type RedisOutcome int

const (
	// RedisExecuted 业务操作已经执行
	RedisExecuted RedisOutcome = iota
	// RedisNullCompensation 空回滚：正向分支从没执行过，业务操作没跑。接口应返回成功
	RedisNullCompensation
	// RedisDuplicated 重复或悬挂：这次调用之前处理过，业务操作没跑。接口应返回成功
	RedisDuplicated
	// RedisFailure 业务逻辑自己拒绝了（比如库存不足）。
	// 屏障键仍然留下 —— 这次调用确实被处理过了，重试不该再跑一遍
	RedisFailure
)

func (o RedisOutcome) String() string {
	switch o {
	case RedisExecuted:
		return "Executed"
	case RedisNullCompensation:
		return "NullCompensation"
	case RedisDuplicated:
		return "Duplicated"
	default:
		return "Failure"
	}
}

// RedisEval 是这个包对 Redis 客户端的**全部**要求。
//
// 故意不依赖任何具体的 Redis 库（go-redis / redigo 都行），你自己包一层：
//
//	// go-redis v9
//	eval := dtmrs.RedisEvalFunc(func(script string, keys, args []string) (any, error) {
//	    a := make([]any, len(args))
//	    for i, v := range args { a[i] = v }
//	    return rdb.Eval(ctx, script, keys, a...).Result()
//	})
type RedisEval interface {
	Eval(script string, keys []string, args []string) (any, error)
}

// RedisEvalFunc 让普通函数直接当 RedisEval 用
type RedisEvalFunc func(script string, keys, args []string) (any, error)

// Eval 实现 RedisEval
func (f RedisEvalFunc) Eval(script string, keys, args []string) (any, error) {
	return f(script, keys, args)
}

// RedisBarrier 业务数据在 Redis 里时的分支屏障
type RedisBarrier struct {
	TransType string
	Gid       string
	BranchID  string
	Op        string
	Prefix    string
	TTL       int
	counter   int
}

// NewRedisBarrier 从 TC 传来的分支信息构造
func NewRedisBarrier(transType, gid, branchID, op string) *RedisBarrier {
	return &RedisBarrier{
		TransType: transType, Gid: gid, BranchID: branchID, Op: op,
		Prefix: "dtmrs:bar:", TTL: RedisBarrierTTL,
	}
}

func (b *RedisBarrier) nextBarrierID() string {
	b.counter++
	return fmt.Sprintf("%02d", b.counter)
}

func (b *RedisBarrier) bkey(op, bid string) string {
	return fmt.Sprintf("%s%s-%s-%s-%s", b.Prefix, b.Gid, b.BranchID, op, bid)
}

// Call 带屏障保护地跑一段业务 Lua。
//
// busi 里可以用 KEYS[1..] 和 ARGV[1..]，编号就是你传进来的顺序 ——
// 屏障自己的键和参数追加在**后面**，不会打乱你的编号。
//
// 业务想拒绝（库存不足之类）就 return 'FAILURE'。
func (b *RedisBarrier) Call(e RedisEval, busi string, keys, args []string) (RedisOutcome, error) {
	if b.TransType == "" || b.Gid == "" || b.BranchID == "" || b.Op == "" {
		return RedisFailure, fmt.Errorf("事务信息不完整: %+v", b)
	}
	if !knownOp(b.Op) {
		return RedisFailure, fmt.Errorf("未知 op: %s", b.Op)
	}
	bid := b.nextBarrierID()
	nk, na := len(keys), len(args)
	// 屏障键接在用户的键后面，用户脚本里的编号才不会被打乱
	kCur, kOrg := nk+1, nk+2
	aTTL, aHasOrigin := na+1, na+2

	// ⚠ 这段的结构必须跟 SQL 版的 Decide 一一对应，也必须跟 Rust / Python /
	// Node / Java 各版逐字一致 —— 五份实现的判定语义不能漂。
	//  1. 补偿类操作先「假装自己是正向分支」占位，占到了 = 正向没来过 = 空回滚
	//  2. 再以自己的身份占位，占不到 = 重复请求或悬挂
	// SET NX 占不到时返回 false，对应 SQL 那边的 rows_affected = 0
	script := fmt.Sprintf(`
local origin_got = false
if ARGV[%d] == '1' then
    origin_got = redis.call('SET', KEYS[%d], 'origin', 'NX', 'EX', ARGV[%d])
end
local cur_got = redis.call('SET', KEYS[%d], 'cur', 'NX', 'EX', ARGV[%d])
if ARGV[%d] == '1' and origin_got then
    return 'NULL_COMPENSATION'
end
if not cur_got then
    return 'DUPLICATED'
end
local function busi()
%s
end
local r = busi()
if r == 'FAILURE' then return 'FAILURE' end
return 'EXECUTED'
`, aHasOrigin, kOrg, aTTL, kCur, aTTL, aHasOrigin, busi)

	origin := originOp(b.Op)
	if origin == "" {
		// 非补偿操作用不到这个键，但 KEYS 个数要固定，塞个同前缀的占位
		origin = "none"
	}
	allKeys := append(append([]string{}, keys...), b.bkey(b.Op, bid), b.bkey(origin, bid))
	hasOrigin := "0"
	if originOp(b.Op) != "" {
		hasOrigin = "1"
	}
	allArgs := append(append([]string{}, args...), strconv.Itoa(b.TTL), hasOrigin)

	v, err := e.Eval(script, allKeys, allArgs)
	if err != nil {
		return RedisFailure, err
	}
	switch fmt.Sprintf("%v", v) {
	case "EXECUTED":
		return RedisExecuted, nil
	case "NULL_COMPENSATION":
		return RedisNullCompensation, nil
	case "DUPLICATED":
		return RedisDuplicated, nil
	default:
		return RedisFailure, nil
	}
}

// CheckAdjustAmount 秒杀那个形状：检查余额够不够，够就调整。
//
// amount 传负数表示扣减。余额不足（会变成负数）或键不存在都返回 RedisFailure ——
// 调用方该把它翻译成「业务明确失败」让 TC 回滚，而不是当成未知去重试。
func (b *RedisBarrier) CheckAdjustAmount(e RedisEval, key string, amount int) (RedisOutcome, error) {
	// 键不存在 → 失败（INCRBY 会把不存在的键当 0，不挡就凭空创建库存了）
	return b.Call(e, `
local v = redis.call('GET', KEYS[1])
if v == false or tonumber(v) + tonumber(ARGV[1]) < 0 then
    return 'FAILURE'
end
redis.call('INCRBY', KEYS[1], ARGV[1])
`, []string{key}, []string{strconv.Itoa(amount)})
}

// QueryPrepared 二阶段消息的回查。
//
// 规则是：**没见过就地写一个 rollback 标记**，让后来的正向分支看到标记后放弃 ——
// 把「不知道」固化成「没提交」，避免两边各自猜出不同的结论。
//
// 返回 RedisFailure 表示「这单没提交，TC 该回滚」。
func (b *RedisBarrier) QueryPrepared(e RedisEval) (RedisOutcome, error) {
	// msg 的正向分支固定是 01/action/01，回查看的就是它那个键
	key := fmt.Sprintf("%s%s-01-action-01", b.Prefix, b.Gid)
	v, err := e.Eval(`
local v = redis.call('GET', KEYS[1])
if v == false then
    redis.call('SET', KEYS[1], 'rollback', 'EX', ARGV[1])
    v = 'rollback'
end
if v == 'rollback' then return 'FAILURE' end
return 'EXECUTED'
`, []string{key}, []string{strconv.Itoa(b.TTL)})
	if err != nil {
		return RedisFailure, err
	}
	if fmt.Sprintf("%v", v) == "FAILURE" {
		return RedisFailure, nil
	}
	return RedisExecuted, nil
}
