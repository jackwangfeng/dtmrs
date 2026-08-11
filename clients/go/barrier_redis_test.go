package dtmrs

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
)

// 跟 Rust / Python / Node / Java 各版**同名同义**的用例。
// 没配 DTMRS_TEST_REDIS_GO 就跳过 —— **跳过不等于通过**。
//
// 这里手写了一个最小 RESP 客户端，是为了**不给这个包引入 Redis 依赖** ——
// barrier_redis.go 只要求调用方给一个 Eval 函数，测试自然也不该多要什么。

type respConn struct {
	c net.Conn
	r *bufio.Reader
}

func dialResp(addr string) (*respConn, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &respConn{c: c, r: bufio.NewReader(c)}, nil
}

func (rc *respConn) cmd(args ...string) (any, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := rc.c.Write([]byte(b.String())); err != nil {
		return nil, err
	}
	return rc.read()
}

func (rc *respConn) read() (any, error) {
	line, err := rc.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return nil, fmt.Errorf("空回复")
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, fmt.Errorf("redis: %s", line[1:])
	case ':':
		return strconv.ParseInt(line[1:], 10, 64)
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, nil // nil bulk
		}
		buf := make([]byte, n+2)
		if _, err := readFull(rc.r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, _ := strconv.Atoi(line[1:])
		out := make([]any, 0, n)
		for i := 0; i < n; i++ {
			v, err := rc.read()
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	}
	return nil, fmt.Errorf("看不懂的回复: %s", line)
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	got := 0
	for got < len(buf) {
		n, err := r.Read(buf[got:])
		if err != nil {
			return got, err
		}
		got += n
	}
	return got, nil
}

func (rc *respConn) eval(script string, keys, args []string) (any, error) {
	a := []string{"EVAL", script, strconv.Itoa(len(keys))}
	a = append(a, keys...)
	a = append(a, args...)
	return rc.cmd(a...)
}

func redisFixture(t *testing.T) (*respConn, RedisEval) {
	t.Helper()
	addr := os.Getenv("DTMRS_TEST_REDIS_GO")
	if addr == "" {
		t.Skip("⚠ 跳过：DTMRS_TEST_REDIS_GO 没配（跳过不等于通过）")
	}
	rc, err := dialResp(addr)
	if err != nil {
		t.Fatalf("连不上 Redis: %v", err)
	}
	// 清掉屏障键和测试业务键
	for _, pat := range []string{"dtmrs:bar:*", "gt:stock:*"} {
		v, err := rc.cmd("KEYS", pat)
		if err != nil {
			t.Fatalf("KEYS: %v", err)
		}
		if arr, ok := v.([]any); ok {
			for _, k := range arr {
				if _, err := rc.cmd("DEL", fmt.Sprintf("%v", k)); err != nil {
					t.Fatalf("DEL: %v", err)
				}
			}
		}
	}
	return rc, RedisEvalFunc(rc.eval)
}

func (rc *respConn) stock(t *testing.T, key string) int {
	t.Helper()
	v, err := rc.cmd("GET", key)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if v == nil {
		return -1
	}
	n, _ := strconv.Atoi(fmt.Sprintf("%v", v))
	return n
}

func TestRedis重复调用同一分支只执行一次(t *testing.T) {
	rc, e := redisFixture(t)
	rc.cmd("SET", "gt:stock:1", "100")

	for _, want := range []RedisOutcome{RedisExecuted, RedisDuplicated} {
		// 每次新建 barrier —— 模拟 TC 重试时是两个独立请求
		b := NewRedisBarrier("saga", "gg-idem", "01", "action")
		got, err := b.CheckAdjustAmount(e, "gt:stock:1", -10)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("想要 %v，得到 %v", want, got)
		}
	}
	if s := rc.stock(t, "gt:stock:1"); s != 90 {
		t.Fatalf("只该扣一次，余额应为 90，实际 %d", s)
	}
}

func TestRedis正向没跑过时补偿要空转(t *testing.T) {
	rc, e := redisFixture(t)
	rc.cmd("SET", "gt:stock:2", "100")

	b := NewRedisBarrier("saga", "gg-null", "01", "compensate")
	got, err := b.CheckAdjustAmount(e, "gt:stock:2", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got != RedisNullCompensation {
		t.Fatalf("想要空回滚，得到 %v", got)
	}
	if s := rc.stock(t, "gt:stock:2"); s != 100 {
		t.Fatalf("空回滚不能动数据，实际 %d", s)
	}
}

func TestRedis补偿先到时晚到的正向必须被丢弃(t *testing.T) {
	rc, e := redisFixture(t)
	rc.cmd("SET", "gt:stock:3", "100")

	b := NewRedisBarrier("saga", "gg-hang", "01", "compensate")
	if got, _ := b.CheckAdjustAmount(e, "gt:stock:3", 10); got != RedisNullCompensation {
		t.Fatalf("补偿先到应空回滚，得到 %v", got)
	}
	// 迟到的正向必须被丢弃，否则扣了款没人补
	b2 := NewRedisBarrier("saga", "gg-hang", "01", "action")
	if got, _ := b2.CheckAdjustAmount(e, "gt:stock:3", -10); got != RedisDuplicated {
		t.Fatalf("悬挂的正向该被丢弃，得到 %v", got)
	}
	if s := rc.stock(t, "gt:stock:3"); s != 100 {
		t.Fatalf("悬挂的正向不能生效，实际 %d", s)
	}
}

func TestRedis库存不足要明确失败(t *testing.T) {
	rc, e := redisFixture(t)
	rc.cmd("SET", "gt:stock:4", "5")

	b := NewRedisBarrier("saga", "gg-low", "01", "action")
	got, err := b.CheckAdjustAmount(e, "gt:stock:4", -10)
	if err != nil {
		t.Fatal(err)
	}
	if got != RedisFailure {
		t.Fatalf("扣完变负数该明确失败，得到 %v", got)
	}
	if s := rc.stock(t, "gt:stock:4"); s != 5 {
		t.Fatalf("失败了不能动数据，实际 %d", s)
	}
}

func TestRedis键不存在不能凭空创建库存(t *testing.T) {
	rc, e := redisFixture(t)
	b := NewRedisBarrier("saga", "gg-nokey", "01", "action")
	got, _ := b.CheckAdjustAmount(e, "gt:stock:404", -1)
	if got != RedisFailure {
		t.Fatalf("键不存在该失败，得到 %v", got)
	}
	if s := rc.stock(t, "gt:stock:404"); s != -1 {
		t.Fatalf("INCRBY 会把不存在的键当 0，必须先挡住，实际 %d", s)
	}
}

func TestRedis回查没见过的单子要固化成回滚(t *testing.T) {
	rc, e := redisFixture(t)

	b := NewRedisBarrier("msg", "gg-query", "01", "action")
	if got, _ := b.QueryPrepared(e); got != RedisFailure {
		t.Fatalf("没见过的单子该答没提交，得到 %v", got)
	}
	// 回查完之后，晚到的正向必须被挡住：
	// 否则 TC 已经按「没提交」回滚了，业务这边却又执行了一次
	rc.cmd("SET", "gt:stock:6", "100")
	b2 := NewRedisBarrier("msg", "gg-query", "01", "action")
	if got, _ := b2.CheckAdjustAmount(e, "gt:stock:6", -10); got != RedisDuplicated {
		t.Fatalf("回查判成回滚后正向不能再执行，得到 %v", got)
	}
	if s := rc.stock(t, "gt:stock:6"); s != 100 {
		t.Fatalf("实际 %d", s)
	}
}
