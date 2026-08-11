import dtmrs.RedisBarrier;
import dtmrs.RedisBarrier.RedisOutcome;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Redis 屏障：跟 Rust / Go / Python / Node 各版**同名同义**的用例。
 *
 * 没配 DTMRS_TEST_REDIS_JAVA 就跳过 —— **跳过不等于通过**。
 *
 * 这里手写了一个最小 RESP 客户端，是为了**不给这个包引入 Jedis 依赖** ——
 * RedisBarrier 只要求调用方给一个 Eval，测试自然也不该多要什么。
 */
public class RedisBarrierTest {

    /** 够用就好的 RESP 客户端：只支持我们要发的那几条命令 */
    static class Resp implements AutoCloseable {
        private final Socket sock;
        private final OutputStream out;
        private final InputStream in;

        Resp(String addr) throws Exception {
            String[] p = addr.split(":");
            sock = new Socket(p[0], Integer.parseInt(p[1]));
            out = sock.getOutputStream();
            in = new BufferedInputStream(sock.getInputStream());
        }

        Object cmd(String... args) throws Exception {
            StringBuilder b = new StringBuilder("*").append(args.length).append("\r\n");
            for (String a : args) {
                b.append('$').append(a.getBytes(StandardCharsets.UTF_8).length)
                        .append("\r\n").append(a).append("\r\n");
            }
            out.write(b.toString().getBytes(StandardCharsets.UTF_8));
            out.flush();
            return read();
        }

        private String line() throws Exception {
            StringBuilder b = new StringBuilder();
            int c;
            while ((c = in.read()) != -1) {
                if (c == '\r') {
                    in.read(); // 吃掉 \n
                    return b.toString();
                }
                b.append((char) c);
            }
            throw new RuntimeException("连接断了");
        }

        Object read() throws Exception {
            String l = line();
            if (l.isEmpty()) throw new RuntimeException("空回复");
            char tag = l.charAt(0);
            String body = l.substring(1);
            switch (tag) {
                case '+': return body;
                case '-': throw new RuntimeException("redis: " + body);
                case ':': return Long.parseLong(body);
                case '$': {
                    int n = Integer.parseInt(body);
                    if (n < 0) return null;
                    byte[] buf = new byte[n];
                    int got = 0;
                    while (got < n) got += in.read(buf, got, n - got);
                    in.read(); in.read(); // \r\n
                    return new String(buf, StandardCharsets.UTF_8);
                }
                case '*': {
                    int n = Integer.parseInt(body);
                    List<Object> arr = new ArrayList<>();
                    for (int i = 0; i < n; i++) arr.add(read());
                    return arr;
                }
                default: throw new RuntimeException("看不懂的回复: " + l);
            }
        }

        /** RedisBarrier 要求的就这一个东西 */
        RedisBarrier.Eval evalFn() {
            return (script, keys, args) -> {
                List<String> a = new ArrayList<>();
                a.add("EVAL");
                a.add(script);
                a.add(String.valueOf(keys.size()));
                a.addAll(keys);
                a.addAll(args);
                return cmd(a.toArray(new String[0]));
            };
        }

        @Override public void close() throws Exception { sock.close(); }

        @SuppressWarnings("unchecked")
        void wipe() throws Exception {
            for (String pat : new String[]{"dtmrs:bar:*", "jt:stock:*"}) {
                Object v = cmd("KEYS", pat);
                if (v instanceof List) {
                    for (Object k : (List<Object>) v) cmd("DEL", String.valueOf(k));
                }
            }
        }

        long stock(String key) throws Exception {
            Object v = cmd("GET", key);
            return v == null ? -1 : Long.parseLong(String.valueOf(v));
        }
    }

    static int passed = 0;

    static void check(boolean ok, String msg) {
        if (!ok) throw new AssertionError(msg);
    }

    public static void main(String[] args) throws Exception {
        String addr = System.getenv("DTMRS_TEST_REDIS_JAVA");
        if (addr == null || addr.isEmpty()) {
            System.out.println("⚠ 跳过 Redis 屏障测试：DTMRS_TEST_REDIS_JAVA 没配（跳过不等于通过）");
            return;
        }

        run(addr, "重复调用同一分支只执行一次", r -> {
            r.cmd("SET", "jt:stock:1", "100");
            RedisOutcome[] want = {RedisOutcome.EXECUTED, RedisOutcome.DUPLICATED};
            for (RedisOutcome w : want) {
                // 每次新建 barrier —— 模拟 TC 重试时是两个独立请求
                RedisBarrier b = new RedisBarrier("saga", "jg-idem", "01", "action");
                check(b.checkAdjustAmount(r.evalFn(), "jt:stock:1", -10) == w, "判定应为 " + w);
            }
            check(r.stock("jt:stock:1") == 90, "只该扣一次");
        });

        run(addr, "正向没跑过时补偿要空转", r -> {
            r.cmd("SET", "jt:stock:2", "100");
            RedisBarrier b = new RedisBarrier("saga", "jg-null", "01", "compensate");
            check(b.checkAdjustAmount(r.evalFn(), "jt:stock:2", 10)
                    == RedisOutcome.NULL_COMPENSATION, "应为空回滚");
            check(r.stock("jt:stock:2") == 100, "空回滚不能动数据");
        });

        run(addr, "补偿先到时晚到的正向必须被丢弃", r -> {
            r.cmd("SET", "jt:stock:3", "100");
            RedisBarrier b = new RedisBarrier("saga", "jg-hang", "01", "compensate");
            check(b.checkAdjustAmount(r.evalFn(), "jt:stock:3", 10)
                    == RedisOutcome.NULL_COMPENSATION, "补偿先到应空回滚");
            // 迟到的正向必须被丢弃，否则扣了款没人补
            RedisBarrier b2 = new RedisBarrier("saga", "jg-hang", "01", "action");
            check(b2.checkAdjustAmount(r.evalFn(), "jt:stock:3", -10)
                    == RedisOutcome.DUPLICATED, "悬挂的正向该被丢弃");
            check(r.stock("jt:stock:3") == 100, "悬挂的正向不能生效");
        });

        run(addr, "库存不足要明确失败", r -> {
            r.cmd("SET", "jt:stock:4", "5");
            RedisBarrier b = new RedisBarrier("saga", "jg-low", "01", "action");
            check(b.checkAdjustAmount(r.evalFn(), "jt:stock:4", -10)
                    == RedisOutcome.FAILURE, "扣完变负数该明确失败");
            check(r.stock("jt:stock:4") == 5, "失败了不能动数据");
        });

        run(addr, "键不存在不能凭空创建库存", r -> {
            RedisBarrier b = new RedisBarrier("saga", "jg-nokey", "01", "action");
            check(b.checkAdjustAmount(r.evalFn(), "jt:stock:404", -1)
                    == RedisOutcome.FAILURE, "键不存在该失败");
            check(r.stock("jt:stock:404") == -1, "INCRBY 会把不存在的键当 0，必须先挡住");
        });

        run(addr, "业务lua可以自定义", r -> {
            r.cmd("SET", "jt:stock:5", "3");
            RedisBarrier b = new RedisBarrier("saga", "jg-cas", "01", "action");
            RedisOutcome got = b.call(r.evalFn(),
                    "\nif redis.call('GET', KEYS[1]) ~= ARGV[1] then return 'FAILURE' end\n"
                    + "redis.call('SET', KEYS[1], ARGV[2])\n",
                    List.of("jt:stock:5"), List.of("3", "7"));
            check(got == RedisOutcome.EXECUTED, "应执行，实际 " + got);
            check(r.stock("jt:stock:5") == 7, "值应被改成 7");
        });

        run(addr, "回查没见过的单子要固化成回滚", r -> {
            RedisBarrier b = new RedisBarrier("msg", "jg-query", "01", "action");
            check(b.queryPrepared(r.evalFn()) == RedisOutcome.FAILURE, "没见过该答没提交");
            // 回查完之后，晚到的正向必须被挡住：
            // 否则 TC 已经按「没提交」回滚了，业务这边却又执行了一次
            r.cmd("SET", "jt:stock:6", "100");
            RedisBarrier b2 = new RedisBarrier("msg", "jg-query", "01", "action");
            check(b2.checkAdjustAmount(r.evalFn(), "jt:stock:6", -10)
                    == RedisOutcome.DUPLICATED, "回查判成回滚后正向不能再执行");
            check(r.stock("jt:stock:6") == 100, "数据不能变");
        });

        System.out.println("Redis 屏障：" + passed + " 条全过");
    }

    interface Case { void run(Resp r) throws Exception; }

    static void run(String addr, String name, Case c) throws Exception {
        try (Resp r = new Resp(addr)) {
            r.wipe();
            c.run(r);
        }
        System.out.println("  ✓ " + name);
        passed++;
    }
}
