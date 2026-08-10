/**
 * JVM 进程里嵌一个 Rust 事务协调器 —— 不部署任何服务。
 *
 * 跑之前：
 *   cargo build -p dtmrs-ffi --release
 *   cd bindings/java && ./run.sh
 *
 * 三个场景，账户余额是真的在动：
 *   ① 正常转账
 *   ② 风控拒绝 → 逆序补偿，钱退回来
 *   ③ 下游超时 → 只重试，不回滚
 */
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class Example {

    // 业务「库」：为了不引 JDBC 依赖，用一个内存表当账本。
    // 真实项目请用数据库，并且**把业务 SQL 和子事务屏障放进同一个本地事务**。
    // 注意用 ConcurrentHashMap —— handler 会被 Rust 的任意线程调用
    static final Map<Integer, Integer> ACCOUNT = new ConcurrentHashMap<>();
    static final List<String> SEEN = Collections.synchronizedList(new ArrayList<>());

    static void move(int from, int to, int amt) {
        ACCOUNT.merge(from, -amt, Integer::sum);
        ACCOUNT.merge(to, amt, Integer::sum);
    }

    static String balances() {
        return new TreeMap<>(ACCOUNT).toString();
    }

    static void flush() {
        synchronized (SEEN) {
            for (String s : SEEN) System.out.println("  " + s);
            SEEN.clear();
        }
    }

    public static void main(String[] args) throws Exception {
        Path db = Paths.get(System.getProperty("java.io.tmpdir"), "dtmrs_java_demo.db");
        Files.deleteIfExists(db);
        ACCOUNT.put(1, 1000);
        ACCOUNT.put(2, 0);

        try (Dtmrs tc = new Dtmrs("sqlite:" + db)) {
            tc.handler("转出", ctx -> {
                move(1, 2, 100);
                SEEN.add("[转出] gid=" + ctx.gid + " branch=" + ctx.branchId
                        + " op=" + ctx.op + " 线程=" + Thread.currentThread().getName());
                return Dtmrs.SUCCESS;
            });
            tc.handler("转出撤销", ctx -> {
                move(2, 1, 100);
                SEEN.add("[转出撤销] gid=" + ctx.gid + " branch=" + ctx.branchId + " op=" + ctx.op);
                return Dtmrs.SUCCESS;
            });
            tc.handler("风控拒绝", ctx -> {
                SEEN.add("[风控拒绝] gid=" + ctx.gid + " branch=" + ctx.branchId + " op=" + ctx.op);
                // 业务**明确**要求回滚 —— 只有这个返回值会触发逆序补偿
                return Dtmrs.FAILURE;
            });
            tc.handler("空补偿", ctx -> {
                SEEN.add("[空补偿] gid=" + ctx.gid + " branch=" + ctx.branchId + " op=" + ctx.op);
                return Dtmrs.SUCCESS;
            });
            tc.handler("下游超时", ctx -> {
                SEEN.add("[下游超时] gid=" + ctx.gid + " 结果未知");
                // 超时**不等于失败**：对方可能已经成功了，回滚会造成不一致
                return Dtmrs.UNKNOWN;
            });
            tc.handler("会抛异常的分支", ctx -> {
                throw new RuntimeException("业务代码炸了");
            });

            tc.start();
            System.out.println("初始余额: " + balances());

            System.out.println("\n① 正常转账");
            tc.submitSaga("java-1", Dtmrs.step("local://转出", "local://转出撤销"));
            String st = tc.waitFinal("java-1", 8000);
            flush();
            System.out.println("  结果: " + st + "  余额: " + balances());

            System.out.println("\n② 风控拒绝 → 逆序补偿，钱要退回来");
            tc.submitSaga("java-2",
                    Dtmrs.step("local://转出", "local://转出撤销"),
                    Dtmrs.step("local://风控拒绝", "local://空补偿"));
            st = tc.waitFinal("java-2", 8000);
            flush();
            System.out.println("  结果: " + st + "  余额: " + balances() + "  ← 转出被补偿抹平了");

            System.out.println("\n③ 下游超时 → 只重试，不回滚");
            tc.submitSaga("java-3", Dtmrs.step("local://下游超时", "local://空补偿"));
            Thread.sleep(600);
            System.out.println("  状态: " + tc.status("java-3") + "  ← 停在 submitted 等重试");
            flush();

            System.out.println("\n④ handler 抛异常 → 也是「结果未知」，绝不回滚");
            tc.submitSaga("java-4", Dtmrs.step("local://会抛异常的分支", "local://空补偿"));
            Thread.sleep(600);
            System.out.println("  状态: " + tc.status("java-4") + "  ← 异常不等于业务失败");
            flush();
        }
    }
}
