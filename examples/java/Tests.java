import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 跨微服务事务的测试套件。可以直接进 CI —— 退出码反映成败。
 *
 * <h2>它跟「跑一遍看看」的区别</h2>
 * <ol>
 *   <li><b>断言最终状态数值</b>，不是接口返回码。「接口通了」和「数据对了」是两回事。</li>
 *   <li><b>轮询等终态，不用 sleep</b>。协调器是异步推进的，写死的 sleep 在你机器上够、
 *       在 CI 上就不够 —— 这是这类测试不稳定的头号来源。</li>
 *   <li><b>测的是失效路径</b>。happy path 只是第一个场景，后面五个才是真正会出事的地方。</li>
 * </ol>
 *
 * <h2>怎么跑</h2>
 * <pre>
 *   # 用已有的 TC（崩溃恢复那条会被跳过）
 *   DTMRS_URL=http://127.0.0.1:36789 DTMRS_AUTH_TOKEN=... java -cp out Tests
 *
 *   # 让测试自己拉 TC（**推荐**，这样崩溃恢复才测得了）
 *   DTMRS_BIN=/path/to/dtmrs java -cp out Tests
 * </pre>
 */
public class Tests {

    static final String[] SVC = {"http://127.0.0.1:8901", "http://127.0.0.1:8902", "http://127.0.0.1:8903"};
    static final HttpClient HTTP = HttpClient.newHttpClient();
    // ⚠ 两个端口都要显式给，**别用「HTTP 端口 +1」这种约定** ——
    //   26788+1 正好撞上另一个实例的 HTTP 端口，症状是「TC 起不来」但看不出原因。
    //   也都避开了临时端口范围（32768-60999），否则会偶发 Address already in use
    static final int TC_PORT = 26788;
    static final int TC_GRPC_PORT = 26795;
    static final AtomicInteger PASS = new AtomicInteger(), FAIL = new AtomicInteger(), SKIP = new AtomicInteger();

    static DtmrsClient tc;
    static Process tcProc;                     // 非空表示 TC 由我们管，能杀能重启
    static String dbPath;

    public static void main(String[] args) throws Exception {
        String bin = System.getenv("DTMRS_BIN");
        if (bin != null && !bin.isEmpty()) {
            dbPath = File.createTempFile("dtmrs-test", ".db").getAbsolutePath();
            new File(dbPath).delete();
            startTc(bin);
            tc = new DtmrsClient("http://127.0.0.1:" + TC_PORT, null);
            System.out.println("TC 由测试自己拉起（能测崩溃恢复）");
        } else {
            tc = DtmrsClient.fromEnv();
            System.out.println("用外部 TC —— 崩溃恢复那条会跳过");
        }
        System.out.println("=".repeat(58));

        t1_全部成功();
        t2_业务拒绝触发逆序补偿();
        t3_超时只重试不回滚();
        t4_重复提交幂等();
        t5_悬挂_补偿先到时晚到的正向必须被丢弃();
        t6_崩溃恢复();
        t7_tcc必须先登记再try();

        System.out.println("\n" + "=".repeat(58));
        System.out.printf("通过 %d  失败 %d  跳过 %d%n", PASS.get(), FAIL.get(), SKIP.get());
        if (SKIP.get() > 0) {
            // ⚠ 跳过不等于通过。不吼一声的话，CI 全绿会给人虚假的安全感
            System.out.println("⚠ 有场景被跳过 —— 那部分等于没测");
        }
        if (tcProc != null) tcProc.destroyForcibly();
        System.exit(FAIL.get() == 0 ? 0 : 1);
    }

    // ==================== 场景 ====================

    static void t1_全部成功() throws Exception {
        head("全部成功");
        reset();
        String gid = gid("ok");
        tc.submitSaga(gid, sagaSteps());
        eq("事务状态", "succeed", waitFinal(gid, 15000));
        state(99, 1, 990, "三个分支各执行一次，补偿一次都没被调用");
    }

    static void t2_业务拒绝触发逆序补偿() throws Exception {
        head("业务明确拒绝（409）→ 逆序补偿");
        reset();
        mode(1, "fail=1");
        String gid = gid("fail");
        tc.submitSaga(gid, sagaSteps());
        eq("事务状态", "failed", waitFinal(gid, 20000));
        state(100, 0, 1000, "全部回到初始值");
        // 账户服务的 action 根本没跑过却收到了 compensate —— 这是刻意的，
        // 多余的那次由屏障空转掉。没有屏障就会在从没扣过款的账上凭空加钱
        contains(2, "空回滚", "账户服务应该记录一次空回滚");
    }

    static void t3_超时只重试不回滚() throws Exception {
        head("第三个分支超时（结果未知）→ 只重试不回滚");
        reset();
        mode(2, "slow=12");
        String gid = gid("slow");
        tc.submitSaga(gid, sagaSteps());

        // ⚠ 这个场景**不能等终态** —— 它本来就不该终结。
        //    要断言的是「等了一段时间之后仍然停在 submitted」
        Thread.sleep(8000);
        eq("事务状态", "submitted", status(tc.query(gid)));
        state(99, 1, 1000, "前两步保留，且**没有被补偿**（对方可能已经成功了）");

        // 放行之后应该靠重试自己走完 —— 验证重试真的在工作
        mode(2, "");
        eq("恢复后最终状态", "succeed", waitFinal(gid, 40000));
        state(99, 1, 990, "重试成功，第三步补上");
    }

    static void t4_重复提交幂等() throws Exception {
        head("同一个 gid 提交三次");
        reset();
        String gid = gid("dup");
        for (int i = 0; i < 3; i++) tc.submitSaga(gid, sagaSteps());
        eq("事务状态", "succeed", waitFinal(gid, 15000));
        // 客户端重试时会重复提交，协调器必须当成功受理而不是报错；
        // 而分支侧由屏障保证只执行一次
        state(99, 1, 990, "状态只变一次");
    }

    static void t5_悬挂_补偿先到时晚到的正向必须被丢弃() throws Exception {
        head("悬挂：补偿先到，晚到的正向必须被丢弃");
        reset();
        mode(0, "slow=10");                  // 库存服务的 action 卡住 10 秒
        String gid = gid("hang");
        tc.submitSaga(gid, sagaSteps());
        Thread.sleep(1500);                  // 让 action 请求发出去并卡在那儿
        tc.abort(gid);                       // 这时候主动中止 → 补偿先到达
        eq("事务状态", "failed", waitFinal(gid, 30000));
        Thread.sleep(11000);                 // 等那个迟到的 action 醒过来

        // 库存必须还是 100：晚到的 action 看到补偿已占位，必须放弃
        state(100, 0, 1000, "晚到的正向被丢弃，库存没有被凭空扣掉");
        // ⚠ 这里断言的是「重复」不是「悬挂」—— 第一版写错过。
        //   空回滚时替正向占了位，所以迟到的正向命中的是「已处理过」这条路径。
        //   状态正确（库存还是 100）才是重点，日志措辞是次要的
        contains(0, "重复", "迟到的正向应该被屏障挡掉");
        mode(0, "");
    }

    static void t6_崩溃恢复() throws Exception {
        head("崩溃恢复：推进途中杀掉 TC");
        if (tcProc == null) {
            System.out.println("    ⏭ 跳过 —— 没设 DTMRS_BIN，测试管不了 TC 进程");
            System.out.println("       （跳过不等于通过：崩溃恢复这条等于没测）");
            SKIP.incrementAndGet();
            return;
        }
        reset();
        mode(2, "slow=6");                   // 让第三步卡住，制造「推到一半」
        String gid = gid("crash");
        tc.submitSaga(gid, sagaSteps());
        Thread.sleep(2500);
        state(99, 1, 1000, "前两步已执行，第三步进行中");

        System.out.println("    → kill -9 TC");
        tcProc.destroyForcibly();
        tcProc.waitFor();
        mode(2, "");                         // 放行第三步
        Thread.sleep(1000);

        System.out.println("    → 重启 TC，等它把这笔捞起来");
        startTc(System.getenv("DTMRS_BIN"));
        // 租约起了 5 秒（见 startTc），所以不用等默认的 30 秒
        eq("重启后最终状态", "succeed", waitFinal(gid, 60000));
        state(99, 1, 990, "事务被重新捞起并推完，分支没有被重复执行");
    }

    static void t7_tcc必须先登记再try() throws Exception {
        head("TCC：先 registerBranch 再 try");
        reset();
        String gid = gid("tcc");
        tc.prepareTcc(gid);
        for (int i = 0; i < 3; i++) {
            String id = String.format("%02d", i + 1);
            tc.registerTccBranch(gid, id, SVC[i] + "/action", SVC[i] + "/action", SVC[i] + "/compensate");
            post(SVC[i] + "/action?gid=" + gid + "&branch_id=" + id);
        }
        tc.submitTcc(gid);
        eq("事务状态", "succeed", waitFinal(gid, 15000));
        state(99, 1, 990, "try 执行一次，confirm 被屏障当重复请求挡掉");
    }

    // ==================== 断言与辅助 ====================

    static void head(String s) { System.out.println("\n── " + s); }

    static void eq(String what, String want, String got) {
        if (want.equals(got)) {
            System.out.printf("    ✅ %s = %s%n", what, got);
            PASS.incrementAndGet();
        } else {
            System.out.printf("    ❌ %s 期望 %s，实际 %s%n", what, want, got);
            FAIL.incrementAndGet();
        }
    }

    /** 断言三个服务的状态值 */
    static void state(int stock, int orders, int balance, String why) throws Exception {
        int[] want = {stock, orders, balance};
        String[] field = {"stock", "orders", "balance"};
        boolean ok = true;
        for (int i = 0; i < 3; i++) {
            int got = num(get(SVC[i] + "/state"), field[i]);
            if (got != want[i]) {
                System.out.printf("    ❌ %s 期望 %d，实际 %d%n", field[i], want[i], got);
                ok = false;
            }
        }
        if (ok) { System.out.println("    ✅ 最终状态正确 —— " + why); PASS.incrementAndGet(); }
        else {
            // 断言失败时把三个服务的调用流水全打出来 —— 光看数值对不上
            // 没法知道是哪笔事务动的手，尤其是跨测试的在途请求
            System.out.println("       ↓ 各服务实际收到的调用：");
            for (String s2 : SVC) System.out.println("         " + get(s2 + "/state"));
            FAIL.incrementAndGet();
        }
    }

    /** 断言某个服务的调用流水里出现过某个关键词 */
    static void contains(int idx, String kw, String why) throws Exception {
        if (get(SVC[idx] + "/state").contains(kw)) {
            System.out.println("    ✅ " + why);
            PASS.incrementAndGet();
        } else {
            System.out.println("    ❌ " + why + "（没找到「" + kw + "」）");
            FAIL.incrementAndGet();
        }
    }

    /**
     * 轮询到终态。**别用 sleep 代替它** —— 协调器异步推进，
     * 写死的等待时间在 CI 上必然不稳。
     */
    static String waitFinal(String gid, int maxMs) throws Exception {
        long end = System.currentTimeMillis() + maxMs;
        String st = "?";
        while (System.currentTimeMillis() < end) {
            try {
                st = status(tc.query(gid));
                if (st.equals("succeed") || st.equals("failed")) return st;
            } catch (Exception ignored) { /* TC 重启中，继续等 */ }
            Thread.sleep(300);
        }
        return st;
    }

    static String status(String json) {
        int i = json.indexOf("\"status\":\"");
        return i < 0 ? "?" : json.substring(i + 10, json.indexOf('"', i + 10));
    }

    static void startTc(String bin) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(bin);
        pb.environment().put("DTMRS_DB", "sqlite:" + dbPath);
        pb.environment().put("DTMRS_ADDR", "127.0.0.1:" + TC_PORT);
        pb.environment().put("DTMRS_GRPC_ADDR", "127.0.0.1:" + TC_GRPC_PORT);
        // 租约调短，崩溃恢复不用等默认的 30 秒
        pb.environment().put("DTMRS_LEASE", "5");
        pb.environment().put("DTMRS_BRANCH_TIMEOUT", "3");
        // ⚠ 别丢弃 stderr。第一版把它 DISCARD 了，结果 TC 因为端口冲突起不来时
        //   只能看到「TC 起不来」四个字，白白多花一轮才定位到原因
        File log = File.createTempFile("dtmrs-tc", ".log");
        pb.redirectOutput(log);
        pb.redirectErrorStream(true);
        tcProc = pb.start();
        for (int i = 0; i < 60; i++) {
            try { get("http://127.0.0.1:" + TC_PORT + "/health"); return; }
            catch (Exception e) { Thread.sleep(300); }
        }
        throw new IllegalStateException("TC 起不来，它的日志：\n"
                + java.nio.file.Files.readString(log.toPath()));
    }

    static List<DtmrsClient.Step> sagaSteps() {
        return List.of(
                new DtmrsClient.Step(SVC[0] + "/action", SVC[0] + "/compensate"),
                new DtmrsClient.Step(SVC[1] + "/action", SVC[1] + "/compensate"),
                new DtmrsClient.Step(SVC[2] + "/action", SVC[2] + "/compensate"));
    }

    /** gid 带上本次运行的标记，分支服务据此过滤外来请求（见 BranchService.TAG） */
    static final String RUN = System.getenv().getOrDefault("BRANCH_TAG", "run");
    static String gid(String tag) { return "t-" + tag + "-" + RUN + "-" + System.currentTimeMillis(); }
    static void reset() throws Exception { for (String s : SVC) { post(s + "/reset"); post(s + "/mode"); } }
    static void mode(int i, String q) throws Exception { post(SVC[i] + "/mode?" + q); }

    static int num(String json, String key) {
        int i = json.indexOf("\"" + key + "\":");
        if (i < 0) return Integer.MIN_VALUE;
        int j = i + key.length() + 3, k = j;
        while (k < json.length() && (Character.isDigit(json.charAt(k)) || json.charAt(k) == '-')) k++;
        return Integer.parseInt(json.substring(j, k));
    }

    static String get(String url) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString()).body();
    }

    static void post(String url) throws Exception {
        HTTP.send(HttpRequest.newBuilder(URI.create(url))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
    }
}
