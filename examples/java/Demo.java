import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

/**
 * 三分支事务的可运行演示，四个场景：
 *
 * <ol>
 *   <li>SAGA 全部成功</li>
 *   <li>SAGA 第二个分支业务拒绝 → 逆序补偿（顺带展示空回滚）</li>
 *   <li>SAGA 第三个分支超时 → <b>只重试不回滚</b></li>
 *   <li>TCC：先 registerBranch 再 try</li>
 * </ol>
 *
 * 每个场景都**断言最终状态数值**，而不是只看接口返回码 ——
 * 「接口通了」和「数据对了」是两回事，后者才是分布式事务要保证的东西。
 *
 * 跑之前先起 {@link BranchService}，见 run.sh。
 */
public class Demo {

    static final String[] SVC = {"http://127.0.0.1:8901", "http://127.0.0.1:8902", "http://127.0.0.1:8903"};
    static final HttpClient HTTP = HttpClient.newHttpClient();
    static DtmrsClient tc;
    static int failed = 0;

    public static void main(String[] args) throws Exception {
        tc = DtmrsClient.fromEnv();

        scenarioAllOk();
        scenarioRollback();
        scenarioUnknown();
        scenarioTcc();

        System.out.println();
        if (failed == 0) {
            System.out.println("全部通过 ✅");
        } else {
            System.out.println(failed + " 个场景没通过 ❌");
            System.exit(1);
        }
    }

    // ---------------- 场景 ----------------

    static void scenarioAllOk() throws Exception {
        title("SAGA：三个分支全部成功");
        reset();
        String gid = "saga-ok-" + System.currentTimeMillis();
        tc.submitSaga(gid, sagaSteps());
        sleep(3000);
        show(gid);
        check(99, 1, 990, "都执行了，没有补偿");
    }

    static void scenarioRollback() throws Exception {
        title("SAGA：第二个分支业务拒绝 → 逆序补偿");
        reset();
        mode(1, "fail=1");                       // 订单服务下次 action 返回 409
        String gid = "saga-fail-" + System.currentTimeMillis();
        tc.submitSaga(gid, sagaSteps());
        sleep(4000);
        show(gid);
        System.out.println("    注意账户服务：action 根本没跑过却收到了 compensate。");
        System.out.println("    这是刻意的 —— 超时的分支可能实际成功了，宁可多发不可漏发，");
        System.out.println("    多余的那次由屏障空转掉。");
        check(100, 0, 1000, "全部回到初始值");
    }

    static void scenarioUnknown() throws Exception {
        title("SAGA：第三个分支超时（结果未知）→ 只重试不回滚");
        reset();
        mode(2, "slow=30");                      // 账户服务卡 30 秒
        String gid = "saga-slow-" + System.currentTimeMillis();
        tc.submitSaga(gid, sagaSteps());
        sleep(12000);
        show(gid);
        System.out.println("    事务应停在 submitted 而不是 failed —— 超时不等于失败。");
        System.out.println("    前两个分支已执行且**没有被补偿**，因为对方可能已经成功了。");
        check(99, 1, 1000, "前两步保留，第三步还在重试");
    }

    static void scenarioTcc() throws Exception {
        title("TCC：必须先 registerBranch 再调 try");
        reset();
        String gid = "tcc-" + System.currentTimeMillis();
        tc.prepareTcc(gid);
        for (int i = 0; i < 3; i++) {
            String id = String.format("%02d", i + 1);
            // ⚠ 顺序不能反：先登记，再 try。
            //    反过来的话 try 成功了但协调器不知道有这个分支，资源永久泄漏
            tc.registerTccBranch(gid, id,
                    SVC[i] + "/action", SVC[i] + "/action", SVC[i] + "/compensate");
            post(SVC[i] + "/action?gid=" + gid + "&branch_id=" + id);
        }
        tc.submitTcc(gid);
        sleep(3000);
        show(gid);
        System.out.println("    TCC 的 confirm 在这个例子里指向同一个 /action，");
        System.out.println("    所以屏障会把它当重复请求挡掉 —— 状态只变一次。");
        check(99, 1, 990, "try 执行一次，confirm 被屏障幂等挡掉");
    }

    // ---------------- 辅助 ----------------

    static List<DtmrsClient.Step> sagaSteps() {
        return List.of(
                new DtmrsClient.Step(SVC[0] + "/action", SVC[0] + "/compensate"),
                new DtmrsClient.Step(SVC[1] + "/action", SVC[1] + "/compensate"),
                new DtmrsClient.Step(SVC[2] + "/action", SVC[2] + "/compensate"));
    }

    static void title(String s) {
        System.out.println("\n════════ " + s + " ════════");
    }

    static void reset() throws Exception {
        for (String s : SVC) post(s + "/reset");
    }

    static void mode(int idx, String q) throws Exception {
        post(SVC[idx] + "/mode?" + q);
    }

    static void show(String gid) throws Exception {
        String q = tc.query(gid);
        int i = q.indexOf("\"status\":\"");
        System.out.println("    事务状态: " + (i < 0 ? "?" : q.substring(i + 10, q.indexOf('"', i + 10))));
        for (String s : SVC) System.out.println("    " + get(s + "/state"));
    }

    static void check(int stock, int orders, int balance, String why) throws Exception {
        int[] want = {stock, orders, balance};
        String[] field = {"stock", "orders", "balance"};
        boolean ok = true;
        for (int i = 0; i < 3; i++) {
            int got = num(get(SVC[i] + "/state"), field[i]);
            if (got != want[i]) {
                System.out.printf("    ❌ %s 期望 %d 实际 %d%n", field[i], want[i], got);
                ok = false;
            }
        }
        if (ok) System.out.println("    ✅ 最终状态正确（" + why + "）");
        else failed++;
    }

    static int num(String json, String key) {
        int i = json.indexOf("\"" + key + "\":");
        if (i < 0) return Integer.MIN_VALUE;
        int j = i + key.length() + 3;
        int k = j;
        while (k < json.length() && (Character.isDigit(json.charAt(k)) || json.charAt(k) == '-')) k++;
        return Integer.parseInt(json.substring(j, k));
    }

    static String get(String url) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString()).body();
    }

    static void post(String url) throws Exception {
        HTTP.send(HttpRequest.newBuilder(URI.create(url))
                        .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { }
    }
}
