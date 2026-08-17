import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * dtmrs 的 Java 客户端 —— **零依赖**，只用 JDK 自带的 java.net.http。
 *
 * <p>直接把这个文件拷进你的项目就能用，Spring Boot 里注册成 {@code @Bean} 即可
 * （见 README 的「接进 Spring Boot」）。没有做成 Maven 依赖是刻意的：
 * 它总共两百行，拷贝比引入一个依赖更省事，也不会跟你项目里的 HTTP 客户端打架。
 *
 * <h2>为什么不用 JSON 库</h2>
 * 报文结构很固定（就是几个字符串字段），手拼比引 Jackson/Gson 干净。
 * 你项目里已经有 JSON 库的话，把 {@code jsonEscape} 那块换掉就行。
 *
 * <h2>⚠ 两条容易写错的地方</h2>
 * <ol>
 *   <li><b>gid 必须由你生成并保证唯一</b>，而且**重试时要用同一个 gid** ——
 *       换了 gid 就是新事务，会重复执行。用业务主键派生最稳（比如
 *       {@code "order-" + orderId}），别用时间戳或随机数。</li>
 *   <li><b>TCC 必须先 registerBranch 再调 try</b>。反过来的话 try 成功了但
 *       协调器不知道有这个分支，那份预留资源永远没人释放。</li>
 * </ol>
 */
public class DtmrsClient {

    private final String base;
    private final String token;
    private final HttpClient http;

    /**
     * @param base  TC 地址，如 {@code http://127.0.0.1:36789}
     * @param token 访问令牌；TC 没开认证就传 {@code null} 或空串
     */
    public DtmrsClient(String base, String token) {
        this.base = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        this.token = token == null ? "" : token;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    // ==================== SAGA ====================

    /** SAGA 的一步：正向动作 + 它的补偿。 */
    public record Step(String action, String compensate) {}

    /**
     * 提交一笔 SAGA。**一次调用带上全部步骤**，所以只有一次网络往返。
     *
     * <p>返回即表示协调器已受理并持久化；分支的执行在后台进行。
     * <b>重复用同一个 gid 提交会成功而不是报错</b>（幂等），所以客户端超时重试是安全的。
     */
    public void submitSaga(String gid, List<Step> steps) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"gid\":").append(str(gid)).append(",\"steps\":[");
        for (int i = 0; i < steps.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append("{\"action\":").append(str(steps.get(i).action()))
              .append(",\"compensate\":").append(str(steps.get(i).compensate())).append('}');
        }
        sb.append("]}");
        post("/api/dtmsvr/submit", sb.toString());
    }

    // ==================== 二阶段消息 ====================

    /**
     * 二阶段消息：先登记，再做你自己的本地事务，最后确认。
     *
     * <p>典型用法：
     * <pre>{@code
     * client.prepareMsg(gid, actions, queryUrl);
     * try (Connection c = ds.getConnection()) {   // 你自己的本地事务
     *     ... 业务写库 ...
     * }
     * client.submitMsg(gid);
     * }</pre>
     *
     * <p><b>queryPrepared 不能省</b>：进程崩在 prepare 和 submit 之间时，
     * 协调器要回调它问「你那个本地事务到底提交了没有」。不给会被直接拒绝 ——
     * 因为猜「已提交」会重复发通知、猜「没提交」会丢单，两种猜法都是错的。
     */
    public void prepareMsg(String gid, List<String> actions, String queryPrepared) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"gid\":").append(str(gid)).append(",\"trans_type\":\"msg\",\"actions\":[");
        for (int i = 0; i < actions.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append(str(actions.get(i)));
        }
        sb.append("],\"query_prepared\":").append(str(queryPrepared)).append('}');
        post("/api/dtmsvr/prepare", sb.toString());
    }

    public void submitMsg(String gid) throws Exception {
        post("/api/dtmsvr/submit", "{\"gid\":" + str(gid) + ",\"trans_type\":\"msg\"}");
    }

    // ==================== TCC ====================

    /** 建一笔空的 TCC 事务，之后逐个登记分支。 */
    public void prepareTcc(String gid) throws Exception {
        post("/api/dtmsvr/prepare", "{\"gid\":" + str(gid) + ",\"trans_type\":\"tcc\"}");
    }

    /**
     * 登记一个 TCC 分支。
     *
     * <p><b>⚠ 必须在调 try 之前调这个</b>。顺序反了的话：try 执行成功、
     * 资源已冻结，但协调器不知道有这个分支 —— 既不会 confirm 也不会 cancel，
     * 那份资源永久泄漏。
     */
    public void registerTccBranch(String gid, String branchId,
                                  String tryUrl, String confirm, String cancel) throws Exception {
        post("/api/dtmsvr/registerBranch",
                "{\"gid\":" + str(gid)
                        + ",\"branch_id\":" + str(branchId)
                        + ",\"try\":" + str(tryUrl)
                        + ",\"confirm\":" + str(confirm)
                        + ",\"cancel\":" + str(cancel) + "}");
    }

    /** 全部分支 try 成功之后调这个，协调器开始 confirm。 */
    public void submitTcc(String gid) throws Exception {
        post("/api/dtmsvr/submit", "{\"gid\":" + str(gid) + ",\"trans_type\":\"tcc\"}");
    }

    // ==================== 其它 ====================

    /** 主动中止一笔未终结的事务，协调器会逆序补偿已执行的分支。 */
    public void abort(String gid) throws Exception {
        post("/api/dtmsvr/abort", "{\"gid\":" + str(gid) + "}");
    }

    /** 查一笔事务的状态和分支明细（原始 JSON）。 */
    public String query(String gid) throws Exception {
        return get("/api/dtmsvr/query?gid=" + URI.create("http://x/?" + gid).getRawQuery());
    }

    /** 让协调器生成一个 gid。**一般不用** —— 用业务主键派生更好，重试时天然一致。 */
    public String newGid() throws Exception {
        String body = get("/api/dtmsvr/newGid");
        int i = body.indexOf("\"gid\":\"");
        return i < 0 ? body : body.substring(i + 7, body.indexOf('"', i + 7));
    }

    // ==================== 内部 ====================

    private void post(String path, String json) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(10))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json));
        if (!token.isEmpty()) b.header("Authorization", "Bearer " + token);

        HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        // ⚠ 协调器的应答**靠 body 里的 dtm_result 表达结果**，光看状态码不够：
        //    合法但当前状态做不了的请求会返回 200 + FAILURE
        if (r.statusCode() / 100 != 2 || r.body().contains("FAILURE")) {
            throw new IllegalStateException("dtmrs 拒绝了请求 [" + r.statusCode() + "] " + r.body());
        }
    }

    private String get(String path) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(10)).GET();
        if (!token.isEmpty()) b.header("Authorization", "Bearer " + token);
        HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() / 100 != 2) {
            throw new IllegalStateException("dtmrs 返回 " + r.statusCode() + ": " + r.body());
        }
        return r.body();
    }

    /** 最小 JSON 字符串转义。项目里有 JSON 库的话换掉即可 */
    private static String str(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default   -> { if (c < 0x20) sb.append(String.format("\\u%04x", (int) c)); else sb.append(c); }
            }
        }
        return sb.append('"').toString();
    }

    /** 便捷构造：从环境变量读地址和令牌 */
    public static DtmrsClient fromEnv() {
        String base = System.getenv().getOrDefault("DTMRS_URL", "http://127.0.0.1:36789");
        return new DtmrsClient(base, System.getenv("DTMRS_AUTH_TOKEN"));
    }

    static List<Step> steps(String... urls) {
        List<Step> out = new ArrayList<>();
        for (int i = 0; i + 1 < urls.length; i += 2) out.add(new Step(urls[i], urls[i + 1]));
        return out;
    }
}
