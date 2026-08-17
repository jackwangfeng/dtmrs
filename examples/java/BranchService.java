import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

/**
 * 三个分支微服务（库存 / 订单 / 账户），各自持有真实状态。
 * 用 JDK 自带的 HttpServer，**没有任何框架依赖** —— 换成 Spring Boot 的
 * {@code @RestController} 时，只有「怎么收请求」变了，里面的屏障逻辑一模一样。
 *
 * <pre>
 *   库存服务 :8901   stock=100     action −1
 *   订单服务 :8902   orders=0      action +1
 *   账户服务 :8903   balance=1000  action −10
 * </pre>
 *
 * <h2>为什么每个分支都要接子事务屏障</h2>
 *
 * 协调器回滚时会补偿<b>所有</b>分支，包括那些 action 可能根本没执行的 ——
 * 因为一个超时的分支<b>可能实际上成功了</b>，宁可多发不可漏发。
 * 没有屏障的话，「库存 +1」会在从没扣减过的账上凭空加库存。
 *
 * 屏障要挡住三件事，只做第一件是不够的：
 * <ul>
 *   <li><b>重复请求</b> —— 协调器会重试，同一个操作可能被调多次</li>
 *   <li><b>空回滚</b> —— 补偿到了，但正向操作根本没执行过</li>
 *   <li><b>悬挂</b> —— 补偿先执行了，迷路的正向操作后到，必须丢弃</li>
 * </ul>
 *
 * 这里为了让例子能独立跑，屏障用内存 Map 实现。<b>生产上必须用
 * {@code dtmrs.Barrier}（clients/java）把屏障记录写进你自己的业务库，
 * 和业务操作在同一个本地事务里提交</b> —— 分开就会出现「屏障记录写了
 * 但业务没做」的窗口，进程一崩就错乱。
 */
public class BranchService {

    record Cfg(String name, String field, int init, int delta) {}

    static final Map<Integer, Cfg> SERVICES = Map.of(
            8901, new Cfg("库存服务", "stock", 100, -1),
            8902, new Cfg("订单服务", "orders", 0, +1),
            8903, new Cfg("账户服务", "balance", 1000, -10));

    static final Map<Integer, State> ST = new HashMap<>();

    static class State {
        int value;
        final Set<String> barrier = new HashSet<>();   // gid|branch|op
        final List<String> calls = new ArrayList<>();
        final Map<String, String> mode = new HashMap<>();
        State(int v) { value = v; }
    }

    public static void main(String[] args) throws Exception {
        for (var e : new TreeMap<>(SERVICES).entrySet()) {
            int port = e.getKey();
            ST.put(port, new State(e.getValue().init()));
            HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 128);
            s.createContext("/", ex -> handle(port, ex));
            s.setExecutor(Executors.newFixedThreadPool(8));
            s.start();
            System.out.printf("  %-8s :%d  %s=%d  action:%+d%n",
                    e.getValue().name(), port, e.getValue().field(), e.getValue().init(), e.getValue().delta());
        }
        System.out.println("-".repeat(52));
        Thread.currentThread().join();
    }

    static void handle(int port, HttpExchange ex) {
        try {
            String path = ex.getRequestURI().getPath();
            Map<String, String> q = query(ex.getRequestURI().getRawQuery());
            Cfg cfg = SERVICES.get(port);
            State st = ST.get(port);
            ex.getRequestBody().readAllBytes();

            synchronized (st) {
                switch (path) {
                    case "/state" -> {
                        reply(ex, 200, String.format(
                                "{\"service\":\"%s\",\"%s\":%d,\"calls\":%s}",
                                cfg.name(), cfg.field(), st.value, jsonList(st.calls)));
                        return;
                    }
                    case "/reset" -> {
                        st.value = cfg.init(); st.barrier.clear(); st.calls.clear(); st.mode.clear();
                        reply(ex, 200, "ok"); return;
                    }
                    case "/mode" -> { st.mode.clear(); st.mode.putAll(q); reply(ex, 200, "ok"); return; }
                    default -> { }
                }
            }

            String gid = q.getOrDefault("gid", "-");
            String br  = q.getOrDefault("branch_id", "-");
            boolean isCompensate = path.startsWith("/compensate");
            String op = isCompensate ? "compensate" : "action";

            // 故意慢：制造「结果未知」。协调器只会重试，**不会当成失败去回滚**
            String slow;
            synchronized (st) { slow = st.mode.get("slow"); }
            if (!isCompensate && slow != null) Thread.sleep(Integer.parseInt(slow) * 1000L);

            synchronized (st) {
                String key = gid + "|" + br + "|" + op;

                // ---- 子事务屏障 ----
                if (st.barrier.contains(key)) {                       // 重复请求
                    st.calls.add(op + ":重复,已幂等挡掉");
                    reply(ex, 200, "SUCCESS"); return;
                }
                if (isCompensate && !st.barrier.contains(gid + "|" + br + "|action")) {
                    // 空回滚：正向没跑过。占位是为了防悬挂 —— 万一正向请求还在路上
                    st.barrier.add(gid + "|" + br + "|action");
                    st.barrier.add(key);
                    st.calls.add("compensate:空回滚,未改状态");
                    reply(ex, 200, "SUCCESS"); return;
                }
                if (!isCompensate && st.barrier.contains(gid + "|" + br + "|compensate")) {
                    st.calls.add("action:悬挂,已丢弃");             // 补偿先到了
                    reply(ex, 200, "SUCCESS"); return;
                }

                // 业务**明确**拒绝 → 409，协调器会逆序补偿。
                // ⚠ 别拿它表示「超时/暂时不可用」—— 那是「结果未知」，
                //    要靠 5xx 或直接超时，让协调器重试而不是回滚
                if (!isCompensate && st.mode.containsKey("fail")) {
                    st.calls.add("action:业务拒绝");
                    reply(ex, 409, "FAILURE"); return;
                }

                st.barrier.add(key);
                st.value += isCompensate ? -cfg.delta() : cfg.delta();
                st.calls.add(op + ":执行 → " + cfg.field() + "=" + st.value);
                reply(ex, 200, "SUCCESS");
            }
        } catch (Exception e) {
            try { reply(ex, 500, "boom: " + e.getMessage()); } catch (Exception ignored) { }
        }
    }

    static Map<String, String> query(String raw) {
        Map<String, String> m = new HashMap<>();
        if (raw == null) return m;
        for (String kv : raw.split("&")) {
            int i = kv.indexOf('=');
            if (i > 0) m.put(kv.substring(0, i), kv.substring(i + 1));
        }
        return m;
    }

    static String jsonList(List<String> xs) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < xs.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(xs.get(i).replace("\"", "\\\"")).append('"');
        }
        return sb.append(']').toString();
    }

    static void reply(HttpExchange ex, int code, String body) throws java.io.IOException {
        byte[] raw = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("content-type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, raw.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(raw); }
    }
}
