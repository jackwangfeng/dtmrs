// 可运行的分支服务示例 —— 用 JDK 自带的 com.sun.net.httpserver，无框架依赖。
// 屏障那部分的逻辑跟你用 Spring Boot 时完全一样。
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dtmrs.Barrier;
import dtmrs.Barrier.Decision;
import dtmrs.Barrier.Dialect;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class Service {
    static String JDBC;

    public static void main(String[] args) throws Exception {
        JDBC = System.getenv("EX_MYSQL_JDBC");
        // 启动时建表一次
        try (Connection c = DriverManager.getConnection(JDBC)) {
            Barrier.migrate(c, Dialect.MYSQL);
        }

        int port = Integer.parseInt(System.getenv("EX_PORT"));
        HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        // 每条路由：正负号表示账户怎么动（0 = 不动账）
        s.createContext("/deduct", e -> branch(e, -1));
        s.createContext("/refund", e -> branch(e, +1));
        s.createContext("/ok",     e -> branch(e, 0));
        s.createContext("/noop",   e -> branch(e, 0));
        // 业务**明确**拒绝 → 409，TC 会逆序补偿
        s.createContext("/reject", e -> reply(e, 409, "FAILURE"));
        s.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(8));
        s.start();
    }

    static void branch(HttpExchange e, int sign) {
        Map<String, String> q = query(e);
        long amount = q.containsKey("amount") ? Long.parseLong(q.get("amount")) : 100;

        try (Connection c = DriverManager.getConnection(JDBC)) {
            c.setAutoCommit(false);
            try {
                Barrier b = new Barrier(Dialect.MYSQL,
                        q.get("trans_type"), q.get("gid"), q.get("branch_id"), q.get("op"));

                if (b.decide(c) == Decision.EXECUTE && sign != 0) {
                    try (PreparedStatement ps = c.prepareStatement(
                            "UPDATE ex_account SET balance = balance + ? "
                          + "WHERE id = 1 AND balance + ? >= 0")) {
                        ps.setLong(1, sign * amount);
                        ps.setLong(2, sign * amount);
                        if (ps.executeUpdate() == 0) {
                            c.rollback();
                            // 余额不足 = 业务明确拒绝 → 409
                            reply(e, 409, "FAILURE");
                            return;
                        }
                    }
                }
                // 空回滚 / 重复请求走到这里，什么都没做，同样返回成功
                c.commit();
                reply(e, 200, "SUCCESS");
            } catch (Exception ex) {
                c.rollback();
                throw ex;
            }
        } catch (Exception ex) {
            // 异常 = 结果**未知** → 5xx 让 TC 重试。绝不能返回 409
            System.err.println("branch error: " + ex.getMessage());
            reply(e, 500, "ONGOING");
        }
    }

    static Map<String, String> query(HttpExchange e) {
        Map<String, String> m = new HashMap<>();
        String raw = e.getRequestURI().getRawQuery();
        if (raw != null) for (String kv : raw.split("&")) {
            String[] p = kv.split("=", 2);
            if (p.length == 2) m.put(java.net.URLDecoder.decode(p[0], StandardCharsets.UTF_8),
                                     java.net.URLDecoder.decode(p[1], StandardCharsets.UTF_8));
        }
        return m;
    }

    static void reply(HttpExchange e, int code, String result) {
        try {
            byte[] body = ("{\"dtm_result\":\"" + result + "\"}").getBytes(StandardCharsets.UTF_8);
            e.getResponseHeaders().add("content-type", "application/json");
            e.sendResponseHeaders(code, body.length);
            try (OutputStream o = e.getResponseBody()) { o.write(body); }
        } catch (Exception ignored) { }
    }
}
