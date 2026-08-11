import dtmrs.Barrier;
import dtmrs.Barrier.Decision;
import dtmrs.Barrier.Dialect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 屏障的三个场景，对着真数据库跑。
 *
 * <pre>
 * DTMRS_TEST_PG='jdbc:postgresql://127.0.0.1:55434/dtmrs?user=postgres&password=dtmrs' \
 * DTMRS_TEST_MYSQL='jdbc:mysql://127.0.0.1:33306/dtmrs?user=root&password=dtmrs' \
 * ./run-test.sh
 * </pre>
 *
 * 没配环境变量就跳过 —— <b>跳过不等于通过</b>。
 */
public class BarrierTest {

    static int failed = 0;

    public static void main(String[] args) throws Exception {
        int ran = 0;
        for (String[] t : new String[][]{
                {"postgres", System.getenv("DTMRS_TEST_PG"), "POSTGRES"},
                {"mysql", System.getenv("DTMRS_TEST_MYSQL"), "MYSQL"}}) {
            if (t[1] == null || t[1].isEmpty()) {
                System.out.println("⚠ 跳过 " + t[0] + "：环境变量没配（跳过不等于通过）");
                continue;
            }
            ran++;
            System.out.println("\n===== " + t[0] + " =====");
            try (Connection c = DriverManager.getConnection(t[1])) {
                run(c, Dialect.valueOf(t[2]), t[0]);
            }
        }
        if (ran == 0) {
            System.out.println("\n⚠ 一个数据库都没配，什么都没验到");
            System.exit(1);
        }
        System.out.println(failed == 0 ? "\n✓ 全部通过" : "\n✗ " + failed + " 项失败");
        System.exit(failed == 0 ? 0 : 1);
    }

    static void check(boolean ok, String what) {
        System.out.println((ok ? "  ✓ " : "  ✗ ") + what);
        if (!ok) failed++;
    }

    static void run(Connection c, Dialect d, String name) throws Exception {
        Barrier.migrate(c, d);
        c.setAutoCommit(false);
        try (Statement st = c.createStatement()) {
            st.execute("DELETE FROM barrier");
            st.execute("DROP TABLE IF EXISTS acct_test");
            st.execute("CREATE TABLE acct_test (id INT PRIMARY KEY, bal BIGINT)");
            st.execute("INSERT INTO acct_test VALUES (1, 1000)");
        }
        c.commit();

        // ---- 场景 1：正常执行 ----
        String gid = "j-1";
        check(once(c, d, gid, "01", "action", -100) == Decision.EXECUTE, "首次调用要执行");
        check(bal(c) == 900, "余额扣掉了：" + bal(c));

        // ---- 场景 2：幂等 —— TC 重试同一个分支 ----
        check(once(c, d, gid, "01", "action", -100) == Decision.DUPLICATED, "重复调用要被识破");
        check(bal(c) == 900, "余额没有被扣第二次：" + bal(c));

        // ---- 场景 3：空回滚 —— 正向没跑过就来补偿 ----
        String gid2 = "j-2";
        check(once(c, d, gid2, "01", "compensate", +100) == Decision.NULL_COMPENSATION,
              "正向没跑过时，补偿必须空转");
        check(bal(c) == 900, "空回滚不该动余额：" + bal(c));

        // ---- 场景 4：悬挂 —— 补偿跑完了，迟到的正向才到 ----
        check(once(c, d, gid2, "01", "action", -100) == Decision.DUPLICATED,
              "补偿之后迟到的正向必须被丢弃（悬挂）");
        check(bal(c) == 900, "悬挂的正向不该扣款：" + bal(c));

        // ---- 场景 5：真补偿 —— 正向跑过，补偿要真执行 ----
        String gid3 = "j-3";
        check(once(c, d, gid3, "01", "action", -100) == Decision.EXECUTE, "正向执行");
        check(bal(c) == 800, "扣款后：" + bal(c));
        check(once(c, d, gid3, "01", "compensate", +100) == Decision.EXECUTE,
              "正向跑过之后，补偿要真的执行");
        check(bal(c) == 900, "补偿把钱退回来了：" + bal(c));
        check(once(c, d, gid3, "01", "compensate", +100) == Decision.DUPLICATED,
              "补偿自己也要幂等");
        check(bal(c) == 900, "补偿没有退第二次：" + bal(c));

        // ---- 场景 6：这条是 MySQL 专属的坑 ----
        // ON DUPLICATE KEY UPDATE 在重复时 affected rows 返回 1 而不是 0，
        // 用它的话上面场景 2 会误判成 EXECUTE → 重复扣款。这里直接验一下差别。
        if (d == Dialect.MYSQL) {
            try (Statement st = c.createStatement()) {
                st.execute("DELETE FROM barrier WHERE gid='dup-probe'");
            }
            c.commit();
            String ins = "INSERT IGNORE INTO barrier"
                    + " (trans_type,gid,branch_id,op,barrier_id,reason,create_time)"
                    + " VALUES ('saga','dup-probe','01','action','01','action',0)";
            int first, second;
            try (Statement st = c.createStatement()) {
                first = st.executeUpdate(ins);
                second = st.executeUpdate(ins);
            }
            c.commit();
            check(first == 1 && second == 0,
                  "MySQL 的 INSERT IGNORE 重复时必须返回 0（实测 " + first + " / " + second + "）");

            String bad = "INSERT INTO barrier"
                    + " (trans_type,gid,branch_id,op,barrier_id,reason,create_time)"
                    + " VALUES ('saga','dup-probe','01','action','01','action',0)"
                    + " ON DUPLICATE KEY UPDATE create_time=create_time";
            int badAffected;
            try (Statement st = c.createStatement()) {
                badAffected = st.executeUpdate(bad);
            }
            c.commit();
            check(badAffected != 0,
                  "对照：ON DUPLICATE KEY UPDATE 重复时返回 " + badAffected
                  + "（≠0，所以绝不能用它做幂等判断）");
        }
    }

    /** 走一次完整的「屏障判定 + 业务 SQL + 提交」，返回判定结果 */
    static Decision once(Connection c, Dialect d, String gid, String branch, String op, long delta)
            throws Exception {
        Barrier b = new Barrier(d, "saga", gid, branch, op);
        try {
            Decision dec = b.decide(c);
            if (dec == Decision.EXECUTE) {
                // 业务 SQL 必须用同一个 Connection、同一个事务
                try (PreparedStatement ps =
                             c.prepareStatement("UPDATE acct_test SET bal = bal + ? WHERE id = 1")) {
                    ps.setLong(1, delta);
                    ps.executeUpdate();
                }
            }
            c.commit();
            return dec;
        } catch (Exception e) {
            c.rollback();
            throw e;
        }
    }

    static long bal(Connection c) throws Exception {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT bal FROM acct_test WHERE id=1")) {
            rs.next();
            long v = rs.getLong(1);
            c.commit();
            return v;
        }
    }
}
