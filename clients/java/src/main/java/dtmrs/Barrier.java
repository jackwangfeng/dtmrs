package dtmrs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * dtmrs 子事务屏障 —— 业务服务（RM）侧接入用。
 *
 * <p>分支接口<b>一定会</b>被重复调用（TC 重试 + 崩溃恢复），而且可能乱序到达。
 * 这个类用一张表 + 一条 {@code INSERT IGNORE} 同时解决三个问题：
 *
 * <table border="1">
 *   <caption>屏障解决的三个问题</caption>
 *   <tr><td><b>幂等</b></td><td>同一分支被调两次 → 只执行一次</td></tr>
 *   <tr><td><b>空回滚</b></td><td>正向分支没跑过就来了补偿 → 补偿空转</td></tr>
 *   <tr><td><b>悬挂</b></td><td>补偿先到、正向后到 → 丢弃迟到的正向</td></tr>
 * </table>
 *
 * <h2>用法</h2>
 *
 * <pre>{@code
 * // 启动时建表一次
 * Barrier.migrate(conn, Barrier.Dialect.MYSQL);
 *
 * // 每次处理分支请求（gid / branchId / op / transType 由 TC 传进来）
 * Barrier b = new Barrier(Barrier.Dialect.MYSQL, transType, gid, branchId, op);
 * conn.setAutoCommit(false);
 * try {
 *     if (b.decide(conn) == Barrier.Decision.EXECUTE) {
 *         // 你的业务 SQL —— 必须用这个 conn，跟屏障记录同一个事务
 *         try (PreparedStatement ps = conn.prepareStatement(
 *                 "UPDATE account SET balance = balance - ? WHERE id = ?")) {
 *             ps.setLong(1, amount); ps.setInt(2, uid); ps.executeUpdate();
 *         }
 *     }
 *     conn.commit();   // 原子性的来源：屏障记录与业务变更同生共死
 * } catch (Exception e) {
 *     conn.rollback();
 *     throw e;
 * }
 * }</pre>
 *
 * <h2>⚠ 两条不能违反的前提</h2>
 *
 * <ol>
 *   <li><b>屏障表必须和业务表在同一个数据库实例</b> —— 不同实例没法共用一个本地
 *       事务，这个方案直接失效。这不是实现限制，是它成立的根本条件。</li>
 *   <li><b>业务 SQL 必须用传给 {@code decide} 的那个 Connection</b>，且在同一个
 *       事务里提交。用另一个连接执行业务 SQL 等于白做。</li>
 * </ol>
 *
 * <h2>返回值语义</h2>
 *
 * {@code NULL_COMPENSATION} 和 {@code DUPLICATED} 都是<b>正常路径</b>，
 * 你的接口应该返回<b>成功</b>而不是失败 —— 返回失败会让 TC 以为分支出错了。
 *
 * <p>线程安全性：本类<b>不是</b>线程安全的，每次请求新建一个实例。
 */
public final class Barrier {

    /** 屏障给出的判定 */
    public enum Decision {
        /** 该干活。调用方在同一个事务里执行业务 SQL */
        EXECUTE,
        /** <b>空回滚</b>：正向分支从没执行过，补偿直接空转。接口应返回成功 */
        NULL_COMPENSATION,
        /** <b>重复或悬挂</b>：这次调用已处理过，跳过。接口应返回成功 */
        DUPLICATED
    }

    /**
     * SQL 方言。三家的「冲突就忽略」写法完全不同，而这个算法<b>整个依赖
     * 冲突时 affected rows 必须是 0</b>。
     */
    public enum Dialect {
        /** {@code INSERT IGNORE} —— MySQL 上<b>绝不能</b>用 ON DUPLICATE KEY UPDATE，见下 */
        MYSQL,
        POSTGRES,
        SQLITE
    }

    private final Dialect dialect;
    private final String transType;
    private final String gid;
    private final String branchId;
    private final String op;
    private final String table;
    private int counter = 0;

    public Barrier(Dialect dialect, String transType, String gid, String branchId, String op) {
        if (gid == null || gid.isEmpty()) throw new IllegalArgumentException("gid 不能为空");
        if (branchId == null || branchId.isEmpty()) throw new IllegalArgumentException("branchId 不能为空");
        if (originOp(op) == null && !isKnownOp(op)) {
            throw new IllegalArgumentException("未知 op: " + op);
        }
        this.dialect = dialect;
        this.transType = transType;
        this.gid = gid;
        this.branchId = branchId;
        this.op = op;
        this.table = "barrier";
    }

    private static boolean isKnownOp(String op) {
        switch (op) {
            case "action": case "compensate": case "try":
            case "confirm": case "cancel": case "commit": case "rollback":
                return true;
            default:
                return false;
        }
    }

    /**
     * 补偿类操作对应的<b>正向</b>操作。判空回滚全靠它。
     * 正向操作返回 null。
     */
    private static String originOp(String op) {
        switch (op) {
            case "compensate": return "action";
            case "rollback":   return "action";
            case "cancel":     return "try";
            default:           return null;
        }
    }

    /** 建屏障表。启动时调一次即可，重复调用无害。 */
    public static void migrate(Connection conn, Dialect dialect) throws SQLException {
        // MySQL 不能对 TEXT 建索引（1170 要 key length），必须定长
        String idText = dialect == Dialect.MYSQL ? "VARCHAR(128)" : "TEXT";
        String idShort = dialect == Dialect.MYSQL ? "VARCHAR(45)" : "TEXT";
        String sql =
            "CREATE TABLE IF NOT EXISTS barrier ("
          + "  trans_type  " + idShort + " NOT NULL,"
          + "  gid         " + idText + " NOT NULL,"
          + "  branch_id   " + idText + " NOT NULL,"
          + "  op          " + idShort + " NOT NULL,"
          + "  barrier_id  " + idShort + " NOT NULL,"
          + "  reason      " + idShort + " NOT NULL,"
          + "  create_time BIGINT NOT NULL,"
          + "  PRIMARY KEY (gid, branch_id, op, barrier_id)"   // ← 全部机关所在
          + ")";
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /**
     * 做出判定。<b>必须在业务事务里调用</b>，且业务 SQL 要用同一个 Connection。
     *
     * <p>算法：补偿方先用「正向分支」的名义插一行去占坑。
     * 占成功了说明正向从没来过（空回滚）；占失败了说明正向真跑过（是真补偿）。
     * 而这个坑一旦被占，迟到的正向分支就再也插不进来（悬挂被丢弃）。
     */
    public Decision decide(Connection conn) throws SQLException {
        String bid = String.format("%02d", ++counter);

        long originAffected = 0;
        String origin = originOp(op);
        if (origin != null) {
            originAffected = insert(conn, origin, bid);
        }
        long currentAffected = insert(conn, op, bid);

        if (origin != null && originAffected > 0) {
            // 正向分支从没跑过（否则那行早被它自己占了）→ 空回滚
            return Decision.NULL_COMPENSATION;
        }
        if (currentAffected == 0) {
            // 这个 (gid, branch, op, bid) 已经处理过了 → 重复请求或悬挂
            return Decision.DUPLICATED;
        }
        return Decision.EXECUTE;
    }

    private long insert(Connection conn, String opValue, String bid) throws SQLException {
        final String sql;
        switch (dialect) {
            case MYSQL:
                // ⚠ 这里**绝不能**用 ON DUPLICATE KEY UPDATE：
                // 它在重复时 affected rows 返回 1（不是 0），整个算法就废了。
                // INSERT IGNORE 重复时返回 0，跟另外两家一致。
                sql = "INSERT IGNORE INTO " + table
                    + " (trans_type,gid,branch_id,op,barrier_id,reason,create_time)"
                    + " VALUES (?,?,?,?,?,?,?)";
                break;
            default:
                sql = "INSERT INTO " + table
                    + " (trans_type,gid,branch_id,op,barrier_id,reason,create_time)"
                    + " VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING";
        }
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, transType);
            ps.setString(2, gid);
            ps.setString(3, branchId);
            ps.setString(4, opValue);
            ps.setString(5, bid);
            ps.setString(6, op);   // reason = 是哪个分支插的这行，排查用
            ps.setLong(7, System.currentTimeMillis() / 1000);
            return ps.executeUpdate();
        }
    }
}
