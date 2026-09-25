/**
 * dtmrs 的 JVM 绑定 —— 把 Rust 写的事务协调器嵌进 JVM 进程，不部署任何服务。
 *
 * <pre>
 * try (Dtmrs tc = new Dtmrs("sqlite:/tmp/app.db")) {
 *     tc.handler("转出", ctx -&gt; {
 *         jdbc.update("UPDATE account SET balance = balance - 100 WHERE id = 1");
 *         return Dtmrs.SUCCESS;
 *     });
 *     tc.start();
 *     tc.submitSaga("order-1", Dtmrs.step("local://转出", "local://转出撤销"));
 *
 *     // TCC：body 正常返回 → submit；抛异常（包括某个 try 没成功）→ abort
 *     tc.tccGlobal("order-2", t -&gt; {
 *         t.tryBranch("local://冻结确认", "local://冻结撤销", bid -&gt; freeze(bid));
 *     });
 *     // XA 同形：xaGlobal + prepareBranch
 *
 *     // 二阶段消息：本地事务成功就 submit，失败就 abort，不知道就交给回查
 *     tc.msgDoAndSubmit("order-3", List.of("local://加积分"), "local://查订单", () -&gt; writeOrder());
 * }
 * </pre>
 *
 * 跑之前先编：{@code cargo build -p dtmrs-ffi --release}
 *
 * <h2>为什么用 JNA 而不是 JNI / FFM</h2>
 *
 * <ul>
 *   <li><b>JNI</b>：得写 C 胶水层再编一个 .so，跨平台分发麻烦</li>
 *   <li><b>FFM</b>（{@code java.lang.foreign}）：零依赖，但 JDK 22+ 才转正，
 *       21 是预览、17 是 incubator。要求用户升到 22+ 太苛刻</li>
 *   <li><b>JNA</b>：Java 8+ 通吃，一个 jar 搞定，而且 —— 见下 —— 替我们
 *       解决了最麻烦的线程问题</li>
 * </ul>
 *
 * <h2>线程模型（这是 JVM 绑定唯一的坑）</h2>
 *
 * 分支回调是从 <b>Rust 的 tokio 线程</b>打进来的，不是 JVM 起的线程。
 * 裸 JNI 遇到这种情况必须自己 {@code AttachCurrentThread}，忘了就是直接崩。
 *
 * <b>JNA 替你做了这件事</b>：它的 CallbackThreadInitializer 会把外来线程
 * attach 到 JVM 再调你的 Java 代码。所以这里可以直接用回调式（push），
 * 不需要像 Node 那样绕成拉取式。
 *
 * 代价是 handler 会被<b>任意线程并发调用</b>，你的 handler 必须线程安全。
 *
 * <h2>返回值的语义（写错就会数据不一致）</h2>
 *
 * 只有 {@link #FAILURE} 会触发逆序补偿。超时、下游 5xx、自己抛异常
 * 一律是 {@link #UNKNOWN}（只重试不回滚）—— 因为超时的时候对方可能
 * 已经成功了，贸然补偿就是不一致。handler 抛异常本类会自动按 UNKNOWN 处理。
 */
import com.sun.jna.Callback;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Dtmrs implements AutoCloseable {

    /** 成功 */
    public static final int SUCCESS = 0;
    /** 业务<b>明确</b>要求回滚。只有这个会触发逆序补偿 */
    public static final int FAILURE = 1;
    /** 还在处理中，别当失败 */
    public static final int ONGOING = 2;
    /** 结果<b>未知</b>（超时 / 5xx / 抛异常）—— 只重试，不回滚 */
    public static final int UNKNOWN = 3;

    private static final int OK = 0;

    /** 分支上下文。业务侧用它做幂等（配合子事务屏障） */
    public static final class Ctx {
        public final String gid;
        public final String branchId;
        /** action | compensate | try | confirm | cancel | commit | rollback */
        public final String op;
        /** 这一步自己的业务数据（{@link #step(String, String, String)} 的第三项），没给就是空串 */
        public final String payload;

        Ctx(String gid, String branchId, String op, String payload) {
            this.gid = gid;
            this.branchId = branchId;
            this.op = op;
            this.payload = payload == null ? "" : payload;
        }

        @Override
        public String toString() {
            return "Ctx(gid=" + gid + " branch=" + branchId + " op=" + op + ")";
        }
    }

    /** 你的分支逻辑。返回 SUCCESS / FAILURE / ONGOING / UNKNOWN 之一 */
    public interface Handler {
        int call(Ctx ctx);
    }

    /** JNA 看到的 C 函数表 */
    interface Lib extends Library {
        Pointer dtmrs_open(String dbUrl);

        int dtmrs_register_ex(Pointer tc, String name, HandlerFn fn, Pointer ud);

        int dtmrs_start(Pointer tc);

        int dtmrs_submit_saga(Pointer tc, String gid, String stepsJson);

        int dtmrs_tcc_begin(Pointer tc, String gid);

        int dtmrs_tcc_register(Pointer tc, String gid, String branchId, String confirm, String cancel);

        int dtmrs_xa_begin(Pointer tc, String gid);

        int dtmrs_xa_register(Pointer tc, String gid, String branchId, String commit, String rollback);

        int dtmrs_msg_prepare(Pointer tc, String gid, String actionsJson, String queryPrepared, int graceSecs);

        int dtmrs_submit(Pointer tc, String gid);

        int dtmrs_abort(Pointer tc, String gid);

        int dtmrs_status(Pointer tc, String gid, byte[] out, long outLen);

        int dtmrs_wait_final(Pointer tc, String gid, int timeoutMs, byte[] out, long outLen);

        void dtmrs_close(Pointer tc);

        String dtmrs_last_error();
    }

    /** C 那边的函数指针类型（dtmrs_handler_ex_fn，带 payload 的那个） */
    interface HandlerFn extends Callback {
        int invoke(String gid, String branchId, String op, String payload, Pointer ud);
    }

    private final Lib lib;
    private final Pointer tc;
    private boolean started = false;
    private boolean closed = false;

    /**
     * JNA 的回调对象<b>必须被强引用住</b>。
     * 一旦被 GC 掉，Rust 那边还留着裸函数指针，下次回调就是野指针 —— 直接段错误。
     * 这是 JNA 用法里最容易踩的坑，所以这里显式存着。
     */
    private final Map<String, HandlerFn> keepAlive = new ConcurrentHashMap<>();
    private final Map<String, Handler> handlers = new ConcurrentHashMap<>();

    public Dtmrs(String dbUrl) {
        this(dbUrl, findLib());
    }

    public Dtmrs(String dbUrl, String libPath) {
        this.lib = Native.load(libPath, Lib.class);
        this.tc = lib.dtmrs_open(dbUrl);
        if (this.tc == null) {
            throw new IllegalStateException("打开失败: " + lib.dtmrs_last_error());
        }
    }

    static String findLib() {
        String env = System.getenv("DTMRS_LIB");
        if (env != null && !env.isEmpty()) return env;
        String os = System.getProperty("os.name").toLowerCase();
        String name = os.contains("mac") ? "libdtmrs.dylib"
                : os.contains("win") ? "dtmrs.dll" : "libdtmrs.so";
        // 从仓库里找：bindings/java → ../../target/{release,debug}
        String[] roots = {"../../target/release", "../../target/debug", "."};
        for (String r : roots) {
            Path p = Paths.get(r, name).toAbsolutePath().normalize();
            if (Files.exists(p)) return p.toString();
        }
        throw new IllegalStateException(
                "找不到 " + name + "。先编：cargo build -p dtmrs-ffi --release，"
                        + "或者用环境变量 DTMRS_LIB 指定路径。");
    }

    /**
     * 注册一个进程内分支。名字对应 saga 步骤里的 {@code local://名字}。
     * 必须在 {@link #start()} 之前调。
     *
     * <p>handler 会被<b>任意线程</b>调用，必须线程安全。抛异常按 UNKNOWN 处理。
     */
    public Dtmrs handler(String name, Handler h) {
        if (started) throw new IllegalStateException("已经 start 了，不能再注册 handler");
        handlers.put(name, h);
        HandlerFn fn = (gid, branchId, op, payload, ud) -> {
            try {
                Handler target = handlers.get(name);
                if (target == null) {
                    // 漏注册：**按未知处理，不是失败** —— 这是部署问题，
                    // 判失败会白白触发回滚
                    System.err.println("[dtmrs] 分支 " + name + " 没注册，按结果未知处理");
                    return UNKNOWN;
                }
                return target.call(new Ctx(gid, branchId, op, payload));
            } catch (Throwable t) {
                // **绝不能让异常穿回 Rust** —— 跨 FFI 边界抛异常是未定义行为。
                // 而且异常意味着不知道业务做没做，只能按未知处理
                System.err.println("[dtmrs] handler " + name + " 抛异常，按结果未知处理: " + t);
                return UNKNOWN;
            }
        };
        keepAlive.put(name, fn); // 别让 GC 回收它，否则下次回调是野指针
        if (lib.dtmrs_register_ex(tc, name, fn, null) != OK) {
            throw new IllegalStateException("注册失败: " + lib.dtmrs_last_error());
        }
        return this;
    }

    /** 启动推进器。上次进程留下的未终结事务会被自动接着推 */
    public Dtmrs start() {
        if (lib.dtmrs_start(tc) != OK) {
            throw new IllegalStateException("启动失败: " + lib.dtmrs_last_error());
        }
        started = true;
        return this;
    }

    /**
     * 造一个 SAGA 步骤。
     *
     * <p>有这个方法是因为直接写 {@code List.of(new String[]{a, b})} 会被 varargs
     * 当成 {@code List<String>} 而编译不过 —— 这是 Java 的经典陷阱，
     * 不该让每个用户都踩一遍。
     *
     * @param action     正向动作
     * @param compensate 对应补偿
     */
    public static String[] step(String action, String compensate) {
        return new String[]{action, compensate};
    }

    /**
     * 造一个带业务数据的 SAGA 步骤。{@code payload} 是这一步自己的
     * （正向和补偿共用），http 分支收到的是请求体，本地分支从 {@link Ctx#payload} 拿。
     */
    public static String[] step(String action, String compensate, String payload) {
        return new String[]{action, compensate, payload};
    }

    /**
     * 提交一个 SAGA（变长参数版，最常用）。
     *
     * <pre>
     * tc.submitSaga("order-1",
     *     Dtmrs.step("local://扣款", "local://扣款撤销"),
     *     Dtmrs.step("grpc://ship:9000/busi.Busi/Ship", "grpc://ship:9000/busi.Busi/Unship"));
     * </pre>
     *
     * <p>刻意做成变长参数而不是让调用方传 {@code List}：
     * {@code List.of(oneArray)} 会被 varargs 解析成 {@code List<String>} 而编译不过，
     * 这是 Java 的经典陷阱。
     */
    public void submitSaga(String gid, String[]... steps) {
        submitSaga(gid, Arrays.asList(steps));
    }

    /**
     * 提交一个 SAGA。
     *
     * @param gid   全局事务号。建议直接用业务单号 —— 那样天然幂等
     * @param steps 每步用 {@link #step} 构造。地址可以是
     *              {@code local://} 、{@code http://} 或 {@code grpc://}，能混用
     */
    public void submitSaga(String gid, List<String[]> steps) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < steps.size(); i++) {
            if (i > 0) sb.append(',');
            String[] st = steps.get(i);
            sb.append("{\"action\":").append(jsonStr(st[0]))
              .append(",\"compensate\":").append(jsonStr(st[1]));
            if (st.length > 2 && st[2] != null) {
                sb.append(",\"payload\":").append(jsonStr(st[2]));
            }
            sb.append('}');
        }
        sb.append(']');
        if (lib.dtmrs_submit_saga(tc, gid, sb.toString()) != OK) {
            throw new IllegalStateException("提交失败: " + lib.dtmrs_last_error());
        }
    }

    // ---------------- TCC / XA / 二阶段消息 ----------------

    /** 一阶段（try / XA prepare）逻辑。拿到分支号，返回 SUCCESS / FAILURE / ONGOING / UNKNOWN */
    public interface Phase1 {
        int call(String branchId) throws Exception;
    }

    /** 本地事务（二阶段消息用）。返回 SUCCESS / FAILURE / ONGOING / UNKNOWN */
    public interface LocalTx {
        int call() throws Exception;
    }

    /** tccGlobal / xaGlobal 的事务体 */
    public interface Body<T> {
        void run(T t) throws Exception;
    }

    /** 一阶段没返回 SUCCESS。在 tccGlobal / xaGlobal 里抛出会触发 abort */
    public static final class BranchFailed extends RuntimeException {
        public final String branchId;
        public final int code;

        BranchFailed(String branchId, int code) {
            super("分支 " + branchId + " 一阶段返回 " + code + "（不是 SUCCESS），整单回滚");
            this.branchId = branchId;
            this.code = code;
        }
    }

    private void check(int rc, String what) {
        if (rc != OK) throw new IllegalStateException(what + "失败: " + lib.dtmrs_last_error());
    }

    /** 跑宿主的一阶段。抛异常 = 不知道做没做 → UNKNOWN（TCC/XA 里照样回滚，cancel 兜得住） */
    private static int callPhase1(Phase1 fn, String bid) {
        try {
            return fn.call(bid);
        } catch (Throwable t) {
            System.err.println("[dtmrs] 分支 " + bid + " 的一阶段抛异常，按结果未知处理: " + t);
            return UNKNOWN;
        }
    }

    /** TCC 和 XA 共用：分支号自动编（01、02……），先登记、登记成功才跑一阶段 */
    public abstract class TwoPhase {
        public final String gid;
        private int next = 0;

        TwoPhase(String gid) {
            this.gid = gid;
        }

        abstract int doRegister(String bid, String fwd, String bwd);

        /** 只登记下一个分支，返回分支号。一阶段自己去做 —— <b>必须在这之后</b> */
        public synchronized String register(String fwd, String bwd) {
            String bid = String.format("%02d", next + 1);
            check(doRegister(bid, fwd, bwd), "登记分支");
            next++; // 登记成功才占号：失败了重试还用同一个号
            return bid;
        }

        String branch(String fwd, String bwd, Phase1 fn) {
            String bid = register(fwd, bwd);
            int code = callPhase1(fn, bid);
            if (code != SUCCESS) throw new BranchFailed(bid, code);
            return bid;
        }

        public void submit() {
            Dtmrs.this.submit(gid);
        }

        public void abort() {
            Dtmrs.this.abort(gid);
        }
    }

    /** 一个进行中的 TCC 事务，由 {@link #tcc} 开 */
    public final class Tcc extends TwoPhase {
        Tcc(String gid) {
            super(gid);
        }

        int doRegister(String bid, String c, String x) {
            return lib.dtmrs_tcc_register(tc, gid, bid, c, x);
        }

        /** 先登记、再跑 tryFn。try 没返回 SUCCESS（含抛异常）就抛 {@link BranchFailed} */
        public String tryBranch(String confirm, String cancel, Phase1 tryFn) {
            return branch(confirm, cancel, tryFn);
        }
    }

    /** 一个进行中的 XA 事务，由 {@link #xa} 开 */
    public final class Xa extends TwoPhase {
        Xa(String gid) {
            super(gid);
        }

        int doRegister(String bid, String c, String r) {
            return lib.dtmrs_xa_register(tc, gid, bid, c, r);
        }

        /** 先登记、再跑 prepareFn（业务 SQL + PREPARE）。没 SUCCESS 就抛 {@link BranchFailed} */
        public String prepareBranch(String commit, String rollback, Phase1 prepareFn) {
            return branch(commit, rollback, prepareFn);
        }
    }

    /** 开一个 TCC 事务（幂等）。多数时候用 {@link #tccGlobal} 更省心 */
    public Tcc tcc(String gid) {
        check(lib.dtmrs_tcc_begin(tc, gid), "开 TCC 事务");
        return new Tcc(gid);
    }

    /** 开一个 XA 事务（幂等） */
    public Xa xa(String gid) {
        check(lib.dtmrs_xa_begin(tc, gid), "开 XA 事务");
        return new Xa(gid);
    }

    /**
     * TCC 全局事务：body 正常返回 → submit；抛异常（包括 {@link BranchFailed}）→ abort，
     * 再把异常原样抛出。超时也是 abort：还没 submit，cancel 会覆盖每个分支
     */
    public void tccGlobal(String gid, Body<Tcc> body) throws Exception {
        global(tcc(gid), body);
    }

    /** XA 全局事务，语义同 {@link #tccGlobal} */
    public void xaGlobal(String gid, Body<Xa> body) throws Exception {
        global(xa(gid), body);
    }

    private <T extends TwoPhase> void global(T t, Body<T> body) throws Exception {
        try {
            body.run(t);
        } catch (Exception e) {
            t.abort();
            throw e;
        }
        t.submit();
    }

    /**
     * 二阶段消息的 prepare。之后自己跑本地事务，再 submit / abort / 什么都不做。
     *
     * @param queryPrepared 必填：崩在本地事务和 submit 之间时 TC 靠它回查 ——
     *                      回查 handler 返回 SUCCESS=本地已提交、FAILURE=没提交、其它=过会再问
     * @param graceSecs     prepare 后多久开始回查，负数用默认 10 秒
     */
    public void msgPrepare(String gid, List<String> actions, String queryPrepared, int graceSecs) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < actions.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append(jsonStr(actions.get(i)));
        }
        sb.append(']');
        check(lib.dtmrs_msg_prepare(tc, gid, sb.toString(), queryPrepared, graceSecs), "msg prepare");
    }

    /**
     * prepare → 跑 localTx → SUCCESS 就 submit、FAILURE 就 abort、
     * 其它（含抛异常）什么都不做，交给回查决断。返回 localTx 的结果码。
     * prepare 失败会直接抛异常，localTx <b>不会跑</b>
     */
    public int msgDoAndSubmit(String gid, List<String> actions, String queryPrepared, LocalTx localTx) {
        msgPrepare(gid, actions, queryPrepared, -1);
        int code;
        try {
            code = localTx.call();
        } catch (Throwable t) {
            // 本地事务抛异常 = 不知道提交了没有。不能猜，交给回查
            System.err.println("[dtmrs] " + gid + " 的本地事务抛异常，交给回查决断: " + t);
            return UNKNOWN;
        }
        if (code == SUCCESS) submit(gid);
        else if (code == FAILURE) abort(gid);
        return code;
    }

    /** tcc / xa / msg 的二阶段提交（幂等） */
    public void submit(String gid) {
        check(lib.dtmrs_submit(tc, gid), "提交");
    }

    /** 主动中止。tcc / xa / msg 已 submit 的会抛异常 —— 方向已定，不能再回滚 */
    public void abort(String gid) {
        check(lib.dtmrs_abort(tc, gid), "中止");
    }

    /** 查状态：prepared | submitted | aborting | succeed | failed */
    public String status(String gid) {
        byte[] out = new byte[64];
        if (lib.dtmrs_status(tc, gid, out, out.length) != OK) {
            throw new IllegalStateException("查询失败: " + lib.dtmrs_last_error());
        }
        return cstr(out);
    }

    /**
     * 阻塞等到终态。
     *
     * <p>JVM 里可以放心用这个阻塞接口（跟 Node 不同）——
     * 分支回调走的是 Rust 自己的线程，不依赖当前线程能不能跑。
     */
    public String waitFinal(String gid, int timeoutMs) {
        byte[] out = new byte[64];
        lib.dtmrs_wait_final(tc, gid, timeoutMs, out, out.length);
        return cstr(out);
    }

    /** 关闭。未终结的事务留在库里，下次 open + start 会接着推 */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        lib.dtmrs_close(tc);
        keepAlive.clear();
    }

    private static String cstr(byte[] b) {
        int n = 0;
        while (n < b.length && b[n] != 0) n++;
        return new String(b, 0, n, StandardCharsets.UTF_8);
    }

    /** 够用的 JSON 字符串转义 —— 不想为一个绑定引一整个 JSON 库 */
    private static String jsonStr(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
            }
        }
        return sb.append('"').toString();
    }
}
