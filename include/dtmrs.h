/* dtmrs —— 嵌入式分布式事务协调器的 C ABI
 *
 * 把 TC 链进你自己的进程，不需要单独部署服务。
 * 编译产物：libdtmrs.so / libdtmrs.a （cargo build -p dtmrs-ffi --release）
 *
 * 线程模型（重要）：
 *   注册的 handler 会被**任意线程**调用，不是你的主线程。handler 必须线程安全。
 *   handler 可以阻塞（库内部走独立的阻塞线程池），但别无限阻塞。
 */
#ifndef DTMRS_H
#define DTMRS_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* 函数返回码 */
#define DTMRS_OK   0
#define DTMRS_ERR  (-1)

/* handler 返回码 —— 数值语义固定，不要改
 *
 * 关键区别：FAILURE 是「业务明确要求回滚」，UNKNOWN 是「不知道成没成」。
 * 网络超时、下游 5xx、自己抛异常 —— 全都该返回 UNKNOWN。
 * 返回 UNKNOWN 只会重试，不会回滚；返回 FAILURE 会立刻触发逆序补偿。
 * 把超时当 FAILURE 是这个领域的头号 bug：对方可能已经成功了。
 *
 * 返回任何不认识的值都按 UNKNOWN 处理。
 */
#define DTMRS_SUCCESS  0
#define DTMRS_FAILURE  1
#define DTMRS_ONGOING  2
#define DTMRS_UNKNOWN  3

typedef struct DtmrsTc DtmrsTc;

typedef int (*dtmrs_handler_fn)(const char *gid,
                                const char *branch_id,
                                const char *op,
                                void *user_data);

/* 带业务数据的 handler：多一个 payload（saga 那步给的数据，没给就是空串 ""）。
 * 老的 dtmrs_handler_fn 签名不能改（已编好的宿主按四个参数调），所以另起一个。 */
typedef int (*dtmrs_handler_ex_fn)(const char *gid,
                                   const char *branch_id,
                                   const char *op,
                                   const char *payload,
                                   void *user_data);

/* 创建句柄。失败返回 NULL。db_url 形如：
 *   sqlite:/tmp/app.db
 *   postgres://user:pass@host:5432/db      mysql://user:pass@host:3306/db
 *   redis://host:6379/0?key_prefix=app1:   （Redis 持久性弱于 SQL，用前读部署文档；
 *                                           key_prefix 可省，多套环境共用时隔开彼此） */
DtmrsTc *dtmrs_open(const char *db_url);

/* 注册进程内分支，必须在 dtmrs_start 之前。
 * name 对应 saga 步骤里的 "local://name"。 */
int dtmrs_register(DtmrsTc *tc, const char *name,
                   dtmrs_handler_fn fn, void *user_data);

/* 同上，回调能拿到 payload。两种可以混用，各管各的名字。 */
int dtmrs_register_ex(DtmrsTc *tc, const char *name,
                      dtmrs_handler_ex_fn fn, void *user_data);

/* 启动推进器。上次进程留下的未终结事务会被自动接着推。 */
int dtmrs_start(DtmrsTc *tc);

/* 提交 SAGA。steps_json 形如：
 *   [{"action":"local://deduct","compensate":"local://deduct_undo","payload":"{\"amount\":30}"},
 *    {"action":"http://svc/ship","compensate":"http://svc/unship"}]
 * payload 可省略，是字符串，这一步的正向和补偿共用。http 分支收到的是请求体，
 * 本地分支从 dtmrs_register_ex 的回调参数 / 拉取任务的 payload 字段拿到。
 * 若有 local:// 名字没注册，这里就会失败（而不是等推到一半才发现）。 */
int dtmrs_submit_saga(DtmrsTc *tc, const char *gid, const char *steps_json);

/* ---- TCC / XA / 二阶段消息 --------------------------------------------
 *
 * 这三种模式的一阶段是**你自己做**的，所以是按 gid 的一串调用，不是一次提交：
 *
 *   TCC:  dtmrs_tcc_begin → (dtmrs_tcc_register "01" → 你跑 try) × N
 *                         → 全成功 dtmrs_submit，否则 dtmrs_abort
 *   XA:   dtmrs_xa_begin  → (dtmrs_xa_register  "01" → 你做业务 SQL + PREPARE) × N
 *                         → 全成功 dtmrs_submit，否则 dtmrs_abort
 *   msg:  dtmrs_msg_prepare → 你提交本地事务
 *                         → 成功 dtmrs_submit / 明确失败 dtmrs_abort / 不知道就什么都别调
 *
 * 三条铁律（都会被库挡住，但知道为什么更好）：
 *   1. **先 register 再做一阶段**。反过来 try 冻结的资源 / PREPARE 的 xid TC 不知道，
 *      回滚时没人收尾 —— XA 会留下永久持锁的 prepared 事务。
 *   2. **一阶段不是 SUCCESS 就 abort**，包括超时（UNKNOWN）：还没 submit，
 *      cancel/rollback 会撤掉每个分支，多余的由屏障空转掉。
 *   3. **submit 之后不能 abort**（返回 DTMRS_ERR）：方向已定，
 *      confirm/commit 失败也只会无限重试，绝不转 cancel/rollback。
 *
 * 分支号由你给：从 "01" 开始、两位补零、每个分支各用各的。原样重试 register
 * 是幂等的；同一个号配了不同地址会报错（两个分支撞号）。
 */

/* 开 TCC / XA 事务。幂等。 */
int dtmrs_tcc_begin(DtmrsTc *tc, const char *gid);
int dtmrs_xa_begin(DtmrsTc *tc, const char *gid);

/* 登记分支。返回 DTMRS_OK 之后才能做这个分支的一阶段。 */
int dtmrs_tcc_register(DtmrsTc *tc, const char *gid, const char *branch_id,
                       const char *confirm, const char *cancel);
int dtmrs_xa_register(DtmrsTc *tc, const char *gid, const char *branch_id,
                      const char *commit, const char *rollback);

/* 二阶段消息。actions_json 是要送达的地址数组：["local://add_points","http://x/y"]
 * query_prepared 必填：崩在本地事务和 submit 之间时 TC 靠它问「本地提交了没有」，
 *   回查 handler 返回 SUCCESS=已提交（继续发）、FAILURE=没提交（作废）、其它=过会再问。
 *   回查调用的 branch_id 是 "00"。
 * grace_secs：prepare 后多久开始回查，负数用默认（10 秒）。 */
int dtmrs_msg_prepare(DtmrsTc *tc, const char *gid, const char *actions_json,
                      const char *query_prepared, int grace_secs);

/* tcc / xa / msg 的二阶段提交。幂等。 */
int dtmrs_submit(DtmrsTc *tc, const char *gid);

/* 主动中止：逆序撤销所有已登记分支。tcc / xa / msg 已 submit 的会返回 DTMRS_ERR。 */
int dtmrs_abort(DtmrsTc *tc, const char *gid);

/* ---- workflow ------------------------------------------------------------
 *
 * 把整个事务写成一个函数，步骤由函数自己决定（可以有 if、循环、依赖前一步的结果）。
 * 崩溃后靠**重放**续跑：函数会被从头再调，已成功的分支不重新执行，而是把上次
 * 记下的结果直接还给你。所以函数必须是确定性的，副作用都要放进分支里。
 *
 *   static int deduct(const char *gid, const char *bid, char *out, size_t n, void *ud) {
 *       ... 扣款 ...
 *       snprintf(out, n, "流水号-123");     // 结果数据，重放时原样还回来
 *       return DTMRS_SUCCESS;
 *   }
 *   static int order(DtmrsWf *wf, const char *gid, const char *input, void *ud) {
 *       char sn[64];
 *       if (dtmrs_wf_branch(wf, "扣款", "local://退款", deduct, NULL, sn, sizeof sn) != DTMRS_OK)
 *           return DTMRS_UNKNOWN;          // 收到 ERR 立刻返回；返回什么都行，以分支的原因为准
 *       if (need_ship(sn) && dtmrs_wf_branch(wf, "发货", "local://退货", ship, NULL, NULL, 0) != DTMRS_OK)
 *           return DTMRS_UNKNOWN;
 *       return DTMRS_SUCCESS;
 *   }
 *   dtmrs_register_workflow(tc, "下单", order, NULL);   // start 之前
 *   dtmrs_start(tc);
 *   dtmrs_submit_workflow(tc, "order-1", "下单", "{\"sku\":7}");
 *
 * 只有回调式；拉取式（Node）没有 workflow —— 函数体得在回调里同步跑完。
 */

typedef struct DtmrsWf DtmrsWf;

/* workflow 函数。返回 DTMRS_SUCCESS=跑完了、DTMRS_FAILURE=业务要求整单回滚、
 * 其它=过会儿重放。**有任何一次 dtmrs_wf_branch 返回过 ERR，这个返回值就不算数。** */
typedef int (*dtmrs_workflow_fn)(DtmrsWf *wf, const char *gid, const char *input,
                                 void *user_data);

/* 分支函数体。结果数据写进 out（'\0' 结尾，最多 1024 字符，可以不写）。 */
typedef int (*dtmrs_wf_branch_fn)(const char *gid, const char *branch_id,
                                  char *out, size_t out_len, void *user_data);

/* 注册 workflow 函数，必须在 dtmrs_start 之前。重启后要注册同名函数。 */
int dtmrs_register_workflow(DtmrsTc *tc, const char *name,
                            dtmrs_workflow_fn fn, void *user_data);

/* 提交 workflow 事务。name 没注册当场报错。幂等。 */
int dtmrs_submit_workflow(DtmrsTc *tc, const char *gid, const char *name,
                          const char *input);

/* 在 workflow 函数里开一个分支。只能在 workflow 函数执行期间、用传进来的 wf 调。
 *   name        逻辑名字，用于重放分岔检测，要稳定
 *   compensate  回滚时调的地址（local:// / http://），NULL 或 "" = 不补偿
 *   fn          函数体；重放时这个分支上次成功过就**不会被调用**
 *   out/out_len 结果数据（新跑的或上次记下的），可传 NULL
 * 返回 DTMRS_OK = 分支成功。DTMRS_ERR = workflow 要停在这里（回滚 / 重试 / 重放走岔），
 * 原因见 dtmrs_last_error()。收到 ERR 要立刻返回 —— 之后再开的分支一律不会执行。 */
int dtmrs_wf_branch(DtmrsWf *wf, const char *name, const char *compensate,
                    dtmrs_wf_branch_fn fn, void *user_data,
                    char *out, size_t out_len);

/* ---- 拉取式分支分发 ----------------------------------------------------
 *
 * dtmrs_register 是「推」：库回调你。简单，但回调必须**同步返回**一个 int。
 * 对 Python / Java 够用；对 Node 这类宿主是硬伤 —— 它们的业务代码几乎全是
 * 异步的（数据库客户端都返回 Promise），同步回调里没法 await。
 *
 * 拉取式是「拉」：你自己在事件循环里取任务、爱怎么异步怎么异步、完事回填。
 * 两种可以在同一进程混用，各管各的分支名。
 *
 * 典型用法（事件循环型宿主）：
 *   dtmrs_register_pull(tc, "deduct");
 *   dtmrs_start(tc);
 *   // 在你的定时器里，每次 tick：
 *   while (dtmrs_next_task(tc, 0, buf, sizeof buf) == 1) {
 *       // 解析 buf 里的 JSON，异步干活，完事后 dtmrs_reply(tc, task_id, ...)
 *   }
 */

/* 把一个分支名登记成拉取式。必须在 dtmrs_start 之前。 */
int dtmrs_register_pull(DtmrsTc *tc, const char *name);

/* 取一个待办分支，JSON 写入 out：
 *   {"task_id":7,"name":"deduct","gid":"order-1","branch_id":"01","op":"action","payload":""}
 * 返回 1=取到, 0=没有, DTMRS_ERR=出错。
 *
 * timeout_ms 传 0 表示**不阻塞**，立刻返回。事件循环型宿主必须传 0 ——
 * 阻塞会卡死循环，那样连回填结果都做不到。
 *
 * 取到之后必须 dtmrs_reply，否则该分支会挂到超时（按结果未知处理，会重试）。 */
int dtmrs_next_task(DtmrsTc *tc, int timeout_ms, char *out, size_t out_len);

/* 回填结果。result 取 DTMRS_SUCCESS/FAILURE/ONGOING/UNKNOWN。
 * 宿主自己抛异常时回 UNKNOWN，别回 FAILURE —— 不知道业务做没做就别回滚。 */
int dtmrs_reply(DtmrsTc *tc, unsigned long long task_id, int result);

/* ---- 查询 -------------------------------------------------------------- */

/* 查状态，写入 out（prepared|submitted|aborting|succeed|failed）。 */
int dtmrs_status(DtmrsTc *tc, const char *gid, char *out, size_t out_len);

/* 阻塞等终态。只适合脚本/测试，生产上事务是异步推进的。 */
int dtmrs_wait_final(DtmrsTc *tc, const char *gid, int timeout_ms,
                     char *out, size_t out_len);

/* 关闭释放。未终结事务留在库里，下次 open+start 继续。传 NULL 安全。
 * 返回时存储连接已经全部关完（sqlite 的 -wal / -shm 已收尾），
 * 可以立刻删除 / 挪动库文件或重新打开同一个库。 */
void dtmrs_close(DtmrsTc *tc);

/* 最近一次错误。返回的指针在下次调用本库任何函数后失效。 */
const char *dtmrs_last_error(void);

#ifdef __cplusplus
}
#endif
#endif /* DTMRS_H */
