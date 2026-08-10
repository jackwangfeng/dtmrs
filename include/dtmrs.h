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

/* 创建句柄。db_url 形如 "sqlite:/tmp/app.db"。失败返回 NULL。 */
DtmrsTc *dtmrs_open(const char *db_url);

/* 注册进程内分支，必须在 dtmrs_start 之前。
 * name 对应 saga 步骤里的 "local://name"。 */
int dtmrs_register(DtmrsTc *tc, const char *name,
                   dtmrs_handler_fn fn, void *user_data);

/* 启动推进器。上次进程留下的未终结事务会被自动接着推。 */
int dtmrs_start(DtmrsTc *tc);

/* 提交 SAGA。steps_json 形如：
 *   [{"action":"local://deduct","compensate":"local://deduct_undo"},
 *    {"action":"http://svc/ship","compensate":"http://svc/unship"}]
 * 若有 local:// 名字没注册，这里就会失败（而不是等推到一半才发现）。 */
int dtmrs_submit_saga(DtmrsTc *tc, const char *gid, const char *steps_json);

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
 *   {"task_id":7,"name":"deduct","gid":"order-1","branch_id":"01","op":"action"}
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

/* 关闭释放。未终结事务留在库里，下次 open+start 继续。传 NULL 安全。 */
void dtmrs_close(DtmrsTc *tc);

/* 最近一次错误。返回的指针在下次调用本库任何函数后失效。 */
const char *dtmrs_last_error(void);

#ifdef __cplusplus
}
#endif
#endif /* DTMRS_H */
