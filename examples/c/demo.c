/* 验证 C 头文件可编译可链接：
 *   gcc -I../../include demo.c -L../../target/release -ldtmrs -o demo && ./demo
 */
#include "dtmrs.h"
#include <stdio.h>
#include <string.h>

static int calls = 0;

static int ok_handler(const char *gid, const char *branch_id,
                      const char *op, void *ud) {
    (void)ud;
    printf("  [C handler] gid=%s branch=%s op=%s\n", gid, branch_id, op);
    calls++;
    return DTMRS_SUCCESS;
}

static int reject(const char *gid, const char *branch_id,
                  const char *op, void *ud) {
    (void)gid; (void)branch_id; (void)op; (void)ud;
    puts("  [C handler] 业务拒绝 → FAILURE");
    return DTMRS_FAILURE;
}

/* 带 payload 的 handler（dtmrs_register_ex） */
static int ex_handler(const char *gid, const char *branch_id, const char *op,
                      const char *payload, void *ud) {
    (void)ud;
    printf("  [C ex handler] gid=%s branch=%s op=%s payload=%s\n",
           gid, branch_id, op, *payload ? payload : "(空)");
    calls++;
    return DTMRS_SUCCESS;
}

/* 自己的「一阶段」。真实业务里是冻结库存 / 写本地订单表 */
static int my_try(const char *bid) { printf("  [try] 分支 %s 冻结资源\n", bid); return 1; }

/* workflow 分支函数体：结果写进 out，重放时原样还回来、不再执行 */
static int wf_deduct(const char *gid, const char *bid, char *out, size_t n, void *ud) {
    (void)gid; (void)ud;
    printf("  [wf 扣款] 分支 %s\n", bid);
    snprintf(out, n, "流水-%s", bid);
    return DTMRS_SUCCESS;
}

static int wf_ship(const char *gid, const char *bid, char *out, size_t n, void *ud) {
    (void)gid; (void)out; (void)n; (void)ud;
    printf("  [wf 发货] 分支 %s\n", bid);
    return DTMRS_SUCCESS;
}

/* workflow 函数：步骤由代码决定。收到 ERR 立刻返回 */
static int wf_order(DtmrsWf *wf, const char *gid, const char *input, void *ud) {
    (void)gid; (void)ud;
    char sn[64];
    if (dtmrs_wf_branch(wf, "扣款", "local://c1", wf_deduct, NULL, sn, sizeof sn) != DTMRS_OK)
        return DTMRS_UNKNOWN;
    printf("  [wf] 扣款流水 %s，input=%s\n", sn, input);
    if (strstr(input, "ship") &&
        dtmrs_wf_branch(wf, "发货", "local://c1", wf_ship, NULL, NULL, 0) != DTMRS_OK)
        return DTMRS_UNKNOWN;
    return DTMRS_SUCCESS;
}

int main(void) {
    remove("/tmp/dtmrs_c_demo.db");
    DtmrsTc *tc = dtmrs_open("sqlite:/tmp/dtmrs_c_demo.db");
    if (!tc) { fprintf(stderr, "open 失败: %s\n", dtmrs_last_error()); return 1; }

    dtmrs_register(tc, "a1", ok_handler, NULL);
    dtmrs_register(tc, "c1", ok_handler, NULL);
    dtmrs_register(tc, "a2", reject, NULL);
    dtmrs_register(tc, "c2", ok_handler, NULL);
    dtmrs_register_ex(tc, "a3", ex_handler, NULL);
    dtmrs_register_ex(tc, "confirm", ex_handler, NULL);
    dtmrs_register_ex(tc, "cancel", ex_handler, NULL);
    dtmrs_register_ex(tc, "notify", ex_handler, NULL);
    dtmrs_register_ex(tc, "query", ex_handler, NULL);
    dtmrs_register_workflow(tc, "下单", wf_order, NULL);
    if (dtmrs_start(tc) != DTMRS_OK) {
        fprintf(stderr, "start 失败: %s\n", dtmrs_last_error()); return 1;
    }

    char st[64];
    puts("① 单步成功");
    dtmrs_submit_saga(tc, "c-1",
        "[{\"action\":\"local://a1\",\"compensate\":\"local://c1\"}]");
    dtmrs_wait_final(tc, "c-1", 5000, st, sizeof st);
    printf("  结果: %s\n", st);

    puts("② 第二步拒绝 → 逆序补偿");
    dtmrs_submit_saga(tc, "c-2",
        "[{\"action\":\"local://a1\",\"compensate\":\"local://c1\"},"
        " {\"action\":\"local://a2\",\"compensate\":\"local://c2\"}]");
    dtmrs_wait_final(tc, "c-2", 5000, st, sizeof st);
    printf("  结果: %s\n", st);

    puts("③ 每步带自己的 payload");
    dtmrs_submit_saga(tc, "c-p",
        "[{\"action\":\"local://a3\",\"compensate\":\"local://c1\",\"payload\":\"{\\\"amount\\\":30}\"}]");
    dtmrs_wait_final(tc, "c-p", 5000, st, sizeof st);
    printf("  结果: %s\n", st);

    puts("④ TCC：先 register，再跑 try；全成功才 submit");
    dtmrs_tcc_begin(tc, "c-tcc");
    int all_ok = 1;
    const char *bids[] = {"01", "02"};
    for (int i = 0; i < 2 && all_ok; i++) {
        if (dtmrs_tcc_register(tc, "c-tcc", bids[i], "local://confirm", "local://cancel") != DTMRS_OK) {
            printf("  登记失败，不能跑 try: %s\n", dtmrs_last_error());
            all_ok = 0;
            break;
        }
        all_ok = my_try(bids[i]);
    }
    if (all_ok) dtmrs_submit(tc, "c-tcc"); else dtmrs_abort(tc, "c-tcc");
    dtmrs_wait_final(tc, "c-tcc", 5000, st, sizeof st);
    printf("  结果: %s\n", st);
    if (dtmrs_abort(tc, "c-tcc") != DTMRS_OK)
        printf("  已 submit（这里已经终结）的 TCC 不能再 abort: %s\n", dtmrs_last_error());

    puts("⑤ 二阶段消息：prepare → 本地事务 → submit");
    dtmrs_msg_prepare(tc, "c-msg", "[\"local://notify\"]", "local://query", -1);
    /* ……这里提交本地事务…… */
    dtmrs_submit(tc, "c-msg");
    dtmrs_wait_final(tc, "c-msg", 5000, st, sizeof st);
    printf("  结果: %s\n", st);

    puts("⑥ workflow：步骤由函数决定");
    dtmrs_submit_workflow(tc, "c-wf", "下单", "{\"ship\":true}");
    dtmrs_wait_final(tc, "c-wf", 5000, st, sizeof st);
    printf("  结果: %s\n", st);

    puts("⑦ 错误处理");
    if (dtmrs_submit_saga(tc, "c-3", "{坏 json}") != DTMRS_OK)
        printf("  坏 JSON 被拒: %s\n", dtmrs_last_error());

    dtmrs_close(tc);
    printf("\nhandler 共被调用 %d 次\n", calls);
    return 0;
}
