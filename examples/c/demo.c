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

int main(void) {
    remove("/tmp/dtmrs_c_demo.db");
    DtmrsTc *tc = dtmrs_open("sqlite:/tmp/dtmrs_c_demo.db");
    if (!tc) { fprintf(stderr, "open 失败: %s\n", dtmrs_last_error()); return 1; }

    dtmrs_register(tc, "a1", ok_handler, NULL);
    dtmrs_register(tc, "c1", ok_handler, NULL);
    dtmrs_register(tc, "a2", reject, NULL);
    dtmrs_register(tc, "c2", ok_handler, NULL);
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

    puts("③ 错误处理");
    if (dtmrs_submit_saga(tc, "c-3", "{坏 json}") != DTMRS_OK)
        printf("  坏 JSON 被拒: %s\n", dtmrs_last_error());

    dtmrs_close(tc);
    printf("\nhandler 共被调用 %d 次\n", calls);
    return 0;
}
