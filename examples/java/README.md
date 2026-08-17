# Java 示例

零依赖，JDK 17+ 就能跑，不需要 Maven/Gradle。

```bash
# 演示（需要一个已在跑的 TC）
DTMRS_URL=http://127.0.0.1:36789 DTMRS_AUTH_TOKEN=<令牌> ./run.sh

# 测试套件 —— 推荐这样跑，测试会自己拉起 TC，崩溃恢复才测得了
DTMRS_BIN=/path/to/dtmrs ./run-tests.sh
```

`run-tests.sh` 的退出码反映成败，可以直接进 CI。

| 文件 | 是什么 |
|---|---|
| `DtmrsClient.java` | **TC 客户端**：saga / 二阶段消息 / TCC / abort / query。拷进项目直接用 |
| `BranchService.java` | 三个分支微服务（库存 / 订单 / 账户），各持真实状态 + 子事务屏障 |
| `Demo.java` | 四个场景的演示（输出好看，适合先跑这个） |
| `Tests.java` | **测试套件**，七个场景 19 条断言，退出码反映成败，可进 CI |

## 四个场景在验什么

跑出来长这样（数值是真实断言的，不是打印的字符串）：

| 场景 | 结果 | 说明 |
|---|---|---|
| 全部成功 | `succeed`，100→99 / 0→1 / 1000→990 | 没有任何补偿被调用 |
| 第二个分支业务拒绝 | `failed`，**全部回到初始值** | 逆序补偿 |
| 第三个分支超时 | **`submitted`**，前两步保留 | 超时≠失败，只重试不回滚 |
| TCC | `succeed`，状态只变一次 | confirm 被屏障当重复请求挡掉 |

第二个场景里有个细节值得看：**账户服务的 action 根本没执行过，却收到了 compensate**。

这不是 bug。协调器回滚时会补偿**所有**分支，因为一个超时的分支**可能实际上成功了** ——
宁可多发不可漏发。多余的那次由子事务屏障空转掉（日志里显示「空回滚,未改状态」）。

**没有屏障的话，这次多余的「余额 +10」会在从没扣过款的账上凭空加钱。**

## 写这类测试时踩到的三个坑

这套测试本身返工了三轮，每一轮都是**测试代码的问题而不是被测系统的问题**。
如果你也要写跨服务的事务测试，这三个大概率会碰到：

**① 别用 sleep，用轮询。** 协调器异步推进，写死的等待在你机器上够、CI 上就不够。
但注意「超时只重试」那个场景**恰恰不能等终态** —— 它本来就不该终结，
要断言的是「等了 N 秒之后仍然是 submitted」。

**② 测试之间别清空屏障。** 第一版的 `/reset` 把屏障也清了，结果上一个测试
留下的在途重试请求在 reset 之后到达、发现屏障是空的，**又执行了一次** ——
为了隔离而清空去重状态，恰好把屏障要防的重复放了进来。
各测试 gid 唯一，屏障键天然不冲突，留着就行。

**③ 固定端口的分支服务会收到外来流量。** 几小时前一笔被故意卡住的事务留在了
另一个长期运行的 TC 里，那个 TC **一直在重试它**；每次分支服务起在同样的端口上，
那笔陈年事务的重试就送达一次、凭空扣一笔钱，表现为「偶发失败」。
现在每次运行生成一个标记，分支服务只认带标记的 gid。

> 顺带一提，第三条正是文档里「**要监控长时间停在非终态的事务**」的真实案例 ——
> 一笔卡住的事务不会自己消失，它会安静地重试到天荒地老。

## 接进 Spring Boot

`DtmrsClient` 只用 `java.net.http`，跟框架无关，注册成 Bean 就行：

```java
@Configuration
public class DtmrsConfig {
    @Bean
    public DtmrsClient dtmrsClient(
            @Value("${dtmrs.url}") String url,
            @Value("${dtmrs.token:}") String token) {
        return new DtmrsClient(url, token);
    }
}
```

发起事务：

```java
@Service
public class OrderService {
    private final DtmrsClient tc;

    @Transactional   // ⚠ 注意：这个注解管的是**你本地的**事务，不是全局事务
    public void placeOrder(Order o) throws Exception {
        // gid 用业务主键派生 —— 重试时天然一致。别用时间戳或 UUID
        String gid = "order-" + o.getId();
        tc.submitSaga(gid, List.of(
            new DtmrsClient.Step(inventoryUrl + "/deduct", inventoryUrl + "/deduct-undo"),
            new DtmrsClient.Step(accountUrl   + "/pay",    accountUrl   + "/pay-undo")));
    }
}
```

分支服务（收协调器的调用）：

```java
@RestController
public class InventoryController {
    @Autowired DataSource ds;

    @PostMapping("/deduct")
    public String deduct(@RequestParam String gid,
                         @RequestParam String branch_id,
                         @RequestParam String op,
                         @RequestParam long skuId) throws Exception {
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            // ⚠ 屏障判定和业务操作**必须在同一个本地事务里**。
            //    分开就会出现「屏障记录写了但业务没做」的窗口，进程一崩就错乱
            Barrier b = new Barrier("saga", gid, branch_id, op);
            if (b.call(c, Dialect.MYSQL) != Decision.EXECUTE) {
                c.commit();
                return "SUCCESS";           // 重复请求 / 空回滚 / 悬挂，都不执行业务
            }
            deductStock(c, skuId);
            c.commit();
        }
        return "SUCCESS";
    }
}
```

`Barrier` 在 [`clients/java`](../../clients/java)，也发到了 Maven Central。

## 分支要遵守的三条

**① 返回值决定协调器怎么走，写错就是数据不一致**

| 你返回 | 协调器认为 | 它会做 |
|---|---|---|
| 200 `SUCCESS` | 成功 | 继续下一步 |
| **409 `FAILURE`** | **业务明确拒绝** | **逆序补偿** |
| 5xx / 超时 / 连不上 | **结果未知** | **只重试，绝不回滚** |

⚠ 别拿 409 表示「暂时不可用」——那会触发本不该发生的回滚。
暂时性故障就让它超时或返回 5xx。

**② 每个分支都必须幂等**，而且要挡住三件事，只做第一件不够：

- 重复请求 —— 协调器会重试
- 空回滚 —— 补偿到了但正向根本没执行过
- 悬挂 —— 补偿先执行了，迷路的正向后到，必须丢弃

**③ 补偿本身也会失败、也会被重试**，所以补偿也要幂等。

## TCC 的顺序不能反

```java
tc.prepareTcc(gid);
for (...) {
    tc.registerTccBranch(gid, id, tryUrl, confirmUrl, cancelUrl);  // 先登记
    callTry(...);                                                   // 再 try
}
tc.submitTcc(gid);
```

反过来的话：try 执行成功、资源已冻结，但协调器不知道有这个分支 ——
既不会 confirm 也不会 cancel，**那份资源永久泄漏**。

XA 更糟，会留下永久持锁的 prepared 事务。
