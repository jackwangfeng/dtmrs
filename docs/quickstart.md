# 五分钟跑通第一笔分布式事务

这篇不解释原理，只让你看到一笔事务**成功**和**回滚**长什么样。原理见 [五种模式怎么选](choosing-a-mode.md)。

## 1. 起 TC

```bash
cargo install dtmrs
DTMRS_DB=sqlite:dtmrs.db DTMRS_ADDR=127.0.0.1:36789 dtmrs
```

另开一个终端验一下：

```bash
curl localhost:36789/health     # → ok
```

## 2. 起一个假业务服务

用 Python 起一个最小的，两个接口各自记录被调了什么：

```python
# busi.py — python3 busi.py
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

class H(BaseHTTPRequestHandler):
    def do_POST(self):
        q = parse_qs(urlparse(self.path).query)
        path = urlparse(self.path).path
        print(f"  被调用: {path}  gid={q.get('gid',[''])[0]} "
              f"branch={q.get('branch_id',[''])[0]} op={q.get('op',[''])[0]}")
        # /fail 返回 409 = 业务明确要求回滚
        code = 409 if path == "/fail" else 200
        self.send_response(code)
        self.end_headers()
        self.wfile.write(b'{}')
    def log_message(self, *a): pass

HTTPServer(("127.0.0.1", 8899), H).serve_forever()
```

```bash
python3 busi.py
```

## 3. 正常提交

```bash
curl -XPOST localhost:36789/api/dtmsvr/submit \
  -H 'content-type: application/json' -d '{
  "gid": "demo-ok",
  "steps": [
    {"action": "http://127.0.0.1:8899/deduct", "compensate": "http://127.0.0.1:8899/deduct-undo"},
    {"action": "http://127.0.0.1:8899/ship",   "compensate": "http://127.0.0.1:8899/ship-undo"}
  ]}'
```

业务服务那边会打出：

```
  被调用: /deduct  gid=demo-ok branch=01 op=action
  被调用: /ship    gid=demo-ok branch=02 op=action
```

查状态：

```bash
curl 'localhost:36789/api/dtmsvr/query?gid=demo-ok'
```

```json
{"gid":"demo-ok","status":"succeed", ...}
```

两个正向分支各调一次，补偿一次都没调。

## 4. 让它回滚

把第二步换成会返回 409 的 `/fail`：

```bash
curl -XPOST localhost:36789/api/dtmsvr/submit \
  -H 'content-type: application/json' -d '{
  "gid": "demo-rollback",
  "steps": [
    {"action": "http://127.0.0.1:8899/deduct", "compensate": "http://127.0.0.1:8899/deduct-undo"},
    {"action": "http://127.0.0.1:8899/fail",   "compensate": "http://127.0.0.1:8899/fail-undo"}
  ]}'
```

业务服务那边：

```
  被调用: /deduct       gid=demo-rollback branch=01 op=action
  被调用: /fail         gid=demo-rollback branch=02 op=action
  被调用: /fail-undo    gid=demo-rollback branch=02 op=compensate   ← 逆序
  被调用: /deduct-undo  gid=demo-rollback branch=01 op=compensate
```

**注意两件事**：

1. **补偿是逆序的**——后执行的先回滚
2. **失败的那个分支也被补偿了**。因为它可能已经产生了副作用只是返回了失败，宁可多补不可漏补——多余的补偿由[子事务屏障](integration.md)空转掉

```bash
curl 'localhost:36789/api/dtmsvr/query?gid=demo-rollback'
# → "status":"failed", "rollback_reason":"分支 02 返回 FAILURE"
```

## 5. 看一个反直觉的行为：超时不回滚

把第二步指向一个不存在的端口：

```bash
curl -XPOST localhost:36789/api/dtmsvr/submit \
  -H 'content-type: application/json' -d '{
  "gid": "demo-timeout",
  "steps": [
    {"action": "http://127.0.0.1:8899/deduct", "compensate": "http://127.0.0.1:8899/deduct-undo"},
    {"action": "http://127.0.0.1:1/nobody",    "compensate": "http://127.0.0.1:8899/undo2"}
  ]}'

sleep 2
curl 'localhost:36789/api/dtmsvr/query?gid=demo-timeout'
```

```json
{"status":"submitted", ...}
```

**它停在 `submitted` 而不是回滚**，并且**一个补偿都没发**。

这是整个系统最重要的设计：连不上代表**结果未知**——对方可能已经成功了。这时候回滚会造成不一致，正确做法是退避重试，直到拿到明确结论。

只有业务**明确**返回 409 / gRPC `ABORTED` / 响应体带 `FAILURE` 才会触发补偿。

## 下一步

上面的假业务服务**没有做幂等**，所以还不能上生产——分支一定会被重复调用。

- **必读**：[业务侧接入指南](integration.md) —— 子事务屏障、返回值语义、自检清单
- [五种模式怎么选](choosing-a-mode.md) —— SAGA 之外还有 TCC / msg / XA / workflow
- [部署与运维](deployment.md) —— 多实例、存储选型、该监控什么
- 不想部署服务？TC 可以[当库嵌进你自己的进程](../README.zh-CN.md#差异化嵌入式-tcdtm-做不到的形态)
