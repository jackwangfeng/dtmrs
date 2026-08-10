# dtmrs-ffi

[dtmrs](https://github.com/jackwangfeng/dtmrs) 的 C ABI —— 让 **Python / Node / JVM /
C++ 等任何语言**把事务协调器嵌进自己的进程。

```bash
cargo build -p dtmrs-ffi --release    # → libdtmrs.so，一个普通的 .so，无运行时包袱
```

Go 做不到这个形态：`c-shared` 会把整个运行时拖进宿主进程。

提供两种分支分发方式：**回调式**（Python / Java / C）和**拉取式**
（Node —— 同步回调里没法 `await`）。C 头文件见 `dtmrs.h`。

完整文档（含中英双语 README、设计说明、各语言绑定）见
[github.com/jackwangfeng/dtmrs](https://github.com/jackwangfeng/dtmrs)。

Apache-2.0。
