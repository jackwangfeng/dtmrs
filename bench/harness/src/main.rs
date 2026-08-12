//! dtmrs 压测工具（Rust 版）。
//!
//! ```bash
//! cargo run --release -p dtmrs-bench -- --db redis --mode msg --steps 1 --n 100000
//! ```
//!
//! # 为什么从 Python 换过来
//!
//! Python 版（`bench/bench.py`）在 2 万笔/秒这个量级上，**自己要吃 9 个核，
//! 而被测系统才 4.8 个**。它测得出数，但有两个问题：
//!
//! 1. 那 9 个核是从被测系统嘴里抢的，同机压测时直接压低了上限
//! 2. Python 的默认值一路埋雷 —— Nagle、accept 队列只有 5、GIL 天花板、
//!    urllib 不复用连接。这些坑**每一个都足以让结论反过来**，
//!    前后一共踩了四次（见 `bench/bench.py` 的文件头）
//!
//! 换成 Rust 之后这些默认值全部消失：hyper 默认 TCP_NODELAY、
//! tokio 的 accept 队列是 1024、没有 GIL、reqwest 自带连接池。
//!
//! **Python 版保留着**，因为它零依赖、改起来快，扫参数很方便。
//! 要精确的绝对数字用这个 Rust 版。
//!
//! # 测的是什么
//!
//! 端到端完成一笔事务：提交 → TC 调各分支 → 落终态。业务分支是本地零操作的
//! HTTP 服务，所以数字基本反映 TC + 存储的开销。
//!
//! 主指标是「最后一步正向动作成功」——业务服务在那个路径上给一个原子计数器
//! 加一，主线程直接读内存，零网络。跑完再**全量核对真实终态**（不计时），
//! 对不上就明确报出来。

mod xa;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BUSI_PORT: u16 = 8898;
/// ⚠ 端口要在内核临时端口范围之外（/proc/sys/net/ipv4/ip_local_port_range，
/// 常见 32768 起）。落在范围内的话，压测时客户端的海量外连随时可能把它
/// 占作本地端口，于是 TC 起不来报 Address already in use（踩过）
const TC_PORT: u16 = 26700;
const DTM_PORT: u16 = 26789;

struct Args {
    db: String,
    mode: String,
    steps: usize,
    n: usize,
    concurrency: usize,
    workers: String,
    tick: String,
    target: String,
    bin: String,
    /// XA 模式的业务库（RM 侧），必须支持两阶段
    busi_db: String,
    no_verify: bool,
    quiet: bool,
}

impl Args {
    fn parse() -> Self {
        let mut a = Args {
            db: "redis".into(),
            mode: "saga".into(),
            steps: 2,
            n: 20000,
            concurrency: 400,
            workers: "16".into(),
            tick: "5".into(),
            target: "dtmrs".into(),
            bin: "target/release/dtmrs".into(),
            busi_db: "postgres://postgres:dtmrs@127.0.0.1:55435/dtmrs".into(),
            no_verify: false,
            quiet: false,
        };
        let v: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < v.len() {
            let key = v[i].clone();
            // 取下一个参数值，同时把游标推过去
            let mut val = || {
                i += 1;
                v.get(i).cloned().unwrap_or_default()
            };
            match key.as_str() {
                "--db" => a.db = val(),
                "--mode" => a.mode = val(),
                "--steps" => a.steps = val().parse().unwrap_or(2),
                "--n" => a.n = val().parse().unwrap_or(20000),
                "--concurrency" => a.concurrency = val().parse().unwrap_or(400),
                "--workers" => a.workers = val(),
                "--tick" => a.tick = val(),
                "--target" => a.target = val(),
                "--bin" => a.bin = val(),
                "--busi-db" => a.busi_db = val(),
                "--no-verify" => a.no_verify = true,
                "--quiet" => a.quiet = true,
                other => {
                    eprintln!("不认识的参数: {other}");
                    std::process::exit(2);
                }
            }
            i += 1;
        }
        a
    }

    fn tc_port(&self) -> u16 {
        if self.target == "dtm" {
            DTM_PORT
        } else {
            TC_PORT
        }
    }

    fn dsn(&self) -> String {
        match self.db.as_str() {
            "sqlite" => "sqlite:/tmp/bench-rs.db".into(),
            "postgres" => std::env::var("BENCH_PG")
                .unwrap_or_else(|_| "postgres://postgres:dtmrs@127.0.0.1:55434/dtmrs".into()),
            "mysql" => std::env::var("BENCH_MYSQL")
                .unwrap_or_else(|_| "mysql://root:dtmrs@127.0.0.1:33306/dtmrs".into()),
            // ⚠ 刻意用 db 1：Redis 测试跑在 db 0，开头要 flush_prefix() 扫全库，
            // 压测一跑就是几十万个 key，会把测试扫到超时（踩过）
            "redis" => {
                std::env::var("BENCH_REDIS").unwrap_or_else(|_| "redis://127.0.0.1:16379/1".into())
            }
            other => other.into(),
        }
    }
}

/// 零操作业务服务。唯一的副作用是给计数器加一，用来做完成判定。
async fn spawn_busi(done: Arc<AtomicU64>, final_path: String) {
    use axum::{extract::State, routing::post, Router};

    let app = Router::new()
        .route(
            "/{*rest}",
            post(
                |State((done, fin)): State<(Arc<AtomicU64>, String)>,
                 uri: axum::http::Uri,
                 _body: axum::body::Bytes| async move {
                    // ⚠ 只比路径，不含 query —— TC 调分支时会带上
                    // ?gid=..&branch_id=..&op=..，拿整个 URI 比永远不相等
                    if uri.path() == fin {
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                    "{}"
                },
            ),
        )
        .with_state((done, final_path));

    let l = tokio::net::TcpListener::bind(("127.0.0.1", BUSI_PORT))
        .await
        .expect("业务服务绑不上端口");
    tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
}

/// 客户端一笔事务里要发的一个请求。
///
/// 分两类是因为**不是所有请求都打 TC**：XA 的一阶段和 TCC 的 try 都是
/// 客户端直接打业务服务的，只有它们之间的登记/提交才走 TC。
enum Call {
    /// 打 TC：(路径, 报文)
    Tc(&'static str, String),
    /// 打业务服务：完整 URL
    Busi(String),
}

/// 一笔事务客户端要按顺序发的请求。
///
/// 各模式的**客户端往返次数不一样**，这是协议决定的，也是对比时最该盯的东西：
///
/// | 模式 | 客户端往返 | 说明 |
/// |---|---|---|
/// | saga | 1 | 一次 submit 带上全部步骤，TC 内联推进 |
/// | msg  | 2 | prepare →（业务自己的本地事务）→ submit |
/// | xa   | 2 + 每分支 1 | 中间那次打业务服务，分支自己 registerBranch + XA PREPARE |
/// | tcc  | 2 + 每分支 2 | 每个分支要先 registerBranch（打 TC）再调 try（打业务） |
///
/// TCC 每个分支两次往返是**语义要求不是实现偷懒**：先登记再 try，
/// 反过来会导致 try 成功但 TC 不知道有这个分支 —— 资源永久泄漏。
fn client_calls(mode: &str, gid: &str, steps: usize, target: &str, rm_base: &str) -> Vec<Call> {
    let acts: Vec<String> = (1..=steps)
        .map(|i| format!("http://127.0.0.1:{BUSI_PORT}/a{i}"))
        .collect();

    if target == "dtm" {
        // DTM 的报文格式：steps 之外还要一个等长的 payloads 数组
        let mut body = serde_json::json!({
            "gid": gid, "trans_type": mode,
            "payloads": vec!["{}"; steps],
        });
        if mode == "saga" {
            body["steps"] = serde_json::json!(acts
                .iter()
                .enumerate()
                .map(|(i, a)| serde_json::json!({
                    "action": a,
                    "compensate": format!("http://127.0.0.1:{BUSI_PORT}/c{}", i + 1)
                }))
                .collect::<Vec<_>>());
            return vec![Call::Tc("/api/dtmsvr/submit", body.to_string())];
        }
        body["steps"] = serde_json::json!(acts
            .iter()
            .map(|a| serde_json::json!({ "action": a }))
            .collect::<Vec<_>>());
        body["query_prepared"] = serde_json::json!(format!("http://127.0.0.1:{BUSI_PORT}/query"));
        let raw = body.to_string();
        return vec![
            Call::Tc("/api/dtmsvr/prepare", raw.clone()),
            Call::Tc("/api/dtmsvr/submit", raw),
        ];
    }

    // XA：prepare（建空事务）→ 调业务分支做一阶段（分支自己去
    // registerBranch + XA PREPARE）→ submit
    if mode == "xa" {
        let mut calls = vec![Call::Tc(
            "/api/dtmsvr/prepare",
            serde_json::json!({"gid": gid, "trans_type": "xa"}).to_string(),
        )];
        for i in 1..=steps {
            calls.push(Call::Busi(format!(
                "{rm_base}/xa1?gid={gid}&branch_id={i:02}"
            )));
        }
        calls.push(Call::Tc(
            "/api/dtmsvr/submit",
            serde_json::json!({"gid": gid, "trans_type": "xa"}).to_string(),
        ));
        return calls;
    }

    // TCC：prepare → 每个分支「registerBranch 再 try」→ submit，TC 随后回调 confirm。
    // try 和 confirm 必须打**不同的路径**（/t1 vs /a1）—— 业务服务靠
    // 命中 /a{steps} 计完成数，共用同一个路径会把 try 也算进去
    if mode == "tcc" {
        let mut calls = vec![Call::Tc(
            "/api/dtmsvr/prepare",
            serde_json::json!({"gid": gid, "trans_type": "tcc"}).to_string(),
        )];
        for i in 1..=steps {
            calls.push(Call::Tc(
                "/api/dtmsvr/registerBranch",
                serde_json::json!({
                    "gid": gid,
                    "branch_id": format!("{:02}", i),
                    "try": format!("http://127.0.0.1:{BUSI_PORT}/t{i}"),
                    "confirm": format!("http://127.0.0.1:{BUSI_PORT}/a{i}"),
                    "cancel": format!("http://127.0.0.1:{BUSI_PORT}/c{i}"),
                })
                .to_string(),
            ));
            calls.push(Call::Busi(format!("http://127.0.0.1:{BUSI_PORT}/t{i}")));
        }
        calls.push(Call::Tc(
            "/api/dtmsvr/submit",
            serde_json::json!({"gid": gid, "trans_type": "tcc"}).to_string(),
        ));
        return calls;
    }

    if mode == "saga" {
        let body = serde_json::json!({
            "gid": gid,
            "steps": acts.iter().enumerate().map(|(i, a)| serde_json::json!({
                "action": a,
                "compensate": format!("http://127.0.0.1:{BUSI_PORT}/c{}", i + 1)
            })).collect::<Vec<_>>(),
        });
        return vec![Call::Tc("/api/dtmsvr/submit", body.to_string())];
    }
    let prepare = serde_json::json!({
        "gid": gid, "trans_type": "msg", "actions": acts,
        // 崩在 prepare 和 submit 之间时 TC 靠它决断，这里跑不到
        "query_prepared": format!("http://127.0.0.1:{BUSI_PORT}/query"),
    });
    vec![
        Call::Tc("/api/dtmsvr/prepare", prepare.to_string()),
        Call::Tc(
            "/api/dtmsvr/submit",
            serde_json::json!({"gid": gid, "trans_type": "msg"}).to_string(),
        ),
    ]
}

/// 跑之前清库。**不能省**：存量数据会让数字随时间往下漂，报出来的
/// 就不可复现了（Python 版踩过 —— 4 万笔存量能让吞吐差 4.4 倍）
fn reset_db(dsn: &str) -> String {
    use std::process::Command;
    let sh = |c: &str| Command::new("sh").arg("-c").arg(c).output();

    if let Some(path) = dsn.strip_prefix("sqlite:") {
        let path = path.split('?').next().unwrap_or(path);
        for suf in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{suf}"));
        }
        return format!("已删除 {path}");
    }
    let out = if dsn.starts_with("redis") {
        // redis://host:port/db
        let rest = dsn.trim_start_matches("redis://");
        let (hostport, db) = rest.split_once('/').unwrap_or((rest, "0"));
        let (h, p) = hostport.split_once(':').unwrap_or((hostport, "6379"));
        sh(&format!("redis-cli -h {h} -p {p} -n {db} flushdb"))
    } else if dsn.starts_with("postgres") {
        sh(&format!(
            "PGPASSWORD=dtmrs psql '{dsn}' -q -c 'TRUNCATE trans_global, trans_branch_op'"
        ))
    } else if dsn.starts_with("mysql") {
        let rest = dsn.trim_start_matches("mysql://");
        let (cred, hostdb) = rest.split_once('@').unwrap_or(("root:dtmrs", rest));
        let (u, pw) = cred.split_once(':').unwrap_or(("root", "dtmrs"));
        let (hostport, db) = hostdb.split_once('/').unwrap_or((hostdb, "dtmrs"));
        let (h, p) = hostport.split_once(':').unwrap_or((hostport, "3306"));
        sh(&format!(
            "mysql -h {h} -P {p} -u {u} -p{pw} {db} -e 'TRUNCATE trans_global; TRUNCATE trans_branch_op'"
        ))
    } else {
        return "⚠ 不认识的 DSN，没清库".into();
    };
    match out {
        Ok(o) if o.status.success() => "已清空".into(),
        Ok(o) => {
            let e = String::from_utf8_lossy(&o.stderr);
            if e.contains("does not exist")
                || e.contains("Unknown table")
                || e.contains("doesn't exist")
            {
                "库是空的（表还没建）".into()
            } else {
                format!("⚠ 没清掉：{}", e.lines().last().unwrap_or("").trim())
            }
        }
        Err(e) => format!("⚠ 没清掉：{e}"),
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let final_path = format!("/a{}", args.steps);
    let tc_url = format!("http://127.0.0.1:{}", args.tc_port());

    let reset = if args.target == "dtmrs" {
        reset_db(&args.dsn())
    } else {
        "外部 TC，未清库".into()
    };

    let done = Arc::new(AtomicU64::new(0));
    let mut rm_base = String::new();
    if args.mode == "xa" {
        // XA 的业务侧必须做真的两阶段，起一个连真库的 RM
        println!(
            "  清理残留       : {}",
            xa::clear_prepared(&args.busi_db).await
        );
        match xa::spawn_rm(&args.busi_db, &tc_url, BUSI_PORT, done.clone()).await {
            Ok(b) => rm_base = b,
            Err(e) => {
                eprintln!("XA 的 RM 起不来: {e}");
                std::process::exit(1);
            }
        }
    } else {
        spawn_busi(done.clone(), final_path).await;
    }

    // DTM 由使用者自己起，我们只起自己的
    let mut child = None;
    if args.target == "dtmrs" {
        let c = std::process::Command::new(&args.bin)
            .env("DTMRS_DB", args.dsn())
            .env("DTMRS_ADDR", format!("127.0.0.1:{TC_PORT}"))
            .env("DTMRS_GRPC_ADDR", format!("127.0.0.1:{}", TC_PORT + 1))
            .env("DTMRS_TICK_MS", &args.tick)
            .env("DTMRS_WORKERS", &args.workers)
            .env("RUST_LOG", "warn")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("起不来 TC，检查 --bin");
        child = Some(c);
    }

    let http = reqwest::Client::builder()
        // 连接池要够大，否则客户端自己成了瓶颈
        .pool_max_idle_per_host(args.concurrency)
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();

    // 等 TC 起来
    for i in 0..300 {
        if http.get(format!("{tc_url}/health")).send().await.is_ok()
            || http
                .get(format!("{tc_url}/api/dtmsvr/newGid"))
                .send()
                .await
                .is_ok()
        {
            break;
        }
        if i == 299 {
            eprintln!("TC 起不来");
            std::process::exit(1);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let prefix = format!(
        "r{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );

    let t0 = Instant::now();
    // 提交：用信号量控制在途并发
    let sem = Arc::new(tokio::sync::Semaphore::new(args.concurrency));
    let mut tasks = Vec::with_capacity(args.n);
    for i in 0..args.n {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let (http, tc_url) = (http.clone(), tc_url.clone());
        let calls = client_calls(
            &args.mode,
            &format!("{prefix}-{i}"),
            args.steps,
            &args.target,
            &rm_base,
        );
        tasks.push(tokio::spawn(async move {
            let _p = permit;
            for call in calls {
                match call {
                    Call::Tc(path, body) => {
                        let _ = http
                            .post(format!("{tc_url}{path}"))
                            .header("content-type", "application/json")
                            .body(body)
                            .send()
                            .await;
                    }
                    Call::Busi(url) => {
                        let _ = http.post(url).send().await;
                    }
                }
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    let submit_secs = t0.elapsed().as_secs_f64();

    // 完成判定：直接读原子计数，不发请求
    let mut stalled = 0;
    let mut last = 0;
    while done.load(Ordering::Relaxed) < args.n as u64 && t0.elapsed() < Duration::from_secs(600) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let cur = done.load(Ordering::Relaxed);
        if cur == last {
            stalled += 1;
            if stalled > 500 {
                break;
            }
        } else {
            stalled = 0;
            last = cur;
        }
    }
    let drive_secs = t0.elapsed().as_secs_f64();
    let finished = done.load(Ordering::Relaxed);

    // 不计时的正确性核对
    let (final_n, dist) = if args.no_verify {
        (u64::MAX, String::from("跳过核对"))
    } else {
        verify(&http, &tc_url, &prefix, args.n, &args.target).await
    };

    if args.quiet {
        println!(
            "{:6} {:5} {:9} workers={:>3}  {:7.0} 笔/秒  终态 {}/{}",
            args.target,
            args.mode,
            args.db,
            args.workers,
            finished as f64 / drive_secs,
            if final_n == u64::MAX {
                -1
            } else {
                final_n as i64
            },
            args.n
        );
    } else {
        println!("\n=== dtmrs 压测（Rust 版）· 存储={} ===", args.db);
        println!("  模式          : {} × {} 步", args.mode, args.steps);
        println!("  事务          : {} 笔（业务分支零操作）", args.n);
        println!("  在途并发      : {}", args.concurrency);
        println!("  推进 worker   : {}", args.workers);
        println!("  跑之前清库    : {reset}");
        println!("  ---");
        println!(
            "  提交阶段      : {:.2}s  →  {:.0} 笔/秒",
            submit_secs,
            args.n as f64 / submit_secs
        );
        println!(
            "  提交+推完     : {:.2}s  →  {:.0} 笔/秒",
            drive_secs,
            finished as f64 / drive_secs
        );
        println!("  ---");
        println!("  终态核对      : {dist}");
        if args.mode == "xa" {
            // 光看终态不够 —— 见 xa::verify_rows 的注释
            let (rows, stuck) = xa::verify_rows(&args.busi_db).await;
            println!("  业务库实际写入: {rows} 行（应为 {}）", args.n);
            println!("  残留 prepared : {stuck}（应为 0）");
            if rows != args.n as i64 {
                println!("  ❌ 行数对不上 —— 一阶段有失败，这个吞吐数字不算数");
            }
        }
        if finished < args.n as u64 {
            println!("  ⚠ 有 {} 笔没在时限内跑完", args.n as u64 - finished);
        }
    }

    if let Some(mut c) = child {
        let _ = c.kill();
        let _ = c.wait();
    }
}

/// 全量核对真实终态。**不计时** —— 这是正确性核对，不是性能指标。
///
/// ⚠ 必须重试着等：主指标是「最后一步动作成功」，它比「落终态」早一次
/// 存储写入，SQL 后端上这个尾巴能拖几秒。一跑完就快照会误报。
async fn verify(
    http: &reqwest::Client,
    tc_url: &str,
    prefix: &str,
    n: usize,
    target: &str,
) -> (u64, String) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut ok = 0u64;
        let mut other = 0u64;
        let sem = Arc::new(tokio::sync::Semaphore::new(64));
        let mut tasks = Vec::with_capacity(n);
        for i in 0..n {
            let permit = sem.clone().acquire_owned().await.unwrap();
            let (http, url) = (
                http.clone(),
                format!("{tc_url}/api/dtmsvr/query?gid={prefix}-{i}"),
            );
            let is_dtm = target == "dtm";
            tasks.push(tokio::spawn(async move {
                let _p = permit;
                let Ok(r) = http.get(&url).send().await else {
                    return false;
                };
                let Ok(v) = r.json::<serde_json::Value>().await else {
                    return false;
                };
                // DTM 把事务包在 transaction 里，我们是平铺的
                let st = if is_dtm {
                    v["transaction"]["status"].as_str().unwrap_or("")
                } else {
                    v["status"].as_str().unwrap_or("")
                };
                st == "succeed" || st == "failed"
            }));
        }
        for t in tasks {
            if t.await.unwrap_or(false) {
                ok += 1
            } else {
                other += 1
            }
        }
        if ok == n as u64 || Instant::now() > deadline {
            return (ok, format!("{ok}/{n} 落终态，{other} 未终结"));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 把 Call 列表压成便于断言的形状：("tc", 路径) 或 ("busi", url)
    fn shape(calls: &[Call]) -> Vec<(&'static str, String)> {
        calls
            .iter()
            .map(|c| match c {
                Call::Tc(p, _) => ("tc", p.to_string()),
                Call::Busi(u) => ("busi", u.clone()),
            })
            .collect()
    }

    fn body_of(calls: &[Call], path: &str) -> String {
        calls
            .iter()
            .find_map(|c| match c {
                Call::Tc(p, b) if *p == path => Some(b.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("没有打到 {path} 的请求"))
    }

    // ============ 协议形状 ============
    //
    // 这几个断言就是压测结论的地基：各模式的吞吐差异**主要来自客户端往返次数**，
    // 数错一次往返，整篇对比文章的结论就是错的。

    #[test]
    fn saga只有一次往返() {
        let c = client_calls("saga", "g1", 2, "dtmrs", "");
        assert_eq!(shape(&c), vec![("tc", "/api/dtmsvr/submit".into())]);
        let b = body_of(&c, "/api/dtmsvr/submit");
        assert!(b.contains("\"action\""), "saga 要带 action: {b}");
        assert!(b.contains("\"compensate\""), "saga 要带 compensate: {b}");
    }

    #[test]
    fn msg是prepare加submit两次往返() {
        let c = client_calls("msg", "g2", 1, "dtmrs", "");
        assert_eq!(
            shape(&c),
            vec![
                ("tc", "/api/dtmsvr/prepare".into()),
                ("tc", "/api/dtmsvr/submit".into()),
            ]
        );
        assert!(
            body_of(&c, "/api/dtmsvr/prepare").contains("query_prepared"),
            "msg 的 prepare 必须带回查地址 —— 客户端崩在中间时 TC 靠它决断"
        );
    }

    #[test]
    fn tcc每个分支都是先登记再try() {
        // 顺序反了会导致 try 成功但 TC 不知道有这个分支 → 资源永久泄漏。
        // 这是「绝对不能破坏的语义」第 5 条，压测工具也必须照做，
        // 否则测出来的往返次数是假的。
        let c = client_calls("tcc", "g3", 2, "dtmrs", "");
        assert_eq!(
            shape(&c),
            vec![
                ("tc", "/api/dtmsvr/prepare".into()),
                ("tc", "/api/dtmsvr/registerBranch".into()),
                ("busi", format!("http://127.0.0.1:{BUSI_PORT}/t1")),
                ("tc", "/api/dtmsvr/registerBranch".into()),
                ("busi", format!("http://127.0.0.1:{BUSI_PORT}/t2")),
                ("tc", "/api/dtmsvr/submit".into()),
            ],
            "每个分支必须是「先 registerBranch 再打业务的 try」"
        );
    }

    #[test]
    fn tcc的try和confirm必须是不同路径() {
        // ⚠ 这条不是洁癖：假业务服务靠**命中 /a{steps}** 数完成笔数。
        // 如果 try 和 confirm 共用一个路径，try 会被算进完成数，
        // 完成判定当场失效 —— 吞吐会虚高一倍且不报错。
        let c = client_calls("tcc", "g4", 1, "dtmrs", "");
        let rb = body_of(&c, "/api/dtmsvr/registerBranch");
        let v: serde_json::Value = serde_json::from_str(&rb).unwrap();
        let (t, cf) = (v["try"].as_str().unwrap(), v["confirm"].as_str().unwrap());
        assert_ne!(t, cf, "try 和 confirm 共用路径会让完成判定失效");
        assert!(cf.ends_with("/a1"), "confirm 必须落在计数路径 /a1 上: {cf}");
        assert!(t.ends_with("/t1"), "try 必须避开计数路径: {t}");
    }

    #[test]
    fn xa的一阶段打业务服务而不是tc() {
        // XA 的一阶段是分支自己去 registerBranch + PREPARE TRANSACTION，
        // 客户端只负责 prepare 和 submit
        let c = client_calls("xa", "g5", 1, "dtmrs", "http://rm:9999");
        assert_eq!(
            shape(&c),
            vec![
                ("tc", "/api/dtmsvr/prepare".into()),
                ("busi", "http://rm:9999/xa1?gid=g5&branch_id=01".into()),
                ("tc", "/api/dtmsvr/submit".into()),
            ]
        );
    }

    #[test]
    fn 分支号从01开始且补零() {
        // branch_id 是 "{:02}"，跟 TC 侧的 branch_id() 一致。
        // 不补零会让 "1" 和 "01" 变成两个分支
        let c = client_calls("tcc", "g6", 3, "dtmrs", "");
        let ids: Vec<String> = c
            .iter()
            .filter_map(|x| match x {
                Call::Tc("/api/dtmsvr/registerBranch", b) => {
                    let v: serde_json::Value = serde_json::from_str(b).unwrap();
                    Some(v["branch_id"].as_str().unwrap().to_string())
                }
                _ => None,
            })
            .collect();
        assert_eq!(ids, vec!["01", "02", "03"]);
    }

    // ============ DTM 的报文格式 ============

    #[test]
    fn dtm的saga要额外带等长的payloads() {
        // DTM 的协议要求 payloads 跟 steps 等长，少了会被拒
        let c = client_calls("saga", "g7", 3, "dtm", "");
        assert_eq!(shape(&c), vec![("tc", "/api/dtmsvr/submit".into())]);
        let v: serde_json::Value = serde_json::from_str(&body_of(&c, "/api/dtmsvr/submit")).unwrap();
        assert_eq!(v["payloads"].as_array().unwrap().len(), 3);
        assert_eq!(v["steps"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn dtm的msg两次发的是同一份报文() {
        // DTM 的 msg 是 prepare 和 submit 发同样的 body
        let c = client_calls("msg", "g8", 2, "dtm", "");
        assert_eq!(shape(&c).len(), 2);
        assert_eq!(
            body_of(&c, "/api/dtmsvr/prepare"),
            body_of(&c, "/api/dtmsvr/submit")
        );
    }

    // ============ 端口选择 ============

    #[test]
    fn 监听端口必须避开临时端口范围() {
        // 落在 ip_local_port_range（通常 32768-60999）里的服务端口会**偶发**
        // 起不来，报 Address already in use 但 ss 显示没人在听 ——
        // 那是某个出站连接临时占用了它。偶发性正是它难查的原因。
        for (名字, 端口) in [("BUSI", BUSI_PORT), ("TC", TC_PORT), ("DTM", DTM_PORT)] {
            assert!(
                !(32768..=60999).contains(&端口),
                "{名字} 端口 {端口} 落在临时端口范围里，会偶发起不来"
            );
        }
    }
}
