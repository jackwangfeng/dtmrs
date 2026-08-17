//! 管理台的登录保护。
//!
//! # 为什么保护范围是「除了 /health 之外全部」
//!
//! 管理台那个 HTML 页面本身没什么可保护的 —— 真正危险的是它调的接口：
//! `abort` 能中止在途事务、`retry` 能改调度、`submit` 能凭空造事务。
//! **只给页面加登录而把 `/api/dtmsvr/*` 敞着，等于没加。**
//! 所以这里是全局中间件，白名单只有 `/health`（反向代理的健康检查要用）
//! 和 `/login` 本身。
//!
//! # 没配密码时不启用
//!
//! `DTMRS_ADMIN_PASSWORD` 没设就完全不拦 —— 内网/本地开发的用法不变。
//! 但**一旦你打算暴露到公网，这个变量就是必须的**，`main.rs` 在监听
//! 非回环地址且没配密码时会打醒目警告。
//!
//! # 会话存在内存里
//!
//! 单进程 TC，没必要引入签名/JWT 那一套。代价是**重启后所有人要重新登录**，
//! 对管理台来说完全可以接受。多实例部署时各实例的会话不互通，
//! 前面挂负载均衡的话要开会话保持（或者干脆每个实例单独登录）。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::Form;
use serde::Deserialize;

/// 会话有效期。管理台是低频操作，给长一点省得老登录
const SESSION_TTL: Duration = Duration::from_secs(12 * 3600);
const COOKIE: &str = "dtmrs_session";

/// 托管令牌的缓存刷新间隔。**作废最长这么久才生效** ——
/// 认证在热路径上（几万 QPS），每请求查一次库会让认证开销盖过事务本身。
///
/// 多实例部署时每个实例各自刷新，所以延迟是各自独立的、不会叠加。
const TOKEN_CACHE_TTL: Duration = Duration::from_secs(10);

pub struct Auth {
    /// 管理台登录用。为空表示不提供登录页（只用 token 认证）
    user: String,
    password: String,
    /// 业务端用的静态令牌。**服务之间调用不该走登录表单+cookie**，
    /// 那是给浏览器设计的。为空表示不接受 token 认证
    token: String,
    /// 会话 token -> 过期时刻
    sessions: Mutex<HashMap<String, Instant>>,
    /// 托管令牌：存储层是权威，这里只是热路径上的缓存。
    /// `(有效哈希集合, 上次刷新时刻)`
    managed: Mutex<(std::collections::HashSet<String>, Instant)>,
    /// 拿托管令牌要用的存储句柄。`None` 表示只用 env 里那个静态 token
    store: Option<dtmrs_store::Store>,
}

impl Auth {
    /// 密码为空返回 `None` —— 调用方据此决定「不启用认证」
    /// 两个都没配返回 `None` —— 调用方据此决定「不启用认证」。
    ///
    /// 两种凭据是**并列**的，满足任一即放行：
    /// - `DTMRS_ADMIN_PASSWORD`：浏览器登录管理台，拿会话 cookie
    /// - `DTMRS_AUTH_TOKEN`：业务服务/SDK 带 `Authorization: Bearer <token>`
    pub fn from_env() -> Option<Arc<Self>> {
        let password = std::env::var("DTMRS_ADMIN_PASSWORD").unwrap_or_default();
        let token = std::env::var("DTMRS_AUTH_TOKEN").unwrap_or_default();
        if password.is_empty() && token.is_empty() {
            return None;
        }
        Some(Arc::new(Self {
            user: std::env::var("DTMRS_ADMIN_USER").unwrap_or_else(|_| "admin".into()),
            password,
            token,
            sessions: Mutex::new(HashMap::new()),
            // 初始时刻设成很久以前，保证第一次校验必定去刷一遍
            managed: Mutex::new((
                std::collections::HashSet::new(),
                Instant::now() - TOKEN_CACHE_TTL * 2,
            )),
            store: None,
        }))
    }

    /// 接上存储，启用「管理台可增删的托管令牌」。
    /// 不接的话只有 `DTMRS_AUTH_TOKEN` 那个静态令牌有效
    pub fn with_store(mut self: Arc<Self>, store: dtmrs_store::Store) -> Arc<Self> {
        // 刚构造出来还没共享，这里一定能拿到独占引用
        if let Some(me) = Arc::get_mut(&mut self) {
            me.store = Some(store);
        }
        self
    }

    /// 校验托管令牌。缓存过期就先刷一遍。
    ///
    /// 命中之后**顺手记一次使用**（异步 spawn，失败只吞掉）——
    /// 统计信息不值得让一次正常的业务调用失败或变慢。
    pub async fn managed_ok(&self, presented: &str, ip: &str) -> bool {
        let Some(store) = self.store.clone() else {
            return false;
        };
        let hash = dtmrs_store::hash_token(presented);

        let need_refresh = {
            let g = self.managed.lock().unwrap();
            g.1.elapsed() >= TOKEN_CACHE_TTL
        };
        if need_refresh {
            if let Ok(list) = store.active_token_hashes().await {
                let mut g = self.managed.lock().unwrap();
                *g = (list.into_iter().collect(), Instant::now());
            }
        }
        let hit = {
            let g = self.managed.lock().unwrap();
            g.0.contains(&hash)
        };
        if hit {
            let (s, h, ip) = (store, hash.clone(), ip.to_string());
            tokio::spawn(async move {
                let _ = s.touch_token(&h, &ip).await;
            });
        }
        hit
    }

    /// 作废之后立刻让缓存失效，省得等最多 10 秒
    pub fn invalidate_cache(&self) {
        let mut g = self.managed.lock().unwrap();
        g.1 = Instant::now() - TOKEN_CACHE_TTL * 2;
    }

    /// 配了密码才提供登录页
    pub fn has_login(&self) -> bool {
        !self.password.is_empty()
    }

    /// 校验业务端的 Bearer token。定长时间比较，理由同 `matches`
    pub fn token_ok(&self, presented: &str) -> bool {
        if self.token.is_empty() {
            return false;
        }
        let (a, b) = (presented.as_bytes(), self.token.as_bytes());
        let mut diff = a.len() ^ b.len();
        for i in 0..a.len().max(b.len()) {
            diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(1));
        }
        diff == 0
    }

    /// 从 `Authorization: Bearer xxx` 里取 token。gRPC 侧的 metadata 同名，复用
    pub fn bearer(v: &str) -> Option<&str> {
        let v = v.trim();
        v.strip_prefix("Bearer ")
            .or_else(|| v.strip_prefix("bearer "))
            .map(str::trim)
    }

    /// ⚠ 定长时间比较。管理台密码通常不长，朴素的 `==` 会随前缀匹配长度
    /// 提前返回，理论上能被逐字节试出来
    fn matches(&self, user: &str, password: &str) -> bool {
        let a = user.as_bytes();
        let b = self.user.as_bytes();
        let c = password.as_bytes();
        let d = self.password.as_bytes();
        let mut diff = (a.len() ^ b.len()) | (c.len() ^ d.len());
        for i in 0..a.len().max(b.len()) {
            diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(1));
        }
        for i in 0..c.len().max(d.len()) {
            diff |= usize::from(c.get(i).copied().unwrap_or(0) ^ d.get(i).copied().unwrap_or(1));
        }
        diff == 0
    }

    fn issue(&self) -> String {
        use rand::Rng;
        let raw: [u8; 32] = rand::thread_rng().gen();
        let token: String = raw.iter().map(|b| format!("{b:02x}")).collect();
        let mut s = self.sessions.lock().unwrap();
        let now = Instant::now();
        s.retain(|_, exp| *exp > now); // 顺手清过期的，省得无限涨
        s.insert(token.clone(), now + SESSION_TTL);
        token
    }

    fn valid(&self, token: &str) -> bool {
        let mut s = self.sessions.lock().unwrap();
        match s.get(token) {
            Some(exp) if *exp > Instant::now() => true,
            Some(_) => {
                s.remove(token);
                false
            }
            None => false,
        }
    }

    fn revoke(&self, token: &str) {
        self.sessions.lock().unwrap().remove(token);
    }
}

/// 取调用方 IP。走反代时真实 IP 在 `X-Forwarded-For` 的第一段；
/// **不能信任它做安全判断**，这里只用于展示「最近谁在用这个令牌」
fn client_ip(req: &Request<Body>) -> String {
    req.headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| "-".into())
}

fn cookie_of(req: &Request<Body>) -> Option<String> {
    req.headers()
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| k.trim() == COOKIE)
        .map(|(_, v)| v.trim().to_string())
}

/// 走反向代理时协议看 `X-Forwarded-Proto`；直连 http 的话不能加 Secure，
/// 否则浏览器根本不会回传这个 cookie
fn is_https(req_headers: &axum::http::HeaderMap) -> bool {
    req_headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("https"))
        .unwrap_or(false)
}

/// 全局中间件：除 `/health` 和 `/login` 外都要求已登录。
///
/// 浏览器来的（Accept 含 text/html）跳转到登录页；
/// 接口调用返回 401，不做跳转 —— 让 curl / SDK 拿到明确的状态码。
pub async fn guard(
    State(auth): State<Arc<Auth>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let path = req.uri().path();
    if path == "/health" || path == "/login" || path == "/logout" {
        return next.run(req).await;
    }
    // 业务端：Authorization: Bearer <token>
    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(Auth::bearer)
        .map(str::to_string);
    if let Some(t) = &presented {
        // 先比 env 里的静态令牌（引导用，不查库）
        if auth.token_ok(t) {
            return next.run(req).await;
        }
        // 再看管理台发的托管令牌（走 10 秒 TTL 缓存，不是每次查库）
        let ip = client_ip(&req);
        if auth.managed_ok(t, &ip).await {
            return next.run(req).await;
        }
    }
    // 浏览器：会话 cookie
    if cookie_of(&req).is_some_and(|t| auth.valid(&t)) {
        return next.run(req).await;
    }
    let wants_html = req
        .headers()
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("text/html"));
    if wants_html {
        Redirect::to("/login").into_response()
    } else {
        (StatusCode::UNAUTHORIZED, "需要登录").into_response()
    }
}

#[derive(Deserialize)]
pub struct LoginForm {
    user: String,
    password: String,
}

pub async fn login_page() -> Html<&'static str> {
    Html(include_str!("login.html"))
}

pub async fn login_submit(
    State(auth): State<Arc<Auth>>,
    headers: axum::http::HeaderMap,
    Form(f): Form<LoginForm>,
) -> Response {
    if !auth.has_login() || !auth.matches(&f.user, &f.password) {
        // 不区分「用户名不存在」和「密码错误」—— 那等于告诉对方用户名猜对了
        return (
            StatusCode::UNAUTHORIZED,
            Html(include_str!("login.html").replace(
                "<!--ERR-->",
                r#"<p class="err">用户名或密码不对</p>"#,
            )),
        )
            .into_response();
    }
    let token = auth.issue();
    let secure = if is_https(&headers) { "; Secure" } else { "" };
    let cookie = format!(
        "{COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}{secure}",
        SESSION_TTL.as_secs()
    );
    ([(header::SET_COOKIE, cookie)], Redirect::to("/console")).into_response()
}

pub async fn logout(State(auth): State<Arc<Auth>>, req: Request<Body>) -> Response {
    if let Some(t) = cookie_of(&req) {
        auth.revoke(&t);
    }
    (
        [(
            header::SET_COOKIE,
            format!("{COOKIE}=; Path=/; HttpOnly; Max-Age=0"),
        )],
        Redirect::to("/login"),
    )
        .into_response()
}

// ==================== 令牌管理接口 ====================
//
// ⚠ 这几个接口**只给管理台用**，必须走会话 cookie。
// 允许用 token 去增删 token 会形成提权链：一个泄漏的业务令牌可以自己
// 再签发一批，作废原来那个也没用。所以这里额外要求「必须是会话登录」。

#[derive(serde::Serialize)]
pub struct TokenView {
    /// 只回哈希的前 12 位，够用来在列表里认人，又不至于把完整哈希摊出去
    pub id: String,
    pub name: String,
    pub create_time: i64,
    /// 0 = 从没用过
    pub last_used: i64,
    pub use_count: i64,
    pub last_ip: String,
    pub revoked: i64,
}

/// 只有会话 cookie 能过 —— 见本节开头的提权说明
fn require_session(auth: &Auth, req: &Request<Body>) -> bool {
    cookie_of(req).is_some_and(|t| auth.valid(&t))
}

pub async fn tokens_list(
    State((auth, store)): State<(Arc<Auth>, dtmrs_store::Store)>,
    req: Request<Body>,
) -> Response {
    if !require_session(&auth, &req) {
        return (StatusCode::FORBIDDEN, "令牌管理只能在管理台里操作").into_response();
    }
    match store.list_tokens().await {
        Ok(list) => axum::Json(
            list.into_iter()
                .map(|t| TokenView {
                    id: t.token_hash.chars().take(12).collect(),
                    name: t.name,
                    create_time: t.create_time,
                    last_used: t.last_used,
                    use_count: t.use_count,
                    last_ip: t.last_ip,
                    revoked: t.revoked,
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
pub struct CreateTokenReq {
    #[serde(default)]
    pub name: String,
}

pub async fn tokens_create(
    State((auth, store)): State<(Arc<Auth>, dtmrs_store::Store)>,
    req: Request<Body>,
) -> Response {
    if !require_session(&auth, &req) {
        return (StatusCode::FORBIDDEN, "令牌管理只能在管理台里操作").into_response();
    }
    let body = axum::body::to_bytes(req.into_body(), 64 * 1024)
        .await
        .unwrap_or_default();
    let name = serde_json::from_slice::<CreateTokenReq>(&body)
        .map(|r| r.name)
        .unwrap_or_default();
    let name = if name.trim().is_empty() {
        "未命名".to_string()
    } else {
        name.trim().to_string()
    };

    // 24 字节 = 192 位熵，爆破不可行
    use rand::Rng;
    let raw: [u8; 24] = rand::thread_rng().gen();
    let token: String = raw.iter().map(|b| format!("{b:02x}")).collect();

    match store.create_token(&dtmrs_store::hash_token(&token), &name).await {
        Ok(()) => {
            auth.invalidate_cache();
            // ⚠ 明文**只在这里返回一次**，库里只有哈希。丢了只能重新生成
            axum::Json(serde_json::json!({ "token": token, "name": name })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
pub struct RevokeReq {
    pub id: String,
}

pub async fn tokens_revoke(
    State((auth, store)): State<(Arc<Auth>, dtmrs_store::Store)>,
    req: Request<Body>,
) -> Response {
    if !require_session(&auth, &req) {
        return (StatusCode::FORBIDDEN, "令牌管理只能在管理台里操作").into_response();
    }
    let body = axum::body::to_bytes(req.into_body(), 64 * 1024)
        .await
        .unwrap_or_default();
    let Ok(r) = serde_json::from_slice::<RevokeReq>(&body) else {
        return (StatusCode::BAD_REQUEST, "缺少 id").into_response();
    };
    // 前端拿到的是哈希前缀，这里要还原成完整哈希
    let full = match store.list_tokens().await {
        Ok(list) => list.into_iter().find(|t| t.token_hash.starts_with(&r.id)),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    let Some(t) = full else {
        return (StatusCode::NOT_FOUND, "没有这个令牌").into_response();
    };
    match store.revoke_token(&t.token_hash).await {
        Ok(done) => {
            auth.invalidate_cache(); // 立刻生效，不用等 10 秒
            axum::Json(serde_json::json!({ "revoked": done })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
