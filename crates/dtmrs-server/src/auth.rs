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

pub struct Auth {
    user: String,
    password: String,
    /// token -> 过期时刻
    sessions: Mutex<HashMap<String, Instant>>,
}

impl Auth {
    /// 密码为空返回 `None` —— 调用方据此决定「不启用认证」
    pub fn from_env() -> Option<Arc<Self>> {
        let password = std::env::var("DTMRS_ADMIN_PASSWORD").unwrap_or_default();
        if password.is_empty() {
            return None;
        }
        Some(Arc::new(Self {
            user: std::env::var("DTMRS_ADMIN_USER").unwrap_or_else(|_| "admin".into()),
            password,
            sessions: Mutex::new(HashMap::new()),
        }))
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
    if !auth.matches(&f.user, &f.password) {
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
