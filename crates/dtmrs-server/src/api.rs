//! TC 的对外操作，**与协议无关**。
//!
//! HTTP 和 gRPC 两套接口都只做「协议 ↔ 这一层」的转换，业务判断全在这里。
//! 分成两处写迟早会漂移 —— 而这一层漂移的后果是「同一个请求走 HTTP 被拒、
//! 走 gRPC 却受理了」，这种不一致在事务系统里是要命的。
//!
//! 错误用 [`ApiError`] 表达，由各协议层翻译成自己的表示：
//!
//! | ApiError | HTTP | gRPC |
//! |---|---|---|
//! | `BadRequest` | 400 | `INVALID_ARGUMENT` |
//! | `NotFound` | 404 | `NOT_FOUND` |
//! | `Conflict` | 200 + `dtm_result=FAILURE` | `FAILED_PRECONDITION` |
//! | `Internal` | 500 | `INTERNAL` |
//!
//! `Conflict` 在 HTTP 上返回 200 是**刻意保留的历史行为**（已终结的事务再调
//! abort），换成 4xx 会打破现有客户端。

use crate::driver;
use crate::{msg_rows_expanded, saga_rows, tcc_rows_with_timeout};
use dtmrs_core::{expand_msg_steps, BranchOp, GlobalStatus, SagaStep, TransType, MSG_TOPIC_PREFIX};
use dtmrs_store::{Store, SubmitOutcome, TopicSub};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    BadRequest(String),
    NotFound(String),
    /// 请求本身合法，但当前状态下做不了
    Conflict(String),
    Internal(String),
}

impl ApiError {
    pub fn message(&self) -> &str {
        match self {
            Self::BadRequest(m) | Self::NotFound(m) | Self::Conflict(m) | Self::Internal(m) => m,
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for ApiError {}

pub type Result<T> = std::result::Result<T, ApiError>;

fn internal(e: impl std::fmt::Display) -> ApiError {
    ApiError::Internal(e.to_string())
}

#[derive(Debug, Clone, Serialize)]
pub struct BranchView {
    pub branch_id: String,
    pub op: String,
    pub url: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransView {
    pub gid: String,
    pub trans_type: String,
    pub status: String,
    pub rollback_reason: String,
    pub create_time: i64,
    pub finish_time: Option<i64>,
    /// 退避重试了多少轮。一直涨说明卡住了（confirm 失败只能重试、订阅方一直失败……）
    pub retry_count: i64,
    /// 下次推进的时刻（unix 秒）
    pub next_cron_time: i64,
    pub branches: Vec<BranchView>,
}

/// 分支登记请求。TCC 用 confirm/cancel，XA 用 commit/rollback。
#[derive(Debug, Clone, Default)]
pub struct RegisterBranch {
    pub gid: String,
    pub branch_id: String,
    pub confirm: String,
    pub cancel: String,
    pub r#try: String,
    pub commit: String,
    pub rollback: String,
    /// 分支的业务数据（同 DTM registerBranch 的 `data`），TCC 的 confirm / cancel
    /// 收到的请求体就是它 —— 二阶段要知道 try 冻结了什么，不用业务方自己另建表回查。
    /// 空表示不带（分支收到 `{}`）。重复登记以第一次的为准
    pub data: String,
}

/// msg prepare 的可选项（DTM 协议之外的扩展，都有默认值）
#[derive(Debug, Clone, Default)]
pub struct PrepareOpts {
    /// 每个 action 对应的请求体。空表示都不带（分支收到 `{}`）；
    /// 非空时必须跟 actions 一样长。`topic://` 的那一步，所有订阅者收到同一份
    pub payloads: Vec<String>,
    /// 主题没有订阅者时放行（展开成 0 个分支）而不是报 `topic not found`。
    /// 给「晚到或漏一次可以接受、但不能挡住业务」的通知类消息用 ——
    /// 发送方往往是在业务本地事务里 prepare 的，报错会把业务本身也带失败
    pub allow_empty_topic: bool,
    /// tcc / xa 用：停在 prepared 多少秒没人 submit / abort 就由 TC 回滚（同 DTM 的
    /// `timeout_to_fail`）。0 = 全局默认（`DTMRS_TIMEOUT_TO_FAIL`，默认 35）
    pub timeout_to_fail: i64,
}

#[derive(Clone)]
pub struct Api {
    pub store: Store,
    /// 提交后**直接开推**用的推进器。`None` 就是老行为：写完就返回，
    /// 等推进器自己抢到再推。见 [`Api::with_inline_driver`]
    inline: Option<crate::driver::Driver>,
    /// 代码里静态登记的订阅（嵌入式启动时给的）：主题 → 地址。
    /// 跟存储里的订阅取并集，静态的排前面
    static_topics: Arc<BTreeMap<String, Vec<String>>>,
    /// 因为 `allow_empty_topic` 被放行成「没人收」的主题次数（按主题计，不按消息）
    empty_topic: Arc<AtomicU64>,
}

impl Api {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            inline: None,
            static_topics: Arc::new(BTreeMap::new()),
            empty_topic: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 代码里静态登记的订阅。重复的地址去掉
    pub fn with_static_topics(mut self, subs: Vec<(String, String)>) -> Self {
        let mut m: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (t, u) in subs {
            let v = m.entry(t).or_default();
            if !v.contains(&u) {
                v.push(u);
            }
        }
        self.static_topics = Arc::new(m);
        self
    }

    /// `allow_empty_topic` 放行了多少次「主题没有订阅者」。
    /// 不该一直涨：涨了说明订阅方没登记上，消息在靠对账兜底
    pub fn empty_topic_count(&self) -> u64 {
        self.empty_topic.load(Ordering::Relaxed)
    }

    /// 开启「提交后直接开推」。
    ///
    /// # 省掉的是那次抢占往返
    ///
    /// 老流程：提交方写完事务就返回，推进器再 `lock_one_due` 抢一次才能推。
    /// 那次抢占**每笔事务都要付**，在 Redis 上是一次 Lua 往返 —— 实测它就是
    /// saga 落后 DTM 的主要原因（saga 只有一次客户端请求，摊不薄）。
    ///
    /// 新流程：建事务的那条写入里**顺便把租约占在自己手上**
    /// （`owner=自己`、`next_cron_time=现在+租约`），写成功就等于抢到了，
    /// 直接推。零额外往返。
    ///
    /// # 跟 DTM 的差别：我们不阻塞提交
    ///
    /// DTM 是在 submit 请求里同步把事务推完，客户端要一直等。这里是
    /// **spawn 出去推，提交立刻返回** —— 省掉往返的同时保住了提交延迟。
    ///
    /// # 代价
    ///
    /// 租约一占就是 `lease` 秒。如果进程在「写完」和「推完」之间挂了，
    /// 这笔要等租约到期才会被别的实例接手，而不是下一个 tick。
    /// 这跟「推进器抢到之后崩了」是同一种情形，不是新引入的风险。
    pub fn with_inline_driver(mut self, d: crate::driver::Driver) -> Self {
        self.inline = Some(d);
        self
    }

    /// 建事务前把租约字段填上。返回是否真的占了 —— 没开内联就不占。
    fn claim_for_inline(&self, g: &mut dtmrs_store::GlobalRow) -> bool {
        let Some(d) = &self.inline else { return false };
        g.owner = d.owner.clone();
        g.next_cron_time = dtmrs_store::now() + d.lease;
        // 租约单独一列（见 GlobalRow::lease_until）；两者相等是「没人插过队」的标记
        g.lease_until = g.next_cron_time;
        true
    }

    /// 把 prepared 推成 submitted，开了内联就**顺便占下租约**。
    ///
    /// 返回 `Advanced` 时事务体一起带回来了，调用方可以直接 [`Self::drive_detached`]，
    /// 不用再读一次（Redis 是脚本尾巴上的 HGETALL，SQL 是本来就要发的那条 SELECT）
    async fn claim_and_submit(&self, gid: &str) -> Result<SubmitOutcome> {
        let (owner, nct) = match &self.inline {
            Some(d) => (d.owner.clone(), dtmrs_store::now() + d.lease),
            None => (String::new(), dtmrs_store::now()),
        };
        self.store
            .submit_prepared(gid, &owner, nct)
            .await
            .map_err(internal)
    }

    /// 把已经拿到租约的事务扔出去推。**不等它跑完** —— 提交要立刻返回。
    fn drive_detached(&self, g: dtmrs_store::GlobalRow) {
        let Some(d) = self.inline.clone() else { return };
        tokio::spawn(async move {
            if let Err(e) = d.process(&g).await {
                // 推失败不影响提交的结果，租约到期后会被重新捞起来
                tracing::warn!(gid = %g.gid, error = %e, "提交后直接推进出错，等租约到期重试");
            }
        });
    }

    /// 时间戳 + 进程内计数。生产建议客户端直接用业务单号当 gid ——
    /// 那样天然幂等，重试不会变成两笔
    pub fn new_gid(&self) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        format!("{}-{}", dtmrs_store::now(), n)
    }

    /// 提交。
    ///
    /// **重复提交同一个 gid 必须成功而不是报错** —— 客户端网络抖动重试时
    /// 返回错误会让它以为没受理，然后换个 gid 再来一次，就成了两笔。
    pub async fn submit(&self, gid: &str, trans_type: &str, steps: &[SagaStep]) -> Result<()> {
        if gid.is_empty() {
            return Err(ApiError::BadRequest("gid 不能为空".into()));
        }
        let Some(tt) = TransType::parse(trans_type) else {
            return Err(ApiError::BadRequest("未知 trans_type".into()));
        };

        match tt {
            TransType::Saga => {
                if steps.is_empty() {
                    // ⚠ 不带步骤的重复提交**必须幂等成功**，不能因为 steps 为空
                    // 就报错 —— 客户端重试时经常只带 gid。只有事务压根不存在，
                    // 才是真的参数错误。（`tc的grpc_api与http同源` 钉着这条）
                    return match self.claim_and_submit(gid).await? {
                        SubmitOutcome::Advanced(g) => {
                            self.drive_detached(*g);
                            Ok(())
                        }
                        SubmitOutcome::Already => Ok(()),
                        SubmitOutcome::Missing => {
                            Err(ApiError::BadRequest("saga 的 steps 不能为空".into()))
                        }
                    };
                }
                let (mut g, branches) = saga_rows(gid, steps);
                // 开了内联推进的话，这条写入顺便把租约占下来，写成功就直接推，
                // 不用再走一次抢占（见 `with_inline_driver`）
                let claimed = self.claim_for_inline(&mut g);
                // **先建，不先查。** `create_global` 本身就是幂等的（已存在返回
                // false 且不覆盖），所以正常路径一次往返就够 —— 这是 saga 提交的
                // 热路径，先查一次等于白付一次往返，而且那还是个 Lua 脚本调用，
                // 比普通命令贵得多（实测多这一次让 saga 吞吐掉了 16%）
                if self
                    .store
                    .create_global(&g, &branches)
                    .await
                    .map_err(internal)?
                {
                    if claimed {
                        self.drive_detached(g);
                    }
                    return Ok(());
                }
                // 已存在。可能是重复提交（幂等返回成功就行），也可能是这个 gid
                // 其实是 prepare 过的 tcc/msg/xa —— 客户端没传 trans_type 时
                // 会被当成 saga。后一种要真的把它推成 submitted，交给下面决断
                if let SubmitOutcome::Advanced(g) = self.claim_and_submit(gid).await? {
                    self.drive_detached(*g);
                }
                Ok(())
            }
            TransType::Tcc | TransType::Msg | TransType::Xa => {
                // prepare 已经建过事务，submit 只是把它推成 submitted。
                //
                // **一次存储调用做完**（原来是 get_global + set_global_status
                // + schedule_now 三次）。Redis 上这三次是 11 条命令，现在 3 条
                match self.claim_and_submit(gid).await? {
                    // 推成 submitted 了。开了内联就直接推 —— 跟 saga 一样，
                    // 省掉那次抢占往返。事务体是 submit_prepared 顺带返回的，
                    // 没有多付一次读
                    SubmitOutcome::Advanced(g) => {
                        self.drive_detached(*g);
                        Ok(())
                    }
                    // 已经提交过 —— 幂等返回成功
                    SubmitOutcome::Already => Ok(()),
                    SubmitOutcome::Missing => {
                        Err(ApiError::BadRequest("tcc/xa/msg 要先调 prepare".into()))
                    }
                }
            }
            // workflow 的「步骤」是**代码**，没法表示成 URL 存进库里，
            // 所以只能在嵌入式形态下提交（Embedded::workflow + submit_workflow）。
            // 这不是暂未实现，是这个模式的本质决定的
            TransType::Workflow => Err(ApiError::BadRequest(
                "workflow 模式只能在嵌入式形态下提交（步骤是进程内的函数，不是 URL）".into(),
            )),
        }
    }

    // ---------------- 主题订阅（对齐 DTM 的 subscribe / unsubscribe / topic） ----------------
    //
    // 错误文案照抄 DTM（`dtmsvr/topics.go`），客户端可能按文案判断。
    // 状态码跟 DTM 不同：DTM 一律 500，这里参数错 400、找不到 404 ——
    // 都是非 2xx，按「失败」处理的客户端行为不变。

    /// 一个主题当前的全部订阅地址：静态的在前，存储里的在后，去重
    pub async fn topic_urls(&self, topic: &str) -> Result<Vec<String>> {
        let mut urls = self.static_topics.get(topic).cloned().unwrap_or_default();
        for s in self
            .store
            .list_subscriptions(Some(topic))
            .await
            .map_err(internal)?
        {
            if !urls.contains(&s.url) {
                urls.push(s.url);
            }
        }
        Ok(urls)
    }

    pub async fn subscribe(&self, topic: &str, url: &str, remark: &str) -> Result<()> {
        if topic.is_empty() {
            return Err(ApiError::BadRequest("empty topic".into()));
        }
        if url.is_empty() {
            return Err(ApiError::BadRequest("empty url".into()));
        }
        if url.starts_with(MSG_TOPIC_PREFIX) {
            // 订阅者必须是能调的地址；主题套主题会在展开时变成一个打不通的分支
            return Err(ApiError::BadRequest("订阅地址不能是 topic://".into()));
        }
        if self.is_static(topic, url) {
            return Err(ApiError::BadRequest("this url exists".into()));
        }
        if self
            .store
            .subscribe(topic, url, remark)
            .await
            .map_err(|e| ApiError::BadRequest(e.to_string()))?
        {
            Ok(())
        } else {
            Err(ApiError::BadRequest("this url exists".into()))
        }
    }

    pub async fn unsubscribe(&self, topic: &str, url: &str) -> Result<()> {
        if topic.is_empty() {
            return Err(ApiError::BadRequest("empty topic".into()));
        }
        if url.is_empty() {
            return Err(ApiError::BadRequest("empty url".into()));
        }
        if self.is_static(topic, url) {
            return Err(ApiError::BadRequest(
                "这是代码里静态登记的订阅，只能改代码去掉".into(),
            ));
        }
        if self.store.unsubscribe(topic, url).await.map_err(internal)? {
            return Ok(());
        }
        let exists = !self
            .store
            .list_subscriptions(Some(topic))
            .await
            .map_err(internal)?
            .is_empty();
        Err(ApiError::NotFound(if exists {
            // 末尾空格是 DTM 原文
            "no such an url ".into()
        } else {
            "no such a topic".into()
        }))
    }

    /// 删整个主题（只删存储里的订阅；静态登记的在代码里，删不掉也不该删）
    pub async fn delete_topic(&self, topic: &str) -> Result<()> {
        if topic.is_empty() {
            return Err(ApiError::BadRequest("empty topic".into()));
        }
        match self.store.delete_topic(topic).await.map_err(internal)? {
            0 => Err(ApiError::NotFound("storage: NotFound".into())),
            _ => Ok(()),
        }
    }

    /// 列订阅（`topic` 为空列全部）。静态登记的也列出来，备注是 `(static)`、
    /// 登记时间是 0 —— 排查「消息发给了谁」时要看全
    pub async fn list_topic_subs(&self, topic: Option<&str>) -> Result<Vec<TopicSub>> {
        let mut out: Vec<TopicSub> = Vec::new();
        for (t, urls) in self.static_topics.iter() {
            if topic.is_some_and(|x| x != t) {
                continue;
            }
            out.extend(urls.iter().map(|u| TopicSub {
                topic: t.clone(),
                url: u.clone(),
                remark: "(static)".into(),
                create_time: 0,
            }));
        }
        for s in self
            .store
            .list_subscriptions(topic)
            .await
            .map_err(internal)?
        {
            if !self.is_static(&s.topic, &s.url) {
                out.push(s);
            }
        }
        // 稳定排序：同一主题内保持「静态在前、再按登记先后」
        out.sort_by(|a, b| a.topic.cmp(&b.topic));
        Ok(out)
    }

    fn is_static(&self, topic: &str, url: &str) -> bool {
        self.static_topics
            .get(topic)
            .is_some_and(|v| v.iter().any(|u| u == url))
    }

    /// 第一阶段。msg 建 prepared 事务 + 正向分支；tcc / xa 只建空事务。
    pub async fn prepare(
        &self,
        gid: &str,
        trans_type: &str,
        actions: &[String],
        query_prepared: &str,
        grace_secs: Option<i64>,
    ) -> Result<()> {
        self.prepare_with(
            gid,
            trans_type,
            actions,
            query_prepared,
            grace_secs,
            &PrepareOpts::default(),
        )
        .await
    }

    /// 同 [`prepare`](Self::prepare)，多了 msg 的 payload 和「主题允许为空」。
    ///
    /// `topic://名字` 的 action **在这里展开**成订阅者（同 DTM：订阅关系在
    /// prepare 那一刻定格，之后的订阅 / 退订不影响这条消息，也不补发历史消息）。
    pub async fn prepare_with(
        &self,
        gid: &str,
        trans_type: &str,
        actions: &[String],
        query_prepared: &str,
        grace_secs: Option<i64>,
        opts: &PrepareOpts,
    ) -> Result<()> {
        if gid.is_empty() {
            return Err(ApiError::BadRequest("gid 不能为空".into()));
        }
        match TransType::parse(trans_type) {
            Some(TransType::Msg) => {
                if actions.is_empty() {
                    return Err(ApiError::BadRequest("msg 的 actions 不能为空".into()));
                }
                if query_prepared.is_empty() {
                    // 没有回查地址，客户端崩在 prepare 和 submit 之间就没人能
                    // 决断这单了。猜「已提交」会重复扣款，猜「没提交」会丢单
                    return Err(ApiError::BadRequest(
                        "msg 必须提供 query_prepared，否则崩溃后无法决断".into(),
                    ));
                }
                // 先把涉及的主题都查好，展开本身是 core 里的纯函数
                let mut subs: BTreeMap<String, Vec<String>> = BTreeMap::new();
                for a in actions {
                    if let Some(t) = a.strip_prefix(MSG_TOPIC_PREFIX) {
                        if !t.is_empty() && !subs.contains_key(t) {
                            subs.insert(t.to_string(), self.topic_urls(t).await?);
                        }
                    }
                }
                let exp = expand_msg_steps(
                    actions,
                    &opts.payloads,
                    |t| subs.get(t).cloned().unwrap_or_default(),
                    opts.allow_empty_topic,
                )
                .map_err(ApiError::BadRequest)?;
                for t in &exp.empty_topics {
                    self.empty_topic.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        gid,
                        topic = %t,
                        "主题没有订阅者，按 allow_empty_topic 放行：这一步没有人会收到消息"
                    );
                }
                let (g, br) =
                    msg_rows_expanded(gid, &exp.branches, query_prepared, grace_secs.unwrap_or(10));
                self.store.create_global(&g, &br).await.map_err(internal)?;
                Ok(())
            }
            Some(tt @ (TransType::Tcc | TransType::Xa)) => {
                if opts.timeout_to_fail < 0 {
                    return Err(ApiError::BadRequest("timeout_to_fail 不能是负数".into()));
                }
                let mut g = tcc_rows_with_timeout(gid, opts.timeout_to_fail);
                g.trans_type = tt;
                self.store.create_global(&g, &[]).await.map_err(internal)?;
                Ok(())
            }
            _ => Err(ApiError::BadRequest(
                "prepare 支持 tcc / xa / msg；saga 直接 submit".into(),
            )),
        }
    }

    /// 分支登记。**必须先登记再做一阶段**：反过来的话一阶段成功但登记失败，
    /// TC 就不知道有这个分支，回滚时不会处理它 —— TCC 是预留资源永久泄漏，
    /// XA 更糟，会留下一个永久持锁的 prepared 事务。
    pub async fn register_branch(&self, r: &RegisterBranch) -> Result<()> {
        if r.gid.is_empty() || r.branch_id.is_empty() {
            return Err(ApiError::BadRequest("gid / branch_id 不能为空".into()));
        }
        // ⚠ 分支号的格式必须在**入口**挡住，放进去之后就来不及了。
        //
        // 这里原先只校验非空，于是客户端随手写个 branch_id="inventory" 就能
        // 触发两个都很难查的故障（都实测过，不是推演）：
        //
        //   · 解析不出下标 → 推进器把整笔事务当成「空事务」直接判 succeed，
        //     confirm 一次都不会调，那份已经 try 冻结的资源永久泄漏；
        //   · 写个 "2000000000" → 推进时按下标开数组，一次 submit 把 RSS
        //     从 38 MB 顶到 3.4 GB，而且这行留在库里，每轮轮询再来一遍。
        //
        // 判据见 `is_canonical_branch_id`：driver 推进时是拿下标**重新生成**
        // 分支号去反查行的，所以还原不出原样的写法一律不收 —— 包括 "1"、"001"
        // 这种「看起来对但少了/多了补零」的，它们会让状态更新静默落空。
        if !driver::is_canonical_branch_id(&r.branch_id) {
            return Err(ApiError::BadRequest(format!(
                "branch_id \"{}\" 格式不对：必须是从 01 开始、至少两位补零的十进制序号\
                 （01、02 …… 99、100），且不超过 {}",
                r.branch_id,
                driver::MAX_BRANCH_INDEX + 1
            )));
        }
        let tt = match self.store.get_global(&r.gid).await {
            // ⚠ 必须挡住「事务已经不可能再推进新分支」的状态。
            //
            // TCC / XA 的正确顺序是**先登记分支再做一阶段**（见 CLAUDE.md
            // 「绝对不能破坏的语义」第 5 条）。如果这里放行，客户端拿到 SUCCESS
            // 之后就会去执行 try / XA PREPARE —— 而 TC 这边事务已经终结或正在
            // 回滚，那份资源**永远不会有人 confirm 或 cancel**：
            //   TCC 是资源永久泄漏，XA 更糟 —— 留下永久持锁的 prepared 事务。
            //
            // 真实触发路径不需要客户端有 bug：多分支 TCC 登记完分支 1、做完 try、
            // 正要登记分支 2 时，这笔事务**超时了**，TC 已经回滚并落终态。
            //
            // 只放行 Prepared（正常流程）和 Submitted（容忍重试 ——
            // register_branch 本身是幂等的，见 `分支登记是幂等的`）。
            Ok(Some(g)) if matches!(g.status, GlobalStatus::Aborting) || g.status.is_final() => {
                return Err(ApiError::Conflict(format!(
                    "事务处于 {} 状态，不能再登记分支（登记后的一阶段将无人收尾）",
                    g.status.as_str()
                )));
            }
            Ok(Some(g)) => g.trans_type,
            Ok(None) => return Err(ApiError::NotFound("gid 不存在，先 prepare".into())),
            Err(e) => return Err(internal(e)),
        };

        let mut ops = Vec::new();
        match tt {
            TransType::Tcc => {
                if r.confirm.is_empty() || r.cancel.is_empty() {
                    return Err(ApiError::BadRequest(
                        "tcc 分支必须提供 confirm 和 cancel".into(),
                    ));
                }
                ops.push((BranchOp::Confirm, r.confirm.clone()));
                ops.push((BranchOp::Cancel, r.cancel.clone()));
                if !r.r#try.is_empty() {
                    ops.push((BranchOp::Try, r.r#try.clone()));
                }
            }
            TransType::Xa => {
                if r.commit.is_empty() || r.rollback.is_empty() {
                    // 缺任一个都可能留下永久持锁的 prepared 事务
                    return Err(ApiError::BadRequest(
                        "xa 分支必须提供 commit 和 rollback".into(),
                    ));
                }
                ops.push((BranchOp::Commit, r.commit.clone()));
                ops.push((BranchOp::Rollback, r.rollback.clone()));
            }
            _ => return Err(ApiError::BadRequest("只有 tcc 和 xa 需要登记分支".into())),
        }

        // ⚠ 重号必须拒绝，不能沿用「登记是幂等的」那条放行。
        //
        // 两种冲突长得一样但结论相反：URL 完全一致是客户端重试（放行），
        // URL 不同则是**两个不同的分支编成了同一个号** —— 存储层是冲突忽略，
        // 第二个分支的 URL 根本没写进去。原先这里照样返回 SUCCESS，
        // 于是客户端接着去调那个分支的 try 把资源冻结上，而 TC 不知道有它，
        // confirm / cancel 都不会调，那份资源永久泄漏。
        //
        // 判定放在存储层（见 `RegisterOutcome`）是为了拿到事务/脚本的原子性：
        // 两个请求同时登记同一个号时，输的那个也能看见赢家的 URL。
        match self
            .store
            .register_branch_with(&r.gid, &r.branch_id, &ops, &r.data)
            .await
            .map_err(internal)?
        {
            dtmrs_store::RegisterOutcome::Registered => Ok(()),
            dtmrs_store::RegisterOutcome::Conflict { op, existing } => {
                Err(ApiError::Conflict(format!(
                    "branch_id \"{}\" 的 {} 已经登记成 {existing}，不能改成另一个地址。\
                     每个分支要用**各自**的分支号（01、02、03…），重号会让后面那个分支\
                     没人 confirm/cancel，资源永久泄漏",
                    r.branch_id,
                    op.as_str()
                )))
            }
        }
    }

    /// 主动中止，触发逆序补偿。
    ///
    /// 改状态是**比较后再写**：读到的是 X 就只在「还是 X」时改成 aborting。
    /// 读和写之间状态被推进器改了（推完落了终态 / 自己转了 aborting）或被并发的
    /// submit 改了，就重读一次、按新状态重新判断 —— 不然已经落了 succeed 的事务会被
    /// 硬拽回 aborting，已 submit 的 tcc 会绕过下面那条守卫。
    pub async fn abort(&self, gid: &str) -> Result<()> {
        // 每一轮都是「读 → 判断 → 比较着写」。输了就说明有人刚改过，重来一次通常就够；
        // 连输几次说明它在被高频改动，报错比无限转更好
        for _ in 0..5 {
            let g = match self.store.get_global(gid).await {
                Ok(Some(g)) => g,
                Ok(None) => return Err(ApiError::NotFound("gid 不存在".into())),
                Err(e) => return Err(internal(e)),
            };
            // ⚠ tcc / xa / msg 一旦 submit，方向就定了，**不能再 abort**。
            //
            // submit 的含义是「一阶段全成功」（try 全成功 / XA 全 prepare 了 /
            // msg 的本地事务已提交）。这时候转 aborting：
            //   · TCC / XA：confirm/commit 做到一半就转 cancel/rollback，
            //     一半提交一半回滚（CLAUDE.md「绝对不能破坏的语义」第 2 条）；
            //   · msg：本地已提交，msg_advance 对 Aborting 直接判 Failed，
            //     剩下的消息一条都不会再发 —— 这正是 msg 要消灭的那种不一致。
            // 原先这里只判「是不是终态」，submitted 的也照样放行。
            //
            // saga / workflow 不受限：它们本来就是「边做边决定」，中途 abort
            // 就是逆序补偿，补偿所有分支的规则兜得住
            if g.status == GlobalStatus::Submitted
                && matches!(
                    g.trans_type,
                    TransType::Tcc | TransType::Xa | TransType::Msg
                )
            {
                return Err(ApiError::Conflict(format!(
                    "{} 事务已经 submit，方向已定，不能再 abort（只能等它推完）",
                    g.trans_type
                )));
            }
            if g.status.is_final() {
                return Err(ApiError::Conflict("事务已终结，无法中止".into()));
            }
            // 已经在回滚了：重复 abort 幂等成功（也不能写 —— from 和 to 相同）
            if g.status == GlobalStatus::Aborting {
                return Ok(());
            }
            if !self
                .store
                .transition(
                    gid,
                    g.status,
                    GlobalStatus::Aborting,
                    g.trans_type,
                    "调用方主动中止",
                )
                .await
                .map_err(internal)?
            {
                continue;
            }
            // 推一把，不用等退避。**正被 worker 持着租约的也可以放心调**：抢占只看
            // lease_until，这里冲不掉它；持有者推完放租约时会看到这次请求，不按退避推迟
            // （原先租约就是 next_cron_time，这一下会把它冲掉，第二个 worker 并发推同一笔 ——
            // `嵌入式tcc_abort不能冲掉推进器刚抢到的租约`）
            let _ = self.store.schedule_now(gid).await;
            return Ok(());
        }
        Err(ApiError::Conflict(
            "事务状态在被并发改动，abort 连续几次都没能生效，稍后重试".into(),
        ))
    }

    /// 立刻重试：把下次调度时间提到现在，并清掉退避累积。
    ///
    /// 只是「排到队首」，不跳过任何安全检查 —— 分支该幂等还是要幂等。
    /// 终态事务不能重试（没意义，而且会让它重新变成活跃事务）。
    pub async fn retry(&self, gid: &str) -> Result<()> {
        match self.store.get_global(gid).await {
            Ok(Some(g)) if !g.status.is_final() => {
                self.store.schedule_now(gid).await.map_err(internal)?;
                Ok(())
            }
            Ok(Some(_)) => Err(ApiError::Conflict("事务已终结，无需重试".into())),
            Ok(None) => Err(ApiError::NotFound("gid 不存在".into())),
            Err(e) => Err(internal(e)),
        }
    }

    pub async fn query(&self, gid: &str) -> Result<TransView> {
        let g = self
            .store
            .get_global(gid)
            .await
            .map_err(internal)?
            .ok_or_else(|| ApiError::NotFound("gid 不存在".into()))?;
        let branches = self.store.list_branches(gid).await.map_err(internal)?;
        Ok(TransView {
            gid: g.gid,
            trans_type: g.trans_type.to_string(),
            status: g.status.as_str().into(),
            rollback_reason: g.rollback_reason,
            create_time: g.create_time,
            finish_time: g.finish_time,
            retry_count: g.retry_count,
            next_cron_time: g.next_cron_time,
            branches: branches
                .into_iter()
                .map(|b| BranchView {
                    branch_id: b.branch_id,
                    op: b.op.as_str().into(),
                    url: b.url,
                    status: b.status.as_str().into(),
                })
                .collect(),
        })
    }

    pub async fn list_recent(&self, limit: i64) -> Vec<TransView> {
        self.store
            .list_recent(limit)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|g| TransView {
                gid: g.gid,
                trans_type: g.trans_type.to_string(),
                status: g.status.as_str().into(),
                rollback_reason: g.rollback_reason,
                create_time: g.create_time,
                finish_time: g.finish_time,
                retry_count: g.retry_count,
                next_cron_time: g.next_cron_time,
                branches: Vec::new(),
            })
            .collect()
    }

    /// 卡住的事务：没终结、退避重试了至少 `min_retries` 轮，最老的在前，
    /// **带上分支明细**（一眼看出卡在哪个分支）。巡检 / 管理台用
    pub async fn list_stuck(&self, min_retries: i64, limit: i64) -> Result<Vec<TransView>> {
        let rows = self
            .store
            .list_stuck(min_retries.max(0), limit.clamp(1, 1000))
            .await
            .map_err(internal)?;
        let mut out = Vec::with_capacity(rows.len());
        for g in rows {
            out.push(self.query(&g.gid).await?);
        }
        Ok(out)
    }
}
