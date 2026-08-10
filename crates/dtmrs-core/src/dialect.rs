//! 方言层 —— 一套 SQL 模板跑 sqlite / postgres / mysql。
//!
//! 加 MySQL 之前只有两种后端，`sqlx::Any` 配 `$N` 占位符就够了。MySQL 一进来
//! **同时打破三条**，所以必须有这一层。全部实测（sqlx 0.8 / pg16 / mysql8.0.44）：
//!
//! | | sqlite | postgres | mysql |
//! |---|---|---|---|
//! | `$1` 占位符 | ✅ | ✅ | ❌ `Unknown column '$1'` |
//! | `?` 占位符 | ✅ | ❌ 语法错误 | ✅ |
//! | `ON CONFLICT DO NOTHING` | ✅ | ✅ | ❌ 1064 语法错误 |
//! | `INSERT IGNORE` | ❌ | ❌ | ✅ |
//! | `TEXT PRIMARY KEY` | ✅ | ✅ | ❌ 1170 要 key length |
//! | `CREATE INDEX IF NOT EXISTS` | ✅ | ✅ | ❌ 1064 语法错误 |
//!
//! 还有一条**踩过就忘不了**的：MySQL 的 `ON DUPLICATE KEY UPDATE` 在重复时
//! `rows_affected` 返回 **1**（不是 0），拿它做幂等判断会把"已存在"误判成
//! "刚插入"。所以 MySQL 必须用 `INSERT IGNORE`。
//!
//! # 写 SQL 的规矩
//!
//! 模板里统一用 `?` 当占位符（跟 MySQL 一致），非 MySQL 后端由 [`Backend::q`]
//! 自动换成 `$1..$n`。
//!
//! ⚠ 所以**模板的字符串字面量里不能出现 `?`** —— 会被当成占位符。
//! 目前所有 SQL 都满足（字面量只有 `''` 和 `'prepared'` 这类）。

/// 支持的后端
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Sqlite,
    Postgres,
    MySql,
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
        })
    }
}

impl Backend {
    pub fn from_url(url: &str) -> Self {
        let u = url.trim().to_ascii_lowercase();
        if u.starts_with("mysql") || u.starts_with("mariadb") {
            Self::MySql
        } else if u.starts_with("postgres") {
            Self::Postgres
        } else {
            Self::Sqlite
        }
    }

    /// 主键/索引列的字符串类型。
    ///
    /// MySQL 不能对 `TEXT` 建索引（1170：要 key length），必须用定长 VARCHAR。
    /// 128 跟 DTM 的 `varchar(128)` 对齐；复合主键三列合计仍在 InnoDB 限内。
    pub fn id_text(&self) -> &'static str {
        match self {
            Self::MySql => "VARCHAR(128)",
            _ => "TEXT",
        }
    }

    /// 短标识列（op 之类），MySQL 下不必给到 128
    pub fn id_short(&self) -> &'static str {
        match self {
            Self::MySql => "VARCHAR(45)",
            _ => "TEXT",
        }
    }

    /// 自由文本列（payload / url / 原因之类）。
    ///
    /// ⚠ **MySQL 上只能用 `VARCHAR`**：经 `sqlx::Any` 读 MySQL 的 `TEXT`
    /// （含 `LONGTEXT`、`MEDIUMTEXT`、显式 `CHARACTER SET utf8mb4`）一律报
    /// `mismatched types; Rust type String is not compatible with SQL type BLOB`。
    /// 五种写法实测下来只有 `VARCHAR` 能解成 String。
    ///
    /// 代价是有长度上限：`n` 要够装最长的内容。MySQL 单行还有 65535 字节的
    /// 总限制（utf8mb4 每字符 4 字节），所以别把每列都开得很大。
    pub fn text(&self, n: usize) -> String {
        match self {
            Self::MySql => format!("VARCHAR({n})"),
            _ => "TEXT".to_string(),
        }
    }

    /// [`Self::id_text`] 列能装多少个字符
    pub const ID_MAX: usize = 128;

    /// [`Self::id_short`] 列能装多少个字符
    pub const ID_SHORT_MAX: usize = 45;

    /// 把 SQL 模板渲染成这个后端能吃的语句。
    ///
    /// 做三件事：
    /// 1. `?` → `$1..$n`（MySQL 保持 `?`）
    /// 2. `{INS}` → `INSERT IGNORE INTO`（MySQL）/ `INSERT INTO`（其它）
    /// 3. `{NOCONFLICT}` → 空（MySQL）/ `ON CONFLICT DO NOTHING`（其它）
    pub fn q(&self, template: &str) -> String {
        let s = template
            .replace("{INS}", self.insert_ignore())
            .replace("{NOCONFLICT}", self.no_conflict());
        self.placeholders(&s)
    }

    fn insert_ignore(&self) -> &'static str {
        match self {
            Self::MySql => "INSERT IGNORE INTO",
            _ => "INSERT INTO",
        }
    }

    fn no_conflict(&self) -> &'static str {
        match self {
            // MySQL 靠 INSERT IGNORE 达到同样效果，句尾不需要东西
            Self::MySql => "",
            _ => "ON CONFLICT DO NOTHING",
        }
    }

    fn placeholders(&self, sql: &str) -> String {
        if *self == Self::MySql {
            return sql.to_string();
        }
        let mut out = String::with_capacity(sql.len() + 8);
        let mut n = 0;
        for c in sql.chars() {
            if c == '?' {
                n += 1;
                out.push('$');
                out.push_str(&n.to_string());
            } else {
                out.push(c);
            }
        }
        out
    }

    /// 建索引。MySQL 不支持 `CREATE INDEX IF NOT EXISTS`，只能靠
    /// 建表时内联 `KEY`，所以这里对 MySQL 返回 `None`。
    pub fn create_index(&self, name: &str, table: &str, cols: &str) -> Option<String> {
        match self {
            Self::MySql => None,
            _ => Some(format!(
                "CREATE INDEX IF NOT EXISTS {name} ON {table}({cols})"
            )),
        }
    }

    /// 建表时内联的索引定义。只有 MySQL 用得上（见 [`Self::create_index`]）。
    pub fn inline_index(&self, name: &str, cols: &str) -> String {
        match self {
            Self::MySql => format!(", KEY {name} ({cols})"),
            _ => String::new(),
        }
    }
}

/// 值超过列宽
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TooLong {
    pub col: &'static str,
    pub len: usize,
    pub max: usize,
}

impl std::fmt::Display for TooLong {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} 超长：{} 个字符，上限 {}（MySQL 上这一列是 VARCHAR({})）",
            self.col, self.len, self.max, self.max
        )
    }
}

impl std::error::Error for TooLong {}

/// 写库前挡住超长的值。**必须在 Rust 侧挡，不能指望数据库报错。**
///
/// MySQL 的 `INSERT IGNORE` 会把 strict mode 的 1406 降级成 1265 警告，
/// 然后**静默截断**（实测：`VARCHAR(32)` 塞 100 个字符 →
/// `Warning 1265 Data truncated`，行插进去了，存的是前 32 个字符）。
///
/// 而幂等插入全都得用 `INSERT IGNORE`（见 [`Backend::q`]），所以在 MySQL 上
/// 超长值不是"插入失败"，是**"插入成功但内容被改了"**：
///
/// - `payload` 被截断 → 存进去的是坏 JSON，事务再也推不动
/// - `gid` 被截断 → 两笔不相关的长 gid 事务在屏障表里**撞成同一行**，
///   一笔的执行会被另一笔当成"已处理过"跳过
///
/// 三家的原生行为还完全不一致：postgres 直接报错、sqlite 根本没有长度限制。
/// 统一在这里挡住，三种后端拿到同一个错误。
///
/// 按**字符**数算而不是字节 —— MySQL 的 `VARCHAR(n)` 数的是字符。
pub fn check_len(col: &'static str, val: &str, max: usize) -> Result<(), TooLong> {
    let len = val.chars().count();
    if len > max {
        return Err(TooLong { col, len, max });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 从url认后端() {
        assert_eq!(Backend::from_url("sqlite::memory:"), Backend::Sqlite);
        assert_eq!(Backend::from_url("sqlite:/tmp/a.db"), Backend::Sqlite);
        assert_eq!(Backend::from_url("postgres://u:p@h/db"), Backend::Postgres);
        assert_eq!(
            Backend::from_url("postgresql://u:p@h/db"),
            Backend::Postgres
        );
        assert_eq!(
            Backend::from_url("mysql://root:x@h:3306/db"),
            Backend::MySql
        );
        assert_eq!(Backend::from_url("MySQL://ROOT@H/DB"), Backend::MySql);
        assert_eq!(Backend::from_url("mariadb://root@h/db"), Backend::MySql);
    }

    #[test]
    fn 占位符按方言转换() {
        let t = "INSERT INTO t (a,b,c) VALUES (?,?,?)";
        assert_eq!(
            Backend::Postgres.q(t),
            "INSERT INTO t (a,b,c) VALUES ($1,$2,$3)"
        );
        assert_eq!(
            Backend::Sqlite.q(t),
            "INSERT INTO t (a,b,c) VALUES ($1,$2,$3)"
        );
        // MySQL 原样保留
        assert_eq!(Backend::MySql.q(t), t);
    }

    #[test]
    fn 冲突忽略按方言展开() {
        let t = "{INS} t (k) VALUES (?) {NOCONFLICT}";
        assert_eq!(
            Backend::Postgres.q(t),
            "INSERT INTO t (k) VALUES ($1) ON CONFLICT DO NOTHING"
        );
        assert_eq!(Backend::MySql.q(t), "INSERT IGNORE INTO t (k) VALUES (?) ");
    }

    #[test]
    fn 自由文本列在mysql上必须是varchar() {
        // sqlx::Any 读 MySQL 的 TEXT 会当成 BLOB，解不成 String
        assert_eq!(Backend::MySql.text(8192), "VARCHAR(8192)");
        assert_eq!(Backend::Postgres.text(8192), "TEXT");
        assert_eq!(Backend::Sqlite.text(8192), "TEXT");
    }

    #[test]
    fn 主键列类型按方言() {
        // MySQL 不能对 TEXT 建索引，必须定长
        assert_eq!(Backend::MySql.id_text(), "VARCHAR(128)");
        assert_eq!(Backend::Postgres.id_text(), "TEXT");
        assert_eq!(Backend::Sqlite.id_text(), "TEXT");
    }

    #[test]
    fn 索引方式按方言二选一() {
        // 非 MySQL：独立 CREATE INDEX，且不要内联
        assert!(Backend::Postgres.create_index("i", "t", "a,b").is_some());
        assert_eq!(Backend::Postgres.inline_index("i", "a,b"), "");
        // MySQL：反过来
        assert!(Backend::MySql.create_index("i", "t", "a,b").is_none());
        assert_eq!(Backend::MySql.inline_index("i", "a,b"), ", KEY i (a,b)");
    }

    #[test]
    fn 编号从1开始且连续() {
        let t = "UPDATE t SET a=?, b=? WHERE c=? AND d=?";
        assert_eq!(
            Backend::Postgres.q(t),
            "UPDATE t SET a=$1, b=$2 WHERE c=$3 AND d=$4"
        );
    }
}
