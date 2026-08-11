// Package dtmrs 提供子事务屏障 —— 业务服务（RM）侧接入 dtmrs 用。
//
// 分支接口**一定会**被重复调用（TC 重试 + 崩溃恢复），而且可能乱序到达。
// 这个包用一张表 + 一条 INSERT IGNORE 同时解决三个问题：
//
//	幂等    同一分支被调两次        → 只执行一次
//	空回滚  正向没跑过就来了补偿    → 补偿空转
//	悬挂    补偿先到、正向后到      → 丢弃迟到的正向
//
// # 用法
//
//	// 启动时建表一次
//	dtmrs.Migrate(db, dtmrs.MySQL)
//
//	// 每次处理分支请求（gid / branchID / op / transType 由 TC 传进来）
//	tx, _ := db.Begin()
//	defer tx.Rollback()
//
//	b := dtmrs.NewBarrier(dtmrs.MySQL, transType, gid, branchID, op)
//	dec, err := b.Decide(tx)
//	if err != nil { return err }
//	if dec == dtmrs.Execute {
//	    // 业务 SQL —— 必须用这个 tx，跟屏障记录同一个事务
//	    tx.Exec("UPDATE account SET balance = balance - ? WHERE id = ?", amt, uid)
//	}
//	tx.Commit()   // 原子性的来源：屏障记录与业务变更同生共死
//
// # ⚠ 两条不能违反的前提
//
//  1. 屏障表必须和业务表在**同一个数据库实例** —— 不同实例没法共用一个本地
//     事务，这个方案直接失效。不是实现限制，是它成立的根本条件。
//  2. 业务 SQL 必须用传给 Decide 的那个 tx。用别的连接执行等于白做。
//
// # 返回值语义
//
// NullCompensation 和 Duplicated 都是**正常路径**，你的接口应该返回**成功**
// 而不是失败 —— 返回失败会让 TC 以为分支出错了。
package dtmrs

import (
	"database/sql"
	"fmt"
	"time"
)

// Decision 是屏障给出的判定。
type Decision int

const (
	// Execute 该干活。调用方在同一个事务里执行业务 SQL
	Execute Decision = iota
	// NullCompensation 空回滚：正向分支从没执行过，补偿直接空转。接口应返回成功
	NullCompensation
	// Duplicated 重复或悬挂：这次调用已处理过，跳过。接口应返回成功
	Duplicated
)

func (d Decision) String() string {
	switch d {
	case Execute:
		return "Execute"
	case NullCompensation:
		return "NullCompensation"
	case Duplicated:
		return "Duplicated"
	}
	return "Unknown"
}

// Dialect 是 SQL 方言。三家的「冲突就忽略」写法完全不同，而这个算法
// **整个依赖冲突时 affected rows 必须是 0**。
type Dialect int

const (
	// MySQL 用 INSERT IGNORE。**绝不能**用 ON DUPLICATE KEY UPDATE，见 insert 里的注释
	MySQL Dialect = iota
	Postgres
	SQLite
)

// Tx 是 *sql.Tx 和 *sql.DB 的公共部分，方便测试时传任意一个。
// 生产上**一定要传事务**（*sql.Tx）—— 屏障记录必须和业务 SQL 同生共死。
type Tx interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// Barrier 是一次分支调用的屏障。**不是并发安全的**，每次请求新建一个。
type Barrier struct {
	dialect   Dialect
	transType string
	gid       string
	branchID  string
	op        string
	table     string
	counter   int
}

// NewBarrier 创建屏障。op 取 action / compensate / try / confirm / cancel / commit / rollback。
func NewBarrier(d Dialect, transType, gid, branchID, op string) *Barrier {
	return &Barrier{
		dialect: d, transType: transType, gid: gid,
		branchID: branchID, op: op, table: "barrier",
	}
}

// originOp 返回补偿类操作对应的**正向**操作，判空回滚全靠它。
// 正向操作返回空串。
func originOp(op string) string {
	switch op {
	case "compensate", "rollback":
		return "action"
	case "cancel":
		return "try"
	}
	return ""
}

func knownOp(op string) bool {
	switch op {
	case "action", "compensate", "try", "confirm", "cancel", "commit", "rollback":
		return true
	}
	return false
}

// Migrate 建屏障表。启动时调一次即可，重复调用无害。
func Migrate(db *sql.DB, d Dialect) error {
	// MySQL 不能对 TEXT 建索引（1170 要 key length），必须定长
	idText, idShort := "TEXT", "TEXT"
	if d == MySQL {
		idText, idShort = "VARCHAR(128)", "VARCHAR(45)"
	}
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS barrier (
  trans_type  %s NOT NULL,
  gid         %s NOT NULL,
  branch_id   %s NOT NULL,
  op          %s NOT NULL,
  barrier_id  %s NOT NULL,
  reason      %s NOT NULL,
  create_time BIGINT NOT NULL,
  PRIMARY KEY (gid, branch_id, op, barrier_id)
)`, idShort, idText, idText, idShort, idShort, idShort))
	return err
}

// Decide 做出判定。**必须在业务事务里调用**，且业务 SQL 要用同一个 tx。
//
// 算法：补偿方先用「正向分支」的名义插一行去占坑。占成功了说明正向从没来过
// （空回滚）；占失败了说明正向真跑过（是真补偿）。而这个坑一旦被占，
// 迟到的正向分支就再也插不进来（悬挂被丢弃）。
func (b *Barrier) Decide(tx Tx) (Decision, error) {
	if b.gid == "" || b.branchID == "" {
		return Execute, fmt.Errorf("dtmrs: gid / branchID 不能为空")
	}
	if !knownOp(b.op) {
		return Execute, fmt.Errorf("dtmrs: 未知 op: %s", b.op)
	}
	b.counter++
	bid := fmt.Sprintf("%02d", b.counter)

	var originAffected int64
	origin := originOp(b.op)
	if origin != "" {
		n, err := b.insert(tx, origin, bid)
		if err != nil {
			return Execute, err
		}
		originAffected = n
	}

	currentAffected, err := b.insert(tx, b.op, bid)
	if err != nil {
		return Execute, err
	}

	if origin != "" && originAffected > 0 {
		// 正向分支从没跑过（否则那行早被它自己占了）→ 空回滚
		return NullCompensation, nil
	}
	if currentAffected == 0 {
		// 这个 (gid, branch, op, bid) 已经处理过了 → 重复请求或悬挂
		return Duplicated, nil
	}
	return Execute, nil
}

func (b *Barrier) insert(tx Tx, op, bid string) (int64, error) {
	var q string
	switch b.dialect {
	case MySQL:
		// ⚠ 这里**绝不能**用 ON DUPLICATE KEY UPDATE：
		// 它在重复时 affected rows 返回 1（不是 0），整个算法就废了。
		// INSERT IGNORE 重复时返回 0，跟另外两家一致。
		q = "INSERT IGNORE INTO " + b.table +
			" (trans_type,gid,branch_id,op,barrier_id,reason,create_time) VALUES (?,?,?,?,?,?,?)"
	case Postgres:
		// Postgres 用 $N 占位符
		q = "INSERT INTO " + b.table +
			" (trans_type,gid,branch_id,op,barrier_id,reason,create_time)" +
			" VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING"
	default:
		q = "INSERT INTO " + b.table +
			" (trans_type,gid,branch_id,op,barrier_id,reason,create_time)" +
			" VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING"
	}
	res, err := tx.Exec(q, b.transType, b.gid, b.branchID, op, bid,
		b.op, // reason = 是哪个分支插的这行，排查用
		time.Now().Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
