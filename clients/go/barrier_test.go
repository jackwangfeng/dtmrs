package dtmrs

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// 三个场景对着真数据库跑。没配环境变量就跳过 —— **跳过不等于通过**。
func TestBarrier(t *testing.T) {
	cases := []struct {
		name, env, driver string
		d                 Dialect
	}{
		{"postgres", "DTMRS_TEST_PG_GO", "postgres", Postgres},
		{"mysql", "DTMRS_TEST_MYSQL_GO", "mysql", MySQL},
	}
	ran := 0
	for _, c := range cases {
		dsn := os.Getenv(c.env)
		if dsn == "" {
			t.Logf("⚠ 跳过 %s：%s 没配（跳过不等于通过）", c.name, c.env)
			continue
		}
		ran++
		t.Run(c.name, func(t *testing.T) { runScenarios(t, c.driver, dsn, c.d) })
	}
	if ran == 0 {
		t.Skip("一个数据库都没配，什么都没验到")
	}
}

func runScenarios(t *testing.T, driver, dsn string, d Dialect) {
	db, err := sql.Open(driver, dsn)
	if err != nil { t.Fatal(err) }
	defer db.Close()
	if err := Migrate(db, d); err != nil { t.Fatal(err) }
	for _, s := range []string{
		"DELETE FROM barrier", "DROP TABLE IF EXISTS acct_go",
		"CREATE TABLE acct_go (id INT PRIMARY KEY, bal BIGINT)",
		"INSERT INTO acct_go VALUES (1, 1000)"} {
		if _, err := db.Exec(s); err != nil { t.Fatal(s, err) }
	}

	bal := func() int64 {
		var v int64
		q := "SELECT bal FROM acct_go WHERE id=1"
		if err := db.QueryRow(q).Scan(&v); err != nil { t.Fatal(err) }
		return v
	}
	// 走一次完整的「判定 + 业务 SQL + 提交」
	once := func(gid, branch, op string, delta int64) Decision {
		tx, err := db.Begin()
		if err != nil { t.Fatal(err) }
		b := NewBarrier(d, "saga", gid, branch, op)
		dec, err := b.Decide(tx)
		if err != nil { tx.Rollback(); t.Fatal(err) }
		if dec == Execute {
			q := "UPDATE acct_go SET bal = bal + ? WHERE id = 1"
			if d == Postgres { q = "UPDATE acct_go SET bal = bal + $1 WHERE id = 1" }
			if _, err := tx.Exec(q, delta); err != nil { tx.Rollback(); t.Fatal(err) }
		}
		if err := tx.Commit(); err != nil { t.Fatal(err) }
		return dec
	}
	eq := func(got, want any, what string) {
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("✗ %s: 期望 %v 实际 %v", what, want, got)
		} else { t.Logf("  ✓ %s", what) }
	}

	eq(once("g-1", "01", "action", -100), Execute, "首次调用要执行")
	eq(bal(), int64(900), "余额扣掉了")
	eq(once("g-1", "01", "action", -100), Duplicated, "重复调用要被识破")
	eq(bal(), int64(900), "余额没有被扣第二次")

	eq(once("g-2", "01", "compensate", 100), NullCompensation, "正向没跑过时补偿必须空转")
	eq(bal(), int64(900), "空回滚不该动余额")
	eq(once("g-2", "01", "action", -100), Duplicated, "补偿之后迟到的正向必须被丢弃（悬挂）")
	eq(bal(), int64(900), "悬挂的正向不该扣款")

	eq(once("g-3", "01", "action", -100), Execute, "正向执行")
	eq(once("g-3", "01", "compensate", 100), Execute, "正向跑过之后补偿要真执行")
	eq(bal(), int64(900), "补偿把钱退回来了")
	eq(once("g-3", "01", "compensate", 100), Duplicated, "补偿自己也要幂等")
	eq(bal(), int64(900), "补偿没有退第二次")
}
