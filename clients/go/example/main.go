// 可运行的分支服务示例 —— 用 net/http 标准库，无框架依赖。
// 屏障那部分的逻辑跟你用 gin/echo 时完全一样。
//
//	go run .    （环境变量见 ../../verify-examples.sh）
package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	dtmrs "github.com/jackwangfeng/dtmrs/clients/go"
)

var db *sql.DB

func main() {
	var err error
	db, err = sql.Open("mysql", os.Getenv("EX_MYSQL_GO"))
	if err != nil {
		log.Fatal(err)
	}
	// 启动时建表一次
	if err := dtmrs.Migrate(db, dtmrs.MySQL); err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/deduct", branch(-1))  // 扣款
	http.HandleFunc("/refund", branch(+1))  // 扣款的补偿
	http.HandleFunc("/ok", branch(0))       // 第二步：什么都不做，成功
	http.HandleFunc("/noop", branch(0))     // 第二步的补偿
	// 第二步：业务**明确**拒绝
	http.HandleFunc("/reject", func(w http.ResponseWriter, r *http.Request) {
		// 业务**明确**拒绝 → 409，TC 会逆序补偿
		w.WriteHeader(409)
		fmt.Fprint(w, `{"dtm_result":"FAILURE"}`)
	})
	log.Fatal(http.ListenAndServe("127.0.0.1:"+os.Getenv("EX_PORT"), nil))
}

// sign: -1 扣款，+1 退款，0 不动账
func branch(sign int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		amount, _ := strconv.ParseInt(q.Get("amount"), 10, 64)
		if amount == 0 {
			amount = 100
		}

		tx, err := db.Begin()
		if err != nil {
			w.WriteHeader(500) // 结果未知 → TC 重试
			return
		}
		defer tx.Rollback() // commit 之后再 rollback 是空操作，安全

		b := dtmrs.NewBarrier(dtmrs.MySQL, q.Get("trans_type"),
			q.Get("gid"), q.Get("branch_id"), q.Get("op"))

		dec, err := b.Decide(tx)
		if err != nil {
			w.WriteHeader(500) // 屏障本身出错也算「未知」
			return
		}

		if dec == dtmrs.Execute && sign != 0 {
			res, err := tx.Exec(
				"UPDATE ex_account SET balance = balance + ? WHERE id = 1 AND balance + ? >= 0",
				int64(sign)*amount, int64(sign)*amount)
			if err != nil {
				w.WriteHeader(500)
				return
			}
			if n, _ := res.RowsAffected(); n == 0 {
				// 余额不足 = 业务明确拒绝 → 409
				w.WriteHeader(409)
				fmt.Fprint(w, `{"dtm_result":"FAILURE"}`)
				return
			}
		}
		// 空回滚 / 重复请求走到这里，什么都没做，同样返回成功
		if err := tx.Commit(); err != nil {
			w.WriteHeader(500)
			return
		}
		fmt.Fprint(w, `{"dtm_result":"SUCCESS"}`)
	}
}
