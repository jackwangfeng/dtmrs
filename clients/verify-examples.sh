#!/usr/bin/env bash
# 验证各语言的示例分支服务真的能跟 dtmrs TC 协同工作。
#
#   ./verify-examples.sh go|python|node|java|all
#
# 对每个语言跑三个场景，断言**账户余额**（不是断言"没报错"）：
#   ① 正常提交      → 扣款生效
#   ② 第二步拒绝    → 逆序补偿，钱退回来
#   ③ 重复推进      → 幂等，不会扣两次
#
# 需要：真 MySQL（DTMRS_TEST_MYSQL_*）和已编译的 dtmrs 二进制。
set -uo pipefail
cd "$(dirname "$0")"

MYSQL_HOST=${MYSQL_HOST:-127.0.0.1}
MYSQL_PORT=${MYSQL_PORT:-33306}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PASS=${MYSQL_PASS:-dtmrs}
MYSQL_DB=${MYSQL_DB:-dtmrs}
TC_HTTP=${TC_HTTP:-127.0.0.1:36999}
BUSI_PORT=${BUSI_PORT:-8811}
DTMRS_BIN=${DTMRS_BIN:-../target/release/dtmrs}

FAILED=0
mysql_q() { docker exec dtmrs-my mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" -N -B -e "$1" 2>/dev/null; }
bal() { mysql_q "SELECT balance FROM ex_account WHERE id=1"; }
ck() { if [ "$2" = "$3" ]; then echo "    ✓ $1（$3）"; else echo "    ✗ $1：期望 $3 实际 $2"; FAILED=$((FAILED+1)); fi; }

reset_db() {
  # ⚠ 只清 barrier 的数据，**不能 DROP** —— 服务只在启动时建表，
  # 把表删掉之后它每个请求都会 500（踩过这个坑）
  mysql_q "DELETE FROM barrier;
           DROP TABLE IF EXISTS ex_account;
           CREATE TABLE ex_account(id INT PRIMARY KEY, balance BIGINT);
           INSERT INTO ex_account VALUES (1, 1000);" >/dev/null
}

# 服务启动前先把 barrier 建好，免得 reset_db 时它还不存在
ensure_barrier() {
  mysql_q "CREATE TABLE IF NOT EXISTS barrier(
     trans_type VARCHAR(45) NOT NULL, gid VARCHAR(128) NOT NULL,
     branch_id VARCHAR(128) NOT NULL, op VARCHAR(45) NOT NULL,
     barrier_id VARCHAR(45) NOT NULL, reason VARCHAR(45) NOT NULL,
     create_time BIGINT NOT NULL,
     PRIMARY KEY (gid, branch_id, op, barrier_id))" >/dev/null
}

wait_port() { for _ in $(seq 1 50); do (echo >/dev/tcp/127.0.0.1/$1) 2>/dev/null && return 0; sleep 0.2; done; return 1; }

submit() { # gid, 第二步是否失败
  local gid=$1 second=$2
  curl -s -XPOST "http://$TC_HTTP/api/dtmsvr/submit" -H 'content-type: application/json' -d "{
    \"gid\":\"$gid\",\"steps\":[
      {\"action\":\"http://127.0.0.1:$BUSI_PORT/deduct\",\"compensate\":\"http://127.0.0.1:$BUSI_PORT/refund\"},
      {\"action\":\"http://127.0.0.1:$BUSI_PORT/$second\",\"compensate\":\"http://127.0.0.1:$BUSI_PORT/noop\"}]}" >/dev/null
}

wait_final() { for _ in $(seq 1 60); do
    s=$(curl -s "http://$TC_HTTP/api/dtmsvr/query?gid=$1" | python3 -c 'import json,sys;print(json.load(sys.stdin)["status"])' 2>/dev/null)
    [ "$s" = succeed ] || [ "$s" = failed ] && { echo "$s"; return; }; sleep 0.5
  done; echo timeout; }

run_scenarios() {  # $1 = 语言名
  echo "  --- $1 ---"
  reset_db

  submit "ex-$1-ok" ok
  ck "正常提交后事务成功" "$(wait_final ex-$1-ok)" "succeed"
  ck "扣款生效" "$(bal)" "900"

  submit "ex-$1-fail" reject
  ck "第二步拒绝后事务失败" "$(wait_final ex-$1-fail)" "failed"
  ck "补偿把钱退回来了" "$(bal)" "900"

  # 幂等：直接对同一个分支再打一次请求，模拟 TC 重试
  curl -s -o /dev/null -XPOST "http://127.0.0.1:$BUSI_PORT/deduct?gid=ex-$1-ok&trans_type=saga&branch_id=01&op=action&amount=100"
  ck "重复调用没有扣第二次（幂等）" "$(bal)" "900"
}

start_tc() {
  rm -f /tmp/ex_tc.db
  DTMRS_DB=sqlite:/tmp/ex_tc.db DTMRS_ADDR=$TC_HTTP DTMRS_GRPC_ADDR=127.0.0.1:36998 \
    setsid "$DTMRS_BIN" >/tmp/ex_tc.log 2>&1 < /dev/null &
  wait_port "${TC_HTTP##*:}" || { echo "TC 起不来，看 /tmp/ex_tc.log"; exit 1; }
}

kill_port() { local p=$(ss -lntp 2>/dev/null | grep ":$1 " | grep -oP 'pid=\K[0-9]+' | head -1); [ -n "$p" ] && kill -9 "$p" 2>/dev/null; }

cleanup() { kill_port "$BUSI_PORT"; kill_port "${TC_HTTP##*:}"; }
trap cleanup EXIT

MYSQL_DSN="$MYSQL_USER:$MYSQL_PASS@tcp($MYSQL_HOST:$MYSQL_PORT)/$MYSQL_DB"
export EX_PORT=$BUSI_PORT EX_MYSQL_HOST=$MYSQL_HOST EX_MYSQL_PORT=$MYSQL_PORT \
       EX_MYSQL_USER=$MYSQL_USER EX_MYSQL_PASS=$MYSQL_PASS EX_MYSQL_DB=$MYSQL_DB \
       EX_MYSQL_GO="$MYSQL_DSN" \
       EX_MYSQL_JDBC="jdbc:mysql://$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DB?user=$MYSQL_USER&password=$MYSQL_PASS" \
       EX_MYSQL_NODE="mysql://$MYSQL_USER:$MYSQL_PASS@$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DB"

start_tc
echo "TC 已启动（$TC_HTTP）"

LANGS=$( [ "${1:-all}" = all ] && echo "go python node java" || echo "$1" )
ensure_barrier
for lang in $LANGS; do
  kill_port "$BUSI_PORT"; sleep 0.5
  case $lang in
    go)     (cd go/example && setsid go run . >/tmp/ex_go.log 2>&1 </dev/null &) ;;
    python) (cd python/example && setsid python3 service.py >/tmp/ex_py.log 2>&1 </dev/null &) ;;
    node)   (cd node/example && setsid node service.js >/tmp/ex_node.log 2>&1 </dev/null &) ;;
    java)   (cd java/example && setsid ./run.sh >/tmp/ex_java.log 2>&1 </dev/null &) ;;
  esac
  if wait_port "$BUSI_PORT"; then run_scenarios "$lang"; else
    echo "  ✗ $lang 服务起不来，日志：/tmp/ex_$lang.log"; tail -5 /tmp/ex_$lang.log 2>/dev/null; FAILED=$((FAILED+1))
  fi
done

echo
[ $FAILED -eq 0 ] && echo "✓ 全部通过" || echo "✗ $FAILED 项失败"
exit $FAILED
