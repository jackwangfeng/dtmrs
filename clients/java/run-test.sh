#!/usr/bin/env bash
# 用 javac 直接编译跑测试，不依赖 maven —— CI 里更轻
set -euo pipefail
cd "$(dirname "$0")"
CP="lib/postgresql-42.7.4.jar:lib/mysql-connector-j-8.4.0.jar"
mkdir -p out
javac -encoding UTF-8 -cp "$CP" -d out \
  src/main/java/dtmrs/Barrier.java src/main/java/dtmrs/RedisBarrier.java \
  src/test/java/BarrierTest.java src/test/java/RedisBarrierTest.java
# 两个都跑完再汇总 —— 否则 set -e 会让前一个失败时后一个根本不执行
rc=0
java -cp "$CP:out" BarrierTest || rc=1
echo
# Redis 屏障（业务数据在 Redis 里时用）
java -cp "$CP:out" RedisBarrierTest || rc=1
exit $rc
