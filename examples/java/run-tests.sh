#!/bin/bash
# 跨微服务事务的测试套件。退出码反映成败，可以直接进 CI。
#
#   DTMRS_BIN=/path/to/dtmrs ./run-tests.sh     # 推荐：能测崩溃恢复
#   DTMRS_URL=http://... ./run-tests.sh         # 用已有 TC，崩溃恢复会跳过
set -e
cd "$(dirname "$0")"
mkdir -p out
javac -d out DtmrsClient.java BranchService.java Tests.java

# 每次运行一个唯一标记：分支服务只认带这个标记的 gid，
# 挡掉别的 TC 打进来的陈年事务重试（踩过，表现为偶发的状态对不上）
export BRANCH_TAG="r$$-$(date +%s)"

java -cp out BranchService & SVC=$!
trap 'kill $SVC 2>/dev/null' EXIT
sleep 1.5

java -cp out Tests
