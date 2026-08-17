#!/bin/bash
# 一键跑三分支事务演示。需要 JDK 17+，没有任何第三方依赖。
#
#   TC 地址和令牌从环境变量读：
#     DTMRS_URL         默认 http://127.0.0.1:36789
#     DTMRS_AUTH_TOKEN  TC 没开认证就不用设
set -e
cd "$(dirname "$0")"
mkdir -p out
echo "编译…"
javac -d out DtmrsClient.java BranchService.java Demo.java

echo "起三个分支服务…"
java -cp out BranchService & SVC=$!
trap 'kill $SVC 2>/dev/null' EXIT
sleep 1.5

java -cp out Demo
