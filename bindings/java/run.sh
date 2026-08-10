#!/usr/bin/env bash
# 编译并跑 JVM 示例。没有 maven/gradle 也能用 —— 只依赖一个 jna jar。
set -euo pipefail
cd "$(dirname "$0")"

JNA=lib/jna-5.14.0.jar
if [ ! -f "$JNA" ]; then
  echo "下载 JNA…"
  mkdir -p lib
  curl -sSL -o "$JNA" \
    https://repo1.maven.org/maven2/net/java/dev/jna/jna/5.14.0/jna-5.14.0.jar
fi

if [ ! -f ../../target/release/libdtmrs.so ] && [ -z "${DTMRS_LIB:-}" ]; then
  echo "先编 .so： cargo build -p dtmrs-ffi --release" >&2
  exit 1
fi

mkdir -p out
javac -encoding UTF-8 -cp "$JNA" -d out Dtmrs.java Example.java
java -cp "$JNA:out" Example
