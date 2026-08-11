#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
CP="lib/postgresql-42.7.4.jar:lib/mysql-connector-j-8.4.0.jar"
mkdir -p out
javac -encoding UTF-8 -cp "$CP" -d out Barrier.java BarrierTest.java
java -cp "$CP:out" BarrierTest
