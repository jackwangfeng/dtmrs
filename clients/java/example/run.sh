#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
CP="../lib/mysql-connector-j-8.4.0.jar"
mkdir -p out
javac -encoding UTF-8 -cp "$CP" -d out ../src/main/java/dtmrs/Barrier.java Service.java
exec java -cp "$CP:out" Service
