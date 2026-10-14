#!/bin/bash
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# CI-only: fallback.groovy installs this probe; manual build/test commands do not require it.
# Run before product tests, with no plugin JARs, to separate host readiness from product failures.
set -euo pipefail

case "${1:-}" in
    databricks) master='local[2]' ;;
    upstream) master='local-cluster[1,2,1024]' ;;
    *) exit 1 ;;
esac
directory=/home/ubuntu/.databricks-fallback
status_file="$directory/spark-readiness-$1.json"
log_file="$directory/spark-readiness-$1.log"
printf 'Spark readiness %s: starting plugin-free check (master=%s, timeout=10m)\n' "$1" "$master"
printf '{"phase":"STARTING","runtime":"%s"}\n' "$1" > "$status_file"

# DBR 18.3 Scala compiler initialization can exhaust the default 1 GiB driver heap.
# spark-shell may exit zero after an initialization error; also require the completed job marker.
set +e
timeout --kill-after=30s 10m "${SPARK_HOME:?}/bin/spark-shell" \
    --master "$master" \
    --driver-memory 4g \
    --conf spark.plugins= \
    --conf spark.shuffle.manager=sort \
    --conf spark.deploy.maxExecutorRetries=0 \
    --conf spark.connect.grpc.binding.port=0 \
    2>&1 <<'SCALA' | tee "$log_file"
try { require(spark.range(0, 100, 1, 2).agg(Map("id" -> "sum")).first().getLong(0) == 4950L); spark.stop(); println("\nDATABRICKS_SPARK_READINESS_OK"); System.exit(0) } catch { case error: Throwable => error.printStackTrace(); System.exit(1) }
SCALA
statuses=("${PIPESTATUS[@]}")
set -e
if [[ "${statuses[0]}" == 0 && "${statuses[1]}" == 0 ]] &&
    grep -qx 'DATABRICKS_SPARK_READINESS_OK' "$log_file"; then
    printf '{"phase":"READY","runtime":"%s"}\n' "$1" > "$status_file"
    printf 'Spark readiness %s: READY\n' "$1"
    exit 0
fi

# Generic SparkException, executor loss, OOM, timeout, bind/configuration and class-loading errors
# are not sufficient infrastructure evidence, even in a plugin-free application.
reason=SPARK_STARTUP_UNCLASSIFIED
if grep -Eq '(java\.io\.[A-Za-z]+Exception|java\.nio\.file\.FileSystemException):.*No space left on device' "$log_file"; then
    reason=SPARK_STARTUP_DISK_FULL
elif grep -Eq 'java\.io\.IOException:.*Input/output error' "$log_file"; then
    reason=SPARK_STARTUP_DISK_IO
fi
printf '{"phase":"FAILED","runtime":"%s","reason":"%s"}\n' "$1" "$reason" > "$status_file"
printf 'Spark readiness %s: FAILED (%s, spark exit=%s, log exit=%s)\n' \
    "$1" "$reason" "${statuses[0]}" "${statuses[1]}"
exit 1
