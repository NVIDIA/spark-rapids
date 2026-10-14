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
# Do not echo inherited JVM options, which may contain credentials.
set +x

case "${1:-}" in
    databricks) master='local[2]' ;;
    upstream) master='local-cluster[1,2,1024]' ;;
    *) exit 1 ;;
esac
directory=/home/ubuntu/.databricks-fallback
status_file="$directory/spark-readiness-$1.json"
log_file="$directory/spark-readiness-$1.log"

diagnostic() {
    printf '\nReadiness diagnostic: %s\n' "$*"
    timeout --kill-after=2s 10s "$@" || printf 'Diagnostic unavailable: %s\n' "$1"
}

# Isolate shell errors and state changes from the actual readiness check.
print_host_diagnostics() (
    printf '\nSpark readiness: pre-test host diagnostics\n'
    diagnostic date -u '+UTC: %Y-%m-%dT%H:%M:%SZ'
    diagnostic uname -a
    diagnostic cat /etc/os-release
    diagnostic hostname -I
    diagnostic ip -brief address
    diagnostic lscpu
    diagnostic uptime
    printf 'SPARK_HOME=%s JAVA_HOME=%s DATABRICKS_RUNTIME_VERSION=%s\n' \
        "${SPARK_HOME:-unset}" "${JAVA_HOME:-unset}" "${DATABRICKS_RUNTIME_VERSION:-unset}"
    if [[ -n "${SPARK_HOME:-}" && -r "$SPARK_HOME/VERSION" ]]; then
        diagnostic head -n 1 "$SPARK_HOME/VERSION"
    fi
    diagnostic free -m
    diagnostic vmstat 1 3
    # Names and resource usage only; process arguments can contain credentials.
    diagnostic ps -eo pid,ppid,comm,pcpu,pmem,rss,vsz --sort=-rss | head -n 18
    diagnostic df -hP /tmp "$directory" "${SPARK_HOME:-/}"
    diagnostic df -iP /tmp "$directory" "${SPARK_HOME:-/}"
    ulimit -a
    # Show pressure and standard cgroup counters without dumping process environment/config.
    for counter in /proc/self/cgroup /proc/pressure/{cpu,memory,io} \
        /sys/fs/cgroup/cpu.{max,stat} /sys/fs/cgroup/memory.{max,current,peak,events} \
        /sys/fs/cgroup/memory/memory.{limit_in_bytes,usage_in_bytes,max_usage_in_bytes,failcnt}; do
        if [[ -r "$counter" ]]; then
            printf '%s:\n' "$counter"
            diagnostic cat "$counter"
        fi
    done
    diagnostic nvidia-smi \
        --query-gpu=index,name,uuid,driver_version,memory.total,memory.used,utilization.gpu \
        --format=csv
    # Provider identifiers survive cluster deletion and help support locate the affected VM.
    # Bypass proxies, avoid credential endpoints, and print no user data, tags or access tokens.
    diagnostic python3 - <<'PY'
import json
import urllib.request

opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
base = "http://169.254.169.254"

def metadata(path, headers, method="GET"):
    request = urllib.request.Request(base + path, headers=headers, method=method)
    with opener.open(request, timeout=2) as response:
        return response.read().decode()

try:
    azure = json.loads(metadata("/metadata/instance?api-version=2021-02-01", {"Metadata": "true"}))
    compute = azure["compute"]
    identity = {key: compute.get(key) for key in
                ("vmId", "resourceId", "subscriptionId", "resourceGroupName", "name",
                 "location", "zone", "vmSize")}
    identity["provider"] = "azure"
    identity["addresses"] = [
        {key: address.get(key) for key in ("privateIpAddress", "publicIpAddress")}
        for interface in azure.get("network", {}).get("interface", [])
        for address in interface.get("ipv4", {}).get("ipAddress", [])]
    print("Cloud instance identity: " + json.dumps(identity, sort_keys=True), flush=True)
except Exception as error:
    print("Azure instance metadata unavailable (%s)" % type(error).__name__, flush=True)
    try:
        token = metadata("/latest/api/token", {"X-aws-ec2-metadata-token-ttl-seconds": "60"}, "PUT")
        headers = {"X-aws-ec2-metadata-token": token}
        aws = json.loads(metadata("/latest/dynamic/instance-identity/document", headers))
        identity = {key: aws.get(key) for key in
                    ("instanceId", "accountId", "region", "availabilityZone", "instanceType",
                     "imageId", "privateIp", "pendingTime")}
        identity["provider"] = "aws"
        try:
            identity["publicIp"] = metadata("/latest/meta-data/public-ipv4", headers).strip()
        except Exception as error:
            identity["publicIp"] = "unavailable (%s)" % type(error).__name__
        print("Cloud instance identity: " + json.dumps(identity, sort_keys=True), flush=True)
    except Exception as error:
        print("AWS instance metadata unavailable (%s)" % type(error).__name__, flush=True)
PY
)

# Console-only and best effort: diagnostics must not affect readiness or trigger retries.
if ! print_host_diagnostics 2>&1; then
    printf 'Readiness host diagnostics incomplete; continuing\n' || :
fi

# -X options are JVM-specific. If the bounded capability check fails, keep the
# original options instead of making optional diagnostics a readiness failure.
readiness_jvm_options="${SPARK_SUBMIT_OPTS:-}"
if timeout --kill-after=2s 10s "${JAVA_HOME:+$JAVA_HOME/bin/}java" \
    -XshowSettings:vm -showversion -version >/dev/null 2>&1; then
    readiness_jvm_options+=' -XshowSettings:vm -showversion'
else
    printf 'Readiness JVM diagnostics unavailable; keeping original JVM options\n' || :
fi
printf 'Spark readiness %s: starting plugin-free check (master=%s, timeout=10m)\n' "$1" "$master"
printf '{"phase":"STARTING","runtime":"%s"}\n' "$1" > "$status_file"

# spark-shell may exit zero after an initialization error; also require the completed job marker.
set +e
# Print the actual driver JVM's version and maximum heap without changing its heap
# or replacing existing JVM options. Do not enable Spark's full command/configuration dumps.
SPARK_SUBMIT_OPTS="$readiness_jvm_options" \
timeout --kill-after=30s 10m "${SPARK_HOME:?}/bin/spark-shell" \
    --master "$master" \
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
