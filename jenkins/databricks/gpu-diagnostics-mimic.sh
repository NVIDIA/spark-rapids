#!/bin/bash
#
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

# TEST ONLY: real native readiness and diagnostics, then pass or a synthetic failure.
set -eo pipefail
for assignment in ${EXTRA_ENVS//,/ }; do
    export "$assignment"
done
export SPARK_HOME="${DB_HOME:-/databricks}/spark"
sudo ln -sfn "${DB_HOME:-/databricks}/jars" "$SPARK_HOME/jars"
# Match setup.sh's permissions for the runtime's local Spark scratch directory.
if [[ -f "$SPARK_HOME/conf/spark-env.sh" ]] &&
    local_dir=$(grep 'SPARK_LOCAL_DIRS=' "$SPARK_HOME/conf/spark-env.sh"); then
    local_dir=${local_dir##*=}
    sudo chmod 777 "$(printf '%s' "$local_dir" | xargs)"
fi
# Avoid a spurious report-copy warning: this experiment does not run pytest.
mkdir -p /home/ubuntu/spark-rapids/integration_tests/target
echo 'TEST ONLY: running the real native Spark readiness sum over 100 rows'
bash "${SPARK_STARTUP_CHECK:?}" databricks

case "${TEST_MODE:?}" in
    CI_PART1)
        echo 'TEST ONLY: quick pass; native Spark readiness completed successfully'
        ;;
    CI_PART2)
        echo 'TEST ONLY: the following Spark/CUDA error is synthetic, not a GPU diagnosis'
        echo 'WILL RUN TESTS WITH SPARK_HOME: synthetic-databricks-case'
        echo 'ai.rapids.cudf.CudaException: 46 cudaErrorDevicesUnavailable: CUDA-capable device(s) is/are busy or unavailable'
        echo 'java.lang.IllegalStateException: Could not find a single GPU to use'
        echo 'FAILED spark-shell smoke test (synthetic diagnostic validation)'
        echo 'TEST ONLY: preserving the intentional failure exit 42'
        exit 42
        ;;
    *)
        echo "Unexpected diagnostic test mode: $TEST_MODE" >&2
        exit 2
        ;;
esac
