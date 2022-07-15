#!/bin/bash
#
# Copyright (c) 2019-2022, NVIDIA CORPORATION. All rights reserved.
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
#

set -ex

nvidia-smi

. jenkins/version-def.sh

ARTF_ROOT="$WORKSPACE/jars"
MVN_GET_CMD="mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -B \
    -Dmaven.repo.local=$WORKSPACE/.m2 \
    $MVN_URM_MIRROR -Ddest=$ARTF_ROOT"

rm -rf $ARTF_ROOT && mkdir -p $ARTF_ROOT

# TODO remove -Dtransitive=false workaround once pom is fixed
$MVN_GET_CMD -DremoteRepositories=$PROJECT_TEST_REPO \
    -Dtransitive=false \
    -DgroupId=com.nvidia -DartifactId=rapids-4-spark-integration-tests_$SCALA_BINARY_VER -Dversion=$PROJECT_TEST_VER -Dclassifier=$SHUFFLE_SPARK_SHIM
if [ "$CUDA_CLASSIFIER"x == x ];then
    $MVN_GET_CMD -DremoteRepositories=$PROJECT_REPO \
        -DgroupId=com.nvidia -DartifactId=rapids-4-spark_$SCALA_BINARY_VER -Dversion=$PROJECT_VER
    export RAPIDS_PLUGIN_JAR="$ARTF_ROOT/rapids-4-spark_${SCALA_BINARY_VER}-$PROJECT_VER.jar"
else
    $MVN_GET_CMD -DremoteRepositories=$PROJECT_REPO \
        -DgroupId=com.nvidia -DartifactId=rapids-4-spark_$SCALA_BINARY_VER -Dversion=$PROJECT_VER -Dclassifier=$CUDA_CLASSIFIER
    export RAPIDS_PLUGIN_JAR="$ARTF_ROOT/rapids-4-spark_${SCALA_BINARY_VER}-$PROJECT_VER-${CUDA_CLASSIFIER}.jar"
fi
RAPIDS_TEST_JAR="$ARTF_ROOT/rapids-4-spark-integration-tests_${SCALA_BINARY_VER}-$PROJECT_TEST_VER-$SHUFFLE_SPARK_SHIM.jar"

export INCLUDE_SPARK_AVRO_JAR=${INCLUDE_SPARK_AVRO_JAR:-"true"}
if [[ "${INCLUDE_SPARK_AVRO_JAR}" == "true" ]]; then
  $MVN_GET_CMD -DremoteRepositories=$PROJECT_REPO \
      -DgroupId=org.apache.spark -DartifactId=spark-avro_$SCALA_BINARY_VER -Dversion=$SPARK_VER
fi

# TODO remove -Dtransitive=false workaround once pom is fixed
$MVN_GET_CMD -DremoteRepositories=$PROJECT_TEST_REPO \
    -Dtransitive=false \
    -DgroupId=com.nvidia -DartifactId=rapids-4-spark-integration-tests_$SCALA_BINARY_VER -Dversion=$PROJECT_TEST_VER -Dclassifier=pytest -Dpackaging=tar.gz

RAPIDS_INT_TESTS_HOME="$ARTF_ROOT/integration_tests/"
# The version of pytest.tar.gz that is uploaded is the one built against spark311 but its being pushed without classifier for now
RAPIDS_INT_TESTS_TGZ="$ARTF_ROOT/rapids-4-spark-integration-tests_${SCALA_BINARY_VER}-$PROJECT_TEST_VER-pytest.tar.gz"

tmp_info=${TMP_INFO_FILE:-'/tmp/artifacts-build.info'}
rm -rf "$tmp_info"
TEE_CMD="tee -a $tmp_info"
GREP_CMD="grep revision"
AWK_CMD=(awk -F'=' '{print $2}')
getRevision() {
  local file=$1
  local properties=$2
  local revision
  if [[ $file == *.jar || $file == *.zip ]]; then
    revision=$(unzip -p "$file" "$properties" | $TEE_CMD | $GREP_CMD | "${AWK_CMD[@]}" || true)
  elif [[ $file == *.tgz || $file == *.tar.gz ]]; then
    revision=$(tar -xzf "$file" --to-command=cat "$properties" | $TEE_CMD | $GREP_CMD | "${AWK_CMD[@]}" || true)
  fi
  echo "$revision"
}

set +x
echo -e "\n==================== ARTIFACTS BUILD INFO ====================\n" >> "$tmp_info"
echo "-------------------- rapids-4-spark BUILD INFO --------------------" >> "$tmp_info"
p_ver=$(getRevision $RAPIDS_PLUGIN_JAR rapids4spark-version-info.properties)
echo "-------------------- rapids-4-spark-integration-tests BUILD INFO --------------------" >> "$tmp_info"
it_ver=$(getRevision $RAPIDS_TEST_JAR rapids4spark-version-info.properties)
echo "-------------------- rapids-4-spark-integration-tests pytest BUILD INFO --------------------" >> "$tmp_info"
pt_ver=$(getRevision $RAPIDS_INT_TESTS_TGZ integration_tests/rapids4spark-version-info.properties)
echo -e "\n==================== ARTIFACTS BUILD INFO ====================\n" >> "$tmp_info"
set -x
cat "$tmp_info" || true

SKIP_REVISION_CHECK=${SKIP_REVISION_CHECK:-'false'}
if [[ "$SKIP_REVISION_CHECK" != "true" && (-z "$p_ver"|| \
      "$p_ver" != "$it_ver" || "$p_ver" != "$pt_ver") ]]; then
  echo "Artifacts revisions are inconsistent!"
  exit 1
fi

tar xzf "$RAPIDS_INT_TESTS_TGZ" -C $ARTF_ROOT && rm -f "$RAPIDS_INT_TESTS_TGZ"

$MVN_GET_CMD -DremoteRepositories=$SPARK_REPO \
    -DgroupId=org.apache -DartifactId=spark -Dversion=$SPARK_VER -Dclassifier=bin-hadoop3.2 -Dpackaging=tgz

export SPARK_HOME="$ARTF_ROOT/spark-$SPARK_VER-bin-hadoop3.2"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
tar zxf $SPARK_HOME.tgz -C $ARTF_ROOT && \
    rm -f $SPARK_HOME.tgz
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip
# Extract 'value' from conda config string 'key: value'
CONDA_ROOT=`conda config --show root_prefix | cut -d ' ' -f2`
PYTHON_VER=`conda config --show default_python | cut -d ' ' -f2`
# Put conda package path ahead of the env 'PYTHONPATH',
# to import the right pandas from conda instead of spark binary path.
export PYTHONPATH="$CONDA_ROOT/lib/python$PYTHON_VER/site-packages:$PYTHONPATH"


echo "----------------------------START TEST------------------------------------"
pushd $RAPIDS_INT_TESTS_HOME

export TEST_PARALLEL=0  # disable spark local parallel in run_pyspark_from_build.sh
export TEST_TYPE="nightly"
export LOCAL_JAR_PATH=$ARTF_ROOT
# test collect-only in advance to terminate earlier if ENV issue
COLLECT_BASE_SPARK_SUBMIT_ARGS="$BASE_SPARK_SUBMIT_ARGS" # if passed custom params
SPARK_SUBMIT_FLAGS="$COLLECT_BASE_SPARK_SUBMIT_ARGS" ./run_pyspark_from_build.sh --collect-only -qqq

export SPARK_TASK_MAXFAILURES=1
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
# enable worker cleanup to avoid "out of space" issue
# if failed, we abort the test instantly, so the failed executor log should still be left there for debugging
export SPARK_WORKER_OPTS="$SPARK_WORKER_OPTS -Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=120 -Dspark.worker.cleanup.appDataTtl=60"
#stop and restart SPARK ETL
stop-slave.sh
stop-master.sh
start-master.sh
start-slave.sh spark://$HOSTNAME:7077
jps

export BASE_SPARK_SUBMIT_ARGS="$BASE_SPARK_SUBMIT_ARGS \
--master spark://$HOSTNAME:7077 \
--conf spark.sql.shuffle.partitions=12 \
--conf spark.task.maxFailures=$SPARK_TASK_MAXFAILURES \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
--conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
--conf spark.sql.session.timeZone=UTC"

export SEQ_CONF="--executor-memory 16G \
--total-executor-cores 6"

export PARALLEL_CONF="--executor-memory 4G \
--total-executor-cores 1 \
--conf spark.executor.cores=1 \
--conf spark.task.cpus=1 \
--conf spark.rapids.sql.concurrentGpuTasks=1 \
--conf spark.rapids.memory.gpu.minAllocFraction=0"

export CUDF_UDF_TEST_ARGS="--conf spark.rapids.memory.gpu.allocFraction=0.1 \
--conf spark.rapids.memory.gpu.minAllocFraction=0 \
--conf spark.rapids.python.memory.gpu.allocFraction=0.1 \
--conf spark.rapids.python.concurrentPythonWorkers=2 \
--conf spark.executorEnv.PYTHONPATH=${RAPIDS_PLUGIN_JAR} \
--conf spark.pyspark.python=/opt/conda/bin/python \
--py-files ${RAPIDS_PLUGIN_JAR}"

export SCRIPT_PATH="$(pwd -P)"
export TARGET_DIR="$SCRIPT_PATH/target"
mkdir -p $TARGET_DIR

run_iceberg_tests() {
  ICEBERG_VERSION="0.13.1"
  # get the major/minor version of Spark
  ICEBERG_SPARK_VER=$(echo $SPARK_VER | cut -d. -f1,2)

  # Iceberg does not support Spark 3.3+ yet
  if [[ "$ICEBERG_SPARK_VER" < "3.3" ]]; then
    SPARK_SUBMIT_FLAGS="$BASE_SPARK_SUBMIT_ARGS $SEQ_CONF \
      --packages org.apache.iceberg:iceberg-spark-runtime-${ICEBERG_SPARK_VER}_2.12:${ICEBERG_VERSION} \
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
      --conf spark.sql.catalog.spark_catalog.type=hadoop \
      --conf spark.sql.catalog.spark_catalog.warehouse=/tmp/spark-warehouse-$$" \
      ./run_pyspark_from_build.sh -m iceberg --iceberg
  else
    echo "Skipping Iceberg tests. Iceberg does not support Spark $ICEBERG_SPARK_VER"
  fi
}

run_test_not_parallel() {
    local TEST=${1//\.py/}
    local LOG_FILE
    case $TEST in
      all)
        SPARK_SUBMIT_FLAGS="$BASE_SPARK_SUBMIT_ARGS $SEQ_CONF" \
          ./run_pyspark_from_build.sh
        ;;

      cudf_udf_test)
        SPARK_SUBMIT_FLAGS="$BASE_SPARK_SUBMIT_ARGS $SEQ_CONF $CUDF_UDF_TEST_ARGS" \
          ./run_pyspark_from_build.sh -m cudf_udf --cudf_udf
        ;;

      cache_serializer)
        SPARK_SUBMIT_FLAGS="$BASE_SPARK_SUBMIT_ARGS $SEQ_CONF \
        --conf spark.sql.cache.serializer=com.nvidia.spark.ParquetCachedBatchSerializer" \
          ./run_pyspark_from_build.sh -k cache_test
        ;;

      iceberg)
        run_iceberg_tests
        ;;

      *)
        echo -e "\n\n>>>>> $TEST...\n"
        LOG_FILE="$TARGET_DIR/$TEST.log"
        # set dedicated RUN_DIRs here to avoid conflict between parallel tests
        RUN_DIR="$TARGET_DIR/run_dir_$TEST" \
          SPARK_SUBMIT_FLAGS="$BASE_SPARK_SUBMIT_ARGS $PARALLEL_CONF $MEMORY_FRACTION_CONF" \
          ./run_pyspark_from_build.sh -k $TEST >"$LOG_FILE" 2>&1

        CODE="$?"
        if [[ $CODE == "0" ]]; then
          sed -n -e '/test session starts/,/deselected,/ p' "$LOG_FILE" || true
        else
          cat "$LOG_FILE" || true
          cat /tmp/artifacts-build.info || true
        fi
        return $CODE
        ;;
    esac
}
export -f run_test_not_parallel

get_cases_by_tags() {
  local cases
  local args=${2}
  cases=$(TEST_TAGS="${1}" SPARK_SUBMIT_FLAGS="$COLLECT_BASE_SPARK_SUBMIT_ARGS" \
           ./run_pyspark_from_build.sh "${args}" --collect-only -p no:warnings -qq 2>/dev/null \
           | grep -oP '(?<=::).*?(?=\[)' | uniq | xargs)
  echo "$cases"
}
export -f get_cases_by_tags

get_tests_by_tags() {
  local tests
  local args=${2}
  tests=$(TEST_TAGS="${1}" SPARK_SUBMIT_FLAGS="$COLLECT_BASE_SPARK_SUBMIT_ARGS" \
           ./run_pyspark_from_build.sh "${args}" --collect-only -qqq -p no:warnings 2>/dev/null \
           | grep -oP '(?<=python/).*?(?=.py)' | xargs)
  echo "$tests"
}
export -f get_tests_by_tags

# TEST_MODE
# - IT_ONLY
# - CUDF_UDF_ONLY
# - ALL: IT+CUDF_UDF
TEST_MODE=${TEST_MODE:-'IT_ONLY'}
if [[ $TEST_MODE == "ALL" || $TEST_MODE == "IT_ONLY" ]]; then
  # integration tests
  if [[ $PARALLEL_TEST == "true" ]] && [ -x "$(command -v parallel)" ]; then
    # separate run for special cases that require smaller parallelism
    special_cases=$(get_cases_by_tags "nightly_resource_consuming_test \
                                      and (nightly_gpu_mem_consuming_case or nightly_host_mem_consuming_case)")
    # hardcode parallelism as 2 for special cases
    export MEMORY_FRACTION_CONF="--conf spark.rapids.memory.gpu.allocFraction=0.45 \
    --conf spark.rapids.memory.gpu.maxAllocFraction=0.45"
    # --halt "now,fail=1": exit when the first job fail, and kill running jobs.
    #                      we can set it to "never" and print failed ones after finish running all tests if needed
    # --group: print stderr after test finished for better readability
    parallel --group --halt "now,fail=1" -j2 run_test_not_parallel ::: ${special_cases}

    resource_consuming_cases=$(get_cases_by_tags "nightly_resource_consuming_test \
                                                and not nightly_gpu_mem_consuming_case \
                                                and not nightly_host_mem_consuming_case")
    other_tests=$(get_tests_by_tags "not nightly_resource_consuming_test")
    tests=$(echo "${resource_consuming_cases} ${other_tests}" | tr ' ' '\n' | awk '!x[$0]++' | xargs)

    if [[ "${PARALLELISM}" == "" ]]; then
      PARALLELISM=$(nvidia-smi --query-gpu=memory.free --format=csv,noheader | \
                    awk '{if (MAX < $1){ MAX = $1}} END {print int(MAX / (2 * 1024))}')
    fi
    # parallelism > 7 could slow down the whole process, so we have a limitation for it
    [[ ${PARALLELISM} -gt 7 ]] && PARALLELISM=7
    MEMORY_FRACTION=$(python -c "print(1/($PARALLELISM + 0.1))")
    export MEMORY_FRACTION_CONF="--conf spark.rapids.memory.gpu.allocFraction=${MEMORY_FRACTION} \
    --conf spark.rapids.memory.gpu.maxAllocFraction=${MEMORY_FRACTION}"
    parallel --group --halt "now,fail=1" -j"${PARALLELISM}" run_test_not_parallel ::: ${tests}
  else
    run_test_not_parallel all
  fi

  if [[ $PARALLEL_TEST == "true" ]] && [ -x "$(command -v parallel)" ]; then
    cache_test_cases=$(get_cases_by_tags "" "-k cache_test")
    # hardcode parallelism as 5
    export MEMORY_FRACTION_CONF="--conf spark.rapids.memory.gpu.allocFraction=0.18 \
    --conf spark.rapids.memory.gpu.maxAllocFraction=0.18 \
    --conf spark.sql.cache.serializer=com.nvidia.spark.ParquetCachedBatchSerializer"
    parallel --group --halt "now,fail=1" -j5 run_test_not_parallel ::: ${cache_test_cases}
  else
    run_test_not_parallel cache_serializer
  fi
fi

# cudf_udf_test
if [[ "$TEST_MODE" == "ALL" || "$TEST_MODE" == "CUDF_UDF_ONLY" ]]; then
  run_test_not_parallel cudf_udf_test
fi

# Iceberg tests
if [[ "$TEST_MODE" == "ALL" || "$TEST_MODE" == "ICEBERG_ONLY" ]]; then
  run_test_not_parallel iceberg
fi

popd
stop-slave.sh
stop-master.sh
