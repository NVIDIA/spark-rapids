#!/bin/bash
#
# Copyright (c) 2019-2021, NVIDIA CORPORATION. All rights reserved.
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
# maven download SNAPSHOT jars: cudf, rapids-4-spark, spark3.0
$MVN_GET_CMD -DremoteRepositories=$CUDF_REPO \
    -DgroupId=ai.rapids -DartifactId=cudf -Dversion=$CUDF_VER -Dclassifier=$CUDA_CLASSIFIER
$MVN_GET_CMD -DremoteRepositories=$PROJECT_REPO \
    -DgroupId=com.nvidia -DartifactId=rapids-4-spark_$SCALA_BINARY_VER -Dversion=$PROJECT_VER
$MVN_GET_CMD -DremoteRepositories=$PROJECT_TEST_REPO \
    -DgroupId=com.nvidia -DartifactId=rapids-4-spark-udf-examples_$SCALA_BINARY_VER -Dversion=$PROJECT_TEST_VER
$MVN_GET_CMD -DremoteRepositories=$PROJECT_TEST_REPO \
    -DgroupId=com.nvidia -DartifactId=rapids-4-spark-integration-tests_$SCALA_BINARY_VER -Dversion=$PROJECT_TEST_VER
if [ "$CUDA_CLASSIFIER"x == x ];then
    CUDF_JAR="$ARTF_ROOT/cudf-$CUDF_VER.jar"
else
    CUDF_JAR="$ARTF_ROOT/cudf-$CUDF_VER-$CUDA_CLASSIFIER.jar"
fi
export RAPIDS_PLUGIN_JAR="$ARTF_ROOT/rapids-4-spark_${SCALA_BINARY_VER}-$PROJECT_VER.jar"
RAPIDS_UDF_JAR="$ARTF_ROOT/rapids-4-spark-udf-examples_${SCALA_BINARY_VER}-$PROJECT_TEST_VER.jar"
RAPIDS_TEST_JAR="$ARTF_ROOT/rapids-4-spark-integration-tests_${SCALA_BINARY_VER}-$PROJECT_TEST_VER.jar"

$MVN_GET_CMD -DremoteRepositories=$PROJECT_TEST_REPO \
    -DgroupId=com.nvidia -DartifactId=rapids-4-spark-integration-tests_$SCALA_BINARY_VER -Dversion=$PROJECT_TEST_VER -Dclassifier=pytest -Dpackaging=tar.gz

RAPIDS_INT_TESTS_HOME="$ARTF_ROOT/integration_tests/"
RAPIDS_INT_TESTS_TGZ="$ARTF_ROOT/rapids-4-spark-integration-tests_${SCALA_BINARY_VER}-$PROJECT_TEST_VER-pytest.tar.gz"
tar xzf "$RAPIDS_INT_TESTS_TGZ" -C $ARTF_ROOT && rm -f "$RAPIDS_INT_TESTS_TGZ"

$MVN_GET_CMD -DremoteRepositories=$SPARK_REPO \
    -DgroupId=org.apache -DartifactId=spark -Dversion=$SPARK_VER -Dclassifier=bin-hadoop3.2 -Dpackaging=tgz

export SPARK_HOME="$ARTF_ROOT/spark-$SPARK_VER-bin-hadoop3.2"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
tar zxf $SPARK_HOME.tgz -C $ARTF_ROOT && \
    rm -f $SPARK_HOME.tgz

IS_SPARK_311_OR_LATER=0
[[ "$(printf '%s\n' "3.1.1" "$SPARK_VER" | sort -V | head -n1)" = "3.1.1" ]] && IS_SPARK_311_OR_LATER=1

export SPARK_TASK_MAXFAILURES=1
[[ "$IS_SPARK_311_OR_LATER" -eq "0" ]] && SPARK_TASK_MAXFAILURES=4

IS_SPARK_311=0
[[ "$SPARK_VER" == "3.1.1" ]] && IS_SPARK_311=1

export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

#stop and restart SPARK ETL
stop-slave.sh
stop-master.sh
start-master.sh
start-slave.sh spark://$HOSTNAME:7077
jps

echo "----------------------------START TEST------------------------------------"
pushd $RAPIDS_INT_TESTS_HOME

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

# currently we hardcode the parallelism and configs based on our CI node's hardware specs,
# we can make it dynamically generated if this script is going to be used in other scenarios in the future
export PARALLEL_CONF="--executor-memory 4G \
--total-executor-cores 2 \
--conf spark.executor.cores=2 \
--conf spark.task.cpus=1 \
--conf spark.rapids.sql.concurrentGpuTasks=2 \
--conf spark.rapids.memory.gpu.allocFraction=0.15 \
--conf spark.rapids.memory.gpu.minAllocFraction=0 \
--conf spark.rapids.memory.gpu.maxAllocFraction=0.15"

export CUDF_UDF_TEST_ARGS="--conf spark.rapids.memory.gpu.allocFraction=0.1 \
--conf spark.rapids.memory.gpu.minAllocFraction=0 \
--conf spark.rapids.python.memory.gpu.allocFraction=0.1 \
--conf spark.rapids.python.concurrentPythonWorkers=2 \
--conf spark.executorEnv.PYTHONPATH=${RAPIDS_PLUGIN_JAR} \
--conf spark.pyspark.python=/opt/conda/bin/python \
--py-files ${RAPIDS_PLUGIN_JAR}"

export TEST_PARALLEL=0  # disable spark local parallel in run_pyspark_from_build.sh
export TEST_TYPE="nightly"
export LOCAL_JAR_PATH=$ARTF_ROOT
export SCRIPT_PATH="$(pwd -P)"
export TARGET_DIR="$SCRIPT_PATH/target"
mkdir -p $TARGET_DIR

run_test() {
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
        --conf spark.sql.cache.serializer=com.nvidia.spark.rapids.shims.spark311.ParquetCachedBatchSerializer" \
          ./run_pyspark_from_build.sh -k cache_test
        ;;

      *)
        echo -e "\n\n>>>>> $TEST...\n"
        LOG_FILE="$TARGET_DIR/$TEST.log"
        # set dedicated RUN_DIRs here to avoid conflict between parallel tests
        RUN_DIR="$TARGET_DIR/run_dir_$TEST" \
          SPARK_SUBMIT_FLAGS="$BASE_SPARK_SUBMIT_ARGS $PARALLEL_CONF" \
          ./run_pyspark_from_build.sh -k $TEST >"$LOG_FILE" 2>&1

        CODE="$?"
        if [[ $CODE == "0" ]]; then
          sed -n -e '/test session starts/,/deselected,/ p' "$LOG_FILE" || true
        else
          cat "$LOG_FILE" || true
        fi
        return $CODE
        ;;
    esac
}
export -f run_test

# integration tests
if [[ $PARALLEL_TEST == "true" ]] && [ -x "$(command -v parallel)" ]; then
  # put most time-consuming tests at the head of queue
  time_consuming_tests="join_test.py generate_expr_test.py parquet_write_test.py"
  tests_list=$(find "$SCRIPT_PATH"/src/main/python/ -name "*_test.py" -printf "%f ")
  tests=$(echo "$time_consuming_tests $tests_list" | tr ' ' '\n' | awk '!x[$0]++' | xargs)
  # --halt "now,fail=1": exit when the first job fail, and kill running jobs.
  #                      we can set it to "never" and print failed ones after finish running all tests if needed
  # --group: print stderr after test finished for better readability
  parallel --group --halt "now,fail=1" -j5 run_test ::: $tests
else
  run_test all
fi
# cudf_udf_test
run_test cudf_udf_test

# Temporarily only run on Spark 3.1.1 (https://github.com/NVIDIA/spark-rapids/issues/3311)
if [[ "$IS_SPARK_311" -eq "1" ]]; then
  run_test cache_serializer
fi

popd
stop-slave.sh
stop-master.sh
