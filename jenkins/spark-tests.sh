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
RAPIDS_PLUGIN_JAR="$ARTF_ROOT/rapids-4-spark_${SCALA_BINARY_VER}-$PROJECT_VER.jar"
RAPIDS_UDF_JAR="$ARTF_ROOT/rapids-4-spark-udf-examples_${SCALA_BINARY_VER}-$PROJECT_TEST_VER.jar"
RAPIDS_TEST_JAR="$ARTF_ROOT/rapids-4-spark-integration-tests_${SCALA_BINARY_VER}-$PROJECT_TEST_VER.jar"

$MVN_GET_CMD -DremoteRepositories=$PROJECT_TEST_REPO \
    -DgroupId=com.nvidia -DartifactId=rapids-4-spark-integration-tests_$SCALA_BINARY_VER -Dversion=$PROJECT_TEST_VER -Dclassifier=pytest -Dpackaging=tar.gz

RAPIDS_INT_TESTS_HOME="$ARTF_ROOT/integration_tests/"
RAPIDS_INT_TESTS_TGZ="$ARTF_ROOT/rapids-4-spark-integration-tests_${SCALA_BINARY_VER}-$PROJECT_TEST_VER-pytest.tar.gz"
tar xzf "$RAPIDS_INT_TESTS_TGZ" -C $ARTF_ROOT && rm -f "$RAPIDS_INT_TESTS_TGZ"

$MVN_GET_CMD -DremoteRepositories=$SPARK_REPO \
    -DgroupId=org.apache -DartifactId=spark -Dversion=$SPARK_VER -Dclassifier=bin-hadoop3.2 -Dpackaging=tgz

SPARK_HOME="$ARTF_ROOT/spark-$SPARK_VER-bin-hadoop3.2"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
tar zxf $SPARK_HOME.tgz -C $ARTF_ROOT && \
    rm -f $SPARK_HOME.tgz

PARQUET_PERF="$WORKSPACE/integration_tests/src/test/resources/parquet_perf"
PARQUET_ACQ="$WORKSPACE/integration_tests/src/test/resources/parquet_acq"
OUTPUT="$WORKSPACE/output"

# spark.sql.cache.serializer conf is ignored for versions prior to 3.1.1
SERIALIZER="--conf spark.sql.cache.serializer=com.nvidia.spark.rapids.shims.spark311.ParquetCachedBatchSerializer"

BASE_SPARK_SUBMIT_ARGS="$BASE_SPARK_SUBMIT_ARGS \
    --master spark://$HOSTNAME:7077 \
    --executor-memory 12G \
    --total-executor-cores 6 \
    --conf spark.sql.shuffle.partitions=12 \
    --conf spark.driver.extraClassPath=${CUDF_JAR}:${RAPIDS_PLUGIN_JAR}:${RAPIDS_UDF_JAR} \
    --conf spark.executor.extraClassPath=${CUDF_JAR}:${RAPIDS_PLUGIN_JAR}:${RAPIDS_UDF_JAR} \
    --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
    --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
    --conf spark.sql.session.timeZone=UTC"
MORTGAGE_SPARK_SUBMIT_ARGS=" --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --class com.nvidia.spark.rapids.tests.mortgage.Main \
    $RAPIDS_TEST_JAR"

CUDF_UDF_TEST_ARGS="--conf spark.rapids.memory.gpu.allocFraction=0.1 \
    --conf spark.rapids.python.memory.gpu.allocFraction=0.1 \
    --conf spark.rapids.python.concurrentPythonWorkers=2 \
    --conf spark.executorEnv.PYTHONPATH=${RAPIDS_PLUGIN_JAR} \
    --conf spark.pyspark.python=/opt/conda/bin/python \
    --py-files ${RAPIDS_PLUGIN_JAR}" # explicitly specify python binary path in env w/ multiple python versions

TEST_PARAMS="$SPARK_VER $PARQUET_PERF $PARQUET_ACQ $OUTPUT"

export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

#stop and restart SPARK ETL
stop-slave.sh
stop-master.sh
start-master.sh
start-slave.sh spark://$HOSTNAME:7077
jps

echo "----------------------------START TEST------------------------------------"
rm -rf $OUTPUT
spark-submit $BASE_SPARK_SUBMIT_ARGS $SERIALIZER $MORTGAGE_SPARK_SUBMIT_ARGS $TEST_PARAMS
pushd $RAPIDS_INT_TESTS_HOME
spark-submit $BASE_SPARK_SUBMIT_ARGS --jars $RAPIDS_TEST_JAR ./runtests.py -v -rfExXs --std_input_path="$WORKSPACE/integration_tests/src/test/resources/"
spark-submit $BASE_SPARK_SUBMIT_ARGS $CUDF_UDF_TEST_ARGS --jars $RAPIDS_TEST_JAR ./runtests.py -m "cudf_udf" -v -rfExXs --cudf_udf
popd
stop-slave.sh
stop-master.sh

