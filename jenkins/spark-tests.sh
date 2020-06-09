#!/bin/bash
#
# Copyright (c) 2019-2020, NVIDIA CORPORATION. All rights reserved.
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
if [ "$CUDF_VER"x == x ];then
    CUDF_VER="0.14-SNAPSHOT"
fi

if [ "$PROJECT_VER"x == x ];then
    PROJECT_VER="0.1-SNAPSHOT"
fi

if [ "$SPARK_VER"x == x ];then
    SPARK_VER="3.0.1-SNAPSHOT"
fi

SCALA_BINARY_VER=${SCALA_BINARY_VER:-2.12}

#default maven server urm
if [ "$SERVER_URL"x == x ]; then
    SERVER_URL="https://urm.nvidia.com:443/artifactory/sw-spark-maven"
fi

echo "CUDF_VER: $CUDF_VER, CUDA_CLASSIFIER: $CUDA_CLASSIFIER, PROJECT_VER: $PROJECT_VER \
        SPARK_VER: $SPARK_VER, SCALA_BINARY_VER: $SCALA_BINARY_VER, SERVER_URL: $SERVER_URL"

ARTF_ROOT="$WORKSPACE/jars"
MVN_GET_CMD="mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -B \
    -Dmaven.repo.local=$WORKSPACE/.m2 \
    -DremoteRepositories=$SERVER_URL \
    -Ddest=$ARTF_ROOT"

rm -rf $ARTF_ROOT && mkdir -p $ARTF_ROOT
# maven download SNAPSHOT jars: cudf, rapids-4-spark, spark3.0
$MVN_GET_CMD \
    -DgroupId=ai.rapids -DartifactId=cudf -Dversion=$CUDF_VER -Dclassifier=$CUDA_CLASSIFIER
$MVN_GET_CMD \
    -DgroupId=ai.rapids -DartifactId=rapids-4-spark_$SCALA_BINARY_VER -Dversion=$PROJECT_VER
$MVN_GET_CMD \
    -DgroupId=ai.rapids -DartifactId=rapids-4-spark-integration-tests_$SCALA_BINARY_VER -Dversion=$PROJECT_VER
if [ "$CUDA_CLASSIFIER"x == x ];then
    CUDF_JAR="$ARTF_ROOT/cudf-$CUDF_VER.jar"
else
    CUDF_JAR="$ARTF_ROOT/cudf-$CUDF_VER-$CUDA_CLASSIFIER.jar"
fi
RAPIDS_PLUGIN_JAR="$ARTF_ROOT/rapids-4-spark_${SCALA_BINARY_VER}-$PROJECT_VER.jar"
RAPIDS_TEST_JAR="$ARTF_ROOT/rapids-4-spark-integration-tests_${SCALA_BINARY_VER}-$PROJECT_VER.jar"

$MVN_GET_CMD \
    -DgroupId=ai.rapids -DartifactId=rapids-4-spark-integration-tests_$SCALA_BINARY_VER -Dversion=$PROJECT_VER -Dclassifier=pytest -Dpackaging=tar.gz

RAPIDS_INT_TESTS_HOME="$ARTF_ROOT/integration_tests/"
RAPDIS_INT_TESTS_TGZ="$ARTF_ROOT/rapids-4-spark-integration-tests_${SCALA_BINARY_VER}-$PROJECT_VER-pytest.tar.gz"
tar xzf "$RAPDIS_INT_TESTS_TGZ" -C $ARTF_ROOT && rm -f "$RAPDIS_INT_TESTS_TGZ"

$MVN_GET_CMD \
    -DgroupId=org.apache -DartifactId=spark -Dversion=$SPARK_VER -Dclassifier=bin-hadoop3 -Dpackaging=tar.gz

SPARK_HOME="$ARTF_ROOT/spark-$SPARK_VER-bin-hadoop3"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
tar zxf $SPARK_HOME.tar.gz -C $ARTF_ROOT && \
    rm -f $SPARK_HOME.tar.gz

PARQUET_PERF="$WORKSPACE/integration_tests/src/test/resources/parquet_perf"
PARQUET_ACQ="$WORKSPACE/integration_tests/src/test/resources/parquet_acq"
OUTPUT="$WORKSPACE/output"
BASE_SPARK_SUBMIT_ARGS="--master spark://$HOSTNAME:7077 --executor-memory 32G \
    --conf spark.sql.shuffle.partitions=12 \
    --conf spark.driver.extraClassPath=${CUDF_JAR}:${RAPIDS_PLUGIN_JAR} \
    --conf spark.executor.extraClassPath=${CUDF_JAR}:${RAPIDS_PLUGIN_JAR} \
    --conf spark.driver.extraJavaOptions=-Duser.timezone=GMT \
    --conf spark.executor.extraJavaOptions=-Duser.timezone=GMT \
    --conf spark.sql.session.timeZone=UTC"
MORTGAGE_SPARK_SUBMIT_ARGS=" --conf spark.plugins=ai.rapids.spark.SQLPlugin \
    --class ai.rapids.sparkexamples.mortgage.Main \
    $RAPIDS_TEST_JAR"

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
spark-submit $BASE_SPARK_SUBMIT_ARGS $MORTGAGE_SPARK_SUBMIT_ARGS $TEST_PARAMS
cd $RAPIDS_INT_TESTS_HOME && spark-submit $BASE_SPARK_SUBMIT_ARGS --jars $RAPIDS_TEST_JAR ./runtests.py -v -rfExXs --std_input_path="$WORKSPACE/integration_tests/src/test/resources/"
