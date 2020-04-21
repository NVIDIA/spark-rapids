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

set -e
if [ "$CUDF_VER"x == x ];then
    CUDF_VER="0.14-SNAPSHOT"
fi

# Empty as default cuda10.0 classifier, 'cuda10-1' as cuda10.1 classifier
if [ "$CUDA_CLASSIFIER"x != x ] && [ "$CUDA_CLASSIFIER" != "cuda10-1" ];then
    echo "Error!!! CUDA_CLASSIFIER: $CUDA_CLASSIFIER -- neither default nor cuda10-1, reset to default"
    CUDA_CLASSIFIER=""
fi

if [ "$PROJECT_VER"x == x ];then
    PROJECT_VER="0.1-SNAPSHOT"
fi

if [ "$SPARK_VER"x == x ];then
    SPARK_VER="3.0.1-SNAPSHOT"
fi

#default maven server gpuwa
if [ "$SERVER_URL"x == x ]; then
    SERVER_URL="https://gpuwa.nvidia.com/artifactory/sw-spark-maven"
fi

ARTF_ROOT=$WORKSPACE/jars
MVN_GET_CMD="mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -B \
    -Dmaven.repo.local=$WORKSPACE/.m2 \
    -DremoteRepositories=$SERVER_URL \
    -Ddest=$ARTF_ROOT"

rm -rf $ARTF_ROOT && mkdir -p $ARTF_ROOT
# maven download SNAPSHOT jars: cudf, rapids-4-spark, spark3.0
$MVN_GET_CMD \
    -DgroupId=ai.rapids -DartifactId=cudf -Dversion=$CUDF_VER -Dclassifier=$CUDA_CLASSIFIER
$MVN_GET_CMD \
    -DgroupId=ai.rapids -DartifactId=rapids-4-spark -Dversion=$PROJECT_VER
$MVN_GET_CMD \
    -DgroupId=ai.rapids -DartifactId=rapids-4-spark-tests -Dversion=$PROJECT_VER
if [ "$CUDA_CLASSIFIER"x == x ];then
    CUDF_JAR="$ARTF_ROOT/cudf-$CUDF_VER.jar"
else
    CUDF_JAR="$ARTF_ROOT/cudf-$CUDF_VER-$CUDA_CLASSIFIER.jar"
fi
RAPIDS_PLUGIN_JAR="$ARTF_ROOT/rapids-4-spark-$PROJECT_VER.jar"
RAPIDS_TEST_JAR="$ARTF_ROOT/rapids-4-spark-tests-$PROJECT_VER.jar"

$MVN_GET_CMD \
    -DgroupId=org.apache -DartifactId=spark -Dversion=$SPARK_VER -Dclassifier=bin-hadoop3 -Dpackaging=tar.gz

SPARK_HOME="$ARTF_ROOT/spark-$SPARK_VER-bin-hadoop3"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
tar zxf $SPARK_HOME.tar.gz -C $ARTF_ROOT && \
    rm -f $SPARK_HOME.tar.gz

PARQUET_PERF="$WORKSPACE/tests/src/test/resources/parquet_perf"
PARQUET_ACQ="$WORKSPACE/tests/src/test/resources/parquet_acq"
OUTPUT="$WORKSPACE/output"
SPARK_SUBMIT_ARGS=" --class ai.rapids.sparkexamples.mortgage.Main \
    --master spark://$HOSTNAME:7077 --executor-memory 32G \
    --jars $CUDF_JAR,$RAPIDS_PLUGIN_JAR \
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
spark-submit $SPARK_SUBMIT_ARGS $TEST_PARAMS
