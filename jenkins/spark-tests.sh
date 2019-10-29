#!/bin/bash
set -e
CUDF_VER=$1
if [ "$CUDF_VER"x == x ];then
    CUDF_VER="0.10-SNAPSHOT"
fi

CUDA_VER=$2
CUDF_JAR=""
if [ "$CUDA_VER" != "cuda10" ];then
    CUDF_JAR="$WORKSPACE/.m2/ai/rapids/cudf/$CUDF_VER/cudf-$CUDF_VER.jar"
else
    CUDF_JAR="$WORKSPACE/.m2/ai/rapids/cudf/$CUDF_VER/cudf-$CUDF_VER-$CUDA_VER.jar"
fi

if [ "$PROJECT_VER"x == x ];then
    PROJECT_VER="0.1-SNAPSHOT"
fi
RAPIDS_PLUGIN_JAR="$WORKSPACE/sql-plugin/target/rapids-4-spark-$PROJECT_VER.jar"
RAPIDS_TEST_JAR="$WORKSPACE/tests/target/rapids-4-spark-tests-$PROJECT_VER.jar"

PARQUET_PERF="$WORKSPACE/tests/src/test/resources/parquet_perf"
PARQUET_ACQ="$WORKSPACE/tests/src/test/resources/parquet_acq"
OUTPUT="$WORKSPACE/output"
SPARK_SUBMIT_ARGS=" --class ai.rapids.sparkexamples.mortgage.Main \
    --master spark://$HOSTNAME:7077 --executor-memory 32G \
    --jars $CUDF_JAR,$RAPIDS_PLUGIN_JAR \
    $RAPIDS_TEST_JAR"

if [ "$SPARK_VER"x == x ];then
    SPARK_VER="3.0.0-SNAPSHOT"
fi

TEST_PARAMS="$SPARK_VER $PARQUET_PERF $PARQUET_ACQ $OUTPUT"

# Download the latest Spark tgz file
SPARK_ROOT="$WORKSPACE/spark"
rm -rf $SPARK_ROOT
mkdir -p $SPARK_ROOT
#default maven server gpuwa
if [ "$SERVER_URL"x == x ]; then
    SERVER_URL="https://gpuwa.nvidia.com/artifactory/sw-spark-maven"
fi
SPARK_FILENAME="spark-$SPARK_VER-bin-hadoop3"
SPARK_HOME="$SPARK_ROOT/$SPARK_FILENAME"
mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get \
    -B -Dmaven.repo.local=$WORKSPACE/.m2 \
    -DremoteRepositories=$SERVER_URL \
    -DgroupId=org.apache -DartifactId=spark -Dversion=$SPARK_VER \
    -Dclassifier=bin-hadoop3 -Dpackaging=tar.gz -Ddest=$SPARK_ROOT && \
    tar zxf $SPARK_HOME.tar.gz -C $SPARK_ROOT && \
    rm -f $SPARK_HOME.tar.gz

export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

#stop and restart SPARK ETL
stop-slave.sh
stop-master.sh
start-master.sh
start-slave.sh spark://$HOSTNAME:7077
jps

echo "----------------------------START TEST------------------------------------"
echo "CUDF_VER: $CUDF_VER, CUDA_VER: $CUDA_VER"
rm -rf $OUTPUT
spark-submit $SPARK_SUBMIT_ARGS $TEST_PARAMS
