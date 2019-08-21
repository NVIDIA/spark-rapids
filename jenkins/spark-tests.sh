#!/bin/bash
set -e
CUDF_VER=$1
if [ "$CUDF_VER"x == x ];then
    CUDF_VER="0.9-SNAPSHOT"
fi

CUDA_VER=$2
CUDF_JAR=""
if [ "$CUDA_VER" != "cuda10" ];then
    CUDF_JAR="$WORKSPACE/.m2/ai/rapids/cudf/$CUDF_VER/cudf-$CUDF_VER.jar"
else
    CUDF_JAR="$WORKSPACE/.m2/ai/rapids/cudf/$CUDF_VER/cudf-$CUDF_VER-$CUDA_VER.jar"
fi

RAPIDS_PLUGIN_JAR="$WORKSPACE/sql-plugin/target/rapids-4-spark-$CUDF_VER.jar"
RAPIDS_TEST_JAR="$WORKSPACE/tests/target/rapids-4-spark-tests-$CUDF_VER.jar"

PARQUET_PERF="$WORKSPACE/tests/src/test/resources/parquet_perf"
PARQUET_ACQ="$WORKSPACE/tests/src/test/resources/parquet_acq"
OUTPUT="$WORKSPACE/output"
SPARK_SUBMIT_ARGS=" --class ai.rapids.sparkexamples.mortgage.Main \
    --master spark://$HOSTNAME:7077 --executor-memory 32G \
    --jars $CUDF_JAR,$RAPIDS_PLUGIN_JAR \
    $RAPIDS_TEST_JAR"

TEST_PARAMS="Spark3.0-ITs $PARQUET_PERF $PARQUET_ACQ $OUTPUT"

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
