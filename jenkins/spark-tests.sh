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

CUDF_UDF_TEST_ARGS="--conf spark.rapids.memory.gpu.allocFraction=0.1 \
    --conf spark.rapids.python.memory.gpu.allocFraction=0.1 \
    --conf spark.rapids.python.concurrentPythonWorkers=2 \
    --conf spark.executorEnv.PYTHONPATH=${RAPIDS_PLUGIN_JAR} \
    --conf spark.pyspark.python=/opt/conda/bin/python \
    --py-files ${RAPIDS_PLUGIN_JAR}" # explicitly specify python binary path in env w/ multiple python versions

export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

#stop and restart SPARK ETL
stop-slave.sh
stop-master.sh
start-master.sh
start-slave.sh spark://$HOSTNAME:7077
jps

echo "----------------------------START TEST------------------------------------"
pushd $RAPIDS_INT_TESTS_HOME
TEST_TYPE="nightly"
spark-submit $BASE_SPARK_SUBMIT_ARGS --jars $RAPIDS_TEST_JAR ./runtests.py -v -rfExXs --std_input_path="$WORKSPACE/integration_tests/src/test/resources/" --test_type=$TEST_TYPE

#disable cudf_udf tests for now until we resolve https://github.com/NVIDIA/spark-rapids/issues/2521
#spark-submit $BASE_SPARK_SUBMIT_ARGS $CUDF_UDF_TEST_ARGS --jars $RAPIDS_TEST_JAR ./runtests.py -m "cudf_udf" -v -rfExXs --cudf_udf --test_type=$TEST_TYPE

#only run cache tests with our serializer in nightly test for Spark version >= 3.1.1
if [ "$(printf '%s\n' "3.1.1" "$SPARK_VER" | sort -V | head -n1)" = "3.1.1" ]; then
  SHIM_PACKAGE=$(echo ${SPARK_VER} | sed 's/\.//g' | sed 's/-SNAPSHOT//')
  spark-submit ${BASE_SPARK_SUBMIT_ARGS} --conf spark.sql.cache.serializer=com.nvidia.spark.rapids.shims.spark${SHIM_PACKAGE}.ParquetCachedBatchSerializer --jars $RAPIDS_TEST_JAR \
  ./runtests.py -v -rfExXs --std_input_path="$WORKSPACE/integration_tests/src/test/resources/" -k cache_test.py -x
fi
popd
stop-slave.sh
stop-master.sh

