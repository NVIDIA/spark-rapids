#!/bin/bash
#
# Copyright (c) 2019-2023, NVIDIA CORPORATION. All rights reserved.
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
MVN_GET_CMD="mvn -Dmaven.wagon.http.retryHandler.count=3 org.apache.maven.plugins:maven-dependency-plugin:2.8:get -B \
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

. jenkins/hadoop-def.sh $SPARK_VER ${SCALA_BINARY_VER}
wget -P $ARTF_ROOT $SPARK_REPO/org/apache/spark/$SPARK_VER/spark-$SPARK_VER-$BIN_HADOOP_VER.tgz

# Download parquet-hadoop jar for parquet-read encryption tests
PARQUET_HADOOP_VER=`mvn help:evaluate -q -N -Dexpression=parquet.hadoop.version -DforceStdout -Dbuildver=${SHUFFLE_SPARK_SHIM/spark/}`
if [[ "$(printf '%s\n' "1.12.0" "$PARQUET_HADOOP_VER" | sort -V | head -n1)" = "1.12.0" ]]; then
  $MVN_GET_CMD -DremoteRepositories=$PROJECT_REPO \
      -DgroupId=org.apache.parquet -DartifactId=parquet-hadoop -Dversion=$PARQUET_HADOOP_VER -Dclassifier=tests
fi

export SPARK_HOME="$ARTF_ROOT/spark-$SPARK_VER-$BIN_HADOOP_VER"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
tar zxf $SPARK_HOME.tgz -C $ARTF_ROOT && \
    rm -f $SPARK_HOME.tgz
# copy python path libs to container /tmp instead of workspace to avoid ephemeral PVC issue
TMP_PYTHON=/tmp/$(date +"%Y%m%d")
rm -rf $TMP_PYTHON && mkdir -p $TMP_PYTHON && cp -r $SPARK_HOME/python $TMP_PYTHON
# Get the correct py4j file.
PY4J_FILE=$(find $TMP_PYTHON/python/lib -type f -iname "py4j*.zip")
export PYTHONPATH=$TMP_PYTHON/python:$TMP_PYTHON/python/pyspark/:$PY4J_FILE

# Extract 'value' from conda config string 'key: value'
CONDA_ROOT=`conda config --show root_prefix | cut -d ' ' -f2`
PYTHON_VER=`conda config --show default_python | cut -d ' ' -f2`
# Put conda package path ahead of the env 'PYTHONPATH',
# to import the right pandas from conda instead of spark binary path.
export PYTHONPATH="$CONDA_ROOT/lib/python$PYTHON_VER/site-packages:$PYTHONPATH"


echo "----------------------------START TEST------------------------------------"
pushd $RAPIDS_INT_TESTS_HOME
export TEST_TYPE="nightly"
export LOCAL_JAR_PATH=$ARTF_ROOT

export SPARK_TASK_MAXFAILURES=1
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
# enable worker cleanup to avoid "out of space" issue
# if failed, we abort the test instantly, so the failed executor log should still be left there for debugging
export SPARK_WORKER_OPTS="$SPARK_WORKER_OPTS -Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=120 -Dspark.worker.cleanup.appDataTtl=60"
#stop and restart SPARK ETL
stop-worker.sh
stop-master.sh
start-master.sh
start-worker.sh spark://$HOSTNAME:7077
jps

# BASE spark test configs
export PYSP_TEST_spark_master=spark://$HOSTNAME:7077
export PYSP_TEST_spark_sql_shuffle_partitions=12
export PYSP_TEST_spark_task_maxFailures=$SPARK_TASK_MAXFAILURES
export PYSP_TEST_spark_dynamicAllocation_enabled=false
export PYSP_TEST_spark_driver_extraJavaOptions=-Duser.timezone=UTC
export PYSP_TEST_spark_executor_extraJavaOptions=-Duser.timezone=UTC
export PYSP_TEST_spark_sql_session_timeZone=UTC

# PARALLEL or non-PARALLEL specific configs
if [[ $PARALLEL_TEST == "true" ]]; then
  export PYSP_TEST_spark_cores_max=1
  export PYSP_TEST_spark_executor_memory=4g
  export PYSP_TEST_spark_executor_cores=1
  export PYSP_TEST_spark_task_cores=1
  export PYSP_TEST_spark_rapids_sql_concurrentGpuTasks=1
  export PYSP_TEST_spark_rapids_memory_gpu_minAllocFraction=0

  if [[ "${PARALLELISM}" == "" ]]; then
    PARALLELISM=$(nvidia-smi --query-gpu=memory.free --format=csv,noheader | \
      awk '{if (MAX < $1){ MAX = $1}} END {print int(MAX / (2 * 1024))}')
  fi
  # parallelism > 5 could slow down the whole process, so we have a limitation for it
  # this is based on our CI gpu types, so we do not put it into the run_pyspark_from_build.sh
  [[ ${PARALLELISM} -gt 5 ]] && PARALLELISM=5
  MEMORY_FRACTION=$(python -c "print(1/($PARALLELISM + 0.1))")

  export TEST_PARALLEL=${PARALLELISM}
  export PYSP_TEST_spark_rapids_memory_gpu_allocFraction=${MEMORY_FRACTION}
  export PYSP_TEST_spark_rapids_memory_gpu_maxAllocFraction=${MEMORY_FRACTION}
else
  export PYSP_TEST_spark_cores_max=6
  export PYSP_TEST_spark_executor_memory=16g
  export TEST_PARALLEL=0
fi

export SCRIPT_PATH="$(pwd -P)"
export TARGET_DIR="$SCRIPT_PATH/target"
mkdir -p $TARGET_DIR

run_delta_lake_tests() {
  echo "run_delta_lake_tests SPARK_VER = $SPARK_VER"
  SPARK_32X_PATTERN="(3\.2\.[0-9])"
  SPARK_33X_PATTERN="(3\.3\.[0-9])"
  SPARK_34X_PATTERN="(3\.4\.[0-9])"

  if [[ $SPARK_VER =~ $SPARK_32X_PATTERN ]]; then
    # There are multiple versions of deltalake that support SPARK 3.2.X
    # but for zorder tests to work we need 2.0.0+
    DELTA_LAKE_VERSIONS="2.0.1"
  fi

  if [[ $SPARK_VER =~ $SPARK_33X_PATTERN ]]; then
    DELTA_LAKE_VERSIONS="2.1.1 2.2.0"
  fi

  if [[ $SPARK_VER =~ $SPARK_34X_PATTERN ]]; then
    DELTA_LAKE_VERSIONS="2.4.0"
  fi

  if [ -z "$DELTA_LAKE_VERSIONS" ]; then
    echo "Skipping Delta Lake tests. $SPARK_VER"
  else
    for v in $DELTA_LAKE_VERSIONS; do
      echo "Running Delta Lake tests for Delta Lake version $v"
      PYSP_TEST_spark_jars_packages="io.delta:delta-core_${SCALA_BINARY_VER}:$v" \
        PYSP_TEST_spark_sql_extensions="io.delta.sql.DeltaSparkSessionExtension" \
        PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        ./run_pyspark_from_build.sh -m delta_lake --delta_lake
    done
  fi
}

run_iceberg_tests() {
  ICEBERG_VERSION=${ICEBERG_VERSION:-0.13.2}
  # get the major/minor version of Spark
  ICEBERG_SPARK_VER=$(echo $SPARK_VER | cut -d. -f1,2)
  IS_SPARK_33_OR_LATER=0
  [[ "$(printf '%s\n' "3.3" "$ICEBERG_SPARK_VER" | sort -V | head -n1)" = "3.3" ]] && IS_SPARK_33_OR_LATER=1

  # RAPIDS-iceberg does not support Spark 3.3+ yet
  if [[ "$IS_SPARK_33_OR_LATER" = "1" ]]; then
    echo "!!!! Skipping Iceberg tests. GPU acceleration of Iceberg is not supported on $ICEBERG_SPARK_VER"
  else
    PYSP_TEST_spark_jars_packages=org.apache.iceberg:iceberg-spark-runtime-${ICEBERG_SPARK_VER}_${SCALA_BINARY_VER}:${ICEBERG_VERSION} \
      PYSP_TEST_spark_sql_extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
      PYSP_TEST_spark_sql_catalog_spark__catalog="org.apache.iceberg.spark.SparkSessionCatalog" \
      PYSP_TEST_spark_sql_catalog_spark__catalog_type="hadoop" \
      PYSP_TEST_spark_sql_catalog_spark__catalog_warehouse="/tmp/spark-warehouse-$RANDOM" \
      ./run_pyspark_from_build.sh -m iceberg --iceberg
  fi
}

run_avro_tests() {
  # Workaround to avoid appending avro jar file by '--jars',
  # which would be addressed by https://github.com/NVIDIA/spark-rapids/issues/6532
  # Adding the Apache snapshots repository since we may be running with a snapshot
  # version of Apache Spark which requires accessing the snapshot repository to
  # fetch the spark-avro jar.
  rm -vf $LOCAL_JAR_PATH/spark-avro*.jar
  PYSP_TEST_spark_jars_packages="org.apache.spark:spark-avro_${SCALA_BINARY_VER}:${SPARK_VER}" \
    PYSP_TEST_spark_jars_repositories="https://repository.apache.org/snapshots" \
    ./run_pyspark_from_build.sh -k avro
}

rapids_shuffle_smoke_test() {
    echo "Run rapids_shuffle_smoke_test..."

    # using MULTITHREADED shuffle
    PYSP_TEST_spark_rapids_shuffle_mode=MULTITHREADED \
    PYSP_TEST_spark_rapids_shuffle_multiThreaded_writer_threads=2 \
    PYSP_TEST_spark_rapids_shuffle_multiThreaded_reader_threads=2 \
    PYSP_TEST_spark_shuffle_manager=com.nvidia.spark.rapids.$SHUFFLE_SPARK_SHIM.RapidsShuffleManager \
    SPARK_SUBMIT_FLAGS="$SPARK_CONF" \
    ./run_pyspark_from_build.sh -m shuffle_test
}

run_pyarrow_tests() {
  ./run_pyspark_from_build.sh -m pyarrow_test --pyarrow_test
}

# TEST_MODE
# - DEFAULT: all tests except cudf_udf tests
# - DELTA_LAKE_ONLY: Delta Lake tests only
# - ICEBERG_ONLY: iceberg tests only
# - AVRO_ONLY: avro tests only (with --packages option instead of --jars)
# - CUDF_UDF_ONLY: cudf_udf tests only, requires extra conda cudf-py lib
# - MULTITHREADED_SHUFFLE: shuffle tests only
TEST_MODE=${TEST_MODE:-'DEFAULT'}
if [[ $TEST_MODE == "DEFAULT" ]]; then
  ./run_pyspark_from_build.sh

  SPARK_SHELL_SMOKE_TEST=1 \
  PYSP_TEST_spark_shuffle_manager=com.nvidia.spark.rapids.${SHUFFLE_SPARK_SHIM}.RapidsShuffleManager \
    ./run_pyspark_from_build.sh

  # ParquetCachedBatchSerializer cache_test
  PYSP_TEST_spark_sql_cache_serializer=com.nvidia.spark.ParquetCachedBatchSerializer \
    ./run_pyspark_from_build.sh -k cache_test
fi

# Delta Lake tests
if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "DELTA_LAKE_ONLY" ]]; then
  run_delta_lake_tests
fi

# Iceberg tests
if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "ICEBERG_ONLY" ]]; then
  run_iceberg_tests
fi

# Avro tests
if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "AVRO_ONLY" ]]; then
  run_avro_tests
fi

# Mutithreaded Shuffle test
if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "MULTITHREADED_SHUFFLE" ]]; then
  rapids_shuffle_smoke_test
fi

# cudf_udf test: this mostly depends on cudf-py, so we run it into an independent CI
if [[ "$TEST_MODE" == "CUDF_UDF_ONLY" ]]; then
  # hardcode config
  [[ ${TEST_PARALLEL} -gt 2 ]] && export TEST_PARALLEL=2
  PYSP_TEST_spark_rapids_memory_gpu_allocFraction=0.1 \
    PYSP_TEST_spark_rapids_memory_gpu_minAllocFraction=0 \
    PYSP_TEST_spark_rapids_python_memory_gpu_allocFraction=0.1 \
    PYSP_TEST_spark_rapids_python_concurrentPythonWorkers=2 \
    PYSP_TEST_spark_executorEnv_PYTHONPATH=${RAPIDS_PLUGIN_JAR} \
    PYSP_TEST_spark_python=${CONDA_ROOT}/bin/python \
    ./run_pyspark_from_build.sh -m cudf_udf --cudf_udf
fi

# Pyarrow tests
if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "PYARROW_ONLY" ]]; then
  run_pyarrow_tests
fi

popd
stop-worker.sh
stop-master.sh
