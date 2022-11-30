#!/bin/bash
#
# Copyright (c) 2020-2022, NVIDIA CORPORATION. All rights reserved.
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

# This script sets the  environment to run tests of RAPIDS Accelerator for Apache Spark on DB.
# All the environments can be overwritten by shell variables:
#   LOCAL_JAR_PATH: Location of the RAPIDS jars
#   SPARK_CONF: Spark configuration parameters
#   BASE_SPARK_VERSION: Spark version [3.1.2, 3.2.1, 3.3.0]. Default is pulled from current instance.
#   ICEBERG_VERSION: The iceberg version. To find the list of supported ICEBERG versions,
#                    check https://iceberg.apache.org/multi-engine-support/#apache-spark
#   SCALA_BINARY_VER: Scala version of the provided binaries. Default is 2.12.
#   TEST_MODE: Can be one of the following (`DEFAULT` is the default value):
#       - DEFAULT: all tests except cudf_udf tests
#       - CUDF_UDF_ONLY: cudf_udf tests only, requires extra conda cudf-py lib
#       - ICEBERG_ONLY: iceberg tests only
#       - DELTA_LAKE_ONLY: delta_lake tests only
# Usage:
# - Running tests on DB10.4/Spark 3.2.1:
#       `BASE_SPARK_VERSION=3.2.1 ./jenkins/databricks/test.sh`
# - Running tests on DB11.3 with ICEBERG Version 1.0.0 instead of default (0.14.1)
#       `BASE_SPARK_VERSION=3.3.0 ICEBERG_VERSION=1.0.0 ./jenkins/databricks/test.sh`
# To add support of a new runtime:
#   1. Check if any more dependencies need to be added to the apt/pip install commands.
#   2. Review the `sw_versions` array, adding the relevant versions required by the new runtime.
#   3. If you had to go beyond the above steps to support the new runtime, then update the
#      instructions accordingly.
set -ex

# Map of software versions for each dependency.
declare -A sw_versions
LOCAL_JAR_PATH=${LOCAL_JAR_PATH:-''}
SPARK_CONF=${SPARK_CONF:-''}
BASE_SPARK_VERSION=${BASE_SPARK_VERSION:-$(< /databricks/spark/VERSION)}
SCALA_BINARY_VER=${SCALA_BINARY_VER:-'2.12'}
[[ -z $SPARK_SHIM_VER ]] && export SPARK_SHIM_VER=spark${BASE_SPARK_VERSION//.}db

# install required packages
sudo apt -y install zip unzip

# Try to use "cudf-udf" conda environment for the python cudf-udf tests.
if [ -d "/databricks/conda/envs/cudf-udf" ]; then
    export PATH=/databricks/conda/envs/cudf-udf/bin:/databricks/conda/bin:$PATH
    export PYSPARK_PYTHON=/databricks/conda/envs/cudf-udf/bin/python
fi
# Try to use the pip from the conda environment if it is available
sudo "$(which pip)" install pytest sre_yield requests pandas pyarrow findspark pytest-xdist pytest-order

export SPARK_HOME=/databricks/spark
# change to not point at databricks confs so we don't conflict with their settings
export SPARK_CONF_DIR=$PWD
# Get Python version (major.minor). i.e., python3.8 for DB10.4 and python3.9 for DB11.3
sw_versions[PYTHON]=$(${PYSPARK_PYTHON} -c 'import sys; print("python{}.{}".format(sys.version_info.major, sys.version_info.minor))')
# Set Iceberg related versions. See https://iceberg.apache.org/multi-engine-support/#apache-spark
case "$BASE_SPARK_VERSION" in
    "3.3.0")
        # Available versions https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/
        sw_versions[ICEBERG]=${ICEBERG_VERSION:-'0.14.1'}
        ;;
    "3.2.1")
        # Available versions https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.2_2.12/
        sw_versions[ICEBERG]=${ICEBERG_VERSION:-'0.13.2'}
        ;;
    "3.1.2")
        # Available versions https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.2_2.12/
        sw_versions[ICEBERG]=${ICEBERG_VERSION:-'0.13.2'}
        ;;
    *) echo "Unexpected Spark version: $BASE_SPARK_VERSION"; exit 1;;
esac
# Set the iceberg_spark to something like 3.3 for DB11.3, 3.2 for DB10.4
sw_versions[ICEBERG_SPARK]=$(echo $BASE_SPARK_VERSION | cut -d. -f1,2)
# Get the correct py4j file.
PY4J_FILE=$(find $SPARK_HOME/python/lib -type f -iname "py4j*.zip")
# Set the path of python site-packages
PYTHON_SITE_PACKAGES=/databricks/python3/lib/${sw_versions[PYTHON]}/site-packages
# Databricks Koalas can conflict with the actual Pandas version, so put site packages first.
# Note that Koala is deprecated for DB10.4+ and it is recommended to use Pandas API on Spark instead.
export PYTHONPATH=$PYTHON_SITE_PACKAGES:$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$PY4J_FILE
sudo ln -s /databricks/jars/ $SPARK_HOME/jars || true
sudo chmod 777 /databricks/data/logs/
sudo chmod 777 /databricks/data/logs/*
echo { \"port\":\"15002\" } > ~/.databricks-connect

CUDF_UDF_TEST_ARGS="--conf spark.python.daemon.module=rapids.daemon_databricks \
    --conf spark.rapids.memory.gpu.minAllocFraction=0 \
    --conf spark.rapids.memory.gpu.allocFraction=0.1 \
    --conf spark.rapids.python.memory.gpu.allocFraction=0.1 \
    --conf spark.rapids.python.concurrentPythonWorkers=2"

## 'spark.foo=1,spark.bar=2,...' to 'export PYSP_TEST_spark_foo=1 export PYSP_TEST_spark_bar=2'
if [ -n "$SPARK_CONF" ]; then
    CONF_LIST=${SPARK_CONF//','/' '}
    for CONF in ${CONF_LIST}; do
        KEY=${CONF%%=*}
        VALUE=${CONF#*=}
        ## run_pyspark_from_build.sh requires 'export PYSP_TEST_spark_foo=1' as the spark configs
        export PYSP_TEST_${KEY//'.'/'_'}=$VALUE
    done

    ## 'spark.foo=1,spark.bar=2,...' to '--conf spark.foo=1 --conf spark.bar=2 --conf ...'
    SPARK_CONF="--conf ${SPARK_CONF/','/' --conf '}"
fi

IS_SPARK_311_OR_LATER=0
[[ "$(printf '%s\n' "3.1.1" "$BASE_SPARK_VERSION" | sort -V | head -n1)" = "3.1.1" ]] && IS_SPARK_311_OR_LATER=1


# TEST_MODE
# - DEFAULT: all tests except cudf_udf tests
# - CUDF_UDF_ONLY: cudf_udf tests only, requires extra conda cudf-py lib
# - ICEBERG_ONLY: iceberg tests only
# - DELTA_LAKE_ONLY: delta_lake tests only
TEST_MODE=${TEST_MODE:-'DEFAULT'}
TEST_TYPE="nightly"
PCBS_CONF="com.nvidia.spark.ParquetCachedBatchSerializer"

# Classloader config is here to work around classloader issues with
# --packages in distributed setups, should be fixed by
# https://github.com/NVIDIA/spark-rapids/pull/5646
ICEBERG_CONFS="--packages org.apache.iceberg:iceberg-spark-runtime-${sw_versions[ICEBERG_SPARK]}_${SCALA_BINARY_VER}:${sw_versions[ICEBERG]} \
 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
 --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
 --conf spark.sql.catalog.spark_catalog.type=hadoop \
 --conf spark.sql.catalog.spark_catalog.warehouse=/tmp/spark-warehouse-$$"

DELTA_LAKE_CONFS=""

# Enable event log for qualification & profiling tools testing
export PYSP_TEST_spark_eventLog_enabled=true
mkdir -p /tmp/spark-events

## limit parallelism to avoid OOM kill
export TEST_PARALLEL=4
if [ -d "$LOCAL_JAR_PATH" ]; then
    if [[ $TEST_MODE == "DEFAULT" ]]; then
        ## Run tests with jars in the LOCAL_JAR_PATH dir downloading from the dependency repo
        LOCAL_JAR_PATH=$LOCAL_JAR_PATH bash $LOCAL_JAR_PATH/integration_tests/run_pyspark_from_build.sh  --runtime_env="databricks" --test_type=$TEST_TYPE

        ## Run cache tests
        if [[ "$IS_SPARK_311_OR_LATER" -eq "1" ]]; then
          PYSP_TEST_spark_sql_cache_serializer=${PCBS_CONF} \
           LOCAL_JAR_PATH=$LOCAL_JAR_PATH bash $LOCAL_JAR_PATH/integration_tests/run_pyspark_from_build.sh  --runtime_env="databricks" --test_type=$TEST_TYPE -k cache_test
        fi
    fi

    if [[ "$TEST_MODE" == "CUDF_UDF_ONLY" ]]; then
        ## Run cudf-udf tests
        CUDF_UDF_TEST_ARGS="$CUDF_UDF_TEST_ARGS --conf spark.executorEnv.PYTHONPATH=`ls $LOCAL_JAR_PATH/rapids-4-spark_*.jar | grep -v 'tests.jar'`"
        LOCAL_JAR_PATH=$LOCAL_JAR_PATH SPARK_SUBMIT_FLAGS="$SPARK_CONF $CUDF_UDF_TEST_ARGS" TEST_PARALLEL=1 \
            bash $LOCAL_JAR_PATH/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" -m "cudf_udf" --cudf_udf --test_type=$TEST_TYPE
    fi

    if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "ICEBERG_ONLY" ]]; then
        ## Run Iceberg tests
        LOCAL_JAR_PATH=$LOCAL_JAR_PATH SPARK_SUBMIT_FLAGS="$SPARK_CONF $ICEBERG_CONFS" TEST_PARALLEL=1 \
            bash $LOCAL_JAR_PATH/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" -m iceberg --iceberg --test_type=$TEST_TYPE
    fi
else
    if [[ $TEST_MODE == "DEFAULT" ]]; then
        ## Run tests with jars building from the spark-rapids source code
        bash /home/ubuntu/spark-rapids/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" --test_type=$TEST_TYPE

        ## Run cache tests
        if [[ "$IS_SPARK_311_OR_LATER" -eq "1" ]]; then
            PYSP_TEST_spark_sql_cache_serializer=${PCBS_CONF} \
            bash /home/ubuntu/spark-rapids/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" --test_type=$TEST_TYPE -k cache_test
        fi
    fi

    if [[ "$TEST_MODE" == "CUDF_UDF_ONLY" ]]; then
        ## Run cudf-udf tests
        CUDF_UDF_TEST_ARGS="$CUDF_UDF_TEST_ARGS --conf spark.executorEnv.PYTHONPATH=`ls /home/ubuntu/spark-rapids/dist/target/rapids-4-spark_*.jar | grep -v 'tests.jar'`"
        SPARK_SUBMIT_FLAGS="$SPARK_CONF $CUDF_UDF_TEST_ARGS" TEST_PARALLEL=1 \
            bash /home/ubuntu/spark-rapids/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks"  -m "cudf_udf" --cudf_udf --test_type=$TEST_TYPE
    fi

    if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "ICEBERG_ONLY" ]]; then
        ## Run Iceberg tests
        SPARK_SUBMIT_FLAGS="$SPARK_CONF $ICEBERG_CONFS" TEST_PARALLEL=1 \
            bash /home/ubuntu/spark-rapids/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks"  -m iceberg --iceberg --test_type=$TEST_TYPE
    fi

    if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "DELTA_LAKE_ONLY" ]]; then
        ## Run Delta Lake tests
        SPARK_SUBMIT_FLAGS="$SPARK_CONF $DELTA_LAKE_CONFS" TEST_PARALLEL=1 \
            bash /home/ubuntu/spark-rapids/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks"  -m "delta_lake" --delta_lake --test_type=$TEST_TYPE
    fi
fi
