#!/bin/bash
#
# Copyright (c) 2020-2023, NVIDIA CORPORATION. All rights reserved.
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
#   BASE_SPARK_VERSION: Spark version [3.2.1, 3.3.0]. Default is pulled from current instance.
#   SHUFFLE_SPARK_SHIM: Set the default value for the shuffle shim. For databricks versions, append
#                       db. Example: spark330 => spark330db
#   TEST_MODE: Can be one of the following (`DEFAULT` is the default value):
#       - DEFAULT: all tests except cudf_udf tests
#       - DELTA_LAKE_ONLY: delta_lake tests only
#       - MULTITHREADED_SHUFFLE: shuffle tests only
# Usage:
# - Running tests on DB10.4/Spark 3.2.1:
#       `BASE_SPARK_VERSION=3.2.1 ./jenkins/databricks/test.sh`
# To add support of a new runtime:
#   1. Check if any more dependencies need to be added to the apt/pip install commands.
#   2. If you had to go beyond the above steps to support the new runtime, then update the
#      instructions accordingly.
set -ex

SOURCE_PATH="/home/ubuntu/spark-rapids"
[[ -d "$LOCAL_JAR_PATH" ]] && cd $LOCAL_JAR_PATH || cd $SOURCE_PATH

# Install python packages for integration tests
source jenkins/databricks/setup.sh

SPARK_CONF=${SPARK_CONF:-''}
BASE_SPARK_VERSION=${BASE_SPARK_VERSION:-$(< /databricks/spark/VERSION)}
SHUFFLE_SPARK_SHIM=${SHUFFLE_SPARK_SHIM:-spark${BASE_SPARK_VERSION//./}db}
SHUFFLE_SPARK_SHIM=${SHUFFLE_SPARK_SHIM//\-SNAPSHOT/}
[[ -z $SPARK_SHIM_VER ]] && export SPARK_SHIM_VER=spark${BASE_SPARK_VERSION//.}db

# Set PYSPARK_PYTHON to keep the version of driver/workers python consistent.
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-"$(which python)"}
# Get Python version (major.minor). i.e., python3.8 for DB10.4 and python3.9 for DB11.3
PYTHON_VERSION=$(${PYSPARK_PYTHON} -c 'import sys; print("python{}.{}".format(sys.version_info.major, sys.version_info.minor))')
# Set the path of python site-packages, packages were installed here by 'jenkins/databricks/setup.sh'.
PYTHON_SITE_PACKAGES="$HOME/.local/lib/${PYTHON_VERSION}/site-packages"

export SPARK_HOME=/databricks/spark
# change to not point at databricks confs so we don't conflict with their settings
export SPARK_CONF_DIR=$PWD
# Get the correct py4j file.
PY4J_FILE=$(find $SPARK_HOME/python/lib -type f -iname "py4j*.zip")
# Databricks Koalas can conflict with the actual Pandas version, so put site packages first.
# Note that Koala is deprecated for DB10.4+ and it is recommended to use Pandas API on Spark instead.
export PYTHONPATH=$PYTHON_SITE_PACKAGES:$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$PY4J_FILE

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

IS_SPARK_321_OR_LATER=0
[[ "$(printf '%s\n' "3.2.1" "$BASE_SPARK_VERSION" | sort -V | head -n1)" = "3.2.1" ]] && IS_SPARK_321_OR_LATER=1


# TEST_MODE
# - DEFAULT: all tests except cudf_udf tests
# - DELTA_LAKE_ONLY: delta_lake tests only
# - MULTITHREADED_SHUFFLE: shuffle tests only
TEST_MODE=${TEST_MODE:-'DEFAULT'}
TEST_TYPE="nightly"
PCBS_CONF="com.nvidia.spark.ParquetCachedBatchSerializer"

# Classloader config is here to work around classloader issues with
# --packages in distributed setups, should be fixed by
# https://github.com/NVIDIA/spark-rapids/pull/5646

# Increase driver memory as Delta Lake tests can slowdown with default 1G (possibly due to caching?)
DELTA_LAKE_CONFS="--driver-memory 2g"

# Enable event log for qualification & profiling tools testing
export PYSP_TEST_spark_eventLog_enabled=true
mkdir -p /tmp/spark-events

rapids_shuffle_smoke_test() {
    echo "Run rapids_shuffle_smoke_test..."

    # using MULTITHREADED shuffle
    PYSP_TEST_spark_rapids_shuffle_mode=MULTITHREADED \
    PYSP_TEST_spark_rapids_shuffle_multiThreaded_writer_threads=2 \
    PYSP_TEST_spark_rapids_shuffle_multiThreaded_reader_threads=2 \
    PYSP_TEST_spark_shuffle_manager=com.nvidia.spark.rapids.$SHUFFLE_SPARK_SHIM.RapidsShuffleManager \
    SPARK_SUBMIT_FLAGS="$SPARK_CONF" \
    bash integration_tests/run_pyspark_from_build.sh -m shuffle_test --runtime_env="databricks" --test_type=$TEST_TYPE
}

## limit parallelism to avoid OOM kill
export TEST_PARALLEL=${TEST_PARALLEL:-4}

if [[ $TEST_MODE == "DEFAULT" ]]; then
    bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" --test_type=$TEST_TYPE

    ## Run cache tests
    if [[ "$IS_SPARK_321_OR_LATER" -eq "1" ]]; then
        PYSP_TEST_spark_sql_cache_serializer=${PCBS_CONF} \
            bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" --test_type=$TEST_TYPE -k cache_test
    fi
fi

## Run tests with jars building from the spark-rapids source code
if [ "$(pwd)" == "$SOURCE_PATH" ]; then
    if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "DELTA_LAKE_ONLY" ]]; then
        ## Run Delta Lake tests
        SPARK_SUBMIT_FLAGS="$SPARK_CONF $DELTA_LAKE_CONFS" TEST_PARALLEL=1 \
            bash integration_tests/run_pyspark_from_build.sh --runtime_env="databricks"  -m "delta_lake" --delta_lake --test_type=$TEST_TYPE
    fi

    if [[ "$TEST_MODE" == "DEFAULT" || "$TEST_MODE" == "MULTITHREADED_SHUFFLE" ]]; then
        ## Mutithreaded Shuffle test
        rapids_shuffle_smoke_test
    fi
fi
