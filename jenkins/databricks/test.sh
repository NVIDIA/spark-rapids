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

set -ex

LOCAL_JAR_PATH=${LOCAL_JAR_PATH:-''}
SPARK_CONF=${SPARK_CONF:-''}
BASE_SPARK_VER=${BASE_SPARK_VER:-'3.1.2'}
[[ -z $SPARK_SHIM_VER ]] && export SPARK_SHIM_VER=spark${BASE_SPARK_VER//.}db

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
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip
if [[ $BASE_SPARK_VER == "3.2.1" ]]
then
  # Databricks Koalas can conflict with the actual Pandas version, so put site packages first
  export PYTHONPATH=/databricks/python3/lib/python3.8/site-packages:$PYTHONPATH
fi
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
[[ "$(printf '%s\n' "3.1.1" "$BASE_SPARK_VER" | sort -V | head -n1)" = "3.1.1" ]] && IS_SPARK_311_OR_LATER=1


# TEST_MODE
# - DEFAULT: all tests except cudf_udf tests
# - CUDF_UDF_ONLY: cudf_udf tests only, requires extra conda cudf-py lib
# - ICEBERG_ONLY: iceberg tests only
# - DELTA_LAKE_ONLY: delta_lake tests only
TEST_MODE=${TEST_MODE:-'DEFAULT'}
TEST_TYPE="nightly"
PCBS_CONF="com.nvidia.spark.ParquetCachedBatchSerializer"

ICEBERG_VERSION=0.13.2
ICEBERG_SPARK_VER=$(echo $BASE_SPARK_VER | cut -d. -f1,2)
# Classloader config is here to work around classloader issues with
# --packages in distributed setups, should be fixed by
# https://github.com/NVIDIA/spark-rapids/pull/5646
ICEBERG_CONFS="--conf spark.rapids.force.caller.classloader=false \
 --packages org.apache.iceberg:iceberg-spark-runtime-${ICEBERG_SPARK_VER}_2.12:${ICEBERG_VERSION} \
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
