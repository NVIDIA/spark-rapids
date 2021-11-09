#!/bin/bash
#
# Copyright (c) 2020-2021, NVIDIA CORPORATION. All rights reserved.
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
sudo "$(which pip)" install pytest sre_yield requests pandas pyarrow findspark pytest-xdist pytest-ordering

export SPARK_HOME=/databricks/spark
# change to not point at databricks confs so we don't conflict with their settings
export SPARK_CONF_DIR=$PWD
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip
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

TEST_TYPE="nightly"
PCBS_CONF="com.nvidia.spark.ParquetCachedBatchSerializer"
## limit parallelism to avoid OOM kill
export TEST_PARALLEL=4
if [ -d "$LOCAL_JAR_PATH" ]; then
    ## Run tests with jars in the LOCAL_JAR_PATH dir downloading from the dependency repo
    LOCAL_JAR_PATH=$LOCAL_JAR_PATH bash $LOCAL_JAR_PATH/integration_tests/run_pyspark_from_build.sh  --runtime_env="databricks" --test_type=$TEST_TYPE

    ## Run cache tests
    if [[ "$IS_SPARK_311_OR_LATER" -eq "1" ]]; then
      PYSP_TEST_spark_sql_cache_serializer=${PCBS_CONF} \
       LOCAL_JAR_PATH=$LOCAL_JAR_PATH bash $LOCAL_JAR_PATH/integration_tests/run_pyspark_from_build.sh  --runtime_env="databricks" --test_type=$TEST_TYPE -k cache_test
    fi

    ## Run cudf-udf tests
# disable cudf_udf test until https://github.com/rapidsai/cudf/issues/9622 get fixed
#    CUDF_UDF_TEST_ARGS="$CUDF_UDF_TEST_ARGS --conf spark.executorEnv.PYTHONPATH=`ls $LOCAL_JAR_PATH/rapids-4-spark_*.jar | grep -v 'tests.jar'`"
#    LOCAL_JAR_PATH=$LOCAL_JAR_PATH SPARK_SUBMIT_FLAGS="$SPARK_CONF $CUDF_UDF_TEST_ARGS" TEST_PARALLEL=1 \
#        bash $LOCAL_JAR_PATH/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" -m "cudf_udf" --cudf_udf --test_type=$TEST_TYPE

else
    ## Run tests with jars building from the spark-rapids source code
    bash /home/ubuntu/spark-rapids/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" --test_type=$TEST_TYPE

    ## Run cache tests
    if [[ "$IS_SPARK_311_OR_LATER" -eq "1" ]]; then
      PYSP_TEST_spark_sql_cache_serializer=${PCBS_CONF} \
       bash /home/ubuntu/spark-rapids/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" --test_type=$TEST_TYPE -k cache_test
    fi

    ## Run cudf-udf tests
# disable cudf_udf test until https://github.com/rapidsai/cudf/issues/9622 get fixed
#    CUDF_UDF_TEST_ARGS="$CUDF_UDF_TEST_ARGS --conf spark.executorEnv.PYTHONPATH=`ls /home/ubuntu/spark-rapids/dist/target/rapids-4-spark_*.jar | grep -v 'tests.jar'`"
#    SPARK_SUBMIT_FLAGS="$SPARK_CONF $CUDF_UDF_TEST_ARGS" TEST_PARALLEL=1 \
#        bash /home/ubuntu/spark-rapids/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks"  -m "cudf_udf" --cudf_udf --test_type=$TEST_TYPE
fi
