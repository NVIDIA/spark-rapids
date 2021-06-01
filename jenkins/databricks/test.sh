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

LOCAL_JAR_PATH=$1
SPARK_CONF=$2

# tests
export PATH=/databricks/conda/envs/databricks-ml-gpu/bin:/databricks/conda/condabin:$PATH
sudo /databricks/conda/envs/databricks-ml-gpu/bin/pip install pytest sre_yield requests pandas \
	pyarrow findspark pytest-xdist

export SPARK_HOME=/databricks/spark
# change to not point at databricks confs so we don't conflict with their settings
export SPARK_CONF_DIR=$PWD
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip
sudo ln -s /databricks/jars/ $SPARK_HOME/jars || true
sudo chmod 777 /databricks/data/logs/
sudo chmod 777 /databricks/data/logs/*
echo { \"port\":\"15002\" } > ~/.databricks-connect

CUDF_UDF_TEST_ARGS="--conf spark.python.daemon.module=rapids.daemon_databricks \
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

TEST_TYPE="nightly"
if [ -d "$LOCAL_JAR_PATH" ]; then
    ## Run tests with jars in the LOCAL_JAR_PATH dir downloading from the denpedency repo
    LOCAL_JAR_PATH=$LOCAL_JAR_PATH bash $LOCAL_JAR_PATH/integration_tests/run_pyspark_from_build.sh  --runtime_env="databricks" --test_type=$TEST_TYPE

    ## Run cudf-udf tests
    #disable cudf_udf tests for now until we resolve https://github.com/NVIDIA/spark-rapids/issues/2521
    #CUDF_UDF_TEST_ARGS="$CUDF_UDF_TEST_ARGS --conf spark.executorEnv.PYTHONPATH=`ls $LOCAL_JAR_PATH/rapids-4-spark_*.jar | grep -v 'tests.jar'`"
    #LOCAL_JAR_PATH=$LOCAL_JAR_PATH SPARK_SUBMIT_FLAGS="$SPARK_CONF $CUDF_UDF_TEST_ARGS" TEST_PARALLEL=1 \
    #    bash $LOCAL_JAR_PATH/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" -m "cudf_udf" --cudf_udf --test_type=$TEST_TYPE
else
    ## Run tests with jars building from the spark-rapids source code
    bash /home/ubuntu/spark-rapids/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" --test_type=$TEST_TYPE

    ## Run cudf-udf tests
    #disable cudf_udf tests for now until we resolve https://github.com/NVIDIA/spark-rapids/issues/2521
    #CUDF_UDF_TEST_ARGS="$CUDF_UDF_TEST_ARGS --conf spark.executorEnv.PYTHONPATH=`ls /home/ubuntu/spark-rapids/dist/target/rapids-4-spark_*.jar | grep -v 'tests.jar'`"
    #SPARK_SUBMIT_FLAGS="$SPARK_CONF $CUDF_UDF_TEST_ARGS" TEST_PARALLEL=1 \
    #    bash /home/ubuntu/spark-rapids/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks"  -m "cudf_udf" --cudf_udf --test_type=$TEST_TYPE
fi
