#!/bin/bash
#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
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

# This script sets the environment to run cudf_udf tests of RAPIDS Accelerator for Apache Spark on DB.
# cudf conda packages need to be installed in advance, please refer to
#   './jenkins/databricks/init_cudf_udf.sh' to install.
# All the environments can be overwritten by shell variables:
#   LOCAL_JAR_PATH: Location of the RAPIDS jars
#   SPARK_CONF: Spark configuration parameters

# Usage:
# - Running tests on Databricks:
#       `./jenkins/databricks/cudf-udf-test.sh`
# To add support of a new runtime:
#   1. Check if any more dependencies need to be added to the apt/conda install commands.
#   2. If you had to go beyond the above steps to support the new runtime, then update the
#      instructions accordingly.
set -ex

# Map of software versions for each dependency.

LOCAL_JAR_PATH=${LOCAL_JAR_PATH:-''}
SPARK_CONF=${SPARK_CONF:-''}

# Try to use "cudf-udf" conda environment for the python cudf-udf tests.
CONDA_HOME=${CONDA_HOME:-"/databricks/conda"}
if [ ! -d "${CONDA_HOME}/envs/cudf-udf" ]; then
    echo "Error not found cudf conda packages! Please refer to './jenkins/databricks/init_cudf_udf.sh' to install."
    exit -1
fi
export PATH=${CONDA_HOME}/envs/cudf-udf/bin:$PATH
export PYSPARK_PYTHON=${CONDA_HOME}/envs/cudf-udf/bin/python
# Get Python version (major.minor). i.e., python3.8 for DB10.4 and python3.9 for DB11.3
PYTHON_VERSION=$(${PYSPARK_PYTHON} -c 'import sys; print("python{}.{}".format(sys.version_info.major, sys.version_info.minor))')

# Install required packages
sudo apt -y install zip unzip

export SPARK_HOME=/databricks/spark
# Change to not point at Databricks confs so we don't conflict with their settings.
export SPARK_CONF_DIR=$PWD

# Get the correct py4j file.
PY4J_FILE=$(find $SPARK_HOME/python/lib -type f -iname "py4j*.zip")
# Set the path of python site-packages.
PYTHON_SITE_PACKAGES="${CONDA_HOME}/envs/cudf-udf/lib/${PYTHON_VERSION}/site-packages"
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

TEST_TYPE="nightly"
PCBS_CONF="com.nvidia.spark.ParquetCachedBatchSerializer"

# Enable event log for qualification & profiling tools testing
export PYSP_TEST_spark_eventLog_enabled=true
mkdir -p /tmp/spark-events

if [ -d "$LOCAL_JAR_PATH" ]; then
    ## Run cudf-udf tests.
    CUDF_UDF_TEST_ARGS="$CUDF_UDF_TEST_ARGS --conf spark.executorEnv.PYTHONPATH=`ls $LOCAL_JAR_PATH/rapids-4-spark_*.jar | grep -v 'tests.jar'`"
    LOCAL_JAR_PATH=$LOCAL_JAR_PATH SPARK_SUBMIT_FLAGS="$SPARK_CONF $CUDF_UDF_TEST_ARGS" TEST_PARALLEL=1 \
        bash $LOCAL_JAR_PATH/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" -m "cudf_udf" --cudf_udf --test_type=$TEST_TYPE
else
    ## Run cudf-udf tests.
    CUDF_UDF_TEST_ARGS="$CUDF_UDF_TEST_ARGS --conf spark.executorEnv.PYTHONPATH=`ls /home/ubuntu/spark-rapids/dist/target/rapids-4-spark_*.jar | grep -v 'tests.jar'`"
    SPARK_SUBMIT_FLAGS="$SPARK_CONF $CUDF_UDF_TEST_ARGS" TEST_PARALLEL=0 \
        bash /home/ubuntu/spark-rapids/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks" -m "cudf_udf" --cudf_udf --test_type=$TEST_TYPE
fi
