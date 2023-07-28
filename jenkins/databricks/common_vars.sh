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

# Setup SPARK_HOME if need
if [[ -z "$SPARK_HOME" ]]; then
    # Configure spark environment on Databricks
    export SPARK_HOME=$DB_HOME/spark
fi

# Set PYSPARK_PYTHON to keep the version of driver/workers python consistent.
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-"$(which python)"}
# Get Python version (major.minor). i.e., python3.8 for DB10.4 and python3.9 for DB11.3
PYTHON_VERSION=$(${PYSPARK_PYTHON} -c 'import sys; print("python{}.{}".format(sys.version_info.major, sys.version_info.minor))')
# Set the path of python site-packages, packages were installed here by 'jenkins/databricks/setup.sh'.
PYTHON_SITE_PACKAGES=${PYTHON_SITE_PACKAGES:-"$HOME/.local/lib/${PYTHON_VERSION}/site-packages"}

# Get the correct py4j file.
PY4J_FILE=$(find $SPARK_HOME/python/lib -type f -iname "py4j*.zip")
# Databricks Koalas can conflict with the actual Pandas version, so put site packages first.
# Note that Koala is deprecated for DB10.4+ and it is recommended to use Pandas API on Spark instead.
export PYTHONPATH=$PYTHON_SITE_PACKAGES:$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$PY4J_FILE
export PCBS_CONF="com.nvidia.spark.ParquetCachedBatchSerializer"
if [[ "$TEST" == "cache_test" || "$TEST" == "cache_test.py" ]]; then
    export PYSP_TEST_spark_sql_cache_serializer="$PCBS_CONF"
fi

export TEST_TYPE=${TEST_TYPE:-"nightly"}

if [[ -n "$LOCAL_JAR_PATH" ]]; then
    export LOCAL_JAR_PATH=$LOCAL_JAR_PATH
fi

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
    export SPARK_CONF="--conf ${SPARK_CONF/','/' --conf '}"
fi
