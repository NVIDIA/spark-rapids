#!/bin/bash
#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
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
# Run integration testing individually by setting environment variable:
#   TEST=xxx
# or
#   TEST_TAGS=xxx
# More details please refer to './integration_tests/run_pyspark_from_build.sh'.
# Note, 'setup.sh' should be executed first to setup proper environment.

set -xe

SPARK_VER=${SPARK_VER:-$(< /databricks/spark/VERSION)}
export SPARK_SHIM_VER=${SPARK_SHIM_VER:-spark${SPARK_VER//.}db}

# Setup SPARK_HOME if need
if [[ -z "$SPARK_HOME" ]]; then
    # Configure spark environment on Databricks
    export SPARK_HOME=$DB_HOME/spark
fi

SCALA_BINARY_VER=${SCALA_BINARY_VER:-'2.12'}
CONDA_HOME=${CONDA_HOME:-"/databricks/conda"}

# Try to use "cudf-udf" conda environment for the python cudf-udf tests.
if [ -d "${CONDA_HOME}/envs/cudf-udf" ]; then
    export PATH=${CONDA_HOME}/envs/cudf-udf/bin:${CONDA_HOME}/bin:$PATH
    export PYSPARK_PYTHON=${CONDA_HOME}/envs/cudf-udf/bin/python
fi

# Get Python version (major.minor). i.e., python3.8 for DB10.4 and python3.9 for DB11.3
python_version=$(${PYSPARK_PYTHON} -c 'import sys; print("python{}.{}".format(sys.version_info.major, sys.version_info.minor))')

# override incompatible versions between databricks and cudf
if [ -d "${CONDA_HOME}/envs/cudf-udf" ]; then
    PATCH_PACKAGES_PATH="$PWD/package-overrides/${python_version}"
fi

# Get the correct py4j file.
PY4J_FILE=$(find $SPARK_HOME/python/lib -type f -iname "py4j*.zip")
# Set the path of python site-packages
PYTHON_SITE_PACKAGES=/databricks/python3/lib/${python_version}/site-packages
# Databricks Koalas can conflict with the actual Pandas version, so put site packages first.
# Note that Koala is deprecated for DB10.4+ and it is recommended to use Pandas API on Spark instead.
export PYTHONPATH=$PATCH_PACKAGES_PATH:$PYTHON_SITE_PACKAGES:$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$PY4J_FILE

# Disable parallel test as multiple tests would be executed by leveraging external parallelism, e.g. Jenkins parallelism
export TEST_PARALLEL=${TEST_PARALLEL:-0}

if [[ "$TEST" == "cache_test" || "$TEST" == "cache_test.py" ]]; then
    export PYSP_TEST_spark_sql_cache_serializer='com.nvidia.spark.ParquetCachedBatchSerializer'
fi

if [[ "$TEST_TAGS" == "iceberg" ]]; then
    ICEBERG_SPARK_VER=$(echo $SPARK_VER | cut -d. -f1,2)

    # Set Iceberg related versions. See https://iceberg.apache.org/multi-engine-support/#apache-spark
    # Available versions https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/
    case "$SPARK_VER" in
        "3.3.0")
            ICEBERG_VERSION=${ICEBERG_VERSION:-0.14.1}
            ;;
        "3.2.1" | "3.1.2")
            ICEBERG_VERSION=${ICEBERG_VERSION:-0.13.2}
            ;;
        *) echo "Unexpected Spark version: $SPARK_VER"; exit 1;;
    esac

    export SPARK_SUBMIT_FLAGS="$SPARK_SUBMIT_FLAGS \
        --packages org.apache.iceberg:iceberg-spark-runtime-${ICEBERG_SPARK_VER}_${SCALA_BINARY_VER}:${ICEBERG_VERSION} \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
        --conf spark.sql.catalog.spark_catalog.type=hadoop \
        --conf spark.sql.catalog.spark_catalog.warehouse=/tmp/spark-warehouse-$$ \
        "
fi

TEST_TYPE=${TEST_TYPE:-"nightly"}

if [[ -n "$LOCAL_JAR_PATH" ]]; then
    export LOCAL_JAR_PATH=$LOCAL_JAR_PATH
fi

# Run integration testing
./integration_tests/run_pyspark_from_build.sh --runtime_env='databricks' --test_type=$TEST_TYPE
