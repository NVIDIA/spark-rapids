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

set -xe

SPARK_VER=${SPARK_VER:-$(< /databricks/spark/VERSION)}
export SPARK_SHIM_VER=${SPARK_SHIM_VER:-spark${SPARK_VER//.}db}

# Setup SPARK_HOME if need
if [[ -z "$SPARK_HOME" ]]; then
    # Configure spark environment on Databricks
    export SPARK_HOME=$DB_HOME/spark
    export PYTHONPATH=$SPARK_HOME/python
fi

if [[ "$TEST" == "cache_test" || "$TEST" == "cache_test.py" ]]; then
    export PYSP_TEST_spark_sql_cache_serializer='com.nvidia.spark.ParquetCachedBatchSerializer'
fi

if [[ "$TEST_TAGS" == "iceberg" ]]; then
    ICEBERG_VERSION=${ICEBERG_VERSION:-0.13.2}
    ICEBERG_SPARK_VER=$(echo $SPARK_VER | cut -d. -f1,2)

    export SPARK_SUBMIT_FLAGS="$SPARK_SUBMIT_FLAGS \
        --packages org.apache.iceberg:iceberg-spark-runtime-${ICEBERG_SPARK_VER}_2.12:${ICEBERG_VERSION} \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
        --conf spark.sql.catalog.spark_catalog.type=hadoop \
        --conf spark.sql.catalog.spark_catalog.warehouse=/tmp/spark-warehouse-$$ \
        "
fi

TEST_TYPE=${TEST_TYPE:-"nightly"}

# Run integration testing
LOCAL_JAR_PATH=`pwd` ./integration_tests/run_pyspark_from_build.sh --runtime_env='databricks' --test_type=$TEST_TYPE
