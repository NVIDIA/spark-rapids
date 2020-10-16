#!/bin/bash
#
# Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
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

set -e

SPARKTGZ=$1
DATABRICKS_VERSION=$2
SCALA_VERSION=$3
CI_RAPIDS_JAR=$4
SPARK_VERSION=$5
CUDF_VERSION=$6
CUDA_CLASSIFIER=$7
CI_CUDF_JAR=$8
BASE_SPARK_POM_VERSION=$9

echo "Spark version is $SPARK_VERSION"
echo "scala version is: $SCALA_VERSION"

# this has to match the Databricks init script
DB_JAR_LOC=/databricks/jars
DB_RAPIDS_JAR_LOC=$DB_JAR_LOC/$CI_RAPIDS_JAR
DB_CUDF_JAR_LOC=$DB_JAR_LOC/$CI_CUDF_JAR
RAPIDS_BUILT_JAR=rapids-4-spark_$SCALA_VERSION-$DATABRICKS_VERSION.jar

# Copy new built spark-rapids jar and latesty cuDF jar. Note that the jar names has to be
# exactly what is in the staticly setup Databricks cluster we use. 
DBFS_RAPIDS_CI_PATH=/dbfs/FileStore/rapids-ci
RAPIDS_JAR=$DBFS_RAPIDS_CI_PATH/$RAPIDS_BUILT_JAR
CUDF_JAR=$DBFS_RAPIDS_CI_PATH/cudf-${CUDF_VERSION}-${CUDA_CLASSIFIER}.jar
echo "Copying rapids jars: $RAPIDS_JAR $DB_RAPIDS_JAR_LOC"
sudo cp $RAPIDS_JAR $DB_RAPIDS_JAR_LOC
echo "Copying cudf jars: $CUDF_JAR $DB_CUDF_JAR_LOC"
sudo cp $CUDF_JAR $DB_CUDF_JAR_LOC

# tests
export PATH=/databricks/conda/envs/databricks-ml-gpu/bin:/databricks/conda/condabin:$PATH
sudo /databricks/conda/envs/databricks-ml-gpu/bin/pip install pytest sre_yield
export SPARK_HOME=/databricks/spark
# change to not point at databricks confs so we don't conflict with their settings
export SPARK_CONF_DIR=$PWD
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip
sudo ln -s /databricks/jars/ $SPARK_HOME/jars || true
sudo chmod 777 /databricks/data/logs/
sudo chmod 777 /databricks/data/logs/*
echo { \"port\":\"15002\" } > ~/.databricks-connect
if [ `ls $DB_JAR_LOC/rapids* | wc -l` -gt 1 ]; then
    echo "ERROR: Too many rapids jars in $DB_JAR_LOC"
    ls $DB_JAR_LOC/rapids*
    exit 1
fi
if [ `ls $DB_JAR_LOC/cudf* | wc -l` -gt 1 ]; then
    echo "ERROR: Too many cudf jars in $DB_JAR_LOC"
    ls $DB_JAR_LOC/cudf*
    exit 1
fi

rm -rf spark-rapids && mkdir spark-rapids
tar -zxf $SPARKTGZ spark-rapids/integration_tests
cd /home/ubuntu/spark-rapids/integration_tests
$SPARK_HOME/bin/spark-submit ./runtests.py --runtime_env="databricks"
