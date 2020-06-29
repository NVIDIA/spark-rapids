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
CUDA_VERSION=$7
CI_CUDF_JAR=$8

echo "Spark version is $SPARK_VERSION"
echo "scala version is: $SCALA_VERSION"

# this has to match the Databricks init script
DB_JAR_LOC=/databricks/jars
DB_RAPIDS_JAR_LOC=$DB_JAR_LOC/$CI_RAPIDS_JAR
DB_CUDF_JAR_LOC=$DB_JAR_LOC/$CI_CUDF_JAR
RAPIDS_BUILT_JAR=rapids-4-spark_$SCALA_VERSION-$DATABRICKS_VERSION.jar

sudo apt install -y maven
rm -rf spark-rapids
mkdir spark-rapids
tar -zxvf $SPARKTGZ -C spark-rapids
cd spark-rapids
mvn clean package || true
M2DIR=/home/ubuntu/.m2/repository
CUDF_JAR=./cudf-${CUDF_VERSION}.jar
mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=./$CUDF_JAR \
   -DgroupId=ai.rapids \
   -DartifactId=cudf \
   -Dversion=$CUDF_VERSION \
   -Dclassifier=$CUDA_VERSION \
   -Dpackaging=jar

# pull normal Spark artifacts and ignore errors then install databricks jars, then build again
JARDIR=/databricks/jars
SQLJAR=----workspace_spark_3_0--sql--core--core-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
CATALYSTJAR=----workspace_spark_3_0--sql--catalyst--catalyst-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
ANNOTJAR=----workspace_spark_3_0--common--tags--tags-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
COREJAR=----workspace_spark_3_0--core--core-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$COREJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-core_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION \
   -Dpackaging=jar

mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$CATALYSTJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-catalyst_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION \
   -Dpackaging=jar

mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$SQLJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-sql_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION \
   -Dpackaging=jar

mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ANNOTJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-annotation_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION \
   -Dpackaging=jar

mvn -Pdatabricks clean package -DskipTests

# Copy so we pick up new built jar and latesty CuDF jar. Note that the jar names has to be
# exactly what is in the staticly setup Databricks cluster we use. 
sudo cp dist/target/$RAPIDS_BUILT_JAR $DB_RAPIDS_JAR_LOC
sudo cp ./$CUDF_JAR $DB_CUDF_JAR_LOC

# tests
export PATH=/databricks/conda/envs/databricks-ml-gpu/bin:/databricks/conda/condabin:$PATH
sudo /databricks/conda/envs/databricks-ml-gpu/bin/pip install pytest sre_yield
cd /home/ubuntu/spark-rapids/integration_tests
export SPARK_HOME=/databricks/spark
# change to not point at databricks confs so we don't conflict with their settings
export SPARK_CONF_DIR=$PWD
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip
sudo ln -s /databricks/jars/ $SPARK_HOME/jars || true
sudo chmod 777 /databricks/data/logs/
sudo chmod 777 /databricks/data/logs/*
echo { \"port\":\"15002\" } > ~/.databricks-connect
$SPARK_HOME/bin/spark-submit ./runtests.py --runtime_env="databricks"
cd /home/ubuntu
tar -zcvf spark-rapids-built.tgz spark-rapids
