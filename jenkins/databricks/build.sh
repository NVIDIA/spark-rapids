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

# this has to match the Databricks init script
DB_JAR_LOC=/databricks/jars/$CI_RAPIDS_JAR
RAPIDS_BUILT_JAR=rapids-4-spark_$SCALA_VERSION-$DATABRICKS_VERSION.jar

sudo apt install -y maven
rm -rf spark-rapids
mkdir spark-rapids
tar -zxvf $SPARKTGZ -C spark-rapids
cd spark-rapids
# pull normal Spark artifacts and ignore errors then install databricks jars, then build again
mvn clean package || true
echo "SCALA VERSION is: $SCALA_VERSION"
M2DIR=/home/ubuntu/.m2/repository
JARDIR=/databricks/jars
SQLJAR=----workspace_spark_3_0--sql--core--core-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
CATALYSTJAR=----workspace_spark_3_0--sql--catalyst--catalyst-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
ANNOTJAR=----workspace_spark_3_0--common--tags--tags-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
COREJAR=----workspace_spark_3_0--core--core-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
VERSIONJAR=----workspace_spark_3_0--core--libcore_generated_resources.jar
VERSION=$SPARK_VERSION
mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$COREJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-core_$SCALA_VERSION \
   -Dversion=$VERSION \
   -Dpackaging=jar

mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$CATALYSTJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-catalyst_$SCALA_VERSION \
   -Dversion=$VERSION \
   -Dpackaging=jar

mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$SQLJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-sql_$SCALA_VERSION \
   -Dversion=$VERSION \
   -Dpackaging=jar

mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ANNOTJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-annotation_$SCALA_VERSION \
   -Dversion=$VERSION \
   -Dpackaging=jar

mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$VERSIONJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-version_$SCALA_VERSION \
   -Dversion=$VERSION \
   -Dpackaging=jar

mvn -Pdatabricks clean verify -DskipTests

# copy so we pick up new built jar
sudo cp dist/target/$RAPIDS_BUILT_JAR $DB_JAR_LOC

# tests
export PATH=/databricks/conda/envs/databricks-ml-gpu/bin:/databricks/conda/condabin:$PATH
sudo /databricks/conda/envs/databricks-ml-gpu/bin/pip install pytest sre_yield
cd /home/ubuntu/spark-rapids/integration_tests
export SPARK_HOME=/databricks/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip
sudo ln -s /databricks/jars/ $SPARK_HOME/jars || true
sudo chmod 777 /databricks/data/logs/
sudo chmod 777 /databricks/data/logs/*
echo { \"port\":\"15002\" } > ~/.databricks-connect
$SPARK_HOME/bin/spark-submit ./runtests.py 2>&1 | tee out

cd /home/ubuntu
tar -zcvf spark-rapids-built.tgz spark-rapids
