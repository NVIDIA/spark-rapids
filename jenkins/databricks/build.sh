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

SPARKTGZ=/home/ubuntu/spark-rapids-ci.tgz
if [ "$1" != "" ]; then
  SPARKTGZ=$1
fi

sudo apt install -y maven
rm -rf spark-rapids
mkdir spark-rapids
tar -zxvf $SPARKTGZ -C spark-rapids
cd spark-rapids
# pull 3.0.0 artifacts and ignore errors then install databricks jars, then build again
mvn clean package || true
M2DIR=/home/ubuntu/.m2/repository
JARDIR=/databricks/jars
SQLJAR=----workspace_spark_3_0--sql--core--core-hive-2.3__hadoop-2.7_2.12_deploy.jar
CATALYSTJAR=----workspace_spark_3_0--sql--catalyst--catalyst-hive-2.3__hadoop-2.7_2.12_deploy.jar
ANNOTJAR=----workspace_spark_3_0--common--tags--tags-hive-2.3__hadoop-2.7_2.12_deploy.jar
COREJAR=----workspace_spark_3_0--core--core-hive-2.3__hadoop-2.7_2.12_deploy.jar
VERSIONJAR=----workspace_spark_3_0--core--libcore_generated_resources.jar
VERSION=3.0.0
mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$COREJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-core_2.12 \
   -Dversion=$VERSION \
   -Dpackaging=jar

mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$CATALYSTJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-catalyst_2.12 \
   -Dversion=$VERSION \
   -Dpackaging=jar

mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$SQLJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-sql_2.12 \
   -Dversion=$VERSION \
   -Dpackaging=jar

mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ANNOTJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-annotation_2.12 \
   -Dversion=$VERSION \
   -Dpackaging=jar

mvn install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$VERSIONJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-version_2.12 \
   -Dversion=$VERSION \
   -Dpackaging=jar

mvn -Pdatabricks clean verify -DskipTests

# copy so we pick up new built jar
sudo cp dist/target/rapids-4-spark_2.12-*-SNAPSHOT.jar /databricks/jars/rapids-4-spark_2.12-0.2.0-SNAPSHOT-ci.jar

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
