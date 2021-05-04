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

set -e

SPARKSRCTGZ=$1
# version of Apache Spark we are building against
BASE_SPARK_VERSION=$2
BUILD_PROFILES=$3
BUILD_PROFILES=${BUILD_PROFILES:-'databricks301,!snapshot-shims'}
BASE_SPARK_VERSION=${BASE_SPARK_VERSION:-'3.0.1'}

echo "tgz is $SPARKSRCTGZ"
echo "Base Spark version is $BASE_SPARK_VERSION"
echo "build profiles $BUILD_PROFILES"

sudo apt install -y maven

# this has to match the Databricks init script
DB_JAR_LOC=/databricks/jars/

if [[ -n $SPARKSRCTGZ ]]
then
    rm -rf spark-rapids
    mkdir spark-rapids
    echo  "tar -zxf $SPARKSRCTGZ -C spark-rapids"
    tar -zxf $SPARKSRCTGZ -C spark-rapids
    cd spark-rapids
fi
export WORKSPACE=`pwd`

SPARK_PLUGIN_JAR_VERSION=`mvn help:evaluate -q -pl dist -Dexpression=project.version -DforceStdout`
CUDF_VERSION=`mvn help:evaluate -q -pl dist -Dexpression=cudf.version -DforceStdout`
SCALA_VERSION=`mvn help:evaluate -q -pl dist -Dexpression=scala.binary.version -DforceStdout`
CUDA_VERSION=`mvn help:evaluate -q -pl dist -Dexpression=cuda.version -DforceStdout`

# the version of spark used when we install the databricks jars in .m2
SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=$BASE_SPARK_VERSION-databricks
RAPIDS_BUILT_JAR=rapids-4-spark_$SCALA_VERSION-$SPARK_PLUGIN_JAR_VERSION.jar
RAPIDS_UDF_JAR=rapids-4-spark-udf-examples_$SCALA_VERSION-$SPARK_PLUGIN_JAR_VERSION.jar

echo "Scala version is: $SCALA_VERSION"
mvn -B -P${BUILD_PROFILES} clean package -DskipTests || true
# export 'M2DIR' so that shims can get the correct cudf/spark dependnecy info
export M2DIR=/home/ubuntu/.m2/repository
CUDF_JAR=${M2DIR}/ai/rapids/cudf/${CUDF_VERSION}/cudf-${CUDF_VERSION}-${CUDA_VERSION}.jar

# pull normal Spark artifacts and ignore errors then install databricks jars, then build again
JARDIR=/databricks/jars
SQLJAR=----workspace_spark_3_0--sql--core--core-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
CATALYSTJAR=----workspace_spark_3_0--sql--catalyst--catalyst-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
ANNOTJAR=----workspace_spark_3_0--common--tags--tags-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
COREJAR=----workspace_spark_3_0--core--core-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
# install the Spark pom file so we get dependencies
COREPOM=spark-core_${SCALA_VERSION}-${BASE_SPARK_VERSION}.pom
COREPOMPATH=$M2DIR/org/apache/spark/spark-core_${SCALA_VERSION}/${BASE_SPARK_VERSION}
mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$COREJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-core_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar \
   -DpomFile=$COREPOMPATH/$COREPOM

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$CATALYSTJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-catalyst_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$SQLJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-sql_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ANNOTJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-annotation_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B -P${BUILD_PROFILES} clean package -DskipTests

cd /home/ubuntu
tar -zcf spark-rapids-built.tgz spark-rapids
