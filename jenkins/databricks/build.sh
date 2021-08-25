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
BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=$4
BUILD_PROFILES=${BUILD_PROFILES:-'databricks311,!snapshot-shims'}
BASE_SPARK_VERSION=${BASE_SPARK_VERSION:-'3.1.1'}
# the version of Spark used when we install the Databricks jars in .m2
# 3.1.0-databricks is add because its actually based on Spark 3.1.1
BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=${BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS:-$BASE_SPARK_VERSION}
SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=$BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS-databricks
# something like spark_3_1 or spark_3_0
SPARK_MAJOR_VERSION_NUM_STRING=$(echo ${BASE_SPARK_VERSION} | sed 's/\./\_/g' | cut -d _ -f 1,2)
SPARK_MAJOR_VERSION_STRING=spark_${SPARK_MAJOR_VERSION_NUM_STRING}

echo "tgz is $SPARKSRCTGZ"
echo "Base Spark version is $BASE_SPARK_VERSION"
echo "build profiles $BUILD_PROFILES"
echo "BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS is $BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS"

sudo apt install -y maven

# this has to match the Databricks init script
DB_JAR_LOC=/databricks/jars/

#if [[ -n $SPARKSRCTGZ ]]
#then
#    rm -rf spark-rapids
#    mkdir spark-rapids
#    echo  "tar -zxf $SPARKSRCTGZ -C spark-rapids"
#    tar -zxf $SPARKSRCTGZ -C spark-rapids
#    cd spark-rapids
#fi

export WORKSPACE=`pwd`

SPARK_PLUGIN_JAR_VERSION=`mvn help:evaluate -q -pl dist -Dexpression=project.version -DforceStdout`
CUDF_VERSION=`mvn help:evaluate -q -pl dist -Dexpression=cudf.version -DforceStdout`
SCALA_VERSION=`mvn help:evaluate -q -pl dist -Dexpression=scala.binary.version -DforceStdout`
CUDA_VERSION=`mvn help:evaluate -q -pl dist -Dexpression=cuda.version -DforceStdout`

RAPIDS_BUILT_JAR=rapids-4-spark_$SCALA_VERSION-$SPARK_PLUGIN_JAR_VERSION.jar
RAPIDS_UDF_JAR=rapids-4-spark-udf-examples_$SCALA_VERSION-$SPARK_PLUGIN_JAR_VERSION.jar

echo "Scala version is: $SCALA_VERSION"
#mvn -B -P${BUILD_PROFILES} clean package -DskipTests || true
# export 'M2DIR' so that shims can get the correct cudf/spark dependnecy info
export M2DIR=/home/ubuntu/.m2/repository
CUDF_JAR=${M2DIR}/ai/rapids/cudf/${CUDF_VERSION}/cudf-${CUDF_VERSION}-${CUDA_VERSION}.jar

# pull normal Spark artifacts and ignore errors then install databricks jars, then build again
JARDIR=/databricks/jars
# install the Spark pom file so we get dependencies
SQLJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--sql--core--core-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
CATALYSTJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--sql--catalyst--catalyst-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
ANNOTJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--common--tags--tags-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
COREJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--core--core-hive-2.3__hadoop-2.7_${SCALA_VERSION}_deploy.jar
COREPOM=spark-core_${SCALA_VERSION}-${BASE_SPARK_VERSION}.pom
HIVEJAR=----workspace_spark_3_1--sql--hive--hive_2.12_deploy_shaded.jar
HIVEEXECJAR=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.apache.hive--hive-exec-core--org.apache.hive__hive-exec-core__2.3.7.jar
HIVESERDEJAR=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.apache.hive--hive-serde--org.apache.hive__hive-serde__2.3.7.jar
COREPOMPATH=$M2DIR/org/apache/spark/spark-core_${SCALA_VERSION}/${BASE_SPARK_VERSION}

PARQUETHADOOPJAR=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.apache.parquet--parquet-hadoop--org.apache.parquet__parquet-hadoop__1.10.1-databricks6.jar
PARQUETCOMMONJAR=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.apache.parquet--parquet-common--org.apache.parquet__parquet-common__1.10.1-databricks6.jar
PARQUETCOLUMNJAR=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.apache.parquet--parquet-column--org.apache.parquet__parquet-column__1.10.1-databricks6.jar
PARQUETFORMATJAR=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.apache.parquet--parquet-format--org.apache.parquet__parquet-format__2.4.0.jar

ARROWFORMATJAR=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.apache.arrow--arrow-format--org.apache.arrow__arrow-format__2.0.0.jar
ARROWMEMORYJAR=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.apache.arrow--arrow-memory-core--org.apache.arrow__arrow-memory-core__2.0.0.jar
ARROWMEMORYNETTYJAR=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.apache.arrow--arrow-memory-netty--org.apache.arrow__arrow-memory-netty__2.0.0.jar
ARROWVECTORJAR=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.apache.arrow--arrow-vector--org.apache.arrow__arrow-vector__2.0.0.jar

NETWORKCOMMON=----workspace_spark_3_1--common--network-common--network-common-hive-2.3__hadoop-2.7_2.12_deploy.jar
COMMONUNSAFE=----workspace_spark_3_1--common--unsafe--unsafe-hive-2.3__hadoop-2.7_2.12_deploy.jar
LAUNCHER=----workspace_spark_3_1--launcher--launcher-hive-2.3__hadoop-2.7_2.12_deploy.jar

KRYO=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--com.esotericsoftware--kryo-shaded--com.esotericsoftware__kryo-shaded__4.0.2.jar

APACHECOMMONS=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--commons-io--commons-io--commons-io__commons-io__2.4.jar
APACHECOMMONSLANG3=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.apache.commons--commons-lang3--org.apache.commons__commons-lang3__3.10.jar

JSON4S=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.json4s--json4s-ast_2.12--org.json4s__json4s-ast_2.12__3.7.0-M5.jar

SCALAREFLECT=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.scala-lang--scala-reflect_2.12--org.scala-lang__scala-reflect__2.12.10.jar

JAVAASSIST=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.javassist--javassist--org.javassist__javassist__3.25.0-GA.jar

#SCALALIB=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.scala-lang--scala-library_2.12--org.scala-lang__scala-library__2.12.10.jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$JAVAASSIST\
   -DgroupId=org.javaassist\
   -DartifactId=javaassist \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

#mvn -B install:install-file \
#   -Dmaven.repo.local=$M2DIR \
#   -Dfile=$JARDIR/$SCALALIB \
#   -DgroupId=org.scala-lang \
#   -DartifactId=scala-library \
#   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
#   -Dpackaging=jar
#
mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$SCALAREFLECT \
   -DgroupId=org.scala-lang \
   -DartifactId=scala-reflect \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$JSON4S \
   -DgroupId=org.json4s \
   -DartifactId=JsonAST \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$APACHECOMMONSLANG3 \
   -DgroupId=org.apache.commons \
   -DartifactId=commons-lang3 \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$APACHECOMMONS \
   -DgroupId=org.apache.commons \
   -DartifactId=commons-io \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$KRYO \
   -DgroupId=com.esotericsoftware.kryo \
   -DartifactId=kryo-shaded-db \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$LAUNCHER \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-launcher_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$NETWORKCOMMON \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-network-common_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$COMMONUNSAFE\
   -DgroupId=org.apache.spark \
   -DartifactId=spark-unsafe_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$COREJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-core_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$HIVEJAR\
   -DgroupId=org.apache.spark \
   -DartifactId=spark-hive_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$HIVEEXECJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-hive-exec-db_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$HIVESERDEJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-hive-serde-db_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

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

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$PARQUETHADOOPJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-parquet-hadoop-db_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$PARQUETCOMMONJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-parquet-common-db_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$PARQUETCOLUMNJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-parquet-column-db_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$PARQUETFORMATJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-parquet-format-db_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ARROWFORMATJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-arrow-format-db_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ARROWMEMORYJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-arrow-memory-db_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar


mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ARROWMEMORYNETTYJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-arrow-memory-netty-db_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ARROWVECTORJAR \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-arrow-vector-db_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

HADOOPCOMMON=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.apache.hadoop--hadoop-common--org.apache.hadoop__hadoop-common__2.7.4.jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$HADOOPCOMMON \
   -DgroupId=org.apache.hadoop \
   -DartifactId=hadoop-common \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

HADOOPMAPRED=----workspace_spark_3_1--maven-trees--hive-2.3__hadoop-2.7--org.apache.hadoop--hadoop-mapreduce-client-core--org.apache.hadoop__hadoop-mapreduce-client-core__2.7.4.jar
mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$HADOOPMAPRED \
   -DgroupId=org.apache.hadoop \
   -DartifactId=hadoop-mapreduce-client \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar




#mvn -B -P${BUILD_PROFILES} clean package -DskipTests

#cd /home/ubuntu
#tar -zcf spark-rapids-built.tgz spark-rapids
