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

set -ex

SPARKSRCTGZ=$1
# version of Apache Spark we are building against
BASE_SPARK_VERSION=$2
BUILD_PROFILES=$3
BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=$4
BUILD_PROFILES=${BUILD_PROFILES:-'databricks312,!snapshot-shims'}
BASE_SPARK_VERSION=${BASE_SPARK_VERSION:-'3.1.2'}
BUILDVER=$(echo ${BASE_SPARK_VERSION} | sed 's/\.//g')db
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

sudo apt install -y maven rsync

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

RAPIDS_BUILT_JAR=rapids-4-spark_$SCALA_VERSION-$SPARK_PLUGIN_JAR_VERSION.jar
RAPIDS_UDF_JAR=rapids-4-spark-udf-examples_$SCALA_VERSION-$SPARK_PLUGIN_JAR_VERSION.jar

echo "Scala version is: $SCALA_VERSION"
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
HIVEJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--sql--hive--hive_2.12_deploy_shaded.jar
COREPOMPATH=$M2DIR/org/apache/spark/spark-core_${SCALA_VERSION}/${BASE_SPARK_VERSION}

# We may ened to come up with way to specify versions but for now hardcode and deal with for next Databricks version
HIVEEXECJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.hive--hive-exec-core--org.apache.hive__hive-exec-core__2.3.7.jar
HIVESERDEJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.hive--hive-serde--org.apache.hive__hive-serde__2.3.7.jar
HIVESTORAGE=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.hive--hive-storage-api--org.apache.hive__hive-storage-api__2.7.2.jar

if [[ $BASE_SPARK_VERSION == "3.1.2" ]]
then
    PARQUETHADOOPJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.parquet--parquet-hadoop--org.apache.parquet__parquet-hadoop__1.10.1-databricks9.jar
    PARQUETCOMMONJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.parquet--parquet-common--org.apache.parquet__parquet-common__1.10.1-databricks9.jar
    PARQUETCOLUMNJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.parquet--parquet-column--org.apache.parquet__parquet-column__1.10.1-databricks9.jar
else
    PARQUETHADOOPJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.parquet--parquet-hadoop--org.apache.parquet__parquet-hadoop__1.10.1-databricks6.jar
    PARQUETCOMMONJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.parquet--parquet-common--org.apache.parquet__parquet-common__1.10.1-databricks6.jar
    PARQUETCOLUMNJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.parquet--parquet-column--org.apache.parquet__parquet-column__1.10.1-databricks6.jar
fi
PARQUETFORMATJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.parquet--parquet-format--org.apache.parquet__parquet-format__2.4.0.jar

NETWORKCOMMON=----workspace_${SPARK_MAJOR_VERSION_STRING}--common--network-common--network-common-hive-2.3__hadoop-2.7_2.12_deploy.jar
COMMONUNSAFE=----workspace_${SPARK_MAJOR_VERSION_STRING}--common--unsafe--unsafe-hive-2.3__hadoop-2.7_2.12_deploy.jar
LAUNCHER=----workspace_${SPARK_MAJOR_VERSION_STRING}--launcher--launcher-hive-2.3__hadoop-2.7_2.12_deploy.jar

KRYO=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--com.esotericsoftware--kryo-shaded--com.esotericsoftware__kryo-shaded__4.0.2.jar

APACHECOMMONS=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--commons-io--commons-io--commons-io__commons-io__2.4.jar

if [[ $BASE_SPARK_VERSION == "3.0.1" ]]
then
    JSON4S=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.json4s--json4s-ast_2.12--org.json4s__json4s-ast_2.12__3.6.6.jar
    APACHECOMMONSLANG3=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.commons--commons-lang3--org.apache.commons__commons-lang3__3.9.jar
    HIVESTORAGE=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.hive--hive-storage-api--org.apache.hive__hive-storage-api__2.7.1.jar
    ARROWFORMATJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.arrow--arrow-format--org.apache.arrow__arrow-format__0.15.1.jar
    ARROWMEMORYJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.arrow--arrow-memory--org.apache.arrow__arrow-memory__0.15.1.jar
    ARROWVECTORJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.arrow--arrow-vector--org.apache.arrow__arrow-vector__0.15.1.jar
    HIVEEXECJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--patched-hive-with-glue--hive-exec-core_shaded.jar
else
    JSON4S=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.json4s--json4s-ast_2.12--org.json4s__json4s-ast_2.12__3.7.0-M5.jar
    APACHECOMMONSLANG3=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.commons--commons-lang3--org.apache.commons__commons-lang3__3.10.jar
    HIVESTORAGE=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.hive--hive-storage-api--org.apache.hive__hive-storage-api__2.7.2.jar
    if [[ $BASE_SPARK_VERSION == "3.1.2" ]]; then
      HIVEEXECJAR=----workspace_spark_3_1--patched-hive-with-glue--hive-exec-core_shaded.jar
    else
      HIVEEXECJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.hive--hive-exec-core--org.apache.hive__hive-exec-core__2.3.7.jar
    fi
    ARROWFORMATJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.arrow--arrow-format--org.apache.arrow__arrow-format__2.0.0.jar
    ARROWMEMORYJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.arrow--arrow-memory-core--org.apache.arrow__arrow-memory-core__2.0.0.jar
    ARROWMEMORYNETTYJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.arrow--arrow-memory-netty--org.apache.arrow__arrow-memory-netty__2.0.0.jar
    ARROWVECTORJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.arrow--arrow-vector--org.apache.arrow__arrow-vector__2.0.0.jar
fi

JAVAASSIST=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.javassist--javassist--org.javassist__javassist__3.25.0-GA.jar

PROTOBUFJAVA=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--com.google.protobuf--protobuf-java--com.google.protobuf__protobuf-java__2.6.1.jar

JACKSONCORE=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--com.fasterxml.jackson.core--jackson-databind--com.fasterxml.jackson.core__jackson-databind__2.10.0.jar
JACKSONANNOTATION=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--com.fasterxml.jackson.core--jackson-annotations--com.fasterxml.jackson.core__jackson-annotations__2.10.0.jar

HADOOPCOMMON=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.hadoop--hadoop-common--org.apache.hadoop__hadoop-common__2.7.4.jar
HADOOPMAPRED=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.hadoop--hadoop-mapreduce-client-core--org.apache.hadoop__hadoop-mapreduce-client-core__2.7.4.jar

# Please note we are installing all of these dependencies using the Spark version (SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS) to make it easier
# to specify the dependencies in the pom files

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
   -Dfile=$JARDIR/$HIVEEXECJAR \
   -DgroupId=org.apache.hive \
   -DartifactId=hive-exec \
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
   -Dfile=$JARDIR/$JACKSONCORE\
   -DgroupId=com.fasterxml.jackson.core \
   -DartifactId=jackson-core \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$JACKSONANNOTATION\
   -DgroupId=com.fasterxml.jackson.core \
   -DartifactId=jackson-annotations \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$PROTOBUFJAVA \
   -DgroupId=com.google.protobuf \
   -DartifactId=protobuf-java \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$JAVAASSIST\
   -DgroupId=org.javaassist\
   -DartifactId=javaassist \
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
   -Dfile=$JARDIR/$HIVESTORAGE \
   -DgroupId=org.apache.hive \
   -DartifactId=hive-storage-api \
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
   -Dfile=$JARDIR/$HIVESERDEJAR \
   -DgroupId=org.apache.hive \
   -DartifactId=hive-serde \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$PARQUETHADOOPJAR \
   -DgroupId=org.apache.parquet \
   -DartifactId=parquet-hadoop \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$PARQUETCOMMONJAR \
   -DgroupId=org.apache.parquet \
   -DartifactId=parquet-common \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$PARQUETCOLUMNJAR \
   -DgroupId=org.apache.parquet \
   -DartifactId=parquet-column \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$PARQUETFORMATJAR \
   -DgroupId=org.apache.parquet \
   -DartifactId=parquet-format \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ARROWFORMATJAR \
   -DgroupId=org.apache.arrow \
   -DartifactId=arrow-format \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ARROWMEMORYJAR \
   -DgroupId=org.apache.arrow \
   -DartifactId=arrow-memory \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ARROWVECTORJAR \
   -DgroupId=org.apache.arrow \
   -DartifactId=arrow-vector \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$HADOOPCOMMON \
   -DgroupId=org.apache.hadoop \
   -DartifactId=hadoop-common \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$HADOOPMAPRED \
   -DgroupId=org.apache.hadoop \
   -DartifactId=hadoop-mapreduce-client \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B -Ddatabricks -Dbuildver=$BUILDVER clean package -DskipTests

cd /home/ubuntu
tar -zcf spark-rapids-built.tgz spark-rapids
