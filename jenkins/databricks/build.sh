#!/bin/bash
#
# Copyright (c) 2020-2022, NVIDIA CORPORATION. All rights reserved.
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

## Environments SPARKSRCTGZ, BASE_SPARK_VERSION, BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS, MVN_OPT
## can be overwritten by shell variables, e.g. "BASE_SPARK_VERSION=3.1.2 MVN_OPT=-DskipTests bash build.sh"

SPARKSRCTGZ=${SPARKSRCTGZ:-''}
# version of Apache Spark we are building against
BASE_SPARK_VERSION=${BASE_SPARK_VERSION:-'3.1.2'}
BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=${BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS:-$BASE_SPARK_VERSION}
## '-Pfoo=1,-Dbar=2,...' to '-Pfoo=1 -Dbar=2 ...'
MVN_OPT=${MVN_OPT//','/' '}

BUILDVER=$(echo ${BASE_SPARK_VERSION} | sed 's/\.//g')db
# the version of Spark used when we install the Databricks jars in .m2
BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=${BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS:-$BASE_SPARK_VERSION}
SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=$BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS-databricks
# something like spark_3_1 or spark_3_0
SPARK_MAJOR_VERSION_NUM_STRING=$(echo ${BASE_SPARK_VERSION} | sed 's/\./\_/g' | cut -d _ -f 1,2)
SPARK_MAJOR_VERSION_STRING=spark_${SPARK_MAJOR_VERSION_NUM_STRING}

echo "tgz is $SPARKSRCTGZ"
echo "Base Spark version is $BASE_SPARK_VERSION"
echo "maven options is $MVN_OPT"
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
SCALA_VERSION=`mvn help:evaluate -q -pl dist -Dexpression=scala.binary.version -DforceStdout`
CUDA_VERSION=`mvn help:evaluate -q -pl dist -Dexpression=cuda.version -DforceStdout`

RAPIDS_BUILT_JAR=rapids-4-spark_$SCALA_VERSION-$SPARK_PLUGIN_JAR_VERSION.jar

echo "Scala version is: $SCALA_VERSION"
# export 'M2DIR' so that shims can get the correct Spark dependency info
export M2DIR=/home/ubuntu/.m2/repository

# pull normal Spark artifacts and ignore errors then install databricks jars, then build again
JARDIR=/databricks/jars
# install the Spark pom file so we get dependencies
case "$BASE_SPARK_VERSION" in
    "3.2.1")
        COMMONS_LANG3_VERSION=3.12.0
        COMMONS_IO_VERSION=2.8.0
        DB_VERSION=-0004
        FASTERXML_JACKSON_VERSION=2.12.3
        HADOOP_VERSION=3.2
        HIVE_FULL_VERSION=2.3.9
        JSON4S_VERSION=3.7.0-M11
        ORC_VERSION=1.6.13
        PARQUET_VERSION=1.12.0
        ;;
    "3.1.2")
        COMMONS_LANG3_VERSION=3.10
        COMMONS_IO_VERSION=2.4
        DB_VERSION=9
        FASTERXML_JACKSON_VERSION=2.10.0
        HADOOP_VERSION=2.7
        HIVE_FULL_VERSION=2.3.7
        JSON4S_VERSION=3.7.0-M5
        ORC_VERSION=1.5.12
        PARQUET_VERSION=1.10.1
        ;;
    *) echo "Unexpected Spark version: $BASE_SPARK_VERSION"; exit 1;;
esac

SQLJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--sql--core--core-hive-2.3__hadoop-${HADOOP_VERSION}_${SCALA_VERSION}_deploy.jar
CATALYSTJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--sql--catalyst--catalyst-hive-2.3__hadoop-${HADOOP_VERSION}_${SCALA_VERSION}_deploy.jar
ANNOTJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--common--tags--tags-hive-2.3__hadoop-${HADOOP_VERSION}_${SCALA_VERSION}_deploy.jar
COREJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--core--core-hive-2.3__hadoop-${HADOOP_VERSION}_${SCALA_VERSION}_deploy.jar

COREPOM=spark-core_${SCALA_VERSION}-${BASE_SPARK_VERSION}.pom
if [[ $BASE_SPARK_VERSION == "3.2.1" ]]
then
    HIVEJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--sql--hive--hive-hive-2.3__hadoop-${HADOOP_VERSION}_${SCALA_VERSION}_deploy_shaded.jar
else
    HIVEJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--sql--hive--hive_${SCALA_VERSION}_deploy_shaded.jar
fi

COREPOMPATH=$M2DIR/org/apache/spark/spark-core_${SCALA_VERSION}/${BASE_SPARK_VERSION}

# We may need to come up with way to specify versions but for now hardcode and deal with for next Databricks version
HIVEEXECJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.hive--hive-exec-core--org.apache.hive__hive-exec-core__${HIVE_FULL_VERSION}.jar
HIVESERDEJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.hive--hive-serde--org.apache.hive__hive-serde__${HIVE_FULL_VERSION}.jar
HIVESTORAGE=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.hive--hive-storage-api--org.apache.hive__hive-storage-api__2.7.2.jar

PARQUETHADOOPJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.parquet--parquet-hadoop--org.apache.parquet__parquet-hadoop__${PARQUET_VERSION}-databricks${DB_VERSION}.jar
PARQUETCOMMONJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.parquet--parquet-common--org.apache.parquet__parquet-common__${PARQUET_VERSION}-databricks${DB_VERSION}.jar
PARQUETCOLUMNJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.parquet--parquet-column--org.apache.parquet__parquet-column__${PARQUET_VERSION}-databricks${DB_VERSION}.jar
ORC_CORE_JAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.orc--orc-core--org.apache.orc__orc-core__${ORC_VERSION}.jar
ORC_SHIM_JAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.orc--orc-shims--org.apache.orc__orc-shims__${ORC_VERSION}.jar
ORC_MAPREDUCE_JAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.orc--orc-mapreduce--org.apache.orc__orc-mapreduce__${ORC_VERSION}.jar

PROTOBUF_JAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--com.google.protobuf--protobuf-java--com.google.protobuf__protobuf-java__2.6.1.jar
if [[ $BASE_SPARK_VERSION == "3.2.1" ]]
then
    PARQUETFORMATJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.parquet--parquet-format-structures--org.apache.parquet__parquet-format-structures__${PARQUET_VERSION}-databricks${DB_VERSION}.jar
else
    PARQUETFORMATJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.parquet--parquet-format--org.apache.parquet__parquet-format__2.4.0.jar
fi

NETWORKCOMMON=----workspace_${SPARK_MAJOR_VERSION_STRING}--common--network-common--network-common-hive-2.3__hadoop-${HADOOP_VERSION}_${SCALA_VERSION}_deploy.jar
NETWORKSHUFFLE=----workspace_${SPARK_MAJOR_VERSION_STRING}--common--network-shuffle--network-shuffle-hive-2.3__hadoop-${HADOOP_VERSION}_${SCALA_VERSION}_deploy.jar
COMMONUNSAFE=----workspace_${SPARK_MAJOR_VERSION_STRING}--common--unsafe--unsafe-hive-2.3__hadoop-${HADOOP_VERSION}_${SCALA_VERSION}_deploy.jar
LAUNCHER=----workspace_${SPARK_MAJOR_VERSION_STRING}--launcher--launcher-hive-2.3__hadoop-${HADOOP_VERSION}_${SCALA_VERSION}_deploy.jar

KRYO=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--com.esotericsoftware--kryo-shaded--com.esotericsoftware__kryo-shaded__4.0.2.jar

APACHECOMMONS=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--commons-io--commons-io--commons-io__commons-io__${COMMONS_IO_VERSION}.jar
APACHECOMMONSLANG3=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.commons--commons-lang3--org.apache.commons__commons-lang3__${COMMONS_LANG3_VERSION}.jar

HIVESTORAGE=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.hive--hive-storage-api--org.apache.hive__hive-storage-api__2.7.2.jar
HIVEEXECJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--patched-hive-with-glue--hive-exec-core_shaded.jar
ARROWFORMATJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.arrow--arrow-format--org.apache.arrow__arrow-format__2.0.0.jar
ARROWMEMORYJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.arrow--arrow-memory-core--org.apache.arrow__arrow-memory-core__2.0.0.jar
ARROWMEMORYNETTYJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.arrow--arrow-memory-netty--org.apache.arrow__arrow-memory-netty__2.0.0.jar
ARROWVECTORJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.arrow--arrow-vector--org.apache.arrow__arrow-vector__2.0.0.jar

JSON4S=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.json4s--json4s-ast_2.12--org.json4s__json4s-ast_2.12__${JSON4S_VERSION}.jar

JAVAASSIST=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.javassist--javassist--org.javassist__javassist__3.25.0-GA.jar

PROTOBUFJAVA=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--com.google.protobuf--protobuf-java--com.google.protobuf__protobuf-java__2.6.1.jar

JACKSONCORE=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--com.fasterxml.jackson.core--jackson-databind--com.fasterxml.jackson.core__jackson-databind__${FASTERXML_JACKSON_VERSION}.jar
JACKSONANNOTATION=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--com.fasterxml.jackson.core--jackson-annotations--com.fasterxml.jackson.core__jackson-annotations__${FASTERXML_JACKSON_VERSION}.jar

HADOOPCOMMON=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.hadoop--hadoop-common--org.apache.hadoop__hadoop-common__2.7.4.jar
HADOOPMAPRED=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-${HADOOP_VERSION}--org.apache.hadoop--hadoop-mapreduce-client-core--org.apache.hadoop__hadoop-mapreduce-client-core__2.7.4.jar

if [[ $BASE_SPARK_VERSION == "3.2.1" ]]
then
   AVROSPARKJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--vendor--avro--avro-hive-2.3__hadoop-3.2_2.12_deploy_shaded.jar
   AVROMAPRED=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-3.2--org.apache.avro--avro-mapred--org.apache.avro__avro-mapred__1.10.2.jar
   AVROJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-3.2--org.apache.avro--avro--org.apache.avro__avro__1.10.2.jar
else
   AVROSPARKJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--vendor--avro--avro_2.12_deploy_shaded.jar
   AVROMAPRED=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.avro--avro-mapred-hadoop2--org.apache.avro__avro-mapred-hadoop2__1.8.2.jar
   AVROJAR=----workspace_${SPARK_MAJOR_VERSION_STRING}--maven-trees--hive-2.3__hadoop-2.7--org.apache.avro--avro--org.apache.avro__avro__1.8.2.jar
fi

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
   -Dfile=$JARDIR/$AVROSPARKJAR\
   -DgroupId=org.apache.spark \
   -DartifactId=spark-avro_$SCALA_VERSION \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$AVROMAPRED\
   -DgroupId=org.apache.avro\
   -DartifactId=avro-mapred \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$AVROJAR \
   -DgroupId=org.apache.avro\
   -DartifactId=avro \
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
   -Dfile=$JARDIR/$NETWORKSHUFFLE \
   -DgroupId=org.apache.spark \
   -DartifactId=spark-network-shuffle_$SCALA_VERSION \
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
   -Dfile=$JARDIR/$ORC_CORE_JAR \
   -DgroupId=org.apache.orc \
   -DartifactId=orc-core \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ORC_SHIM_JAR \
   -DgroupId=org.apache.orc \
   -DartifactId=orc-shims \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$ORC_MAPREDUCE_JAR \
   -DgroupId=org.apache.orc \
   -DartifactId=orc-mapreduce \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B install:install-file \
   -Dmaven.repo.local=$M2DIR \
   -Dfile=$JARDIR/$PROTOBUF_JAR \
   -DgroupId=com.google.protobuf \
   -DartifactId=protobuf-java \
   -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
   -Dpackaging=jar

mvn -B -Ddatabricks -Dbuildver=$BUILDVER clean package -DskipTests $MVN_OPT

cd /home/ubuntu
tar -zcf spark-rapids-built.tgz spark-rapids
