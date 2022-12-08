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

# This script installs dependencies required to build RAPIDS Accelerator for Apache Spark on DB.
# All the environments can be overwritten by shell variables:
#   SPARKSRCTGZ: Archive file location of the plugin repository. Default is empty.
#   BASE_SPARK_VERSION: Spark version [3.1.2, 3.2.1, 3.3.0]. Default is pulled from current instance.
#   BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS: The version of Spark used when we install the
#       Databricks jars in .m2. Default is {BASE_SPARK_VERSION}.
#   MVN_OPT: Options to be passed to the MVN commands. Note that "-DskipTests" is hardcoded in the
#       build command.
#   SKIP_DEP_INSTALL: Skips installation of dependencies when set to 1. Default is 0.
# Usage:
# - build for DB10.4/Spark 3.2.1:
#       `BASE_SPARK_VERSION=3.2.1 ./jenkins/databricks/build.sh`
# - Build without dependency installation:
#       `BASE_SPARK_VERSION=3.2.1 SKIP_DEP_INSTALL=1 ./jenkins/databricks/build.sh`
# To add support of new runtime:
#   1. Review `set_jars_prefixes()` to make sure that the prefix of the jar files is set
#      correctly. If not, then add a new if-else block to set the variables as necessary.
#   2. Review `set_sw_versions(). If the new runtime depends on different versions, then add a new
#      block to define the correct versions.
#   3. The jar files and their artifacts are defined in `set_dep_jars()`.
#      You may need to add another conditional block because some runtimes may require special
#      handling.
#      For example, "3.1.2" had different patters for a few JARs (i.e., HIVE).
#   4. If you had to go beyond the above steps to support the new runtime, then update the
#      instructions accordingly.

set -ex

# Map of software versions for each dependency.
declare -A sw_versions
# Map of jar file locations of all dependencies
declare -A dep_jars
# Map of string arrays to hold the groupId and the artifactId for each JAR
declare -A artifacts

# Initializes the scripts and the variables based on teh arguments passed to the script.
initialize()
{
    # install rsync to be used for copying onto the databricks nodes
    sudo apt install -y maven rsync

    # Archive file location of the plugin repository
    SPARKSRCTGZ=${SPARKSRCTGZ:-''}

    # Version of Apache Spark we are building against
    BASE_SPARK_VERSION=${BASE_SPARK_VERSION:-$(< /databricks/spark/VERSION)}

    ## '-Pfoo=1,-Dbar=2,...' to '-Pfoo=1 -Dbar=2 ...'
    MVN_OPT=${MVN_OPT//','/' '}
    BUILDVER=$(echo ${BASE_SPARK_VERSION} | sed 's/\.//g')db
    # the version of Spark used when we install the Databricks jars in .m2
    BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=${BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS:-$BASE_SPARK_VERSION}
    SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=${BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS}-databricks

    # pull normal Spark artifacts and ignore errors then install databricks jars, then build again.
    # this should match the databricks init script.
    JARDIR=/databricks/jars

    if [[ -n $SPARKSRCTGZ ]]
    then
        rm -rf spark-rapids
        mkdir spark-rapids
        echo  "tar -zxf $SPARKSRCTGZ -C spark-rapids"
        tar -zxf $SPARKSRCTGZ -C spark-rapids
        cd spark-rapids
    fi

    # Now, we can set the WORKSPACE
    export WORKSPACE=$PWD
    # set the retry count for mvn commands
    MVN_CMD="mvn -Dmaven.wagon.http.retryHandler.count=3"
    # getting the versions of CUDA, SCALA and SPARK_PLUGIN
    SPARK_PLUGIN_JAR_VERSION=$($MVN_CMD help:evaluate -q -pl dist -Dexpression=project.version -DforceStdout)
    SCALA_VERSION=$($MVN_CMD help:evaluate -q -pl dist -Dexpression=scala.binary.version -DforceStdout)
    CUDA_VERSION=$($MVN_CMD help:evaluate -q -pl dist -Dexpression=cuda.version -DforceStdout)
    RAPIDS_BUILT_JAR=rapids-4-spark_$SCALA_VERSION-$SPARK_PLUGIN_JAR_VERSION.jar
    # If set to 1, skips installing dependencies into mvn repo.
    SKIP_DEP_INSTALL=${SKIP_DEP_INSTALL:-'0'}
    # export 'M2DIR' so that shims can get the correct Spark dependency info
    export M2DIR=/home/ubuntu/.m2/repository

    # Print a banner of the build configurations.
    printf '+ %*s +\n' 100 '' | tr ' ' =
    echo "Initializing build for Databricks:"
    echo
    echo "tgz                                           : ${SPARKSRCTGZ}"
    echo "Base Spark version                            : ${BASE_SPARK_VERSION}"
    echo "maven options                                 : ${MVN_OPT}"
    echo "BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS : ${BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS}"
    echo "workspace                                     : ${WORKSPACE}"
    echo "Scala version                                 : ${SCALA_VERSION}"
    echo "CUDA version                                  : ${CUDA_VERSION}"
    echo "Rapids build jar                              : ${RAPIDS_BUILT_JAR}"
    echo "Build Version                                 : ${BUILDVER}"
    echo "Skip Dependencies                             : ${SKIP_DEP_INSTALL}"
    printf '+ %*s +\n' 100 '' | tr ' ' =
}

# Sets the JAR files prefixes based on the build version.
# DB9.1 and 10.4 uses ----workspace as a prefix.
# DB 11.3 uses more abbreviations (i.e., workspace becomes ws).
set_jars_prefixes()
{
    # something like spark_3_1 or spark_3_0
    SPARK_MAJOR_VERSION_NUM_STRING=$(echo ${BASE_SPARK_VERSION} | sed 's/\./\_/g' | cut -d _ -f 1,2)

    # get the hive prefix. something like hive-2.3
    HIVE_VER_STRING=hive-$(echo ${sw_versions[HIVE_FULL]} | cut -d. -f 1,2)

    # defaults are for 3.1.2, and 3.2.1
    PREFIX_WS=----workspace
    SPARK_MAJOR_VERSION_STRING=spark_${SPARK_MAJOR_VERSION_NUM_STRING}
    PREFIX_SPARK=${PREFIX_WS}_${SPARK_MAJOR_VERSION_STRING}
    PREFIX_MVN_TREE=${PREFIX_SPARK}--maven-trees
    PREFIX_WS_SP_MVN_HADOOP=${PREFIX_MVN_TREE}--${HIVE_VER_STRING}__hadoop-${sw_versions[HADOOP]}

    if [[ $BASE_SPARK_VERSION == "3.3.0" ]]
    then
        #something like hadoop3
        HADOOP_MAJOR_VERSION_NUM_STRING=$(echo "${sw_versions[HADOOP]}" | sed 's/\./\_/g' | cut -d _ -f 1)
        HADOOP_MAJOR_VERSION_STRING=hadoop${HADOOP_MAJOR_VERSION_NUM_STRING}

        PREFIX_WS=----ws
        SPARK_MAJOR_VERSION_STRING=${SPARK_MAJOR_VERSION_NUM_STRING}
        PREFIX_SPARK=${PREFIX_WS}_${SPARK_MAJOR_VERSION_STRING}
        PREFIX_MVN_TREE=${PREFIX_SPARK}--mvn
        PREFIX_WS_SP_MVN_HADOOP=${PREFIX_MVN_TREE}--${HADOOP_MAJOR_VERSION_STRING}
    fi
}

# Defines the software version compatible with each runtime.
set_sw_versions()
{
    case "$BASE_SPARK_VERSION" in
        "3.3.0")
            sw_versions[ARROW]="7.0.0"
            sw_versions[AVRO]="1.11.0"
            sw_versions[COMMONS_IO]="2.11.0"
            sw_versions[COMMONS_LANG3]="3.12.0"
            sw_versions[DB]="-0007"
            sw_versions[FASTERXML_JACKSON]="2.13.4"
            sw_versions[HADOOP]="3.2"
            sw_versions[HIVE_FULL]="2.3.9"
            sw_versions[HIVESTORAGE_API]="2.7.2"
            sw_versions[JAVAASSIST]="3.25.0-GA"
            sw_versions[JSON4S]="3.7.0-M11"
            sw_versions[KRYO]="4.0.2"
            sw_versions[ORC]="1.7.6"
            sw_versions[PARQUET]="1.12.0"
            sw_versions[PROTOBUF]="2.6.1"
            ;;
        "3.2.1")
            sw_versions[ARROW]="2.0.0"
            sw_versions[AVRO]="1.10.2"
            sw_versions[COMMONS_IO]="2.8.0"
            sw_versions[COMMONS_LANG3]="3.12.0"
            sw_versions[DB]="-0007"
            sw_versions[FASTERXML_JACKSON]="2.12.3"
            sw_versions[HADOOP]="3.2"
            sw_versions[HIVE_FULL]="2.3.9"
            sw_versions[HIVESTORAGE_API]="2.7.2"
            sw_versions[JAVAASSIST]="3.25.0-GA"
            sw_versions[JSON4S]="3.7.0-M11"
            sw_versions[KRYO]="4.0.2"
            sw_versions[ORC]="1.6.13"
            sw_versions[PARQUET]="1.12.0"
            sw_versions[PROTOBUF]="2.6.1"
            ;;
        "3.1.2")
            sw_versions[COMMONS_LANG3]="3.10"
            sw_versions[COMMONS_IO]="2.4"
            sw_versions[DB]="9"
            sw_versions[FASTERXML_JACKSON]="2.10.0"
            sw_versions[HADOOP]="2.7"
            sw_versions[HIVE_FULL]="2.3.7"
            sw_versions[JSON4S]="3.7.0-M5"
            sw_versions[ORC]="1.5.12"
            sw_versions[PARQUET]="1.10.1"
            sw_versions[HIVESTORAGE_API]="2.7.2"
            sw_versions[PROTOBUF]="2.6.1"
            sw_versions[KRYO]="4.0.2"
            sw_versions[ARROW]="2.0.0"
            sw_versions[JAVAASSIST]="3.25.0-GA"
            sw_versions[AVRO]="1.8.2"
            ;;
        *) echo "Unexpected Spark version: $BASE_SPARK_VERSION"; exit 1;;
    esac
}

# Define dep_jars and the groupId/artifactId for each Jar.
# Note that it is unlikely that there are different groupId/artifactId for each Spark version.
set_dep_jars()
{
    # Note that while some jar file names partially depends on the groupId, and artifactId, the code will become more
    # complex.
    artifacts[NETWORKCOMMON]="-DgroupId=org.apache.spark -DartifactId=spark-network-common_${SCALA_VERSION}"
    dep_jars[NETWORKCOMMON]=${PREFIX_SPARK}--common--network-common--network-common-${HIVE_VER_STRING}__hadoop-${sw_versions[HADOOP]}_${SCALA_VERSION}_deploy.jar
    artifacts[NETWORKSHUFFLE]="-DgroupId=org.apache.spark -DartifactId=spark-network-shuffle_${SCALA_VERSION}"
    dep_jars[NETWORKSHUFFLE]=${PREFIX_SPARK}--common--network-shuffle--network-shuffle-${HIVE_VER_STRING}__hadoop-${sw_versions[HADOOP]}_${SCALA_VERSION}_deploy.jar
    artifacts[COMMONUNSAFE]="-DgroupId=org.apache.spark -DartifactId=spark-unsafe_${SCALA_VERSION}"
    dep_jars[COMMONUNSAFE]=${PREFIX_SPARK}--common--unsafe--unsafe-${HIVE_VER_STRING}__hadoop-${sw_versions[HADOOP]}_${SCALA_VERSION}_deploy.jar
    artifacts[LAUNCHER]="-DgroupId=org.apache.spark -DartifactId=spark-launcher_${SCALA_VERSION}"
    dep_jars[LAUNCHER]=${PREFIX_SPARK}--launcher--launcher-${HIVE_VER_STRING}__hadoop-${sw_versions[HADOOP]}_${SCALA_VERSION}_deploy.jar
    artifacts[SQL]="-DgroupId=org.apache.spark -DartifactId=spark-sql_${SCALA_VERSION}"
    dep_jars[SQL]=${PREFIX_SPARK}--sql--core--core-${HIVE_VER_STRING}__hadoop-${sw_versions[HADOOP]}_${SCALA_VERSION}_deploy.jar
    artifacts[CATALYST]="-DgroupId=org.apache.spark -DartifactId=spark-catalyst_${SCALA_VERSION}"
    dep_jars[CATALYST]=${PREFIX_SPARK}--sql--catalyst--catalyst-${HIVE_VER_STRING}__hadoop-${sw_versions[HADOOP]}_${SCALA_VERSION}_deploy.jar
    artifacts[ANNOT]="-DgroupId=org.apache.spark -DartifactId=spark-annotation_${SCALA_VERSION}"
    dep_jars[ANNOT]=${PREFIX_SPARK}--common--tags--tags-${HIVE_VER_STRING}__hadoop-${sw_versions[HADOOP]}_${SCALA_VERSION}_deploy.jar
    artifacts[CORE]="-DgroupId=org.apache.spark -DartifactId=spark-core_${SCALA_VERSION}"
    dep_jars[CORE]=${PREFIX_SPARK}--core--core-${HIVE_VER_STRING}__hadoop-${sw_versions[HADOOP]}_${SCALA_VERSION}_deploy.jar
    artifacts[HIVE]="-DgroupId=org.apache.spark -DartifactId=spark-hive_${SCALA_VERSION}"
    dep_jars[HIVE]=${PREFIX_SPARK}--sql--hive--hive-${HIVE_VER_STRING}__hadoop-${sw_versions[HADOOP]}_${SCALA_VERSION}_deploy_shaded.jar
    artifacts[HIVEEXEC]="-DgroupId=org.apache.hive -DartifactId=hive-exec"
    dep_jars[HIVEEXEC]=${PREFIX_SPARK}--patched-hive-with-glue--hive-exec-core_shaded.jar
    artifacts[HIVESERDE]="-DgroupId=org.apache.hive -DartifactId=hive-serde"
    dep_jars[HIVESERDE]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.hive--hive-serde--org.apache.hive__hive-serde__${sw_versions[HIVE_FULL]}.jar
    artifacts[HIVESTORAGE]="-DgroupId=org.apache.hive -DartifactId=hive-storage-api"
    dep_jars[HIVESTORAGE]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.hive--hive-storage-api--org.apache.hive__hive-storage-api__${sw_versions[HIVESTORAGE_API]}.jar
    artifacts[PARQUETHADOOP]="-DgroupId=org.apache.parquet -DartifactId=parquet-hadoop"
    dep_jars[PARQUETHADOOP]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.parquet--parquet-hadoop--org.apache.parquet__parquet-hadoop__${sw_versions[PARQUET]}-databricks${sw_versions[DB]}.jar
    artifacts[PARQUETCOMMON]="-DgroupId=org.apache.parquet -DartifactId=parquet-common"
    dep_jars[PARQUETCOMMON]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.parquet--parquet-common--org.apache.parquet__parquet-common__${sw_versions[PARQUET]}-databricks${sw_versions[DB]}.jar
    artifacts[PARQUETCOLUMN]="-DgroupId=org.apache.parquet -DartifactId=parquet-column"
    dep_jars[PARQUETCOLUMN]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.parquet--parquet-column--org.apache.parquet__parquet-column__${sw_versions[PARQUET]}-databricks${sw_versions[DB]}.jar
    artifacts[ORC_CORE]="-DgroupId=org.apache.orc -DartifactId=orc-core"
    dep_jars[ORC_CORE]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.orc--orc-core--org.apache.orc__orc-core__${sw_versions[ORC]}.jar
    artifacts[ORC_SHIM]="-DgroupId=org.apache.orc -DartifactId=orc-shims"
    dep_jars[ORC_SHIM]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.orc--orc-shims--org.apache.orc__orc-shims__${sw_versions[ORC]}.jar
    artifacts[ORC_MAPREDUCE]="-DgroupId=org.apache.orc -DartifactId=orc-mapreduce"
    dep_jars[ORC_MAPREDUCE]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.orc--orc-mapreduce--org.apache.orc__orc-mapreduce__${sw_versions[ORC]}.jar
    artifacts[PROTOBUF]="-DgroupId=com.google.protobuf -DartifactId=protobuf-java"
    dep_jars[PROTOBUF]=${PREFIX_WS_SP_MVN_HADOOP}--com.google.protobuf--protobuf-java--com.google.protobuf__protobuf-java__${sw_versions[PROTOBUF]}.jar
    artifacts[PARQUETFORMAT]="-DgroupId=org.apache.parquet -DartifactId=parquet-format"
    dep_jars[PARQUETFORMAT]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.parquet--parquet-format-structures--org.apache.parquet__parquet-format-structures__${sw_versions[PARQUET]}-databricks${sw_versions[DB]}.jar
    artifacts[KRYO]="-DgroupId=com.esotericsoftware.kryo -DartifactId=kryo-shaded-db"
    dep_jars[KRYO]=${PREFIX_WS_SP_MVN_HADOOP}--com.esotericsoftware--kryo-shaded--com.esotericsoftware__kryo-shaded__${sw_versions[KRYO]}.jar
    artifacts[APACHECOMMONS]="-DgroupId=org.apache.commons -DartifactId=commons-io"
    dep_jars[APACHECOMMONS]=${PREFIX_WS_SP_MVN_HADOOP}--commons-io--commons-io--commons-io__commons-io__${sw_versions[COMMONS_IO]}.jar
    artifacts[APACHECOMMONSLANG3]="-DgroupId=org.apache.commons -DartifactId=commons-lang3"
    dep_jars[APACHECOMMONSLANG3]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.commons--commons-lang3--org.apache.commons__commons-lang3__${sw_versions[COMMONS_LANG3]}.jar
    artifacts[ARROWFORMAT]="-DgroupId=org.apache.arrow -DartifactId=arrow-format"
    dep_jars[ARROWFORMAT]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.arrow--arrow-format--org.apache.arrow__arrow-format__${sw_versions[ARROW]}.jar
    artifacts[ARROWMEMORY]="-DgroupId=org.apache.arrow -DartifactId=arrow-memory"
    dep_jars[ARROWMEMORY]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.arrow--arrow-memory-core--org.apache.arrow__arrow-memory-core__${sw_versions[ARROW]}.jar
    artifacts[ARROWVECTOR]="-DgroupId=org.apache.arrow -DartifactId=arrow-vector"
    dep_jars[ARROWVECTOR]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.arrow--arrow-vector--org.apache.arrow__arrow-vector__${sw_versions[ARROW]}.jar
    artifacts[JSON4S]="-DgroupId=org.json4s -DartifactId=JsonAST"
    dep_jars[JSON4S]=${PREFIX_WS_SP_MVN_HADOOP}--org.json4s--json4s-ast_${SCALA_VERSION}--org.json4s__json4s-ast_${SCALA_VERSION}__${sw_versions[JSON4S]}.jar
    artifacts[JAVAASSIST]="-DgroupId=org.javaassist -DartifactId=javaassist"
    dep_jars[JAVAASSIST]=${PREFIX_WS_SP_MVN_HADOOP}--org.javassist--javassist--org.javassist__javassist__${sw_versions[JAVAASSIST]}.jar
    artifacts[JACKSONCORE]="-DgroupId=com.fasterxml.jackson.core -DartifactId=jackson-core"
    dep_jars[JACKSONCORE]=${PREFIX_WS_SP_MVN_HADOOP}--com.fasterxml.jackson.core--jackson-databind--com.fasterxml.jackson.core__jackson-databind__${sw_versions[FASTERXML_JACKSON]}.jar
    artifacts[JACKSONANNOTATION]="-DgroupId=com.fasterxml.jackson.core -DartifactId=jackson-annotations"
    dep_jars[JACKSONANNOTATION]=${PREFIX_WS_SP_MVN_HADOOP}--com.fasterxml.jackson.core--jackson-annotations--com.fasterxml.jackson.core__jackson-annotations__${sw_versions[FASTERXML_JACKSON]}.jar
    artifacts[AVROSPARK]="-DgroupId=org.apache.spark -DartifactId=spark-avro_${SCALA_VERSION}"
    dep_jars[AVROSPARK]=${PREFIX_SPARK}--vendor--avro--avro-${HIVE_VER_STRING}__hadoop-${sw_versions[HADOOP]}_${SCALA_VERSION}_deploy_shaded.jar
    artifacts[AVROMAPRED]="-DgroupId=org.apache.avro -DartifactId=avro-mapred"
    dep_jars[AVROMAPRED]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.avro--avro-mapred--org.apache.avro__avro-mapred__${sw_versions[AVRO]}.jar
    artifacts[AVRO]="-DgroupId=org.apache.avro -DartifactId=avro"
    dep_jars[AVRO]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.avro--avro--org.apache.avro__avro__${sw_versions[AVRO]}.jar

    # spark-3.1.2 overrides some jar naming conventions
    if [[ $BASE_SPARK_VERSION == "3.1.2" ]]
    then
        dep_jars[HIVE]=${PREFIX_SPARK}--sql--hive--hive_${SCALA_VERSION}_deploy_shaded.jar
        dep_jars[PARQUETFORMAT]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.parquet--parquet-format--org.apache.parquet__parquet-format__2.4.0.jar
        dep_jars[AVROSPARK]=${PREFIX_SPARK}--vendor--avro--avro_${SCALA_VERSION}_deploy_shaded.jar
        dep_jars[AVROMAPRED]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.avro--avro-mapred-hadoop2--org.apache.avro__avro-mapred-hadoop2__${sw_versions[AVRO]}.jar
        dep_jars[AVRO]=${PREFIX_WS_SP_MVN_HADOOP}--org.apache.avro--avro--org.apache.avro__avro__${sw_versions[AVRO]}.jar
    fi
}

# Install dependency jars to MVN repository.
install_dependencies()
{
    set_sw_versions
    set_jars_prefixes
    set_dep_jars
    # Please note we are installing all of these dependencies using the Spark version
    # (SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS) to make it easier to specify the dependencies in
    # the pom files
    for key in "${!artifacts[@]}"; do
        echo "running mvn command for $key..."
        $MVN_CMD -B install:install-file \
            -Dmaven.repo.local=$M2DIR \
            -Dfile=$JARDIR/${dep_jars[$key]} \
            ${artifacts[$key]} \
            -Dversion=$SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS \
            -Dpackaging=jar
    done
}

##########################
# Main script starts here
##########################

initialize
if [[ $SKIP_DEP_INSTALL == "1" ]]
then
    echo "SKIP_DEP_INSTALL is set to $SKIP_DEP_INSTALL. Skipping dependencies."
else
    # Install required dependencies.
    install_dependencies
fi
# Build the RAPIDS plugin by running package command for databricks
mvn -B -Ddatabricks -Dbuildver=$BUILDVER clean package -DskipTests $MVN_OPT

cd /home/ubuntu
tar -zcf spark-rapids-built.tgz spark-rapids
