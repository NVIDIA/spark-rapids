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
rm -rf deploy
mkdir -p deploy
cd deploy
tar -zxf ../spark-rapids-built.tgz
cd spark-rapids
echo "Maven mirror is $MVN_URM_MIRROR"
SERVER_ID='snapshots'
SERVER_URL="$URM_URL-local"
SCALA_VERSION=`mvn help:evaluate -q -pl dist -Dexpression=scala.binary.version -DforceStdout`
# remove the periods so change something like 3.0.1 to 301
VERSION_NUM=${BASE_SPARK_VERSION//.}
SPARK_VERSION_STR=spark$VERSION_NUM
SPARK_PLUGIN_JAR_VERSION=`mvn help:evaluate -q -pl dist -Dexpression=project.version -DforceStdout`
DB_SHIM_DIRECTORY=${SPARK_VERSION_STR}db
DBJARFPATH=./shims/${DB_SHIM_DIRECTORY}/target/rapids-4-spark-shims-$SPARK_VERSION_STR-databricks_$SCALA_VERSION-$SPARK_PLUGIN_JAR_VERSION.jar
echo "Databricks jar is: $DBJARFPATH"
mvn -B deploy:deploy-file $MVN_URM_MIRROR '-P!snapshot-shims' -Durl=$SERVER_URL -DrepositoryId=$SERVER_ID \
    -Dfile=$DBJARFPATH -DpomFile=shims/${DB_SHIM_DIRECTORY}/pom.xml
