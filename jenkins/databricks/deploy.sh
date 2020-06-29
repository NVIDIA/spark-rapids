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
rm -rf deploy
mkdir -p deploy
cd deploy
tar -zxvf ../spark-rapids-built.tgz
cd spark-rapids
echo "Maven mirror is $MVN_URM_MIRROR"
SERVER_ID='snapshots'
SERVER_URL='https://urm.nvidia.com:443/artifactory/sw-spark-maven-local'
FPATH=./dist/target/rapids-4-spark_$SCALA_VERSION-$DATABRICKS_VERSION.jar
mvn -B deploy:deploy-file $MVN_URM_MIRROR -Durl=$SERVER_URL -DrepositoryId=$SERVER_ID \
    -Dfile=$FPATH -DpomFile=dist/pom.xml 
