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

set -ex

if [ "$SPARK_VER"x == x ];then
    SPARK_VER="3.0.1-SNAPSHOT"
fi

SCALA_BINARY_VER=${SCALA_BINARY_VER:-2.12}

#default maven server gpuwa
if [ "$SERVER_URL"x == x ]; then
    SERVER_URL="https://gpuwa.nvidia.com/artifactory/sw-spark-maven"
fi

echo "CUDA_CLASSIFIER: $CUDA_CLASSIFIER SPARK_VER: $SPARK_VER, \
    SCALA_BINARY_VER: $SCALA_BINARY_VER, SERVER_URL: $SERVER_URL"

ARTF_ROOT="$WORKSPACE/.download"
MVN_GET_CMD="mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -B \
    -DremoteRepositories=$SERVER_URL \
    -Ddest=$ARTF_ROOT"

rm -rf $ARTF_ROOT && mkdir -p $ARTF_ROOT

# Download a full version of spark
$MVN_GET_CMD \
    -DgroupId=org.apache -DartifactId=spark -Dversion=$SPARK_VER -Dclassifier=bin-hadoop3 -Dpackaging=tar.gz

export SPARK_HOME="$ARTF_ROOT/spark-$SPARK_VER-bin-hadoop3"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
tar zxf $SPARK_HOME.tar.gz -C $ARTF_ROOT && \
    rm -f $SPARK_HOME.tar.gz

mvn -U -B "$@" clean verify

# The jacoco coverage should have been collected, but because of how the shade plugin
# works and jacoco we need to clean some things up so jacoco will only report for the
# things we care about
mkdir -p target/jacoco_classes/
FILE=$(ls dist/target/rapids-4-spark_2.12-*.jar | grep -v test | xargs readlink -f)
pushd target/jacoco_classes/
jar xf $FILE
rm -rf ai/rapids/shaded/ org/openucx/
popd
