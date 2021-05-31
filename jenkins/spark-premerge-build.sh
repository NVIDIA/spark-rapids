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

nvidia-smi

. jenkins/version-def.sh

ARTF_ROOT="$WORKSPACE/.download"
MVN_GET_CMD="mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -B \
    $MVN_URM_MIRROR -DremoteRepositories=$URM_URL \
    -Ddest=$ARTF_ROOT"

rm -rf $ARTF_ROOT && mkdir -p $ARTF_ROOT

# Download a full version of spark
$MVN_GET_CMD \
    -DgroupId=org.apache -DartifactId=spark -Dversion=$SPARK_VER -Dclassifier=bin-hadoop3.2 -Dpackaging=tgz

export SPARK_HOME="$ARTF_ROOT/spark-$SPARK_VER-bin-hadoop3.2"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
tar zxf $SPARK_HOME.tgz -C $ARTF_ROOT && \
    rm -f $SPARK_HOME.tgz

mvn -U -B $MVN_URM_MIRROR '-P!snapshot-shims,pre-merge' clean verify -Dpytest.TEST_TAGS='' \
    -Dpytest.TEST_TYPE="pre-commit" -Dpytest.TEST_PARALLEL=4 -Dcuda.version=$CUDA_CLASSIFIER
# Run the unit tests for other Spark versions but dont run full python integration tests
# NOT ALL TESTS NEEDED FOR PREMERGE
#env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Pspark301tests,snapshot-shims test -Dpytest.TEST_TAGS='' -Dcuda.version=$CUDA_CLASSIFIER
env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Pspark302tests,snapshot-shims test -Dpytest.TEST_TAGS='' -Dcuda.version=$CUDA_CLASSIFIER
env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Pspark303tests,snapshot-shims test -Dpytest.TEST_TAGS='' -Dcuda.version=$CUDA_CLASSIFIER
env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Pspark311tests,snapshot-shims test -Dpytest.TEST_TAGS='' -Dcuda.version=$CUDA_CLASSIFIER
env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Pspark312tests,snapshot-shims test -Dpytest.TEST_TAGS='' -Dcuda.version=$CUDA_CLASSIFIER
env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Pspark313tests,snapshot-shims test -Dpytest.TEST_TAGS='' -Dcuda.version=$CUDA_CLASSIFIER
# Disabled until Spark 3.2 source incompatibility fixed, see https://github.com/NVIDIA/spark-rapids/issues/2052
#env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Pspark320tests,snapshot-shims test -Dpytest.TEST_TAGS='' -Dcuda.version=$CUDA_CLASSIFIER

# The jacoco coverage should have been collected, but because of how the shade plugin
# works and jacoco we need to clean some things up so jacoco will only report for the
# things we care about
mkdir -p target/jacoco_classes/
FILE=$(ls dist/target/rapids-4-spark_2.12-*.jar | grep -v test | xargs readlink -f)
pushd target/jacoco_classes/
jar xf $FILE
rm -rf com/nvidia/shaded/ org/openucx/
popd
