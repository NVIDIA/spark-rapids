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

. jenkins/version-def.sh

## export 'M2DIR' so that shims can get the correct cudf/spark dependency info
export M2DIR="$WORKSPACE/.m2"
# Install all the versions we support
mvn -U -B -Dbuildver=301 install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Dbuildver=302 install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Dbuildver=303 install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Dbuildver=304 install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Dbuildver=311 install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Dbuildver=312 install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Dbuildver=313 install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Dbuildver=311cdh install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
# Disabled until Spark 3.2 source incompatibility fixed, see https://github.com/NVIDIA/spark-rapids/issues/2052
#mvn -U -B -Pspark320tests,snapshot-shims test $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Pallsnapshotshims deploy $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR \
    -Dpytest.TEST_TAGS='' -Dpytest.TEST_TYPE="nightly" -Dcuda.version=$CUDA_CLASSIFIER

# Parse cudf and spark files from local mvn repo
jenkins/printJarVersion.sh "CUDFVersion" "$M2DIR/ai/rapids/cudf/${CUDF_VER}" "cudf-${CUDF_VER}" "-${CUDA_CLASSIFIER}.jar" $SERVER_ID
jenkins/printJarVersion.sh "SPARKVersion" "$M2DIR/org/apache/spark/spark-core_2.12/${SPARK_VER}" "spark-core_2.12-${SPARK_VER}" ".jar" $SERVER_ID
