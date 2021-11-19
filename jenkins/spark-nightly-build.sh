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
# build, install, and deploy all the versions we support, but skip deploy of individual dist module since we
# only want the combined jar to be pushed.
# Note this does not run any integration tests
mvn -U -B -Dbuildver=302 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -B -Dbuildver=302 deploy -pl -dist $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DskipTests
mvn -U -B -Dbuildver=303 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -B -Dbuildver=303 deploy -pl -dist $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DskipTests
mvn -U -B -Dbuildver=304 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -B -Dbuildver=304 deploy -pl -dist $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DskipTests
mvn -U -B -Dbuildver=311 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -B -Dbuildver=311 deploy -pl -dist $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DskipTests
mvn -U -B -Dbuildver=312 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -B -Dbuildver=312 deploy -pl -dist $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DskipTests
mvn -U -B -Dbuildver=313 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -B -Dbuildver=313 deploy -pl -dist $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DskipTests
mvn -U -B -Dbuildver=311cdh clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -B -Dbuildver=311cdh deploy -pl -dist $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DskipTests
mvn -U -B -Dbuildver=320 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -B -Dbuildver=320 deploy -pl -dist $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DskipTests
mvn -U -B -Dbuildver=321 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -B -Dbuildver=321 deploy -pl -dist $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DskipTests
# temporarily skip tests on Spark 3.3.0 - https://github.com/NVIDIA/spark-rapids/issues/4031
mvn -U -B -Dbuildver=330 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DskipTests
mvn -B -Dbuildver=330 deploy -pl -dist $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DskipTests

mvn -B -Dbuildver=301 -PsnapshotsWithDatabricks clean deploy $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER

# Parse cudf and spark files from local mvn repo
jenkins/printJarVersion.sh "CUDFVersion" "$M2DIR/ai/rapids/cudf/${CUDF_VER}" "cudf-${CUDF_VER}" "-${CUDA_CLASSIFIER}.jar" $SERVER_ID
jenkins/printJarVersion.sh "SPARKVersion" "$M2DIR/org/apache/spark/spark-core_2.12/${SPARK_VER}" "spark-core_2.12-${SPARK_VER}" ".jar" $SERVER_ID
