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

. jenkins/version-def.sh

mvn -U -B -Pinclude-databricks,snapshot-shims clean deploy $MVN_URM_MIRROR -Dmaven.repo.local=$WORKSPACE/.m2
# Run unit tests against other spark versions
mvn -U -B -Pspark301tests,snapshot-shims test $MVN_URM_MIRROR -Dmaven.repo.local=$WORKSPACE/.m2
mvn -U -B -Pspark302tests,snapshot-shims test $MVN_URM_MIRROR -Dmaven.repo.local=$WORKSPACE/.m2
mvn -U -B -Pspark310tests,snapshot-shims test $MVN_URM_MIRROR -Dmaven.repo.local=$WORKSPACE/.m2

# Parse cudf and spark files from local mvn repo
jenkins/printJarVersion.sh "CUDFVersion" "${WORKSPACE}/.m2/ai/rapids/cudf/${CUDF_VER}" "cudf-${CUDF_VER}" "-${CUDA_CLASSIFIER}.jar"
jenkins/printJarVersion.sh "SPARKVersion" "${WORKSPACE}/.m2/org/apache/spark/spark-core_2.12/${SPARK_VER}" "spark-core_2.12-${SPARK_VER}" ".jar"
