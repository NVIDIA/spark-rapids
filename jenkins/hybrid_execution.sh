#!/bin/bash
#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
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

test_hybrid_feature() {
    echo "Run hybrid execution test cases..."

    # parameters for Hybrid featrue
    spark_prefix="${SPARK_VER:0:3}" # get prefix from SPARK_VER, e.g.: 3.2, 3.3 ... 3.5
    GLUTEN_BUNDLE_JAR="gluten-velox-bundle-spark${spark_prefix}_2.12-ubuntu_${GLUTEN_FOR_OS}_x86_64-${GLUTEN_VERSION}.jar"
    HYBRID_JAR="rapids-4-spark-hybrid_2.12-${PROJECT_TEST_VER}.jar"
    GLUTEN_THIRD_PARTY_JAR="gluten-thirdparty-lib-${GLUTEN_VERSION}-ubuntu-${GLUTEN_FOR_OS}-x86_64.jar"

    # download Gluten, Hybrid jars
    mvn -B dependency:get -DgroupId=com.nvidia \
                       -DartifactId=gluten-velox-bundle \
                       -Dversion=${GLUTEN_VERSION} \
                       -Dpackaging=jar \
                       -Dclassifier=spark${spark_prefix}_2.12-ubuntu_${GLUTEN_FOR_OS}
                       -Dtransitive=false \
                       -Ddest=/tmp/$GLUTEN_BUNDLE_JAR
    mvn -B dependency:get -DgroupId=com.nvidia \
                       -DartifactId=rapids-4-spark-hybrid_2.12 \
                       -Dversion=${PROJECT_TEST_VER} \
                       -Dpackaging=jar \
                       -Dtransitive=false \
                       -Ddest=/tmp/$HYBRID_JAR
    wget -O /tmp/${GLUTEN_THIRD_PARTY_JAR} ${MVN_URM_MIRROR}/com/nvidia/gluten-thirdparty-lib/${GLUTEN_VERSION}/${GLUTEN_THIRD_PARTY_JAR}

    # run Hybrid Python tests
    LOAD_HYBRID_BACKEND=1 \
    HYBRID_BACKEND_JARS=/tmp/${HYBRID_JAR},/tmp/${GLUTEN_BUNDLE_JAR},/tmp/${GLUTEN_THIRD_PARTY_JAR} \
    ./integration_tests/run_pyspark_from_build.sh -m hybrid_test
}
