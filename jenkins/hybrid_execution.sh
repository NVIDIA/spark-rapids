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

export GLUTEN_BUNDLE_JAR
export HYBRID_JAR
export GLUTEN_THIRDPARTY_JAR

hybrid_prepare(){
    echo "Checking hybrid exeicution tests environmet..."
    local cup_arch=$(uname -m)
    local os_version=$(source /etc/os-release > /dev/null 2>&1 && echo ${ID}_${VERSION_ID})
    local spark_prefix="${SPARK_VER:0:3}" # get prefix from SPARK_VER, e.g.: 3.2, 3.3 ... 3.5
    if [[ ! "$SUPPORTED_HYBRID_SHIMS" == *"$spark_prefix"* ]]; then
        echo "SKIP! spark $spark_prefix is not in the support hybrid shim list $SUPPORTED_HYBRID_SHIMS"
        return 1
    fi
    echo "cup_arch=$cup_arch, os_version=$os_version, SCALA_BINARY_VER=$SCALA_BINARY_VER"
    if [[ ! ("$cup_arch" == "x86_64" && ("$os_version" == "ubuntu_20.04" || "$os_version" == "ubuntu_22.04") && "$SCALA_BINARY_VER" == "2.12") ]]; then
        echo "SKIP! Only supports running Scala 2.12 hybrid execution tests on an x86_64 processor under Ubuntu 20.04 or 22.04."
        return 1
    fi

    echo "Downloading hybrid execution dependency jars..."
    GLUTEN_BUNDLE_JAR="gluten-velox-bundle-${GLUTEN_VERSION}-spark${spark_prefix}_${SCALA_BINARY_VER}-${os_version}_${cup_arch}.jar"
    HYBRID_JAR="rapids-4-spark-hybrid_${SCALA_BINARY_VER}-${PROJECT_VER}.jar"
    GLUTEN_THIRDPARTY_JAR="gluten-thirdparty-lib-${GLUTEN_VERSION}-${os_version}-${cup_arch}.jar"
    wget -q -O /tmp/$GLUTEN_BUNDLE_JAR $URM_URL/com/nvidia/gluten-velox-bundle/$GLUTEN_VERSION/$GLUTEN_BUNDLE_JAR
    wget -q -O /tmp/$HYBRID_JAR $URM_URL/com/nvidia/rapids-4-spark-hybrid_${SCALA_BINARY_VER}/$PROJECT_VER/$HYBRID_JAR
    wget -q -O /tmp/$GLUTEN_THIRDPARTY_JAR  $URM_URL/com/nvidia/gluten-thirdparty-lib/$GLUTEN_VERSION/$GLUTEN_THIRDPARTY_JAR
}

hybrid_test() {
    echo "Run hybrid execution tests..."
    LOAD_HYBRID_BACKEND=1 HYBRID_BACKEND_JARS=/tmp/${HYBRID_JAR},/tmp/${GLUTEN_BUNDLE_JAR},/tmp/${GLUTEN_THIRDPARTY_JAR} \
        integration_tests/run_pyspark_from_build.sh -m hybrid_test
}
