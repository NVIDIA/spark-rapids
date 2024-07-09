#!/bin/bash
#
# Copyright (c) 2020-2024, NVIDIA CORPORATION. All rights reserved.
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

# Split abc=123 from $OVERWRITE_PARAMS
# $OVERWRITE_PARAMS patten 'abc=123;def=456;'
PRE_IFS=$IFS
IFS=";"
for VAR in $OVERWRITE_PARAMS; do
    echo $VAR && export $VAR
done
IFS=$PRE_IFS


CUDF_VER=${CUDF_VER:-"24.08.0-SNAPSHOT"}
CUDA_CLASSIFIER=${CUDA_CLASSIFIER:-"cuda11"}
CLASSIFIER=${CLASSIFIER:-"$CUDA_CLASSIFIER"} # default as CUDA_CLASSIFIER for compatibility
PROJECT_VER=${PROJECT_VER:-"24.08.0-SNAPSHOT"}
PROJECT_TEST_VER=${PROJECT_TEST_VER:-"24.08.0-SNAPSHOT"}
SPARK_VER=${SPARK_VER:-"3.1.1"}
SPARK_VER_213=${SPARK_VER_213:-"3.3.0"}
# Make a best attempt to set the default value for the shuffle shim.
# Note that SPARK_VER for non-Apache Spark flavors (i.e. databricks,
# cloudera, and others) may not be a simple as just the version number, so
# this variable should be set accordingly.
SHUFFLE_SPARK_SHIM=${SHUFFLE_SPARK_SHIM:-spark${SPARK_VER//./}}
SHUFFLE_SPARK_SHIM=${SHUFFLE_SPARK_SHIM//\-SNAPSHOT/}
SCALA_BINARY_VER=${SCALA_BINARY_VER:-"2.12"}
SERVER_ID=${SERVER_ID:-"snapshots"}

CUDF_REPO=${CUDF_REPO:-"$URM_URL"}
PROJECT_REPO=${PROJECT_REPO:-"$URM_URL"}
PROJECT_TEST_REPO=${PROJECT_TEST_REPO:-"$URM_URL"}
SPARK_REPO=${SPARK_REPO:-"$URM_URL"}

echo "CUDF_VER: $CUDF_VER, CUDA_CLASSIFIER: $CUDA_CLASSIFIER, CLASSIFIER: $CLASSIFIER, PROJECT_VER: $PROJECT_VER \
    SPARK_VER: $SPARK_VER, SCALA_BINARY_VER: $SCALA_BINARY_VER"

# Spark shim versions
# get Spark shim versions from pom
function set_env_var_SPARK_SHIM_VERSIONS_ARR() {
    PROFILE_OPT=$1
    SPARK_SHIM_VERSIONS_STR=$(mvn -B help:evaluate -q -pl dist $PROFILE_OPT -Dexpression=included_buildvers -DforceStdout)
    SPARK_SHIM_VERSIONS_STR=$(echo $SPARK_SHIM_VERSIONS_STR)
    IFS=", " <<< $SPARK_SHIM_VERSIONS_STR read -r -a SPARK_SHIM_VERSIONS_ARR
}

if [[ $SCALA_BINARY_VER == "2.13" ]]; then
    # Psnapshots: snapshots + noSnapshots
    set_env_var_SPARK_SHIM_VERSIONS_ARR -PsnapshotsScala213
    SPARK_SHIM_VERSIONS_SNAPSHOTS=("${SPARK_SHIM_VERSIONS_ARR[@]}")
    # PnoSnapshots: noSnapshots only
    set_env_var_SPARK_SHIM_VERSIONS_ARR -PnoSnapshotsScala213
    SPARK_SHIM_VERSIONS_NOSNAPSHOTS=("${SPARK_SHIM_VERSIONS_ARR[@]}")
    # PsnapshotOnly : snapshots only
    set_env_var_SPARK_SHIM_VERSIONS_ARR -PsnapshotScala213Only
    SPARK_SHIM_VERSIONS_SNAPSHOTS_ONLY=("${SPARK_SHIM_VERSIONS_ARR[@]}")
else
    # Psnapshots: snapshots + noSnapshots
    set_env_var_SPARK_SHIM_VERSIONS_ARR -Psnapshots
    SPARK_SHIM_VERSIONS_SNAPSHOTS=("${SPARK_SHIM_VERSIONS_ARR[@]}")
    # PnoSnapshots: noSnapshots only
    set_env_var_SPARK_SHIM_VERSIONS_ARR -PnoSnapshots
    SPARK_SHIM_VERSIONS_NOSNAPSHOTS=("${SPARK_SHIM_VERSIONS_ARR[@]}")
    # PsnapshotOnly : snapshots only
    set_env_var_SPARK_SHIM_VERSIONS_ARR -PsnapshotOnly
    SPARK_SHIM_VERSIONS_SNAPSHOTS_ONLY=("${SPARK_SHIM_VERSIONS_ARR[@]}")
fi

# PHASE_TYPE: CICD phase at which the script is called, to specify Spark shim versions.
# regular: noSnapshots + snapshots
# pre-release: noSnapshots only
# *: shim versions to build, e.g., PHASE_TYPE="320 321"
PHASE_TYPE=${PHASE_TYPE:-"regular"}
case $PHASE_TYPE in
    # SPARK_SHIM_VERSIONS will be used for nightly artifact build
    pre-release)
        SPARK_SHIM_VERSIONS=("${SPARK_SHIM_VERSIONS_NOSNAPSHOTS[@]}")
        ;;

    regular)
        SPARK_SHIM_VERSIONS=("${SPARK_SHIM_VERSIONS_SNAPSHOTS[@]}")
        ;;

    *)
        SPARK_SHIM_VERSIONS=(`echo "$PHASE_TYPE"`)
        ;;
esac
# base version
SPARK_BASE_SHIM_VERSION=${SPARK_SHIM_VERSIONS[0]}
# tail snapshots
SPARK_SHIM_VERSIONS_SNAPSHOTS_TAIL=("${SPARK_SHIM_VERSIONS_SNAPSHOTS[@]:1}")
# tail noSnapshots
SPARK_SHIM_VERSIONS_NOSNAPSHOTS_TAIL=("${SPARK_SHIM_VERSIONS_NOSNAPSHOTS[@]:1}")
# build and run unit tests on one specific version for each sub-version (e.g. 320, 330)
# separate the versions to two parts (premergeUT1, premergeUT2) for balancing the duration
set_env_var_SPARK_SHIM_VERSIONS_ARR -PpremergeUT1
SPARK_SHIM_VERSIONS_PREMERGE_UT_1=("${SPARK_SHIM_VERSIONS_ARR[@]}")
set_env_var_SPARK_SHIM_VERSIONS_ARR -PpremergeUT2
SPARK_SHIM_VERSIONS_PREMERGE_UT_2=("${SPARK_SHIM_VERSIONS_ARR[@]}")
# utf-8 cases
set_env_var_SPARK_SHIM_VERSIONS_ARR -PpremergeUTF8
SPARK_SHIM_VERSIONS_PREMERGE_UTF8=("${SPARK_SHIM_VERSIONS_ARR[@]}")
# scala 2.13 cases
set_env_var_SPARK_SHIM_VERSIONS_ARR -PpremergeScala213
SPARK_SHIM_VERSIONS_PREMERGE_SCALA213=("${SPARK_SHIM_VERSIONS_ARR[@]}")
# jdk11 cases
set_env_var_SPARK_SHIM_VERSIONS_ARR -Pjdk11-test
SPARK_SHIM_VERSIONS_JDK11=("${SPARK_SHIM_VERSIONS_ARR[@]}")
# jdk17 cases
set_env_var_SPARK_SHIM_VERSIONS_ARR -Pjdk17-test
SPARK_SHIM_VERSIONS_JDK17=("${SPARK_SHIM_VERSIONS_ARR[@]}")
# databricks shims
set_env_var_SPARK_SHIM_VERSIONS_ARR -Pdatabricks
SPARK_SHIM_VERSIONS_DATABRICKS=("${SPARK_SHIM_VERSIONS_ARR[@]}")

echo "SPARK_BASE_SHIM_VERSION: $SPARK_BASE_SHIM_VERSION"
