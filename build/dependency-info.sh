#!/usr/bin/env bash

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

# This script generates the dependency info.
# Arguments:
#   CUDF_VER - The version of cudf
#   CUDA_CLASSIFIER - The cuda runtime for cudf
#   SPARK_VER - The version of spark

# Parse cudf and spark dependency versions

CUDF_VER=$1
CUDA_CLASSIFIER=$2
SERVER_ID=snapshots
${WORKSPACE}/jenkins/printJarVersion.sh "cudf_version" "${HOME}/.m2/repository/ai/rapids/cudf/${CUDF_VER}" "cudf-${CUDF_VER}" "-${CUDA_CLASSIFIER}.jar" $SERVER_ID

SPARK_VER=$3
SPARK_SQL_VER=`${WORKSPACE}/jenkins/printJarVersion.sh "spark_version" "${HOME}/.m2/repository/org/apache/spark/spark-sql_2.12/${SPARK_VER}" "spark-sql_2.12-${SPARK_VER}" ".jar" $SERVER_ID`

# Split spark version from spark-sql_2.12 jar filename
echo ${SPARK_SQL_VER/"-sql_2.12"/}
