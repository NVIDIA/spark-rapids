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

set -e

# Split abc=123 from $OVERWRITE_PARAMS
# $OVERWRITE_PARAMS patten 'abc=123;def=456;'
PRE_IFS=$IFS
IFS=";"
for VAR in $OVERWRITE_PARAMS;do
    echo $VAR && export $VAR
done
IFS=$PRE_IFS

CUDF_VER=${CUDF_VER:-"21.06.0-SNAPSHOT"}
CUDA_CLASSIFIER=${CUDA_CLASSIFIER:-"cuda11"}
PROJECT_VER=${PROJECT_VER:-"21.06.0-SNAPSHOT"}
PROJECT_TEST_VER=${PROJECT_TEST_VER:-"21.06.0-SNAPSHOT"}
SPARK_VER=${SPARK_VER:-"3.0.1"}
SCALA_BINARY_VER=${SCALA_BINARY_VER:-"2.12"}
SERVER_ID=${SERVER_ID:-"snapshots"}

CUDF_REPO=${CUDF_REPO:-"$URM_URL"}
PROJECT_REPO=${PROJECT_REPO:-"$URM_URL"}
PROJECT_TEST_REPO=${PROJECT_TEST_REPO:-"$URM_URL"}
SPARK_REPO=${SPARK_REPO:-"$URM_URL"}

echo "CUDF_VER: $CUDF_VER, CUDA_CLASSIFIER: $CUDA_CLASSIFIER, PROJECT_VER: $PROJECT_VER \
    SPARK_VER: $SPARK_VER, SCALA_BINARY_VER: $SCALA_BINARY_VER"
