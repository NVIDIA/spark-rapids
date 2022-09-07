#!/bin/bash
#
# Copyright (c) 2020-2022, NVIDIA CORPORATION. All rights reserved.
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

function get_spark_shim_versions() {
    PROFILE_OPT=$1
    SPARK_SHIM_VERSIONS_STR=$(mvn -B help:evaluate -q -pl dist $PROFILE_OPT -Dexpression=included_buildvers -DforceStdout)
    SPARK_SHIM_VERSIONS_STR=$(echo $SPARK_SHIM_VERSIONS_STR)
    PRE_IFS=$IFS
    IFS=", " <<< $SPARK_SHIM_VERSIONS_STR read -r -a SPARK_SHIM_VERSIONS
    IFS=$PRE_IFS
}