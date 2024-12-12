#!/bin/bash

# Copyright (c) 2024, NVIDIA CORPORATION.
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

set -e

scala_ver=${1:-"2.12"}
base_URL="https://oss.sonatype.org/service/local/artifact/maven/resolve"
project_jni="spark-rapids-jni"
project_private="rapids-4-spark-private_${scala_ver}"

jni_ver=$(mvn help:evaluate -q -pl dist -Dexpression=spark-rapids-jni.version -DforceStdout)
private_ver=$(mvn help:evaluate -q -pl dist -Dexpression=spark-rapids-private.version -DforceStdout)

jni_sha1=$(curl -s -H "Accept: application/json" \
  "${base_URL}?r=snapshots&g=com.nvidia&a=${project_jni}&v=${jni_ver}&c=&e=jar&wt=json" \
  | jq .data.sha1) || $(date +'%Y-%m-%d')
private_sha1=$(curl -s -H "Accept: application/json" \
  "${base_URL}?r=snapshots&g=com.nvidia&a=${project_private}&v=${private_ver}&c=&e=jar&wt=json" \
  | jq .data.sha1) || $(date +'%Y-%m-%d')

sha1md5=$(echo -n "${jni_sha1}_${private_sha1}" | md5sum | awk '{print $1}')

echo $sha1md5
