#!/usr/bin/env bash

#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
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

[[ "$1" != "true" ]] || { export TEMP=$(mvn help:all-profiles -pl . $2 | sort | uniq | awk '/([^-])release[0-9]/ {print substr($3, 8)}');
TEMP=$(echo -n $TEMP);
[[ -n $TEMP ]] || { echo -e 'Error setting databricks versions'; };
<<< $TEMP read -r -a SPARK_SHIM_VERSIONS_ARR;
SNAPSHOTS=();
NO_SNAPSHOTS=();
for ver in ${SPARK_SHIM_VERSIONS_ARR[@]}; do
    TEST=$(mvn -B help:evaluate -q -pl dist $2 -Dexpression=spark.version -Dbuildver="$ver" -DforceStdout);
    if [[ "$TEST" != *"-SNAPSHOT" ]]; then NO_SNAPSHOTS+=(" $ver");
    else SNAPSHOTS+=(" $ver");
    fi
done;
echo "${SNAPSHOTS[@]} | ${NO_SNAPSHOTS[@]}"; };
