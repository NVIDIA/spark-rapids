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

[[ "$1" != "true" ]] || {
PKG_OK=$(dpkg-query -W -f='${Status}' libxml-xpath-perl 2>/dev/null | grep -c "ok installed");
if [ ${PKG_OK} -eq 0 ]; then
    TEMP=$(mvn help:all-profiles -pl . -f $2 | sort | uniq | awk '/release[0-9]/ {print substr($3, 8)}');
else
    TEMP=$(xpath -q -e "//profiles/profile/id/text()" "$2/pom.xml" |grep "release" | sort | uniq);
fi
TEMP=$(echo -n $TEMP);
[[ -n $TEMP ]] || { echo -e 'Error setting release versions'; };
<<< $TEMP read -r -a SPARK_SHIM_VERSIONS_ARR;
SNAPSHOTS=();
NO_SNAPSHOTS=();
for ver in ${SPARK_SHIM_VERSIONS_ARR[@]}; do
    buildver=$([ "$PKG_OK" == 0 ] && echo $ver || echo ${ver:7} );
    if [ ${PKG_OK} -eq 0 ]; then
        TEST=$(mvn -B help:evaluate -q -pl dist -f $2 -Dexpression=spark.version -Dbuildver="$buildver" -DforceStdout);
    else
        TEST=$(xpath -q -e "//spark${buildver}.version/text()" "$2/pom.xml");
    fi
    if [[ "$TEST" != *"-SNAPSHOT" ]]; then NO_SNAPSHOTS+=(" ${buildver}");
    else SNAPSHOTS+=(" ${buildver}");
    fi
done;
echo "${PKG_OK} | ${SNAPSHOTS[@]} | ${NO_SNAPSHOTS[@]}"; };
