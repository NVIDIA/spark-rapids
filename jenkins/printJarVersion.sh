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

function print_ver(){
    TAG=$1
    REPO=$2
    VERSION=$3
    SUFFIX=$4
    SERVER_ID=$5
    
    if [[ "$VERSION" == *"-SNAPSHOT" ]]; then
        PREFIX=${VERSION%-SNAPSHOT}
        TIMESTAMP=`grep -oP '(?<=timestamp>)[^<]+' < $REPO/maven-metadata-$SERVER_ID.xml`
        BUILD_NUM=`grep -oP '(?<=buildNumber>)[^<]+' < $REPO/maven-metadata-$SERVER_ID.xml`
        echo $TAG=$PREFIX-$TIMESTAMP-$BUILD_NUM$SUFFIX
    else
        echo $TAG=$VERSION$SUFFIX
    fi
}

print_ver $1 $2 $3 $4 $5
