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
    type=$1
    repo=$2
    prefix=$3
    suffix=$4
    timestamp=`grep -oP '(?<=timestamp>)[^<]+' < $repo/maven-metadata-snapshots.xml`
    buildnumber=`grep -oP '(?<=buildNumber>)[^<]+' < $repo/maven-metadata-snapshots.xml`
    echo $type: $prefix$timestamp-$buildnumber$suffix
}

print_ver $1 $2 $3 $4

