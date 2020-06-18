#!/bin/bash
#
# Copyright (c) 2019-2020, NVIDIA CORPORATION. All rights reserved.
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
mkdir deploy
cd deploy
tar -zxvf spark-rapids-built.tgz
cd spark-rapids
echo "Maven mirror is $MVN_URM_MIRROR"
echo mvn -o -U -B -Pdatabricks clean deploy $MVN_URM_MIRROR
