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

set -ex

nvidia-smi

. jenkins/version-def.sh

# Run the unit tests for other Spark versions
mvn -U -B $MVN_URM_MIRROR -Pspark303tests,snapshot-shims test -Dpytest.TEST_TAGS='' -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B $MVN_URM_MIRROR -Pspark304tests,snapshot-shims test -Dpytest.TEST_TAGS='' -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B $MVN_URM_MIRROR -Pspark312tests,snapshot-shims test -Dpytest.TEST_TAGS='' -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B $MVN_URM_MIRROR -Pspark313tests,snapshot-shims test -Dpytest.TEST_TAGS='' -Dcuda.version=$CUDA_CLASSIFIER
