#!/bin/bash
#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
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
# Argument(s):
#   SPARK_VER: spark version. e.g. 3.3.0
#

set -e

if [[ `echo -e "$1\n3.3.0" | sort -V | head -n 1` == "3.3.0" ]]; then # spark version >= 3.3.0
    BIN_HADOOP_VER="bin-hadoop3"
    BIN_HADOOP_URL="to be defined"
else
    BIN_HADOOP_VER="bin-hadoop3.2"
    BIN_HADOOP_URL="to be defined"
fi
