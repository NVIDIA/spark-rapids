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

output_file=/tmp/system_props.yaml
num_workers=$1
worker_ip=$2

echo "Fetching system information from worker - $worker_ip"
scp -q ./discoveryScript.sh "$worker_ip":/tmp
ssh "$worker_ip" "bash /tmp/discoveryScript.sh $num_workers $output_file"
scp -q "$worker_ip":$output_file ./
echo -e "\nYAML file copied to driver"

