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

# This script copies and executes discoveryScript.sh to the worker and copies
# back the generated YAML file. The YAML file is used by the AutoTuner for
# recommending Spark RAPIDS configurations.
# Usage: ./getWorkerInfo.sh [num-workers] [worker-ip]

OUTPUT_FILE=/tmp/system_props.yaml
NUM_WORKERS=$1
WORKER_IP=$2

echo "Fetching system information from worker - $WORKER_IP"
scp -q ./discoveryScript.sh "$WORKER_IP":/tmp
ssh "$WORKER_IP" "bash /tmp/discoveryScript.sh $NUM_WORKERS $OUTPUT_FILE"
scp -q "$WORKER_IP":$OUTPUT_FILE ./
echo -e "\nYAML file copied to driver"
