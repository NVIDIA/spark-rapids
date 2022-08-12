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

# This script generates a YAML file containing system properties. The YAML
# file is used by the AutoTuner for recommending Spark RAPIDS configurations.
# Usage: ./discoveryScript.sh [num-workers] [output-file]

function usage() {
  echo "Usage: ./discoveryScript.sh [num-workers] [output-file]"
}

if [ "$#" -ne 2 ]; then
  echo "Illegal number of parameters"
  usage
  exit 1
fi

OS=$(uname -s)
NUM_WORKERS=$1
OUTPUT_FILE=$2

function echo_warn {
  CLEAR='\033[0m'
  YELLOW='\033[0;33m'

  echo -e "${YELLOW}[WARN] $1${CLEAR}\n";
}

function get_linux_properties() {
  local memInKb_=$(cat /proc/meminfo | grep MemTotal)
  local memInKb="$(echo $memInKb_ | cut -d' ' -f2)"
  memInGb=$((memInKb / (1024 * 1024)))
  memInGb="${memInGb}gb"

  local numCores_=$(lscpu | grep "CPU(s)")
  numCores="$(echo $numCores_ | cut -d' ' -f2)"
  cpuArch=$(arch)

  timeZone=$(cat /etc/timezone)
}

function get_macos_properties() {
  local memInB=$(sysctl -n hw.memsize)
  memInGb=$((memInB / (1024 * 1024 * 1024)))
  memInGb="${memInGb}gb"

  numCores=$(sysctl -n hw.ncpu)
  cpuArch=$(arch)

  timeZone=$(readlink /etc/localtime | sed 's#/var/db/timezone/zoneinfo/##g')
}

function get_gpu_properties() {
  if ! command -v nvidia-smi &> /dev/null; then
      echo_warn "nvidia-smi could not be found. Cannot get gpu properties".
      return
  fi
  
  local gpuInfo=$(nvidia-smi --query-gpu=count,name,memory.total --format=csv,noheader)

  if [ $? -ne 0 ]; then
      echo_warn "nvidia-smi did not exit successfully. Cannot get gpu properties".
      return
  fi
  
  IFS=',' read -ra gpuInfoArr <<< "$gpuInfo"
  gpuCount=${gpuInfoArr[0]}
  gpuName=${gpuInfoArr[1]}
  local gpuMemoryInMb="$(echo ${gpuInfoArr[2]} | cut -d' ' -f1)"
  gpuMemoryInGb=$((gpuMemoryInMb / 1024))
  gpuMemoryInGb="${gpuMemoryInGb}gb"
}

function get_disk_space() {
  local freeDiskSpaceInKb=$(df -Pk . | sed 1d | grep -v used | awk '{ print $4 "\t" }')
  freeDiskSpaceInGb=$((freeDiskSpaceInKb / (1024 * 1024)))
  freeDiskSpaceInGb="${freeDiskSpaceInGb}gb"
}

function read_system_properties() {
  if [[ $OS == Linux ]];then
      get_linux_properties
  elif [[ $OS == Darwin ]];then
      get_macos_properties
  else
      echo_warn "Unsupported OS: $OS. Cannot get cpu and memory properties."
  fi
  get_gpu_properties
  get_disk_space
}

function write_system_properties() {
  cat > "$OUTPUT_FILE" << EOF
system:
  num_cores: $numCores
  cpu_arch: $cpuArch
  memory: $memInGb
  free_disk_space: $freeDiskSpaceInGb
  time_zone: $timeZone
  num_workers: $NUM_WORKERS
gpu:
  count: $gpuCount
  memory: $gpuMemoryInMb
  name: $gpuName
EOF

  echo "YAML file generated at $OUTPUT_FILE"
  echo "Contents - "
  cat "$OUTPUT_FILE"
}

read_system_properties
write_system_properties
