#!/bin/bash
# Copyright (c) 2022, NVIDIA CORPORATION.
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


mkdir -p /databricks/driver/logs

TTYD_BIN_FILE=/databricks/spark/scripts/ttyd/ttyd

if [[ -f "$TTYD_BIN_FILE" ]]; then
  $TTYD_BIN_FILE -p 7681 -d 7 -P 30 -t disableReconnect=true -t disableLeaveAlert=true bash --rcfile /databricks/spark/scripts/ttyd/webTerminalBashrc >> /databricks/driver/logs/ttyd_logs 2>&1 &
  echo $! > /var/run/ttyd-daemon.pid
else
  echo "could not find ttyd binary at /databricks/spark/scripts/ttyd/ttyd"
fi
