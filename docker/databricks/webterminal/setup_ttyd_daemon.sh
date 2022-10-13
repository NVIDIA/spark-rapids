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


# source spark-env.sh to load SPARK_LOCAL_DIRS environment variables. spark-env.sh may
# not be available under some situation like DriverLocalTest.
SPARK_ENV_PATH="/databricks/spark/conf/spark-env.sh"
if [[ -f SPARK_ENV_PATH ]]; then
  source SPARK_ENV_PATH
fi

TTYD_DAEMON_FILE=/etc/monit/conf.d/ttyd-daemon-not-active

if [[ -f "$TTYD_DAEMON_FILE" && "$DISABLE_WEB_TERMINAL" != true ]]; then
  ln -s $TTYD_DAEMON_FILE /etc/monit/conf.d/ttyd-daemon.cfg

  if [[ -z $(pgrep -f /databricks/spark/scripts/ttyd/ttyd) ]]; then
    bash /databricks/spark/scripts/ttyd/start_ttyd_daemon.sh
  else
    echo "not starting ttyd because ttyd process exists"
  fi
  /etc/init.d/monit reload
else
  echo "ttyd-daemon.cfg does not exist"
fi
