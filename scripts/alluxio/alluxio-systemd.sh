#!/bin/bash
#
# Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
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

# alluxio home
readonly ALLUXIO_HOME=${ALLUXIO_HOME:-"/opt/alluxio-2.8.0"}

# The user who will run Alluxio, default is ubuntu
readonly RUN_USER=${RUN_USER:-"ubuntu"}

# create systemd service for master and start the service
set_systemd_and_start_master() {
  # create service file
  cat > "/etc/systemd/system/alluxio-master.service" <<EOF
[Unit]
Description=Alluxio Master
After=default.target
[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${ALLUXIO_HOME}
ExecStart=${ALLUXIO_HOME}/bin/launch-process master -c
Restart=on-failure
[Install]
WantedBy=multi-user.target
EOF

  # enable service
  systemctl enable alluxio-master

  # start service
  systemctl restart alluxio-master
}

# create systemd service for worker and start the service
set_systemd_and_start_worker() {

  # create service file
  cat > "/etc/systemd/system/alluxio-worker.service" <<EOF
[Unit]
Description=Alluxio Worker
After=default.target
[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${ALLUXIO_HOME}
ExecStart=${ALLUXIO_HOME}/bin/launch-process worker -c
Restart=on-failure
[Install]
WantedBy=multi-user.target
EOF

  # enable service
  systemctl enable alluxio-worker
  
  # start service
  systemctl restart alluxio-worker

}

if [ "$1" = "master" ]; then
  set_systemd_and_start_master
elif [ "$1" = "worker" ]; then
  set_systemd_and_start_worker
else
  echo "Please specify the role: master or worker. "
  echo "Usage: sudo sh -c 'ALLUXIO_HOME=/opt/alluxio-2.8.0 RUN_USER=ubuntu ./alluxio-systemd.sh [ master | worker ]'"
  echo "For example: sudo sh -c 'ALLUXIO_HOME=/opt/alluxio-2.8.0 RUN_USER=ubuntu ./alluxio-systemd.sh master'"
  echo "For example: sudo sh -c 'ALLUXIO_HOME=/opt/alluxio-2.8.0 RUN_USER=ubuntu ./alluxio-systemd.sh worker'"
fi