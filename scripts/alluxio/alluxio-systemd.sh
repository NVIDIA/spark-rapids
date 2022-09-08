#!/bin/bash

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