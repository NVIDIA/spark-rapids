#!/bin/bash

mkdir -p /databricks/driver/logs

TTYD_BIN_FILE=/databricks/spark/scripts/ttyd/ttyd

if [[ -f "$TTYD_BIN_FILE" ]]; then
  $TTYD_BIN_FILE -p 7681 -d 7 -P 30 -t disableReconnect=true -t disableLeaveAlert=true bash --rcfile /databricks/spark/scripts/ttyd/webTerminalBashrc >> /databricks/driver/logs/ttyd_logs 2>&1 &
  echo $! > /var/run/ttyd-daemon.pid
else
  echo "could not find ttyd binary at /databricks/spark/scripts/ttyd/ttyd"
fi
