#!/bin/bash

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
