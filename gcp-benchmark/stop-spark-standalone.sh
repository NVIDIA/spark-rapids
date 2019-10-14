#!/bin/bash
set -e
if [ "$#" -ne 2 ]; then
  echo "Invalid number of arguments"
  echo "Usage: "$0" <cluster-name> <cluster-size>"
  exit 1
fi

if [ "$1" == "-h" ] ; then
    echo "Usage: "$0" <cluster-name>"
    echo "Example: "$0" my_dataproc_rapids_cluster"
    exit 0
fi

CLUSTER_NAME="$1"
MASTER=$CLUSTER_NAME-m
CLUSTER_SIZE="$2"
WORKERS=$(seq -f "$CLUSTER_NAME-w-%g" 0 $(($CLUSTER_SIZE-1)))

gcloud compute scp stop-master.sh $USER@$MASTER:$HOME
for worker in $WORKERS;do gcloud compute scp stop-slave.sh $USER@$worker:$HOME;done

gcloud compute ssh $MASTER -- 'chmod +x stop-master.sh && ./stop-master.sh'
for worker in $WORKERS;do gcloud compute ssh $worker -- 'chmod +x stop-slave.sh && ./stop-slave.sh';done