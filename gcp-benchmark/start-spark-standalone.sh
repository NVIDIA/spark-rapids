#!/bin/bash

set -e
if [ "$#" -ne 3 ]; then
  echo "Invalid number of arguments"
  echo "Usage: "$0" <cluster-name> <gcs-bucket-name> <name-of-spark-tgz>"
  exit 1
fi

if [ "$1" == "-h" ] ; then
    echo "Usage: "$0" <cluster-name> <gcs-bucket-name> <name-of-spark-tgz>"
    echo "Example: "$0" my_dataproc_rapids_cluster my_dataproc_rapids_bucket spark-3.0.0-SNAPSHOT.tar.gz "
    exit 0
fi

source cluster-vars.env "$1" "$2" "$3"

gcloud compute ssh --zone $ZONE $MASTER -- "gsutil cp gs://$ETL_BUCKET/$USER/$SPARK_TGZ $HOME && mv $SPARK_TGZ spark.tgz"
for worker in $WORKERS;do gcloud compute ssh --zone $ZONE $worker -- "gsutil cp gs://$ETL_BUCKET/$USER/$SPARK_TGZ $HOME && mv $SPARK_TGZ spark.tgz"; done

gcloud compute scp --zone $ZONE startup-spark-master.sh $USER@$MASTER:$HOME_DIR/
for worker in $WORKERS;do gcloud compute scp --zone $ZONE startup-spark-slaves.sh $USER@$worker:$HOME_DIR/; done

gcloud compute ssh --zone $ZONE $MASTER -- 'chmod +x startup-spark-master.sh && ./startup-spark-master.sh'

for worker in $WORKERS;do gcloud compute ssh --zone $ZONE $worker -- "chmod +x startup-spark-slaves.sh && ./startup-spark-slaves.sh spark://$MASTER.c.rapids-spark.internal:7077"; done

#This allows any spark shell or spark submit sessions to have access to cudf and rapids spark plugin jars during executor init
echo $SHIPPED_JARS
for jar in $SHIPPED_JARS
do
  gcloud compute scp --zone $ZONE $jar $USER@$MASTER:$HOME_DIR/spark*/jars/
  for worker in $WORKERS;do gcloud compute scp --zone $ZONE $jar $USER@$worker:$HOME_DIR/spark*/jars/; done
done

gcloud compute ssh --zone $ZONE $MASTER -- "gsutil cp gs://$ETL_BUCKET/gcs-connector*.jar $HOME"
