#!/bin/bash
set -e
if [ "$#" -ne 3 ]; then
  echo "Invalid number of arguments"
  echo "Usage: "$0" <cluster-name> <gcs-bucket-name> <local_path-to-spark-tgz>"
  exit 1
fi

if [ "$1" == "-h" ] ; then
    echo "Usage: "$0" <cluster-name> <gcs-bucket-name> <local_path-to-spark-tgz>"
    echo "Example: "$0" my_big_small_cluster my_big_small_bucket spark-3.0.0-SNAPSHOT.tar.gz "
    exit 0
fi

source cluster-vars.env "$1" "$2" "$3"

gsutil cp install-opengl.sh gs://$ETL_BUCKET/$USER/
gsutil cp install-cuda10.0.sh gs://$ETL_BUCKET/$USER/
gsutil cp $SPARK_TGZ gs://$ETL_BUCKET/$USER/
gcloud beta dataproc clusters create $CLUSTER_NAME  \
--enable-component-gateway \
--image-version "1.4-ubuntu18" \
--initialization-actions gs://$ETL_BUCKET/$USER/install-opengl.sh,gs://$DATAPROC_BUCKET/rapids/rapids.sh,gs://$ETL_BUCKET/$USER/install-cuda10.0.sh \
--initialization-action-timeout 30m \
--master-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS_IN_MASTER \
--master-boot-disk-type pd-ssd \
--master-machine-type n1-standard-32 \
--metadata "JUPYTER_PORT=8888" \
--no-address \
--num-workers $NUM_WORKERS \
--optional-components=ANACONDA,JUPYTER \
--region "us-central1" \
--worker-boot-disk-type pd-ssd \
--worker-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS_IN_WORKER \
--worker-machine-type n1-highmem-32 \
--zone $ZONE

