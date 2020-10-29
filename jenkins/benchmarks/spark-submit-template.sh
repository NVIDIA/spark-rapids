$SPARK_HOME/bin/spark-submit \
  --master $K8S_MASTER \
  --deploy-mode cluster \
  --name benchmark-runner \
  --driver-memory 48G \
  --num-executors 8 \
  --executor-memory 90G \
  --executor-cores 8 \
  --conf spark.task.cpus=8 \
  --conf spark.task.resource.gpu.amount=1 \
  --conf spark.locality.wait=0 \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.memory.gpu.pooling.enabled=false \
  --conf spark.rapids.memory.pinnedPool.size=8g \
  --conf spark.rapids.sql.batchSizeRows=10000000 \
  --conf spark.rapids.sql.concurrentGpuTasks=2 \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.rapids.sql.explain=ALL \
  --conf spark.rapids.sql.hasNans=false \
  --conf spark.rapids.sql.incompatibleOps.enabled=true \
  --conf spark.rapids.sql.variableFloatAgg.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.files.maxPartitionBytes=2g \
  --conf spark.kubernetes.driverEnv.LIBCUDF_INCLUDE_DIR=/tmp \
  --conf spark.kubernetes.namespace=default \
  --conf spark.kubernetes.container.image=quay.io/nvidia/spark:k8s-cicd-benchmark \
  --conf spark.kubernetes.container.image.pullSecrets=quayio-userpass \
  --conf spark.kubernetes.container.image.pullPolicy=Always \
  --conf spark.kubernetes.driver.volumes.hostPath.raid.options.path=/raid \
  --conf spark.kubernetes.driver.volumes.hostPath.raid.mount.path=/raid \
  --conf spark.kubernetes.executor.volumes.hostPath.raid.options.path=/raid \
  --conf spark.kubernetes.executor.volumes.hostPath.raid.mount.path=/raid \
  --conf spark.executor.resource.gpu.vendor=nvidia.com \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.executor.resource.gpu.discoveryScript=/opt/spark/examples/src/main/scripts/getGpusResources.sh \
  --conf spark.executorEnv.LIBCUDF_INCLUDE_DIR=/tmp \
  --conf spark.hadoop.fs.s3a.endpoint=$S3_ENDPOINT_URL \
  --conf spark.hadoop.fs.s3a.access.key=$S3_USERNAME \
  --conf spark.hadoop.fs.s3a.secret.key=$S3_PASSWORD \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \