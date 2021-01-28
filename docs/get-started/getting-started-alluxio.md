---
layout: page
title: Alluxio
nav_order: 5
parent: Getting-Started
---
# Get Started with RAPIDS with Alluxio

Alluxio is a data orchestration platform brings your data closer to compute across clusters,
regions, clouds, and countries. This guide will go through how to set up the RAPIDS Accelerator
for Apache Spark 3.0 with Alluxio and on premise cluster.

## Prerequisites

Assuming that user has successfully run the RAPIDS Accelerator with on premise cluster according
to [this doc](getting-started-on-prem.md)

This guide takes Yarn cluster as an example and assumes that there are 2 datanodes and 1 namenode in Yarn cluster, and their hostnames are respectively

``` json
namenode_hostname
datanode1_hostname
datanode2_hostname
```

## Alluxio setup

It is recommending to deploy Alluxio workers in each datanode and Alluxio master in namenode.

1. download the latest Alluxio version alluxio-${LATEST}-bin.tar.gz from [alluxio website](https://www.alluxio.io/download/)
2. copy `alluxio-${LATEST}-bin.tar.gz` to all the datanodes and namenode
3. extract `alluxio-${LATEST}-bin.tar.gz` to `/opt/alluxio-${LATEST}` in datanode and namenode
   ``` shell
   tar xvf alluxio-${LATEST}-bin.tar.gz -C /opt
   ```
4. configure alluxio
    - Alluxio master configuration

   add below recommending configuration in `/opt/alluxio-${LATEST}/conf/alluxio-site.properties`
   ``` xml
   alluxio.master.hostname=namenode_hostname

   ######### worker properties #########
   ## configure async cache manager
   alluxio.worker.network.async.cache.manager.threads.max=64
   alluxio.worker.network.async.cache.manager.queue.max=51200
   alluxio.worker.tieredstore.levels=1
   alluxio.worker.tieredstore.level0.alias=SSD
   alluxio.worker.tieredstore.level0.dirs.mediumtype=SSD
   alluxio.worker.tieredstore.level0.dirs.path=/YOUR_CACHE_DIR
   alluxio.worker.tieredstore.level0.dirs.quota=/YOUR_CACHE_QUOTA

   # enable domain socket
   alluxio.worker.data.server.domain.socket.address=/YOUR_DOMAIN_SOCKET_PATH
   alluxio.worker.data.server.domain.socket.as.uuid=true
   ####################################

   ######### User properties #########
   ## enable circuit
   alluxio.user.short.circuit.preferred=true
   alluxio.user.short.circuit.enabled=true
   ## use LocalFirstPolicy
   alluxio.user.ufs.block.read.location.policy=alluxio.client.block.policy.LocalFirstPolicy
   ####################################

   # configure s3
   alluxio.underfs.s3.endpoint=/S3_ENDPOINT
   alluxio.underfs.s3.inherit.acl=false
   alluxio.underfs.s3.default.mode=0755
   alluxio.underfs.s3.disable.dns.buckets=true
   ```

   add Alluxio worker hostnames into `/opt/alluxio-${LATEST}/conf/workers`

   ``` json
   datanode1_hostname
   datanode2_hostname
   ```

   - copy configuration from Alluxio master to Alluxio workers
   ``` shell
   /opt/alluxio-${LATEST}/bin/alluxio copyDir /opt/alluxio-${LATEST}/conf
   ```
   - Alluxio worker configuration
   add worker and user hostname respectively in the Alluxio configuration of each Alluxio worker
   ``` xml
   alluxio.worker.hostname=datanodeX_hostname
   alluxio.user.hostname=datanodeX_hostname
   ```
5. mount ufs
   
   ``` bash
   /opt/alluxio-${LATEST}/bin/alluxio fs mount \
      --option aws.accessKeyId=<AWS_ACCESS_KEY_ID> \
      --option aws.secretKey=<AWS_SECRET_KEY_ID> \
      alluxio://namenode:19998/s3 s3a://<S3_BUCKET>/<S3_DIRECTORY>
   ``` 
6. start Alluxio cluster
   login to Alluxio master node, and run
   ``` bash
   /opt/alluxio-${LATEST}/bin/alluxio-start.sh all
   ```

## RAPIDS Configuration

There are two ways to leverage Alluxio in RAPIDS.

1. explicitly specify alluxio path

This may require user to change code.

eg. change below code

``` scala
val df = spark.read.parquet("s3a://<S3_BUCKET>/<S3_DIRECTORY>/foo.parquet")
```

to

``` scala
val df = spark.read.parquet("alluxio://namenode_hostname:19998/s3/foo.parquet")
```

2. transparently replace in RAPIDS

RAPIDS has added a configuration `spark.rapids.alluxio.pathsToReplace` which can allow RAPIDS to replace the input file paths to alluxio paths transparently at runtime. So there is no any code change for users.

eg, at startup
``` shell
--conf spark.rapids.alluxio.pathsToReplace="s3:/foo->alluxio://datanode_hostname:19998/foo,gs:/bar->alluxio://datanode_hostname:19998/bar"
```

this configuration allows RAPIDS to replace any file paths prefixed `s3:/foo` to `alluxio://datanode_hostname:19998/foo` and `gs:/bar` to `alluxio://datanode_hostname:19998/bar`

3. submit an application
   First, user needs to copy alluxio client jar to spark jars
   ``` shell
   cp /opt/alluxio-${LATEST}/client/alluxio-${LATEST}-client.jar ${SPARK_HOME}/jars
   ```
   
   ``` shell
   ${SPARK_HOME}/bin/spark-submit \
      ...                          \
      --conf spark.rapids.alluxio.pathsToReplace="REPLACEMENT_RULES" \
      --conf spark.executor.extraJavaOptions="-Dalluxio.conf.dir=/opt/alluxio-2.4.1/conf" \
   ```
