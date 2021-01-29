---
layout: page
title: Alluxio
nav_order: 5
parent: Getting-Started
---
# Get Started with RAPIDS with Alluxio

RAPIDS plugin can remarkably accelerate the computing part of the whole SQL query by
leveraging GPUs, but it’s hard to accelerate the data reading process when the data
is in the cloud file system because of network overhead. Alluxio is an open source
data orchestration platform that brings your data closer to compute across clusters,
regions, clouds, and countries for reducing the network overhead. This guide will
go through how to set up the RAPIDS Accelerator for Apache Spark with Alluxio with
a on premise cluster.

## Prerequisites

Assuming that user has successfully setup and ran the RAPIDS Accelerator with on premise
cluster according to [this doc](getting-started-on-prem.md)

This guide will deploy Alluxio on Yarn cluster with 2 NodeManagers and 1 ResourceManager,
and describe the instructions to configure Swiftstack as Alluxio’s under storage system.

Let's assuem the hostnames of Yarn cluster are respectively

``` json
RM_hostname
NM_hostname_1
NM_hostname_2
```

## Alluxio setup

It is recommending to deploy Alluxio workers in each NodeManager and Alluxio master in
ResourceManager.

1. Download the latest Alluxio version (2.4.1) **alluxio-${LATEST}-bin.tar.gz** from [alluxio website](https://www.alluxio.io/download/).
2. Copy `alluxio-${LATEST}-bin.tar.gz` to all the NodeManagers and ResourceManager.
3. Extract `alluxio-${LATEST}-bin.tar.gz` to the directory specified by **ALLUXIO_HOME**
   in NodeManager and ResourceManager.

   ``` shell
   # Let's assume to extract alluxio to /opt
   mkdir -p /opt
   tar xvf alluxio-${LATEST}-bin.tar.gz -C /opt
   export ALLUXIO_HOME=/opt/alluxio-${LATEST}
   ```

4. Configure alluxio.
   - Alluxio master configuration

   Add below recommending configuration in `${ALLUXIO_HOME}/conf/alluxio-site.properties`.

   ``` xml
   alluxio.master.hostname=RM_hostname

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

   # Running Alluxio Locally with S3
   alluxio.underfs.s3.endpoint=/S3_ENDPOINT
   alluxio.underfs.s3.inherit.acl=false
   alluxio.underfs.s3.default.mode=0755
   alluxio.underfs.s3.disable.dns.buckets=true
   ```

   For the explanation of each configuration, please refer to [Alluxio Configuration](https://docs.alluxio.io/os/user/stable/en/reference/Properties-List.html) and [Amazon AWS S3](https://docs.alluxio.io/os/user/stable/en/ufs/S3.html).

   Add Alluxio worker hostnames into `${ALLUXIO_HOME}/conf/workers`.

   ``` json
   NM_hostname_1
   NM_hostname_2
   ```

   - Copy configuration from Alluxio master to Alluxio workers.

   ``` shell
   ${ALLUXIO_HOME}/bin/alluxio copyDir ${ALLUXIO_HOME}/conf
   ```
   - Alluxio worker configuration.

   Add worker and user hostname respectively in the Alluxio configuration of each Alluxio worker.

   ``` xml
   alluxio.worker.hostname=NM_hostname_X
   alluxio.user.hostname=NM_hostname_X
   ```
5. Mount an existing S3 bucket to Alluxio.

   ``` bash
   ${ALLUXIO_HOME}/bin/alluxio fs mount \
      --option aws.accessKeyId=<AWS_ACCESS_KEY_ID> \
      --option aws.secretKey=<AWS_SECRET_KEY_ID> \
      alluxio://RM_hostname:19998/s3 s3a://<S3_BUCKET>/<S3_DIRECTORY>
   ```

   for other filesystem, please refer to [this site](https://www.alluxio.io/)

6. Start Alluxio cluster.

   Login Alluxio master node, and run

   ``` bash
   ${ALLUXIO_HOME}/bin/alluxio-start.sh all
   ```

## RAPIDS Configuration

There are two ways to leverage Alluxio in RAPIDS.

1. Explicitly specify alluxio path.

   This may require user to change code.

   Eg. Change below code

   ``` scala
   val df = spark.read.parquet("s3a://<S3_BUCKET>/<S3_DIRECTORY>/foo.parquet")
   ```

   to

   ``` scala
   val df = spark.read.parquet("alluxio://RM_hostname:19998/s3/foo.parquet")
   ```

2. Transparently replace in RAPIDS.

   RAPIDS has added a configuration `spark.rapids.alluxio.pathsToReplace` which can allow RAPIDS
   to replace the input file paths to alluxio paths transparently at runtime. So there is no any
   code change for users.

   Eg, at startup
   ``` shell
   --conf spark.rapids.alluxio.pathsToReplace="s3:/foo->alluxio://RM_hostname:19998/foo,gs:/bar->alluxio://RM_hostname:19998/bar"
   ```

   This configuration allows RAPIDS to replace any file paths prefixed `s3:/foo` to
   `alluxio://RM_hostname:19998/foo` and `gs:/bar` to `alluxio://RM_hostname:19998/bar`

3. Submit an application.

   Spark driver and tasks will parse `alluxio://` schema and access Alluxio cluster by
   `alluxio-${LATEST}-client.jar` which must be distributed across the all nodes where Spark drivers
   or executors are running.

   The Alluxio client jar must be in the classpath of all Spark drivers and executors in order for
   Spark applications to access Alluxio.

   We can specify it in the configuration of `spark.driver.extraClassPath` and
   `spark.executor.extraClassPath`, but the simplest way is copy `alluxio-${LATEST}-client.jar`
   into spark jars directory.

   ``` shell
   cp ${ALLUXIO_HOME}/client/alluxio-${LATEST}-client.jar ${SPARK_HOME}/jars/
   ```

   ``` shell
   ${SPARK_HOME}/bin/spark-submit \
      ...                          \
      --conf spark.rapids.alluxio.pathsToReplace="REPLACEMENT_RULES" \
      --conf spark.executor.extraJavaOptions="-Dalluxio.conf.dir=${ALLUXIO_HOME}/conf" \
   ```

## Alluxio troubleshoot
This section will give some links about how to configure, tune Alluxio and some troubleshooting.
- [Quick Start Guide](https://docs.alluxio.io/os/user/stable/en/overview/Getting-Started.html)
- [Amazon S3 as Alluxio’s under storage system](https://docs.alluxio.io/os/user/stable/en/ufs/S3.html)
- [Alluxio metrics](https://docs.alluxio.io/os/user/stable/en/reference/Metrics-List.html)
- [Alluxio configuration](https://docs.alluxio.io/os/user/stable/en/reference/Properties-List.html)
- [Running Spark on Alluxio](https://docs.alluxio.io/os/user/stable/en/compute/Spark.html)
- [Performance Tuning](https://docs.alluxio.io/ee/user/stable/en/operation/Performance-Tuning.html)
