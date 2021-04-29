---
layout: page
title: Alluxio
nav_order: 6
parent: Getting-Started
---
# Getting Started with RAPIDS and Alluxio

The RAPIDS plugin can remarkably accelerate the computing part of a SQL query by leveraging
GPUs, but it’s hard to accelerate the data reading process when the data is in a cloud
filesystem because of network overhead.

[Alluxio](https://www.alluxio.io/) is an open source data orchestration platform that
brings your data closer to compute across clusters, regions, clouds, and countries for
reducing the network overhead. Compute applications talking to Alluxio can transparently
cache frequently accessed data from multiple sources, especially from remote locations.

This guide will go through how to set up the RAPIDS Accelerator for Apache Spark with
Alluxio in an on-premise cluster.

## Prerequisites

This guide assumes the user has successfully setup and run the RAPIDS Accelerator in an
on-premise cluster according to [this doc](getting-started-on-prem.md).

This guide will go through deployment of Alluxio in a Yarn cluster with 2 NodeManagers and
1 ResourceManager, It will describe how to configure an S3 compatible filesystem as
Alluxio’s underlying storage system.

We may want to put the Alluxio workers on the NodeManagers so they are on the same nodes as
the Spark tasks will run. The Alluxio master can go anywhere, we pick ResourceManager for
convenience.

Let's assume the hostnames are:

``` console
RM_hostname
NM_hostname_1
NM_hostname_2
```

## Alluxio setup

1. Download the latest Alluxio version (2.4.1-1) **alluxio-${LATEST}-bin.tar.gz**
   from [alluxio website](https://www.alluxio.io/download/).
2. Copy `alluxio-${LATEST}-bin.tar.gz` to all the NodeManagers and ResourceManager.
3. Extract `alluxio-${LATEST}-bin.tar.gz` to the directory specified by **ALLUXIO_HOME**
   in the NodeManagers and ResourceManager.

   ``` shell
   # Let's assume we extract alluxio to /opt
   mkdir -p /opt
   tar xvf alluxio-${LATEST}-bin.tar.gz -C /opt
   export ALLUXIO_HOME=/opt/alluxio-${LATEST}
   ```

4. Configure alluxio.
   - Alluxio master configuration

      On the master node, create `${ALLUXIO_HOME}/conf/alluxio-site.properties` configuration
      file from the template.

      ```console
      cp ${ALLUXIO_HOME}/conf/alluxio-site.properties.template ${ALLUXIO_HOME}/conf/alluxio-site.properties
      ```

      Add the recommended configuration below to `${ALLUXIO_HOME}/conf/alluxio-site.properties` .

      ``` xml
      # set the hostname of the single master node
      alluxio.master.hostname=RM_hostname

      ########################### worker properties  ##############################
      # The maximum number of storage tiers in Alluxio. Currently, Alluxio supports 1,
      # 2, or 3 tiers.
      alluxio.worker.tieredstore.levels=1

      # The alias of top storage tier 0. Currently, there are 3 aliases, MEM, SSD, and HDD.
      alluxio.worker.tieredstore.level0.alias=SSD

      # The paths of storage directories in top storage tier 0, delimited by comma.
      # It is suggested to have one storage directory per hardware device for the
      # SSD and HDD tiers. You need to create YOUR_CACHE_DIR first,
      # For example,
      #       export YOUR_CACHE_DIR=/opt/alluxio/cache
      #       mkdir -p $YOUR_CACHE_DIR
      alluxio.worker.tieredstore.level0.dirs.path=/YOUR_CACHE_DIR

      # The quotas for all storage directories in top storage tier 0
      # For example, set the quota to 100G.
      alluxio.worker.tieredstore.level0.dirs.quota=100G

      # The path to the domain socket. Short-circuit reads make use of a UNIX domain
      # socket when this is set (non-empty). This is a special path in the file system
      # that allows the client and the AlluxioWorker to communicate. You will need to
      # set a path to this socket. The AlluxioWorker needs to be able to create the
      # path. If alluxio.worker.data.server.domain.socket.as.uuid is set, the path
      # should be the home directory for the domain socket. The full path for the domain
      # socket with be {path}/{uuid}.
      # For example,
      #      export YOUR_DOMAIN_SOCKET_PATH=/opt/alluxio/domain_socket
      #      mkdir -p YOUR_DOMAIN_SOCKET_PATH
      alluxio.worker.data.server.domain.socket.address=/YOUR_DOMAIN_SOCKET_PATH
      alluxio.worker.data.server.domain.socket.as.uuid=true

      # Configure async cache manager
      # When large amounts of data are expected to be asynchronously cached concurrently,
      # it may be helpful to increase below async cache configuration to handle a higher
      # workload.

      # The number of asynchronous threads used to finish reading partial blocks.
      alluxio.worker.network.async.cache.manager.threads.max=64

      # The maximum number of outstanding async caching requests to cache blocks in each
      # data server.
      alluxio.worker.network.async.cache.manager.queue.max=2000
      ############################################################################

      ########################### Client properties ##############################
      # When short circuit and domain socket both enabled, prefer to use short circuit.
      alluxio.user.short.circuit.preferred=true
      ############################################################################

      # Running Alluxio locally with S3
      # Optionally, to reduce data latency or visit resources which are separated in
      # different AWS regions, specify a regional endpoint to make AWS requests.
      # An endpoint is a URL that is the entry point for a web service.
      #
      # For example, s3.cn-north-1.amazonaws.com.cn is an entry point for the Amazon S3
      # service in beijing region.
      alluxio.underfs.s3.endpoint=<endpoint_url>

      # Optionally, specify to make all S3 requests path style
      alluxio.underfs.s3.disable.dns.buckets=true
      ```

      For more explanations of each configuration, please refer to
      [Alluxio Configuration](https://docs.alluxio.io/os/user/stable/en/reference/Properties-List.html)
      and [Amazon AWS S3](https://docs.alluxio.io/os/user/stable/en/ufs/S3.html).

      Note, when preparing to mount S3 compatible file system to the root of Alluxio namespace, the user
      needs to add below AWS credentials configuration to `${ALLUXIO_HOME}/conf/alluxio-site.properties`
      in Alluxio master node.

      ``` xml
      alluxio.master.mount.table.root.ufs=s3a://<S3_BUCKET>/<S3_DIRECTORY>
      alluxio.master.mount.table.root.option.aws.accessKeyId=<AWS_ACCESS_KEY_ID>
      alluxio.master.mount.table.root.option.aws.secretKey=<AWS_SECRET_ACCESS_KEY>
      ```

      Instead, this guide demonstrates how to mount the S3 compatible file system with AWS credentials
      to any path of Alluxio namespace, and please refer to [RAPIDS Configuration](#rapids-configuration).
      For more explanations of AWS S3 credentials, please refer to
      [Amazon AWS S3 Credentials setup](https://docs.alluxio.io/os/user/stable/en/ufs/S3.html#advanced-setup).

      Note, this guide demonstrates how to deploy Alluxio cluster in a insecure way, for the Alluxio security,
      please refer to [this site](https://docs.alluxio.io/os/user/stable/en/operation/Security.html)

      - Add Alluxio worker hostnames into `${ALLUXIO_HOME}/conf/workers`.

         ``` json
         NM_hostname_1
         NM_hostname_2
         ```

      - Copy configuration from Alluxio master to Alluxio workers.

         ``` shell
         ${ALLUXIO_HOME}/bin/alluxio copyDir ${ALLUXIO_HOME}/conf
         ```

         This command will copy the `conf/` directory to all the workers specified in the `conf/workers` file.
         Once this command succeeds, all the Alluxio nodes will be correctly configured.

   - Alluxio worker configuration.

      After copying configuration to every Alluxio worker from Alluxio master, User
      needs to add below extra configuration for each Alluxio worker.

      ``` xml
      # the hostname of Alluxio worker
      alluxio.worker.hostname=NM_hostname_X
      # The hostname to use for an Alluxio client
      alluxio.user.hostname=NM_hostname_X
      ```

      Note that Alluxio can manage other storage media (e.g. MEM, HDD) in addition to SSD,
      so local data access speed may vary depending on the local storage media. To learn
      more about this topic, please refer to the
      [tiered storage document](https://docs.alluxio.io/os/user/stable/en/core-services/Caching.html#multiple-tier-storage).

5. Mount an existing S3 bucket to Alluxio.

   ``` bash
   ${ALLUXIO_HOME}/bin/alluxio fs mount \
      --option aws.accessKeyId=<AWS_ACCESS_KEY_ID> \
      --option aws.secretKey=<AWS_SECRET_KEY_ID> \
      alluxio://RM_hostname:19998/s3 s3a://<S3_BUCKET>/<S3_DIRECTORY>
   ```

   For other filesystems, please refer to [this site](https://www.alluxio.io/).

6. Start Alluxio cluster.

   Login to Alluxio master node, and run

   ``` bash
   ${ALLUXIO_HOME}/bin/alluxio-start.sh all
   ```

   To verify that Alluxio is running, visit [http://RM_hostname:19999](http://RM_hostname:19999)
   to see the status page of the Alluxio master.

## RAPIDS Configuration

There are two ways to leverage Alluxio in RAPIDS.

1. Explicitly specify the Alluxio path.

   This may require user to change code. For example, change

   ``` scala
   val df = spark.read.parquet("s3a://<S3_BUCKET>/<S3_DIRECTORY>/foo.parquet")
   ```

   to

   ``` scala
   val df = spark.read.parquet("alluxio://RM_hostname:19998/s3/foo.parquet")
   ```

2. Transparently replace in RAPIDS.

   RAPIDS has added a configuration `spark.rapids.alluxio.pathsToReplace` which can allow RAPIDS
   to replace the input file paths to the Alluxio paths transparently at runtime. So there is no
   code change for users.

   Eg, at startup

   ``` shell
   --conf spark.rapids.alluxio.pathsToReplace="s3://foo->alluxio://RM_hostname:19998/foo,gs://bar->alluxio://RM_hostname:19998/bar"
   ```

   This configuration allows RAPIDS to replace any file paths prefixed `s3://foo` with
   `alluxio://RM_hostname:19998/foo` and `gs://bar` with `alluxio://RM_hostname:19998/bar`.

   Note, one side affect of using Alluxio in this way results in the sql function
   **`input_file_name`** printing the `alluxio://` path rather than the original path.
   Below is an example of using input_file_name.

   ``` python
   spark.read.parquet(data_path)
     .filter(f.col('a') > 0)
     .selectExpr('a', 'input_file_name()', 'input_file_block_start()', 'input_file_block_length()')
   ```

3. Submit an application.

   Spark driver and tasks will parse `alluxio://` schema and access Alluxio cluster using
   `alluxio-${LATEST}-client.jar`.

   The Alluxio client jar must be in the classpath of all Spark drivers and executors in order
   for Spark applications to access Alluxio.

   We can specify it in the configuration of `spark.driver.extraClassPath` and
   `spark.executor.extraClassPath`, but the alluxio client jar should be present on the Yarn nodes.

   The other simplest way is copy `alluxio-${LATEST}-client.jar` into spark jars directory.

   ``` shell
   cp ${ALLUXIO_HOME}/client/alluxio-${LATEST}-client.jar ${SPARK_HOME}/jars/
   ```

   ``` shell
   ${SPARK_HOME}/bin/spark-submit \
      ...                          \
      --conf spark.rapids.alluxio.pathsToReplace="REPLACEMENT_RULES" \
      --conf spark.executor.extraJavaOptions="-Dalluxio.conf.dir=${ALLUXIO_HOME}/conf" \
   ```

## Alluxio Troubleshooting

This section will give some links about how to configure, tune Alluxio and some troubleshooting.

- [Quick Start Guide](https://docs.alluxio.io/os/user/stable/en/overview/Getting-Started.html)
- [Amazon S3 as Alluxio’s under storage system](https://docs.alluxio.io/os/user/stable/en/ufs/S3.html)
- [Alluxio metrics](https://docs.alluxio.io/os/user/stable/en/reference/Metrics-List.html)
- [Alluxio configuration](https://docs.alluxio.io/os/user/stable/en/reference/Properties-List.html)
- [Running Spark on Alluxio](https://docs.alluxio.io/os/user/stable/en/compute/Spark.html)
- [Performance Tuning](https://docs.alluxio.io/os/user/stable/en/operation/Performance-Tuning.html )
- [Alluxio troubleshooting](https://docs.alluxio.io/os/user/stable/en/operation/Troubleshooting.html)
