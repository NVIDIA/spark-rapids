---
layout: page
title: Databricks
nav_order: 3
parent: Getting-Started
---

# Getting started with RAPIDS Accelerator on Databricks
This guide will run through how to set up the RAPIDS Accelerator for Apache Spark 3.x on Databricks.
At the end of this guide, the reader will be able to run a sample Apache Spark application that runs
on NVIDIA GPUs on Databricks.

## Prerequisites
    * Apache Spark 3.x running in Databricks Runtime 10.4 ML or 11.3 ML with GPU
    * AWS: 10.4 LTS ML (GPU, Scala 2.12, Spark 3.2.1) or 11.3 LTS ML (GPU, Scala 2.12, Spark 3.3.0)
    * Azure: 10.4 LTS ML (GPU, Scala 2.12, Spark 3.2.1) or 11.3 LTS ML (GPU, Scala 2.12, Spark 3.3.0)

Databricks may do [maintenance
releases](https://docs.databricks.com/release-notes/runtime/maintenance-updates.html) for their
runtimes which may impact the behavior of the plugin. 

The number of GPUs per node dictates the number of Spark executors that can run in that node.

## Limitations

1. When selecting GPU nodes, Databricks UI requires the driver node to be a GPU node. However you 
   can use Databricks API to create a cluster with CPU driver node.
   Outside of Databricks the plugin can operate with the driver as a CPU node and workers as GPU nodes.

2. Cannot spin off multiple executors on a multi-GPU node. 

   Even though it is possible to set `spark.executor.resource.gpu.amount=1` in the in Spark 
   Configuration tab, Databricks overrides this to `spark.executor.resource.gpu.amount=N` 
   (where N is the number of GPUs per node). This will result in failed executors when starting the
   cluster.

3. Parquet rebase mode is set to "LEGACY" by default.

   The following Spark configurations are set to `LEGACY` by default on Databricks:
   
   ```
   spark.sql.legacy.parquet.datetimeRebaseModeInWrite
   spark.sql.legacy.parquet.int96RebaseModeInWrite
   ```
   
   These settings will cause a CPU fallback for Parquet writes involving dates and timestamps.
   If you do not need `LEGACY` write semantics, set these configs to `EXCEPTION` which is
   the default value in Apache Spark 3.0 and higher.

4. Databricks makes changes to the runtime without notification.

    Databricks makes changes to existing runtimes, applying patches, without notification.
    [Issue-3098](https://github.com/NVIDIA/spark-rapids/issues/3098) is one example of this.  We run
    regular integration tests on the Databricks environment to catch these issues and fix them once
    detected.
   
5. In Databricks 11.3, an incorrect result is returned for window frames defined by a range in case 
   of DecimalTypes with precision greater than 38. There is a bug filed in Apache Spark for it 
   [here](https://issues.apache.org/jira/browse/SPARK-41793), whereas when using the plugin the 
   correct result will be returned.
   
## Start a Databricks Cluster
Before creating the cluster, we will need to create an [initialization script](https://docs.databricks.com/clusters/init-scripts.html) for the 
cluster to install the RAPIDS jars. Databricks recommends storing all cluster-scoped init scripts using workspace files. 
Each user has a Home directory configured under the /Users directory in the workspace. 
Navigate to your home directory in the UI and select **Create** > **File** from the menu, 
create an `init.sh` scripts with contents:   
   ```bash
   #!/bin/bash
   sudo wget -O /databricks/jars/rapids-4-spark_2.12-23.04.0.jar https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.04.0/rapids-4-spark_2.12-23.04.0.jar
   ```
Then create a Databricks cluster by going to "Compute", then clicking `+ Create compute`.  Ensure the
cluster meets the prerequisites above by configuring it as follows:
1. Select the Databricks Runtime Version from one of the supported runtimes specified in the
   Prerequisites section.
2. Choose the number of workers that matches the number of GPUs you want to use.
3. Select a worker type. On AWS, use nodes with 1 GPU each such as `p3.2xlarge` or `g4dn.xlarge`.
   For Azure, choose GPU nodes such as Standard_NC6s_v3. For GCP, choose N1 or A2 instance types with GPUs. 
4. Select the driver type. Generally this can be set to be the same as the worker.
5. Click the “Edit” button, then navigate down to the “Advanced Options” section. Select the “Init Scripts” tab in 
   the advanced options section, and paste the workspace path to the initialization script:`/Users/user@domain/init.sh`, then click “Add”.
   ![Init Script](../img/Databricks/initscript.png)
6. Now select the “Spark” tab, and paste the following config options into the Spark Config section.
   Change the config values based on the workers you choose. See Apache Spark
   [configuration](https://spark.apache.org/docs/latest/configuration.html) and RAPIDS Accelerator
   for Apache Spark [descriptions](../configs.md) for each config.

    The
    [`spark.task.resource.gpu.amount`](https://spark.apache.org/docs/latest/configuration.html#scheduling)
    configuration is defaulted to 1 by Databricks. That means that only 1 task can run on an
    executor with 1 GPU, which is limiting, especially on the reads and writes from Parquet. Set
    this to 1/(number of cores per executor) which will allow multiple tasks to run in parallel just
    like the CPU side. Having the value smaller is fine as well.
    Note: Please remove the `spark.task.resource.gpu.amount` config for a single-node Databricks 
    cluster because Spark local mode does not support GPU scheduling.
   
    ```bash
    spark.plugins com.nvidia.spark.SQLPlugin
    spark.task.resource.gpu.amount 0.1
    spark.rapids.memory.pinnedPool.size 2G
    spark.rapids.sql.concurrentGpuTasks 2
    ```

    ![Spark Config](../img/Databricks/sparkconfig.png)

    If running Pandas UDFs with GPU support from the plugin, at least three additional options
    as below are required. The `spark.python.daemon.module` option is to choose the right daemon module
    of python for Databricks. On Databricks, the python runtime requires different parameters than the
    Spark one, so a dedicated python deamon module `rapids.daemon_databricks` is created and should
    be specified here. Set the config
    [`spark.rapids.sql.python.gpu.enabled`](../configs.md#sql.python.gpu.enabled) to `true` to
    enable GPU support for python. Add the path of the plugin jar (supposing it is placed under
    `/databricks/jars/`) to the `spark.executorEnv.PYTHONPATH` option. For more details please go to
    [GPU Scheduling For Pandas UDF](../additional-functionality/rapids-udfs.md#gpu-support-for-pandas-udf)

    ```bash
    spark.rapids.sql.python.gpu.enabled true
    spark.python.daemon.module rapids.daemon_databricks
    spark.executorEnv.PYTHONPATH /databricks/jars/rapids-4-spark_2.12-23.04.1.jar:/databricks/spark/python
    ```
   Note that since python memory pool require installing the cudf library, so you need to install cudf library in 
   each worker nodes `pip install cudf-cu11 --extra-index-url=https://pypi.nvidia.com` or disable python memory pool
   `spark.rapids.python.memory.gpu.pooling.enabled=false`.
   
7. Click `Create Cluster`, it is now enabled for GPU-accelerated Spark.

## RAPIDS Accelerator for Apache Spark Docker container for Databricks

Github repo [spark-rapids-container](https://github.com/NVIDIA/spark-rapids-container) provides the 
Dockerfile and scripts to build custom Docker containers with RAPIDS Accelerator for Apache Spark.

Please refer to [Databricks doc](https://github.com/NVIDIA/spark-rapids-container/tree/main/Databricks) 
for more details.

## Import the GPU Mortgage Example Notebook
Import the example [notebook](../demo/Databricks/Mortgage-ETL-db.ipynb) from the repo into your
workspace, then open the notebook. Please find this [instruction](https://github.com/NVIDIA/spark-rapids-examples/blob/main/docs/get-started/xgboost-examples/dataset/mortgage.md)
to download the dataset.

```bash
%sh

USER_ID=<your_user_id>
 
wget http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000.tgz -P /Users/${USER_ID}/
 
mkdir -p /dbfs/FileStore/tables/mortgage
mkdir -p /dbfs/FileStore/tables/mortgage_parquet_gpu/perf
mkdir /dbfs/FileStore/tables/mortgage_parquet_gpu/acq
mkdir /dbfs/FileStore/tables/mortgage_parquet_gpu/output
 
tar xfvz /Users/${USER_ID}/mortgage_2000.tgz --directory /dbfs/FileStore/tables/mortgage
```

In Cell 3, update the data paths if necessary. The example notebook merges the columns and prepares
the data for XGBoost training. The temp and final output results are written back to the dbfs.

```bash
orig_perf_path='dbfs:///FileStore/tables/mortgage/perf/*'
orig_acq_path='dbfs:///FileStore/tables/mortgage/acq/*'
tmp_perf_path='dbfs:///FileStore/tables/mortgage_parquet_gpu/perf/'
tmp_acq_path='dbfs:///FileStore/tables/mortgage_parquet_gpu/acq/'
output_path='dbfs:///FileStore/tables/mortgage_parquet_gpu/output/'
```
Run the notebook by clicking “Run All”. 

## Hints
Spark logs in Databricks are removed upon cluster shutdown. It is possible to save logs in a cloud
storage location using Databricks [cluster log
delivery](https://docs.databricks.com/clusters/configure.html#cluster-log-delivery-1).  Enable this
option before starting the cluster to capture the logs.

