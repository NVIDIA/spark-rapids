---
layout: page
title: Best Practices on Spark RAPIDS
nav_order: 15
parent: Getting-Started
---

# Best Practices on Spark RAPIDS

This article explains the most common best practices using the RAPIDS Accelerator especially for 
performance tuning and troubleshooting.

## Workload Qualification

By following [Workload Qualification](./getting-started-workload-qualification.md), you can identify
the best candidate Spark applications for the RAPIDS Accelerator and also the feature gaps.

Based on [Qualification tool](../spark-qualification-tool.md)'s output, you can start with the top N
recommended CPU Spark jobs, especially if those jobs are computation heavy jobs (Large Joins, 
HashAggregate, Windowing, Sorting).

After those candidate jobs are run on GPU using the RAPIDS Accelerator, check the Spark driver 
log to find the not-supported messages starting with `!`. Some missing features might be enabled by
turning on some config, but please understand the corresponding limitation detailed in 
[the advanced configuration document](../additional-functionality/advanced_configs.md).
For other unsupported features, please file feature request on 
[spark-rapids github repo](https://github.com/NVIDIA/spark-rapids/issues) with a minimum reproduce
and the not-supported messages if they are non-senstive data.

## Performance Tuning

### 1. Use Hive Parquet or ORC tables instead of Hive Text table as the intermediate tables for CTAS

The suggested change is to add `stored as parquet` for each CTAS(`Create Table AS`) queries.
Alternatively, you can set `hive.default.fileformat=Parquet` to create Parquet files by default.
Refer to this 
[Hive Doc](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-hive.default.fileformat) 
for more details.

This can save the disk space and reduce disk IO for reading/writing those intermediate tables. 
Of course, this can benefit CPU Spark jobs as well.

### 2. Avoid having too many small files in existing Hive text tables.

The suggested change is to recreate the Hive text tables with many small files to a Hive parquet 
table with a reasonable file size.
GPUs process data much more efficiently when they have a large amount of data to process in parallel. 
Loading data from fewer, large input files will perform better than loading data from many small 
input files. Ideally input files should be on the order of a few gigabytes rather than megabytes or 
smaller.

Refer to [Tuning Guide](../tuning-guide.md#input-files) for more details.

### 3. Use NVME as shuffling disks

Dataproc: [Local SSD](https://cloud.google.com/dataproc/docs/concepts/compute/dataproc-local-ssds)
is recommended for Spark scratch space to improve IO. For example, when creating Dataproc cluster, 
you can add below:

```
--worker-local-ssd-interface=NVME
--num-secondary-worker-local-ssds=2
--worker-local-ssd-interface=NVME
--secondary-worker-local-ssd-interface=NVME
```

Refer to [Getting Started on GCP Dataproc](./getting-started-gcp.md) for more details.

On-Prem cluster: Try to use enough NVME or SSDs as shuffling disks to avoid local disk IO bottleneck.

### 4. Spark RAPIDS tuning guide

To understand more Spark RAPIDS related configurations on performance tuning, please refer to 
[Tuning Guide](../tuning-guide.md) for more details.

### 5. Exclude bad nodes from YARN resource

Just in case there are bad nodes due to hardware failure issues (including GPU failure), we suggest 
setting `spark.yarn.executor.launch.excludeOnFailure.enabled=true` so that the problematic node can 
be excluded.

Refer to [Running Spark on YARN](https://spark.apache.org/docs/latest/running-on-yarn.html) for more 
details.

## How to handle GPU OOM issues

Spark jobs running out of GPU memory is the most common issue because GPU memory is usually much 
smaller than host memory. Below are some common tips to help avoid GPU OOM issue.

### 1. Reduce the number of concurrent tasks per GPU

This is controlled by `spark.rapids.sql.concurrentGpuTasks`. Try to decrease this value to 1. 
If the job still hit OOMs, try the following steps.

### 2. Install CUDA 11.5 or above version

The default value for `spark.rapids.memory.gpu.pool` is changed to `ASYNC` from `ARENA` for CUDA 
11.5+. For CUDA 11.4 and older, it will fall back to `ARENA`.
Using ASYNC allocator can avoid certain memory fragmentation issue.

The Spark executor log should print below messages to indicate how much memory the executor could 
have used:

```
INFO GpuDeviceManager: Initializing RMM ASYNC pool size = 17840.349609375 MB on gpuId 0
```

### 3. Identify which SQL, job and stage runs OOM

The relationship between SQL/job/stage are: Stage belongs to a Job which belongs to SQL.
Firstly check Spark UI to identify the problematic SQL ID, Job ID, and Stage ID.

Then find the failed stage in the `stages` page in Spark UI, and go into that stage to look at tasks.
If some tasks completed successfully while some tasks failed with OOM, check the amount of input 
types or shuffle bytes read per task to see if there is any data skew.

Check the DAG of the problematic stage to see if there are any suspicious operators which may 
consume huge memory, such as windowing, collect_list/collect_set, explode, etc. 

### 4. Increase the # of tasks/partitions based on the type of the problematic stage

#### a. Table Scan Stage

If it is a table scan stage on Parquet/ORC tables, then the # of tasks/partitions is normally 
determined by `spark.sql.files.maxPartitionBytes`. We can decrease its value to increase the # of 
tasks/partitions for this stage so that the memory pressure of each task is less. 

Iceberg or Delta 
tables may have different settings to control the concurrency in the table scan stage. For example, 
Iceberg uses `read.split.target-size` to control the split size.

#### b. Shuffle Stage

If it is a shuffle stage, then the # of tasks/partitions is normally determined by 
`spark.sql.shuffle.partitions`(default value=200), and also AQE's Coalescing Post Shuffle Partitions
feature (Such as parameters `spark.sql.adaptive.coalescePartitions.minPartitionSize`, 
`spark.sql.adaptive.advisoryPartitionSizeInBytes` etc.).

We can adjust the above parameters to increase the # of tasks/partitions for this shuffle stage to 
reduce the memory pressure for each task. For example, we can start with increasing 
`spark.sql.shuffle.partitions`(2x,4x,8x,etc.).

Even though there is no OOM issue but if we found lots of spilling based on SQL plan metrics from 
Spark UI in this shuffle stage, increasing the # of tasks/partitions could decrease the spilling 
data size to improve performance.

Note: AQE's Coalescing Post Shuffle Partitions feature could have different behaviors in different 
Spark 3.x versions. For example, in Spark 3.1.3, `spark.sql.adaptive.coalescePartitions.minPartitionNum`
by default is set to `spark.sql.shuffle.partitions`(default value=200). However in Spark 3.2 or 3.3, 
`minPartitionNum` does not exist any more. So always check the correct version of Spark doc.

### 5. Reduce columnar batch size and file reader batch size

Please refer to [Tuning Guide](../tuning-guide.md#columnar-batch-size) to understand below 2 Spark 
RAPIDS parameters:
```
spark.rapids.sql.batchSizeBytes
spark.rapids.sql.reader.batchSizeBytes
```

### 6. File an issue on github repo

If you are still getting an OOM exception, please get the Spark eventlog and stack trace from the 
executor (the whole executor log ideally) and send to spark-rapids-support@nvidia.com , or file a 
github issue on [spark-rapids github repo](https://github.com/NVIDIA/spark-rapids/issues) if it is 
not sensitive.