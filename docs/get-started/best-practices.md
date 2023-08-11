---
layout: page
title: Best Practices
nav_order: 15
parent: Getting-Started
---

# Best Practices on the RAPIDS Accelerator for Apache Spark

This article explains the most common best practices using the RAPIDS Accelerator especially for 
performance tuning and troubleshooting.

## Workload Qualification

By following [Workload Qualification](./getting-started-workload-qualification.md) guide, you can 
identify the best candidate Spark applications for the RAPIDS Accelerator and also the feature gaps.

Based on [Qualification tool](../spark-qualification-tool.md)'s output, you can start with the top N
recommended CPU Spark jobs, especially if those jobs are computation heavy jobs (large joins, 
hash aggregates, windowing, sorting).

After those candidate jobs are run on GPU using the RAPIDS Accelerator, check the Spark driver 
log to find the not-supported messages starting with `!`. Some missing features might be enabled by
turning on a configuration, but please understand the corresponding limitations detailed in 
[the advanced configuration document](../additional-functionality/advanced_configs.md).
For other unsupported features, please file feature requests on 
[spark-rapids Github repo](https://github.com/NVIDIA/spark-rapids/issues) with a minimum reproduce
and the not-supported messages if they are non-sensitive data.

## Performance Tuning
Refer to [Tuning Guide](../tuning-guide.md) for more details.

## How to handle GPU OOM issues

Spark jobs running out of GPU memory is the most common issue because GPU memory is usually much 
smaller than host memory. Below are some common tips to help avoid GPU OOM issues.

### 1. Reduce the number of concurrent tasks per GPU

This is controlled by `spark.rapids.sql.concurrentGpuTasks`. Try to decrease this value to 1. 
If the job still hit OOMs, try the following steps.

### 2. Install CUDA 11.5 or above version

The default value for `spark.rapids.memory.gpu.pool` is changed to `ASYNC` from `ARENA` for CUDA 
11.5+. For CUDA 11.4 and older, it will fall back to `ARENA`.
Using ASYNC allocator can avoid some memory fragmentation issues.

The Spark executor log will contain a message like the following when using the ASYNC allocator:

```
INFO GpuDeviceManager: Initializing RMM ASYNC pool size = 17840.349609375 MB on gpuId 0
```

### 3. Identify which SQL, job and stage is involved in the error

The relationship between SQL/job/stage is: Stage belongs to a Job which belongs to SQL.
First check the Spark UI to identify the problematic SQL ID, Job ID, and Stage ID.

Then find the failed stage in the `Stages` page in the Spark UI, and go into that stage to look at tasks.
If some tasks completed successfully while some tasks failed with OOM, check the amount of input 
bytes or shuffle bytes read per task to see if there is any data skew.

Check the DAG of the problematic stage to see if there are any suspicious operators which may 
consume huge amounts of memory, such as windowing, collect_list/collect_set, explode, expand, etc. 

### 4. Increase the number of tasks/partitions based on the type of the problematic stage

#### a. Table Scan Stage

If it is a table scan stage on Parquet/ORC tables, then the number of tasks or partitions is normally 
determined by `spark.sql.files.maxPartitionBytes`. We can decrease its value to increase the
number of tasks or partitions for this stage so that the memory pressure of each task is less. 

Iceberg or Delta  tables may have different settings to control the concurrency in the table
scan stage. For example, Iceberg uses `read.split.target-size` as a table property or read option
to control the split size.

#### b. Shuffle Stage

If it is a shuffle stage, then the number of tasks or partitions is normally determined by 
`spark.sql.shuffle.partitions` (default value=200), and also AQE's Coalescing Post Shuffle Partitions
feature (such as parameters `spark.sql.adaptive.coalescePartitions.minPartitionSize`, 
`spark.sql.adaptive.advisoryPartitionSizeInBytes`, etc.).

We can adjust the above parameters to increase the number of tasks or partitions for this shuffle
stage to  reduce the memory pressure for each task. For example, we can start with increasing 
`spark.sql.shuffle.partitions` by a factor of 2, then 4, then 8, etc.

Even without an OOM error, if the SQL plan metrics show lots of spilling from the
Spark UI in this stage, increasing the number of tasks or partitions could decrease the
spilled data size to improve performance.

Note: AQE's Coalescing Post Shuffle Partitions feature could have different behaviors in different 
Spark 3.x versions. For example, in Spark 3.1.3, `spark.sql.adaptive.coalescePartitions.minPartitionNum`
by default is set to `spark.sql.shuffle.partitions`(default value=200). However in Spark 3.2 or 3.3, 
`minPartitionNum` was removed. So always check the correct version of the Spark documentation.

### 5. Reduce columnar batch size and file reader batch size

Please refer to the [Tuning Guide](../tuning-guide.md#columnar-batch-size) for details on the following 
RAPIDS parameters:
```
spark.rapids.sql.batchSizeBytes
spark.rapids.sql.reader.batchSizeBytes
```

### 6. File an issue or ask a question on the Github repo

If you are still getting an OOM exception, please get the Spark eventlog and stack trace from the 
executor (the whole executor log ideally) and send to spark-rapids-support@nvidia.com , or file a 
Github issue on [spark-rapids GitHub repo](https://github.com/NVIDIA/spark-rapids/issues) if it is 
not sensitive.  Or, open a [discussion thread](https://github.com/NVIDIA/spark-rapids/discussions).  