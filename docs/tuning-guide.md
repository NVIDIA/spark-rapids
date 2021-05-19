---
layout: page
title: Tuning
nav_order: 7
---
# RAPIDS Accelerator for Apache Spark Tuning Guide
Tuning a Spark job's configuration settings from the defaults can often improve job performance,
and this remains true for jobs leveraging the RAPIDS Accelerator plugin for Apache Spark. This
document provides guidelines on how to tune a Spark job's configuration settings for improved
performance when using the RAPIDS Accelerator plugin.

## Number of Executors
The RAPIDS Accelerator plugin only supports a one-to-one mapping between GPUs and executors.

## Number of Tasks per Executor 
Running multiple, concurrent tasks per executor is supported in the same manner as standard
Apache Spark. For example, if the cluster nodes each have 24 CPU cores and 4 GPUs then setting
`spark.executor.cores=6` will run each executor with 6 cores and 6 concurrent tasks per executor,
assuming the default setting of one core per task, i.e.: `spark.task.cpus=1`.

Note that when Apache Spark schedules GPU resources then the GPU resource amount per task,
controlled by `spark.task.resource.gpu.amount`, can limit the number of concurrent tasks further.
For example, if Apache Spark is scheduling for GPUs and `spark.task.resource.gpu.amount=1` then
only one task will run concurrently per executor since the RAPIDS Accelerator only supports
one GPU per executor. When Apache Spark is scheduling for GPUs, set
`spark.task.resource.gpu.amount` to the reciprocal of the desired executor task concurrency, e.g.:
`spark.task.resource.gpu.amount=0.125` will allow up to 8 concurrent tasks per executor.

It is recommended to run more than one concurrent task per executor as this allows overlapping
I/O and computation.  For example one task can be communicating with a distributed filesystem to
fetch an input buffer while another task is decoding an input buffer on the GPU. Configuring too
many concurrent tasks on an executor can lead to excessive I/O, overload host memory, and spilling
from GPU memory.
Counter-intuitively leaving some CPU cores idle may actually speed up your overall job. We
typically find that two times the number of concurrent GPU tasks is a good starting point.

The [number of concurrent tasks running on a GPU](#number-of-concurrent-tasks-per-gpu)
is configured separately.

Be aware that even if you restrict the concurrency of GPU tasks having many tasks
per executor can result in spilling from GPU memory. Because of I/O and the fairness of the
semaphore that restricts GPU access, tasks tend to run on the GPU in a round-robin fashion.
Some algorithms, in order to be able to support processing more data than can fit in GPU memory
like sort or join, can keep part of the data cached on the GPU in between outputting batches.
If there are too many tasks this can increase the memory pressure on the GPU and result in more
spilling.

## Pooled Memory
Configuration key: [`spark.rapids.memory.gpu.pooling.enabled`](configs.md#memory.gpu.pooling.enabled)

Default value: `true`

Configuration key: [`spark.rapids.memory.gpu.allocFraction`](configs.md#memory.gpu.allocFraction)

Default value: `0.9`

Allocating memory on a GPU can be an expensive operation. RAPIDS uses a pooling allocator
called [RMM](https://github.com/rapidsai/rmm) to mitigate this overhead. By default, on startup
the plugin will allocate `90%` (`0.9`) of the memory on the GPU and keep it as a pool that can
be allocated from. If the pool is exhausted more memory will be allocated and added to the pool.
Most of the time this is a huge win, but if you need to share the GPU with other 
[libraries](additional-functionality/ml-integration.md) that are not aware of RMM this can lead
to memory issues, and you may need to disable pooling.

## Pinned Memory
Configuration key: [`spark.rapids.memory.pinnedPool.size`](configs.md#memory.pinnedPool.size)

Default value: `0`

Pinned memory refers to memory pages that the OS will keep in system RAM and will not relocate
or swap to disk.  Using pinned memory significantly improves performance of data transfers between
the GPU and host memory as the transfer can be performed asynchronously from the CPU.  Pinned
memory is relatively expensive to allocate and can cause system issues if too much memory is
pinned, so by default no pinned memory is allocated.

It is recommended to use some amount of pinned memory when using the RAPIDS Accelerator.
Ideally the amount of pinned memory allocated would be sufficient to hold the input
partitions for the number of concurrent tasks that Spark can schedule for the executor.

Note that the specified amount of pinned memory is allocated _per executor_.  For example, if
each node in the cluster has 4 GPUs and therefore 4 executors per node, a configuration setting
of `spark.rapids.memory.pinnedPool.size=4G` will allocate a total of 16 GiB of memory on the
system.

When running on YARN, make sure to account for the extra memory consumed by setting
`spark.executor.memoryOverhead` to a value at least as large as the amount of pinned memory
allocated by each executor.  Note that pageable, off-heap host memory is used once the pinned
memory pool is exhausted, and that would also need to be accounted for in the memory overhead
setting.

Pinned memory can also be used to speed up spilling from GPU memory if there is more data
than can fit in GPU memory. [The spill storage](#spill-storage) is configured separately, but
for best performance spill storage should be taken into account when allocating pinned memory.
For example if I have 4 concurrent tasks per executor, each processing 1 GiB batches, I will
want about 4 GiB of pinned memory to handle normal input/output, but if I am going to sort a
large amount of data I might want to increase the pinned memory pool to 8 GiB and give 4 GiB to
the spill storage, so it can use pinned memory too.

## Spill Storage

Configuration key: [`spark.rapids.memory.host.spillStorageSize`](configs.md#memory.host.spillStorageSize)

Default value: `1g`

This is the amount of host memory that is used to cache spilled data before it is flushed to disk.
The GPU Accelerator employs different algorithms that allow it to process more data than can fit in
the GPU's memory. We do not support this for all operations, and are constantly trying to add more.
The way that this can work is by spilling parts of the data to host memory or to disk, and then
reading them back in when needed. Spilling in general is more expensive than not spilling, but
spilling to a slow disk can be especially expensive. If you see spilling to disk happening in
your query, and you are not using GPU Direct storage for spilling, you may want to add more
spill storage.

You can configure this larger than the amount of pinned memory. This number is just an upper limit.
If the pinned memory is used up then it allocates and uses non-pinned memory.

## Locality Wait
Configuration key:
[`spark.locality.wait`](http://spark.apache.org/docs/latest/configuration.html#scheduling)

Default value: `3s`

This configuration setting controls how long Spark should wait to obtain better locality for tasks.
When tasks complete quicker than this setting, the Spark scheduler can end up not leveraging all
of the executors in the cluster during a stage.  If you see stages in the job where it appears
Spark is running tasks serially through a small subset of executors it is probably due to this
setting.  Some queries will see significant performance gains by setting this to `0`.

## Number of Concurrent Tasks per GPU
Configuration key: [`spark.rapids.sql.concurrentGpuTasks`](configs.md#sql.concurrentGpuTasks)

Default value: `1`

The RAPIDS Accelerator can further limit the number of tasks that are actively sharing the GPU.
This is useful for avoiding GPU out of memory errors while still allowing full concurrency for the
portions of the job that are not executing on the GPU.  Some queries benefit significantly from
setting this to a value between `2` and `4`, with `2` typically providing the most benefit, and
higher numbers giving diminishing returns, but a lot of it depends on the size of the GPU you have.
An 80 GiB A100 will be able to run a lot more in parallel without seeing degradation
compared to a 16 GiB T4. This is both because of the amount of memory available and also the
raw computing power.

Setting this value too high can lead to GPU out of memory errors or poor runtime
performance. Running multiple tasks concurrently on the GPU will reduce the memory available
to each task as they will be sharing the GPU's total memory. As a result, some queries that fail
to run with a higher concurrent task setting may run successfully with a lower setting.

To mitigate the out of memory errors you can often reduce the batch size, which will keep less
data active in a batch at a time, but can increase the overall runtime as less data is being
processed per batch.

Note that when Apache Spark is scheduling GPUs as a resource, the configured GPU resource amount
per task may be too low to achieve the desired concurrency. See the
[section on configuring the number of tasks per executor](#number-of-tasks-per-executor) for more
details.

## Shuffle Partitions
Configuration key:
[`spark.sql.shuffle.partitions`](https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options)

Default value: `200`

The number of partitions produced between Spark stages can have a significant performance impact
on a job.  Too few partitions and a task may run out of memory as some operations require all of
the data for a task to be in memory at once.  Too many partitions and partition processing
overhead dominates the task runtime.

Partitions have a higher incremental cost for GPU processing than CPU processing, so it is
recommended to keep the number of partitions as low as possible without running out of memory in a
task.  This also has the benefit of increasing the amount of data each task will process which
reduces the overhead costs of GPU processing.  Note that setting the partition count too low could
result in GPU out of memory errors.  In that case the partition count will need to be increased.

Try setting the number of partitions to either the number of GPUs or the number of concurrent GPU
tasks in the cluster.  The number of concurrent GPU tasks is computed by multiplying the number of
GPUs in the Spark cluster multiplied by the
[number of concurrent tasks allowed per GPU](#number-of-concurrent-tasks-per-gpu).  This will
set the number of partitions to matching the computational width of the Spark cluster which
provides work for all GPUs.

## Input Files
GPUs process data much more efficiently when they have a large amount of data to process in
parallel.  Loading data from fewer, large input files will perform better than loading data
from many small input files.  Ideally input files should be on the order of a few gigabytes
rather than megabytes or smaller.

Note that the GPU can encode Parquet and ORC data much faster than the CPU, so the costs of
writing large files can be significantly lower.

## Input Partition Size

Similar to the discussion on [input file size](#input-files), many queries can benefit from using
a larger input partition size than the default setting.  This allows the GPU to process more data
at once, amortizing overhead costs across a larger set of data.  Many queries perform better when
this is set to 256MB or 512MB.  Note that setting this value too high can cause tasks to fail with
GPU out of memory errors.

The configuration settings that control the input partition size depend upon the method used
to read the input data.

### Input Partition Size with DataSource API

Configuration key:
[`spark.sql.files.maxPartitionBytes`](https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options)


Default value: `128MB`

Using the `SparkSession` methods to read data (e.g.: `spark.read.`...) will go through the
DataSource API.

### Input Partition Size with Hive API

Configuration keys:
- `spark.hadoop.mapreduce.input.fileinputformat.split.minsize`
- `spark.hadoop.mapred.min.split.size`

Default value: `0`

## Columnar Batch Size
Configuration key: [`spark.rapids.sql.batchSizeBytes`](configs.md#sql.batchSizeBytes)

Default value: `2147483647` (just under 2 GiB)

The RAPIDS Accelerator plugin processes data on the GPU in a columnar format.  Data is processed
in a series of columnar batches. During processing multiple batches may be concatenated
into a single batch to make the GPU processing more efficient, or in other cases the output size
of an operation like join or sort will target that batch size for output. This setting controls the upper
limit on batches that are output by these tasks.  Setting this value too low can result in a
large amount of GPU processing overhead and slower task execution. Setting this value too high
can lead to GPU out of memory errors.  If tasks fail due to GPU out of memory errors after the
query input partitions have been read, try setting this to a lower value. The maximum size is just
under 2 GiB. In general, we recommend setting the batch size to

```
min(2GiB - 1 byte, (gpu_memory - 1 GiB) / gpu_concurrency / 4)
```

Where `gpu_memory` is the amount of memory on the GPU, or the maximum pool size if using pooling.
We then subtract from that 1 GiB for overhead in holding CUDA kernels/etc and divide by the
[`gpu_concurrency`](#number-of-concurrent-tasks-per-gpu). Finally, we divide by 4. This is set
by the amount of working memory that different algorithms need to succeed. Joins need a
batch for the left-hand side, one for the right-hand side, one for working memory to do the join,
and finally one for the output batch, which results in 4 times the target batch size.  Not all
algorithms are exact with this. Some will require all of the data for a given key to be in memory
at once, others require all of the data for a task to be in memory at once. We are working on
getting as many algorithms as possible to stay under this 4 batch size limit, but depending on your
query you many need to adjust the batch size differently from this algorithm.

As an example processing on a 16 GiB T4 with a concurrency of 1 it is recommended to set the batch
size to `(16 GiB - 1 GiB) / 1 / 4` which results in 3.75 GiB, but the maximum size is just
under to 2 GiB, so use that instead. If we are processing on a 16 GiB V100 with a concurrency
of 2 we get `(16 GiB - 1 GiB) / 2 / 4` which results in 1920 MiB. Finally, for an 80 GiB A100
with a concurrency of 8 we get `(80 GiB - 1 GiB) / 8 / 4` we get about 2.5 GiB which is over 2 GiB
so we stick with the maximum.

In the future we may adjust the default value to follow this pattern once we have enough
algorithms updated to keep within this 4 batch size limit.

### File Reader Batch Size
Configuration key: [`spark.rapids.sql.reader.batchSizeRows`](configs.md#sql.reader.batchSizeRows)

Default value: `2147483647`

Configuration key: [`spark.rapids.sql.reader.batchSizeBytes`](configs.md#sql.reader.batchSizeBytes)

Default value: `2147483647`

When reading data from a file, this setting is used to control the maximum batch size separately
from the main [columnar batch size](#columnar-batch-size) setting.  Some transcoding jobs (e.g.:
load CSV files then write Parquet files) need to lower this setting when using large task input
partition sizes to avoid GPU out of memory errors.

### Enable Incompatible Operations
Configuration key: 
[`spark.rapids.sql.incompatibleOps.enabled`](configs.md#sql.incompatibleOps.enabled)

Default value: `false`

There are several operators/expressions that are not 100% compatible with the CPU version. These
incompatibilities are documented [here](compatibility.md) and in the 
[configuration documentation](./configs.md). Many of these incompatibilities are around corner
cases that most queries do not encounter, or that would not result in any meaningful difference
to the resulting output.  By enabling these operations either individually or with the 
`spark.rapids.sql.incompatibleOps.enabled` config it can greatly improve performance of your
queries. Over time, we expect the number of incompatible operators to reduce.

If you want to understand if an operation is or is not on the GPU and why see second on
[explain in the FAQ](FAQ.md#explain)

The following configs all enable different types of incompatible operations that can improve
performance.
- [`spark.rapids.sql.variableFloatAgg.enabled`](configs.md#sql.variableFloatAgg.enabled) 
- [`spark.rapids.sql.hasNans`](configs.md#sql.hasNans)
- [`spark.rapids.sql.castFloatToString.enabled`](configs.md#sql.castFloatToString.enabled)
- [`spark.rapids.sql.castStringToFloat.enabled`](configs.md#sql.castStringToFloat.enabled)
