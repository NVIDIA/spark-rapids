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
the plugin will allocate `90%` (`0.9`) of the _available_ memory on the GPU and keep it as a pool
that can be allocated from. If the pool is exhausted more memory will be allocated and added to
the pool.
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

This configuration setting controls how long Spark should wait to obtain a better locality for tasks.
If your tasks are long and see poor locality, you can increase this value. If the data sets are small 
and the cost of waiting will have less impact on the job's overall completion time, you can reduce this 
value to get higher parallelization. In a cluster with high I/O bandwidth you can set it to 0 because it 
will be faster to not wait when you can get the data across the network fast enough. 

## Number of Concurrent Tasks per GPU
Configuration key: [`spark.rapids.sql.concurrentGpuTasks`](configs.md#sql.concurrentGpuTasks)

Default value: `1`

The RAPIDS Accelerator can further limit the number of tasks that are actively sharing the GPU.
It does this using a semaphore. When metrics or documentation refers to the GPU semaphore it
is referring to this. This restriction is useful for avoiding GPU out of memory errors while 
still allowing full concurrency for the portions of the job that are not executing on the GPU. 
Care is taken to try and avoid doing I/O or other CPU operations while the GPU semaphore is held. 
But in the case of a join two batches are required for processing, and it is not always possible 
to avoid this case.

Some queries benefit significantly from
setting this to a value between `2` and `4`, with `2` typically providing the most benefit, and
higher numbers giving diminishing returns, but a lot of it depends on the size of the GPU you have.
An 80 GiB A100 will be able to run a lot more in parallel without seeing degradation
compared to a 16 GiB T4. This is both because of the amount of memory available and also the
raw computing power.

Setting this value too high can lead to GPU out of memory errors or poor runtime
performance. Running multiple tasks concurrently on the GPU will reduce the memory available
to each task as they will be sharing the GPU's total memory. As a result, some queries that fail
to run with a higher concurrent task setting may run successfully with a lower setting.

As of the 23.04 release of the RAPIDS Accelerator for Apache Spark
many out of memory errors result in parts of the query being rolled back and retried instead
of a task failure. The fact that this is happening will show up in the task metrics.
These metrics include `gpuRetryCount` which is the number of times that a retry was attempted.
As a part of this the normal `OutOfMemoryError` is thrown much less. Instead a `RetryOOM`
or `SplitAndRetryOOM` exception is thrown.

To mitigate the out of memory errors you can often reduce the batch size, which will keep less
data active in a batch at a time, but can increase the overall runtime as less data is being
processed per batch.

Note that when Apache Spark is scheduling GPUs as a resource, the configured GPU resource amount
per task may be too low to achieve the desired concurrency. See the
[section on configuring the number of tasks per executor](#number-of-tasks-per-executor) for more
details.

This value can be set on a per-job basis like most other configs. If multiple tasks are running
at the same time with different concurrency levels, then it is interpreted as how much of the
GPU each task is allowed to use (1/concurrentGpuTasks). For example if the concurrency is set
to 1, then each task is assumed to take the entire GPU, so only one of those tasks will
be allowed on the GPU at a time. If it is set to 2, then each task takes 1/2 of the GPU
and up to 2 of them could be on the GPU at once. This also works for mixing tasks with
different settings. For example 1 task with a concurrency of 2 could share the GPU with
2 tasks that have a concurrency of 4. In practice this is not likely to show up frequently.

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

## Input Files' column order
When there are a large number of columns for file formats like Parquet and ORC the size of the 
contiguous data for each individual column can be very small. This can result in doing lots of very 
small random reads to the file system to read the data for the subset of columns that are needed.

We suggest reordering the columns needed by the queries and then rewrite the files to make those
columns adjacent. This could help both Spark on CPU and GPU.

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

## Input File Caching

If the Spark application accesses the same data multiple times, it may benefit from the
RAPIDS Accelerator file cache. See the [filecache documentation](additional-functionality/filecache.md)
for details.

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

## Metrics

### SQL

Custom Spark SQL Metrics are available which can help identify performance bottlenecks in a query.

| Key               | Name                         | Description                                                                                                                                                                                                                                                            |
|-------------------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| bufferTime        | buffer time                  | Time spent buffering input from file data sources. This buffering time happens on the CPU, typically with no GPU semaphore held. For Multi-threaded readers, `bufferTime` measures the amount of time we are blocked on while the threads that are buffering are busy. |
| readFsTime        | time to read fs data         | Time spent actually reading the data and writing it to on-heap memory. This is a part of `bufferTime`                                                                                                                                                                  |
| writeBufferTime   | time to write data to buffer | Time spent moving the on-heap buffered data read from the file system to off-heap memory so the GPU can access it. This is a part of `bufferTime`                                                                                                                      |
| buildDataSize     | build side size              | Size in bytes of the build-side of a join.                                                                                                                                                                                                                             |
| buildTime         | build time                   | Time to load the build-side of a join.                                                                                                                                                                                                                                 |
| collectTime       | collect time                 | For a broadcast the amount of time it took to collect the broadcast data back to the driver before broadcasting it back out.                                                                                                                                           |
| computeAggTime    | aggregation time             | Time computing an aggregation.                                                                                                                                                                                                                                         |
| concatTime        | concat batch time            | Time to concatenate batches. Runs on CPU.                                                                                                                                                                                                                              |
| copyBufferTime    | copy buffer time             | Time spent on copying upstreaming data into Rapids buffers.                                                                                                                                                                                                            |
| filterTime        | filter time                  | Time spent applying filters within other operators, such as joins.                                                                                                                                                                                                     |
| gpuDecodeTime     | GPU decode time              | Time spent on GPU decoding encrypted or compressed data.                                                                                                                                                                                                               |
| joinOutputRows    | join output rows             | The number of rows produced by a join before any filter expression is applied.                                                                                                                                                                                         |
| joinTime          | join time                    | Time doing a join operation.                                                                                                                                                                                                                                           |
| numInputBatches   | input columnar batches       | Number of columnar batches that the operator received from its child operator(s).                                                                                                                                                                                      |
| numInputRows      | input rows                   | Number of rows that the operator received from its child operator(s).                                                                                                                                                                                                  |
| numOutputBatches  | output columnar batches      | Number of columnar batches that the operator outputs.                                                                                                                                                                                                                  |
| numOutputRows     | output rows                  | Number of rows that the operator outputs.                                                                                                                                                                                                                              |
| numPartitions     | partitions                   | Number of output partitions from a file scan or shuffle exchange.                                                                                                                                                                                                      |
| opTime            | op time                      | Time that an operator takes, exclusive of the time for executing or fetching results from child operators, and typically outside of the time it takes to acquire the GPU semaphore. <br/> Note: Sometimes contains CPU times, e.g.: concatTime                         |
| partitionSize     | partition data size          | Total size in bytes of output partitions.                                                                                                                                                                                                                              |
| peakDevMemory     | peak device memory           | Peak GPU memory used during execution of an operator.                                                                                                                                                                                                                  |
| sortTime          | sort time                    | Time spent in sort operations in GpuSortExec and GpuTopN.                                                                                                                                                                                                              |                                                                                                                                                                                          |
| streamTime        | stream time                  | Time spent reading data from a child. This generally happens for the stream side of a hash join or for columnar to row and row to columnar operations.                                                                                                                 |

Not all metrics are enabled by default. The configuration setting `spark.rapids.sql.metrics.level` can be set
to `DEBUG`, `MODERATE`, or `ESSENTIAL`, with `MODERATE` being the default value. More information about this
configuration option is available in the [configuration documentation](configs.md#sql.metrics.level).

Output row and batch counts show up for operators where the number of output rows or batches are
expected to change. For example a filter operation would show the number of rows that passed the
filter condition. These can be used to detect small batches. The GPU is much more efficient when the
batch size is large enough to offset the overhead of launching CUDA kernels.

Input rows and batches are really only for debugging and can mostly be ignored.

Many of the questions people really want to answer with the metrics are around how long various
operators take. Where is the bottleneck in my query? How much of my query is executing on the GPU?
How long does operator X take on the GPU vs the CPU?

### Task

Custom Task level accumulators are also included. These metrics are not for individual
operators in the SQL plan, but are per task and roll up to stages in the plan. Timing metrics
are reported in the format of HH:MM:SS.sss. It should be noted that spill metrics,
including the spill to memory and disk sizes, are not isolated to a single
task, or even a single stage in the plan. The amount of data spilled is the amount of
data that this particular task needed to spill in order to make room for the task to
allocate new memory. The spill time metric is how long it took that task to spill
that memory. It could have spilled memory associated with a different task,
or even a different stage or job in the plan.  The spill read time metric is how
long it took to read back in the data it needed to complete the task. This does not
correspond to the data that was spilled by this task.

| Name              | Description                                                                                                                                                                   |
|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| gpuSemaphoreWait  | The time the task spent waiting on the GPU semaphore.                                                                                                                         |
| gpuSpillBlockTime | The time that this task was blocked spilling data from the GPU.                                                                                                               |
| gpuSpillReadTime  | The time that this task was blocked reading data to the GPU that was spilled previously.                                                                                      |
| gpuRetryCount | The number of times that a retry exception was thrown in an attempt to roll back processing to free memory.                                                                   |
| gpuSplitAndRetryCount | The number of times that a split and retry exception was thrown in an attempt to roll back processing to free memory, and split the input to make more room.                  |
| gpuRetryBlockTime | The amount of time that this task was blocked either hoping that other tasks will free up more memory or after a retry exception was thrown to wait until the task can go on. |

The spill data sizes going to host/CPU memory and disk are the same as used by Spark task level
metrics.

### Time taken on the GPU

`opTime` mainly convey the GPU time.
If GPU operators have some workload on CPU, the GPU time is: `opTime` - CPU time, e.g.:
`opTime` - `concatTime`.
Nearly all GPU operators will have an `op time` metric. This metric times how long a given
operation took to complete on the GPU separate from anything upstream or down stream of the
operator. By looking at the `op time` for each operator you should be able to get a feeling of
how long each section of a query took in terms of time on the GPU.

For many complex operations, like joins and aggregations, the time taken can be broken down further.
These metrics typically only appear in `DEBUG` mode for the metrics though. But can provide extra
information when trying to understand what is happening in a query without having to do profiling
of the GPU query execution.

#### Spilling

Some operators provide out of core algorithms, or algorithms that can process data that is larger
than can fit in GPU memory. This is often done by breaking the problem up into smaller pieces and
letting some of those pieces be moved out of GPU memory when not being worked on. Apache Spark does
similar things when processing data on the CPU. When these types of algorithms are used
the task level spill metrics will indicate that spilling happened. Be aware that
the same metrics are used both for both the GPU code and the original Spark CPU code. The
GPU spills will always be timed and show up as `gpuSpillBlockTime` in the task level
metrics.

### Time taken on the CPU

Operations that deal with the CPU as well as the GPU will often have multiple metrics broken out,
like in the case of reading data from a parquet file. There will be metrics for how long it took
to read the data to CPU memory, `buffer time`, along with how much time was taken to transfer the
data to the GPU and decode it, `GPU decode time`.

There is also a metric for how long an operation was blocked waiting on the GPU semaphore before
it got a chance to run on the GPU. This metric is enabled in `DEBUG` mode mostly because it is not
complete. Spark does not provide a way for us to accurately report that metric during a shuffle.

Apache Spark provides a `duration` metric for code generation blocks that is intended to measure how
long it took to do the given processing. However, `duration` is measured from the time that the
first row is processed to the time that the operation is done. In most cases this is very close to
the total runtime for the task, and ends up not being that useful in practice. Apache Spark does
not want to try and measure it more accurately, because it processes data one row
at a time with multiple different operators intermixed in the same generated code. In this case
the overhead of measuring how long a single row took to process would likely be very large compared
to the amount of time to actually process the data.

But the RAPIDS Accelerator for Apache Spark does provide a workaround. When data is transferred
from the CPU to the GPU or from the GPU to the CPU the `stream time` is reported. When going from
the CPU to the GPU it is the amount of time take to collect a batches worth of data on CPU before
sending it to the GPU. When going from the GPU to the CPU it is the amount of time taken to get the
batch and put it into a format that the GPU can start to process one row at a time. This can allow
you to get an approximate measurement of the amount of time taken to process a section of the query
on the CPU by subtracting the `stream time` before going to the CPU from the `stream time` after
coming back to the GPU. Please note that this is really only valid in showing the amount of time
a section of a mixed GPU/CPU query took. It should not be used to indicate how long an operation on
the CPU is likely to take in a pure CPU only workload. This is because the memory access patterns
when going from the GPU to the CPU and vise versa are very different from when the data stays on
the CPU the entire time. This can result in very different timings between the different situations. 

## Window Operations

Apache Spark supports a few optimizations for different windows patterns. Generally Spark
buffers all the data for a partition by key in memory and then loops through the rows looking for
boundaries. When it finds a boundary change, it will then calculate the aggregation on that window.
This ends up being `O(N^2)` where N is the size of the window. In a few cases in can improve on
that. These optimizations include.

  * Lead/Lag. In this case Spark keeps an offset pointer and can output the result from the
    buffered data in linear time. Lead and Lag only support row based windows and set up the
    row ranges automatically based off of the lead/lag requested.
  * Unbounded Preceding to Unbounded Following. In this case Spark will do a single aggregation
    and duplicate the result multiple times. This works for both row and range based windows.
    There is no difference in the calculation in this case because the window is the size of
    the partition by group.
  * Unbounded Preceding to some specific bound. For this case Spark keeps running state as it
    walks through each row and outputs an updated result each time. This also works for both
    row and range based windows. For row based queries it just adds a row at a time. For
    range based queries it adds rows until the order by column changes.
  * Some specific bound to Unbounded Following. For this case Spark will still recalculate
    aggregations for each window group. The complexity of this is `O(N^2)` but it only has to
    check lower bounds when doing the aggregation. This also works for row or range based
    windows, and for row based windows the performance improvement is very minimal because it
    is just removing a row each time instead of doing a check for equality on the order by
    columns.

Some proprietary implementations have further optimizations. For example Databricks has a special
case for running windows (rows between unbounded preceding to current row) which allows it to
avoid caching the entire window in memory and just cache the running state in between rows.

CUDF and the RAPIDS Accelerator do not have these same set of optimizations yet and so the
performance can be different based off of the window sizes and the aggregation operations. Most
of the time the window size is small enough that the parallelism of the GPU can offset the
difference in the complexity of the algorithm and beat the CPU. In the general case if `N` is
the size of the window and `G` is the parallelism of the GPU then the complexity of a window
operations is `O(N^2/G)`. The main optimization currently supported by the RAPIDS Accelerator
is for running window (rows between unbounded preceding and current row). This is only for a
specific set of aggregations.

  * MIN
  * MAX
  * SUM
  * COUNT
  * ROW_NUMBER
  * RANK
  * DENSE_RANK

For these operations the GPU can use specialized hardware to do the computation in approximately
`O(N/G * LOG(N))` time. The details of how this works is a bit complex, but it is described
somewhat generally
[here](https://developer.nvidia.com/gpugems/gpugems3/part-vi-gpu-computing/chapter-39-parallel-prefix-sum-scan-cuda).

Some aggregations can be done in constant time like `count` on a non-nullable
column/value, `lead` or `lag`. These allow us to compute the result in approximately `O(N/G)` time.
For all other cases large windows, including skewed values in partition by and order by data, can
result in slow performance. If you do run into one of these situations please file an
[issue](https://github.com/NVIDIA/spark-rapids/issues/new/choose) so we can properly prioritize
our work to support more optimizations.
