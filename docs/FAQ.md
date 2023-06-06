---
layout: page
title: Frequently Asked Questions
nav_order: 12
---
# Frequently Asked Questions

* TOC
{:toc}

### What versions of Apache Spark does the RAPIDS Accelerator for Apache Spark support?

The RAPIDS Accelerator for Apache Spark requires version 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.3.0, 3.3.1 or 3.3.2 of
Apache Spark. Because the plugin replaces parts of the physical plan that Apache Spark considers to
be internal the code for those plans can change even between bug fix releases. As a part of our
process, we try to stay on top of these changes and release updates as quickly as possible.

### Which distributions are supported?

The RAPIDS Accelerator for Apache Spark officially supports:
- [Apache Spark](get-started/getting-started-on-prem.md)
- [AWS EMR 6.2+](get-started/getting-started-aws-emr.md)
- [Databricks Runtime 10.4, 11.3](get-started/getting-started-databricks.md)
- [Google Cloud Dataproc 2.0](get-started/getting-started-gcp.md)
- [Azure Synapse](get-started/getting-started-azure-synapse-analytics.md)
- Cloudera provides the plugin packaged through
  [CDS 3.2](https://docs.cloudera.com/cdp-private-cloud-base/7.1.7/cds-3/topics/spark-spark-3-overview.html)
  and [CDS 3.3](https://docs.cloudera.com/cdp-private-cloud-base/7.1.8/cds-3/topics/spark-spark-3-overview.html).

Most distributions based on a supported Apache Spark version should work, but because the plugin
replaces parts of the physical plan that Apache Spark considers to be internal the code for those
plans can change from one distribution to another. We are working with most cloud service providers
to set up testing and validation on their distributions.

### What CUDA versions are supported?

CUDA 11.x is currently supported.  Please look [here](download.md) for download links for the latest
release.

### What hardware is supported?

The plugin is tested and supported on P100, V100, T4, A2, A10, A30, A100 and L4 datacenter GPUs.  It is possible
to run the plugin on GeForce desktop hardware with Volta or better architectures.  GeForce hardware
does not support [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html#forward-compatibility-title),
and will need CUDA 11.5 installed. If not, the following error will be displayed:

```
ai.rapids.cudf.CudaException: forward compatibility was attempted on non supported HW
        at ai.rapids.cudf.Cuda.getDeviceCount(Native Method)
        at com.nvidia.spark.rapids.GpuDeviceManager$.findGpuAndAcquire(GpuDeviceManager.scala:78)
```

More information about cards that support forward compatibility can be found
[here](https://docs.nvidia.com/deploy/cuda-compatibility/index.html#faq).

### How can I check if the RAPIDS Accelerator is installed and which version is running?

On startup the RAPIDS Accelerator will log a warning message on the Spark driver showing the
version with a message that looks something like this:
```
WARN RapidsPluginUtils: RAPIDS Accelerator 22.10.0 using cudf 22.10.0.
```

The full RAPIDS Accelerator, RAPIDS Accelerator JNI and cudf build properties are logged at `INFO` 
level in the Spark driver and executor logs with messages that are similar to the following:
```
INFO RapidsPluginUtils: RAPIDS Accelerator build: {version=22.10.0-SNAPSHOT, user=, url=https://github.com/NVIDIA/spark-rapids.git, date=2022-09-02T12:41:30Z, revision=66450a3549d7cbb23799ec7be2f6f02b253efb85, cudf_version=22.10.0-SNAPSHOT, branch=HEAD}
INFO RapidsPluginUtils: RAPIDS Accelerator JNI build: {version=22.10.0-SNAPSHOT, user=, url=https://github.com/NVIDIA/spark-rapids-jni.git, date=2022-09-02T03:35:21Z, revision=76b71b9ffa1fa4237365b51485d11362cbfb99e5, branch=HEAD}
INFO RapidsPluginUtils: cudf build: {version=22.10.0-SNAPSHOT, user=, url=https://github.com/rapidsai/cudf.git, date=2022-09-02T03:35:21Z, revision=c273da4d6285d6b6f9640585cb3b8cf11310bef6, branch=HEAD}
```

### What parts of Apache Spark are accelerated?

Currently a limited set of SQL and DataFrame operations are supported, please see the
[configs](configs.md) and [supported operations](supported_ops.md) for a more complete list of what
is supported. Some of the MLlib functions, such as `PCA` are supported.
Some of structured streaming is likely to be accelerated, but it has not been an area
of focus right now. Other areas like GraphX or RDDs are not accelerated.

### Is the Spark `Dataset` API supported?

The RAPIDS Accelerator supports the `DataFrame` API which is implemented in Spark as `Dataset[Row]`.
If you are using `Dataset[Row]` that is equivalent to the `DataFrame` API. In either case the
operations that are supported for acceleration on the GPU are limited. For example using custom
classes or types with `Dataset` are not supported. Neither are using APIs that take `Row` as an input,
or ones that take Scala or Java functions to operate. This includes operators like `flatMap`, `foreach`,
or `foreachPartition`. Such queries will still execute correctly when
using the RAPIDS Accelerator, but it is likely most query operations will not be performed on the
GPU.

With custom types the `Dataset` API generates query plans that use opaque lambda expressions to
access the custom types. The opaque expressions prevent the RAPIDS Accelerator from translating any
operation with these opaque expressions to the GPU, since the RAPIDS Accelerator cannot determine
how the expression operates.

### What is the road-map like?

Please look at the github repository
[https://github.com/nvidia/spark-rapids](https://github.com/nvidia/spark-rapids). It contains issue
tracking and planning for sprints and releases.

### How much faster will my query run?

Any single operator isn’t a fixed amount faster. So there is no simple algorithm to see how much
faster a query will run. In addition, Apache Spark can store intermediate data to disk and send it
across the network, both of which we typically see as bottlenecks in real world queries. Generally
for complicated queries where all the processing can run on the GPU we see between 3x and 7x
speedup, with a 4x speedup typical. We have seen as high as 100x in some specific cases.

### What operators are best suited for the GPU?

* Group by operations with high cardinality
* Joins with a high cardinality
* Sorts with a high cardinality
* Window operations, especially for large windows
* Complicated processing
* Writing Parquet/ORC
* Reading CSV
* Transcoding (reading an input file and doing minimal processing before writing it out again,
possibly in a different format, like CSV to Parquet)

### Are there initialization costs?

From our tests the GPU typically takes about 2 to 3 seconds to initialize when an executor first
starts. If you are only going to run a single query that only takes a few seconds to run this can
be problematic. In general if you are going to do 30 seconds or more of processing within a single
session the overhead can be amortized.

### How long does it take to translate a query to run on the GPU?

The time it takes to translate the Apache Spark physical plan to one that can run on the GPU
is proportional to the size of the plan. But, it also depends on the CPU you are
running on and if the JVM has optimized that code path yet. The first queries run in a client will
be worse than later queries. Small queries can typically be translated in a millisecond or two while
larger queries can take tens of milliseconds. In all cases tested the translation time is orders of
magnitude smaller than the total runtime of the query.

See the entry on [explain](#explain) for details on how to measure this for your queries.

### How can I tell what will run on the GPU and what will not run on it?
<a name="explain"></a>

An Apache Spark plan is transformed and optimized into a set of operators called a physical plan.
This plan is then run through a set of rules to translate it to a version that runs on the GPU.
If you want to know what will run on the GPU and what will not along with an explanation why you
can set [spark.rapids.sql.explain](configs.md#sql.explain) to `ALL`. If you just want to see the
operators not on the GPU you may set it to `NOT_ON_GPU` (which is the default setting value). Be
aware that some queries end up being broken down into multiple jobs, and in those cases a separate
log message might be output for each job. These are logged each time a query is compiled into an
`RDD`, not just when the job runs.
Because of this calling `explain` on a DataFrame will also trigger this to be logged.

The format of each line follows the pattern
```
indicator operation<NAME> operator? explanation
```

In this `indicator` is one of the following
  * `*` for operations that will run on the GPU
  * `@` for operations that could run on the GPU but will not because they are a part of a larger
    section of the plan that will not run on the GPU
  * `#` for operations that have been removed from the plan. The reason they are removed will be
    in the explanation.
  * `!` for operations that cannot run on the GPU

`operation` indicates the type of the operator.
  * `Expression` These are typically functions that operate on columns of data and produce a column
    of data.
  * `Exec` These are higher level operations that operate on an entire table at a time.
  * `Partitioning` These are different types of partitioning used when reorganizing data to move to
    different tasks.
  * `Input` These are different input formats used with a few input statements, but not all.
  * `Output` These are different output formats used with a few output statements, but not all.
  * `NOT_FOUND` These are for anything that the plugin has no replacement rule for.

`NAME` is the name of the operator given by Spark.

`operator?` is an optional string representation of the operator given by Spark.

`explanation` is a text explanation saying if this will
  * run on the GPU
  * could run on the GPU but will not because of something outside this operator and an
    explanation why
  * will not run on the GPU with an explanation why
  * will be removed from the plan with a reason why

Generally if an operator is not compatible with Spark for some reason and is off, the explanation
will include information about how it is incompatible and what configs to set to enable the
operator if you can accept the incompatibility.

These messages are logged at the WARN level so even in `spark-shell` which by default only logs
at WARN or above you should see these messages.

This translation takes place in two steps. The first step looks at the plan, figures out what
can be translated to the GPU, and then does the translation. The second step optimizes the
transitions between the CPU and the GPU.
Explain will also log how long these translations took at the INFO level with lines like.

```
INFO GpuOverrides: Plan conversion to the GPU took 3.13 ms
INFO GpuOverrides: GPU plan transition optimization took 1.66 ms
```

Because it is at the INFO level, the default logging level for `spark-shell` is not going to display
this information. If you want to monitor this number for your queries you might need to adjust your
logging configuration.

### Why does the plan for the GPU query look different from the CPU query?

Typically, there is a one to one mapping between CPU stages in a plan and GPU stages.  There are a
few places where this is not the case.

* `WholeStageCodeGen` - The GPU plan typically does not do code generation, and does not support
  generating code for an entire stage in the plan. Code generation reduces the cost of processing
  data one row at a time. The GPU plan processes the data in a columnar format, so the costs
  of processing a batch is amortized over the entire batch of data and code generation is not
  needed.

* `ColumnarToRow` and `RowToColumnar` transitions - The CPU version of Spark plans typically process
  data in a row based format. The main exception to this is reading some kinds of columnar data,
  like Parquet. Transitioning between the CPU and the GPU also requires transitioning between row
  and columnar formatted data.

* `GpuCoalesceBatches` and `GpuShuffleCoalesce` - Processing data on the GPU scales
  sublinearly. That means doubling the data does often takes less than half the time. Because of
  this we want to process larger batches of data when possible. These operators will try to combine
  smaller batches of data into fewer, larger batches to process more efficiently.

* `SortMergeJoin` - The RAPIDS Accelerator does not support sort merge joins yet. For now, we
  translate sort merge joins into shuffled hash joins. Because of this there are times when sorts
  may be removed or other sorts added to meet the ordering requirements of the query.

* `TakeOrderedAndProject` - The `TakeOrderedAndProject` operator will take the top N entries in
  each task, shuffle the results to a single executor and then take the top N results from that.
  The GPU plan often has more metrics than the CPU versions do, and when we tried to combine all of
  these operations into a single stage the metrics were confusing to understand. Instead, we split
  the single stage up into multiple smaller parts, so the metrics are clearer.

### Why does `explain()` show that the GPU will be used even after setting `spark.rapids.sql.enabled` to `false`?

Apache Spark caches what is used to build the output of the `explain()` function. That cache has no
knowledge about configs, so it may return results that are not up to date with the current config
settings. This is true of all configs in Spark. If you changed
`spark.sql.autoBroadcastJoinThreshold` after running `explain()` on a `DataFrame`, the resulting
query would not change to reflect that config and still show a `SortMergeJoin` even though the new
config might have changed to be a `BroadcastHashJoin` instead. When actually running something like
with `collect`, `show` or `write` a new `DataFrame` is constructed causing Spark to re-plan the
query. This is why `spark.rapids.sql.enabled` is still respected when running, even if explain shows
stale results.

### How are failures handled?

The RAPIDS Accelerator does not change the way failures are normally handled by Apache Spark.

### How does the Spark scheduler decide what to do on the GPU vs the CPU?

Technically the Spark scheduler does not make those decisions. The plugin has a set of rules that
decide if an operation can safely be replaced by a GPU enabled version. We are working on some cost
based optimizations to try and improve performance for some situations where it might be more
efficient to stay on the CPU instead of going back and forth.

### Is Dynamic Partition Pruning (DPP) Supported?

Yes, DPP works.  

### Is Adaptive Query Execution (AQE) Supported?

Any operation that is supported on GPU will stay on the GPU when AQE is enabled.

#### Why does my query show as not on the GPU when Adaptive Query Execution is enabled?

When running an `explain()` on a query where AQE is on, it is possible that AQE has not finalized
the plan.  In this case a message stating `AdaptiveSparkPlan isFinalPlan=false` will be printed at
the top of the physical plan, and the explain output will show the query plan with CPU operators.
As the query runs, the plan on the UI will update and show operations running on the GPU.  This can
happen for any AdaptiveSparkPlan where `isFinalPlan=false`.

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- ...
```

Once the query has been executed you can access the finalized plan on WebUI and in the user code
running on the Driver, e.g. in a REPL or notebook, to confirm that the query has executed on GPU:

```Python
>>> df=spark.range(0,100).selectExpr("sum(*) as sum")
>>> df.explain()
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[sum(id#0L)])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#11]
      +- HashAggregate(keys=[], functions=[partial_sum(id#0L)])
         +- Range (0, 100, step=1, splits=16)


>>> df.collect()
[Row(sum=4950)]
>>> df.explain()
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   GpuColumnarToRow false
   +- GpuHashAggregate(keys=[], functions=[gpubasicsum(id#0L, LongType, false)]), filters=ArrayBuffer(None))
      +- GpuShuffleCoalesce 2147483647
         +- ShuffleQueryStage 0
            +- GpuColumnarExchange gpusinglepartitioning$(), ENSURE_REQUIREMENTS, [id=#64]
               +- GpuHashAggregate(keys=[], functions=[partial_gpubasicsum(id#0L, LongType, false)]), filters=ArrayBuffer(None))
                  +- GpuRange (0, 100, step=1, splits=16)
+- == Initial Plan ==
   HashAggregate(keys=[], functions=[sum(id#0L)])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#11]
      +- HashAggregate(keys=[], functions=[partial_sum(id#0L)])
         +- Range (0, 100, step=1, splits=16)
```

### Are cache and persist supported?

Yes cache and persist are supported, the cache is GPU accelerated
but still stored on the host memory.
Please refer to [RAPIDS Cache Serializer](./additional-functionality/cache-serializer.md)
for more details.

### Can I cache data into GPU memory?

No, that is not currently supported.
It would require much larger changes to Apache Spark to be able to support this.

### Is PySpark supported?

Yes

### Are the R APIs for Spark supported?

Yes, but we don't actively test them, because the RAPIDS Accelerator hooks into Spark not at
the various language APIs but at the Catalyst level after all the various APIs have converged into
the DataFrame API.

### Are the Java APIs for Spark supported?

Yes, but we don't actively test them, because the RAPIDS Accelerator hooks into Spark not at
the various language APIs but at the Catalyst level after all the various APIs have converged into
the DataFrame API.

### Are the Scala APIs for Spark supported?

Yes

### Is the GPU needed on the driver?  Are there any benefits to having a GPU on the driver?

The GPU is not needed on the driver and there is no benefit to having one available on the driver
for the RAPIDS plugin.

### Are table layout formats supported?

Yes, there is GPU support for [Delta Lake](./additional-functionality/delta-lake-support.md) and
[Apache Iceberg](./additional-functionality/iceberg-support.md). See the additional support
documentation for specifics on the operations supported for these formats.

### How many tasks can I run per executor? How many should I run per executor?

There is no limit on the number of tasks per executor that you can run. Generally we recommend 2 to
6 tasks per executor and 1 GPU per executor. The GPU typically benefits from having 2 tasks run
in [parallel](configs.md#sql.concurrentGpuTasks) on it at a time, assuming your GPU has enough
memory to support that. Having 2 to 3 times as many tasks off of the GPU as on the GPU allows for
I/O to be run in parallel with the processing. If you increase the tasks too high you can overload
the I/O and starting the initial processing can suffer. But if you have a lot of processing that
cannot be done on the GPU, like complex UDFs, the more tasks you have the more CPU processing you
can throw at it.

### How are `spark.executor.cores`, `spark.task.resource.gpu.amount`, and `spark.rapids.sql.concurrentGpuTasks` related?

The `spark.executor.cores` and `spark.task.resource.gpu.amount` configuration settings are inputs
to the Spark task scheduler and control the maximum number of tasks that can be run concurrently
on an executor, regardless of whether they are running CPU or GPU code at any point in time. See
the [Number of Tasks per Executor](tuning-guide.md#number-of-tasks-per-executor) section in the
tuning guide for more details.

The `spark.rapids.sql.concurrentGpuTasks` configuration setting is specific to the RAPIDS
Accelerator and further limits the number of concurrent tasks that are _actively_ running code on
the GPU or using GPU memory at any point in time. See the
[Number of Concurrent Tasks per GPU](tuning-guide.md#number-of-concurrent-tasks-per-gpu) section
of the tuning guide for more details.

### Why are multiple GPUs per executor not supported?

The RAPIDS Accelerator only supports a single GPU per executor because that was a limitation of
[RAPIDS cudf](https://github.com/rapidsai/cudf), the foundation of the Accelerator. Basic support
for working with multiple GPUs has only recently been added to RAPIDS cudf, and there are no plans
for its individual operations to leverage multiple GPUs (e.g.: a single task's join operation
processed by multiple GPUs).

Many Spark setups avoid allocating too many concurrent tasks to the same executor, and often
multiple executors are run per node on the cluster. Therefore this feature has not been
prioritized, as there has not been a compelling use-case that requires it.

### Why are multiple executors per GPU not supported?

There are multiple reasons why this a problematic configuration:
- Apache Spark does not support scheduling a fractional number of GPUs to an executor
- CUDA context switches between processes sharing a single GPU can be expensive
- Each executor would have a fraction of the GPU memory available for processing

### Is [Multi-Instance GPU (MIG)](https://www.nvidia.com/en-gb/technologies/multi-instance-gpu/) supported?

Yes, but it requires support from the underlying cluster manager to isolate the MIG GPU instance
for each executor (e.g.: by setting `CUDA_VISIBLE_DEVICES`,
[YARN with docker isolation](https://github.com/NVIDIA/spark-rapids-examples/tree/main/examples/MIG-Support)
or other means).

Note that MIG is not recommended for use with the RAPIDS Accelerator since it significantly
reduces the amount of GPU memory that can be used by the Accelerator for each executor instance.
If the cluster is purpose-built to run Spark with the RAPIDS Accelerator then we recommend running
without MIG. Also note that the UCX-based shuffle plugin will not work as well in this
configuration because
[MIG does not support direct GPU to GPU transfers](https://docs.nvidia.com/datacenter/tesla/mig-user-guide/index.html#app-considerations).

However MIG can be advantageous if the cluster is intended to be shared amongst other processes
(like ML / DL jobs).

### How can I run custom expressions/UDFs on the GPU?

The RAPIDS Accelerator provides the following solutions for running
user-defined functions on the GPU:

#### RAPIDS Accelerated UDFs

UDFs can provide a RAPIDS accelerated implementation which allows the RAPIDS Accelerator to perform
the operation on the GPU.  See the [RAPIDS accelerated UDF documentation](additional-functionality/rapids-udfs.md)
for details.

#### Automatic Translation of Scala UDFs to Apache Spark Operations

The RAPIDS Accelerator has an experimental byte-code analyzer which can translate some simple
Scala UDFs into equivalent Apache Spark operations in the query plan. The RAPIDS Accelerator then
translates these operations into GPU operations just like other query plan operations.

The Scala UDF byte-code analyzer is disabled by default and must be enabled by the user via the
[`spark.rapids.sql.udfCompiler.enabled`](configs.md#sql.udfCompiler.enabled) configuration
setting.

#### Optimize a row-based UDF in a GPU operation

If the UDF can not be implemented by RAPIDS Accelerated UDFs or be automatically translated to
Apache Spark operations, the RAPIDS Accelerator has an experimental feature to transfer only the
data it needs between GPU and CPU inside a query operation, instead of falling this operation back
to CPU. This feature can be enabled by setting `spark.rapids.sql.rowBasedUDF.enabled` to true.


### Why is the size of my output Parquet/ORC file different?

This can come down to a number of factors.  The GPU version often compresses data in smaller chunks
to get more parallelism and performance. This can result in larger files in some instances. We have
also seen instances where the ordering of the data can have a big impact on the output size of the
files.  Spark tends to prefer sort based joins, and in some cases sort based aggregations, whereas
the GPU versions are all hash based. This means that the resulting data can come out in a different
order for the CPU and the GPU. This is not wrong, but can make the size of the output data
different because of compression. Users can turn on
[spark.rapids.sql.hashOptimizeSort.enabled](configs.md#sql.hashOptimizeSort.enabled) to have
the GPU try to replicate more closely what the output ordering would have been if sort were used,
like on the CPU.

### Why am I getting the error `Failed to open the timezone file` when reading files?

When reading from a file that contains data referring to a particular timezone, e.g.: reading
timestamps from an ORC file, the system's timezone database at `/usr/share/zoneinfo/` must contain
the timezone in order to process the data properly. This error often indicates the system is
missing the timezone database. The timezone database is provided by the `tzdata` package on many
Linux distributions.

### Why am I getting an error when trying to use pinned memory?

```
Caused by: ai.rapids.cudf.CudaException: OS call failed or operation not supported on this OS
	at ai.rapids.cudf.Cuda.hostAllocPinned(Native Method)
	at ai.rapids.cudf.PinnedMemoryPool.<init>(PinnedMemoryPool.java:254)
	at ai.rapids.cudf.PinnedMemoryPool.lambda$initialize$1(PinnedMemoryPool.java:185)
	at java.util.concurrent.FutureTask.run(FutureTask.java:264)
```

This is typically caused by the IOMMU being enabled.  Please see the
[CUDA docs](https://docs.nvidia.com/cuda/cuda-c-programming-guide/index.html#iommu-on-linux)
for this issue.

To fix it you can either disable the IOMMU, or you can disable using pinned memory by setting
[`spark.rapids.memory.pinnedPool.size`](configs.md#memory.pinnedPool.size) to 0.

### Why am I getting a buffer overflow error when using the KryoSerializer?
Buffer overflow will happen when trying to serialize an object larger than
[`spark.kryoserializer.buffer.max`](https://spark.apache.org/docs/latest/configuration.html#compression-and-serialization),
and may result in an error such as:
```
Caused by: com.esotericsoftware.kryo.KryoException: Buffer overflow. Available: 0, required: 636
    at com.esotericsoftware.kryo.io.Output.require(Output.java:167)
    at com.esotericsoftware.kryo.io.Output.writeBytes(Output.java:251)
    at com.esotericsoftware.kryo.io.Output.write(Output.java:219)
    at java.base/java.io.ObjectOutputStream$BlockDataOutputStream.write(ObjectOutputStream.java:1859)
    at java.base/java.io.ObjectOutputStream.write(ObjectOutputStream.java:712)
    at java.base/java.io.BufferedOutputStream.write(BufferedOutputStream.java:123)
    at java.base/java.io.DataOutputStream.write(DataOutputStream.java:107)
    ...
```
Try increasing the
[`spark.kryoserializer.buffer.max`](https://spark.apache.org/docs/latest/configuration.html#compression-and-serialization)
from a default of 64M to something larger, for example 512M.

### Is speculative execution supported?

Yes, speculative execution in Spark is fine with the RAPIDS Accelerator plugin.

As with all speculative execution, it may or may not be beneficial depending on the nature of why a
particular task is slow and how easily speculation is triggered. You should monitor your Spark jobs
to see how often task speculation occurs and how often the speculating task (i.e.: the one launched
later) finishes before the slow task that triggered speculation. If the speculating task often
finishes first then that's good, it is working as intended. If many tasks are speculating, but the
original task always finishes first then this is a pure loss, the speculation is adding load to
the Spark cluster with no benefit.

### Why is my query in GPU mode slower than CPU mode?

Below are some troubleshooting tips on GPU query performance issue:
* Identify the most time consuming part of the query. You can use the
  [Profiling tool](./spark-profiling-tool.md) to process the Spark event log to get more insights of
  the query performance. For example, if I/O is the bottleneck, we suggest optimizing the backend
  storage I/O performance because the most suitable query type is computation bound instead of
  I/O or network bound.

* Make sure at least the most time consuming part of the query is on the GPU. Please refer to
  [Getting Started on Spark workload qualification](./get-started/getting-started-workload-qualification.md)
  for more details. Ideally we hope the whole query is fully on the GPU, but if some minor part of
  the query, eg. a small JDBC table scan, can not run on the GPU, it won't cause much performance
  overhead. If there are some CPU fallbacks, check if those are some known features which can be
  enabled by turning on some RAPIDS Accelerator parameters. If the features needed do not exist in
  the most recent release of the RAPIDS Accelerator, please file a
  [feature request](https://github.com/NVIDIA/spark-rapids/issues) with a minimum reproducing example.

* Tune the Spark and RAPIDS Accelerator parameters such as `spark.sql.shuffle.partitions`,
  `spark.sql.files.maxPartitionBytes` and `spark.rapids.sql.concurrentGpuTasks` as these configurations can affect performance of queries significantly.
  Please refer to [Tuning Guide](./tuning-guide.md) for more details.

### Why is the Avro library not found by RAPIDS?

If you are getting a warning `Avro library not found by the RAPIDS plugin.` or if you are getting the
`java.lang.NoClassDefFoundError: org/apache/spark/sql/v2/avro/AvroScan` error, make sure you ran the
Spark job by using the `--jars` or `--packages` option followed by the file path or maven path to
RAPIDS jar since that is the preferred way to run RAPIDS accelerator.

Note, you can add locally installed jars for external packages such as Avro Data Sources and the 
RAPIDS Accelerator jars via  `spark.driver.extraClassPath` (--driver-class-path in the client mode) 
on the driver side, and `spark.executor.extraClassPath` on the executor side. However, you should not 
mix the deploy methods for either of the external modules.  Either deploy both Spark Avro and RAPIDS 
Accelerator jars as local jars via `extraClassPath` settings or use the `--jars` or `--packages` options.

As a consequence, per [issue-5796](https://github.com/NVIDIA/spark-rapids/issues/5796), if you also 
use the RAPIDS Shuffle Manager, your deployment option may be limited to the extraClassPath method.

### What is the default RMM pool allocator?

Starting from 22.06, the default value for `spark.rapids.memory.gpu.pool` is changed to `ASYNC` from
`ARENA` for CUDA 11.5+. For CUDA 11.4 and older, it will fall back to `ARENA`.

### What is a `RetryOOM` or `SplitAndRetryOOM` exception?

In the 23.04 release of the accelerator two new exceptions were added to replace a
regular `OutOfMemoryError` that was thrown before when the GPU ran out of memory.
Originally we used `OutOfMemoryError` like on the CPU thinking that it would help to
trigger GC in case handles pointing to GPU memory were leaked in the JVM heap. But
`OutOfMemoryError` is technically a fatal exception and recovering from it is
not strictly supported. As such Apache Spark treats it as a fatal exception and will
kill the process that sees this exception. This can result in a lot of tasks
being rerun if the GPU runs out of memory. These new exceptions prevent that. They
also provide an indication to various GPU operators that the GPU ran out of memory
and how that operator might be able to recover. `RetryOOM` indicates that the operator
should roll back to a known good spot and then wait until the memory allocation
framework decides that it should be retried. `SplitAndRetryOOM` is used
when there is really only one task unblocked and the only way to recover would be to
roll back to a good spot and try to split the input so that less total GPU memory is
needed.

These are not implemented for all GPU operations. A number of GPU operations that
use a significant amount of memory have been updated to handle `RetryOOM`, but fewer
have been updated to handle `SplitAndRetryOOM`. If you do run into these exceptions
it is an indication that you are using too much GPU memory. The tuning guide can
help you to reduce your memory usage. Be aware that some algorithms do not have
a way to split their usage, things like window operations over some large windows.
If tuning does not fix the problem please file an issue to help us understand what
operators may need better out of core algorithm support.

### Encryption Support

The RAPIDS Accelerator for Apache Spark has several components that may or may not follow
the encryption configurations that Apache Spark provides. The following documents the 
exceptions that are known at the time of writing this FAQ entry:

Local storage encryption (`spark.io.encryption.enabled`) is not supported for spilled buffers that the 
plugin uses to help with GPU out-of-memory situations. The RAPIDS Shuffle Manager does not implement 
local storage encryption for shuffle blocks when configured for UCX, but it does when configured in 
MULTITHREADED mode.

Network encryption (`spark.network.crypto.enabled`) is not supported in the RAPIDS Shuffle Manager
when configured for UCX, but it is supported when configured in MULTITHREADED mode.

If your environment has specific encryption requirements for network or IO, please make sure
that the RAPIDS Accelerator suits your needs, and file and issue or discussion if you have doubts
or would like expanded encryption support.

### I have more questions, where do I go?
We use github to track bugs, feature requests, and answer questions. File an
[issue](https://github.com/NVIDIA/spark-rapids/issues/new/choose) for a bug or feature request. Ask
or answer a question on the [discussion board](https://github.com/NVIDIA/spark-rapids/discussions).
