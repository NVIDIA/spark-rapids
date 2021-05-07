---
layout: page
title: Frequently Asked Questions
nav_order: 11
---
# Frequently Asked Questions

* TOC
{:toc}

### What versions of Apache Spark does the RAPIDS Accelerator for Apache Spark support?

The RAPIDS Accelerator for Apache Spark requires version 3.0.1, 3.0.2 or 3.1.1 of Apache
Spark. Because the plugin replaces parts of the physical plan that Apache Spark considers to be
internal the code for those plans can change even between bug fix releases. As a part of our
process, we try to stay on top of these changes and release updates as quickly as possible.

### Which distributions are supported?

The RAPIDS Accelerator for Apache Spark officially supports:
- [Apache Spark](get-started/getting-started-on-prem.md)
- [AWS EMR 6.2.0](get-started/getting-started-aws-emr.md)
- [Databricks Runtime 7.3](get-started/getting-started-databricks.md)
- [Google Cloud Dataproc](get-started/getting-started-gcp.md)

Most distributions based on a supported Apache Spark version should work, but because the plugin
replaces parts of the physical plan that Apache Spark considers to be internal the code for those
plans can change from one distribution to another. We are working with most cloud service providers
to set up testing and validation on their distributions.

### What CUDA versions are supported?

CUDA 10.1, 10.2 and 11.0 are currently supported, but you need to download the cudf jar that 
corresponds to the version you are using. Please look [here](download.md) for download 
links for the latest release.

### How can I check if the RAPIDS Accelerator is installed and which version is running?

On startup the RAPIDS Accelerator will log a warning message on the Spark driver showing the
version with a message that looks something like this:
```
21/04/14 22:14:55 WARN SQLExecPlugin: RAPIDS Accelerator 0.5.0 using cudf 0.19. To disable GPU support set `spark.rapids.sql.enabled` to false
```

The full RAPIDS Accelerator and cudf build properties are logged at `INFO` level in the
Spark driver and executor logs with messages that are similar to the following:
```
21/04/14 17:20:20 INFO RapidsExecutorPlugin: RAPIDS Accelerator build: {version=0.5.0-SNAPSHOT, user=jlowe, url=, date=2021-04-14T22:12:14Z, revision=79a5cf8acd615587b2c7835072b0d8b0d4604f8b, cudf_version=0.19-SNAPSHOT, branch=branch-0.5}
21/04/14 17:20:20 INFO RapidsExecutorPlugin: cudf build: {version=0.19-SNAPSHOT, user=, date=2021-04-13T08:42:40Z, revision=a5d2407b93de444a6a7faf9db4b7dbf4ecbfe9ed, branch=HEAD}
```

### What is the right hardware setup to run GPU accelerated Spark?

Reference architectures should be available around Q1 2021.

### What parts of Apache Spark are accelerated?

Currently a limited set of SQL and DataFrame operations are supported, please see the
[configs](configs.md) and [supported operations](supported_ops.md) for a more complete list of what
is supported. Some of structured streaming is likely to be accelerated, but it has not been an area
of focus right now. Other areas like MLLib, GraphX or RDDs are not accelerated.

### Is the Spark `Dataset` API supported?

The RAPIDS Accelerator supports the `DataFrame` API which is implemented in Spark as `Dataset[Row]`.
If you are using `Dataset[Row]` that is equivalent to the `DataFrame` API. However using custom
classes or types with `Dataset` is not supported.  Such queries should still execute correctly when
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

### How can I tell what will run on the GPU and what will not run on it?
<a name="explain"></a>

An Apache Spark plan is transformed and optimized into a set of operators called a physical plan.
This plan is then run through a set of rules to translate it to a version that runs on the GPU.
If you want to know what will run on the GPU and what will not along with an explanation why you
can set [spark.rapids.sql.explain](configs.md#sql.explain) to `ALL`. If you just want to see the
operators not on the GPU you may set it to `NOT_ON_GPU`. Be aware that some queries end up being
broken down into multiple jobs, and in those cases a separate log message might be output for each
job. These are logged each time a query is compiled into an `RDD`, not just when the job runs.
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

Generally if an operator is not compatible with Spark for some reason and is off the explanation
will include information about how it is incompatible and what configs to set to enable the
operator if you can accept the incompatibility.

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

Yes, DPP still works.  It might not be as efficient as it could be, and we are working to improve it.

### Is Adaptive Query Execution (AQE) Supported?

In the 0.2 release, AQE is supported but all exchanges will default to the CPU.  As of the 0.3 
release, running on Spark 3.0.1 and higher any operation that is supported on GPU will now stay on 
the GPU when AQE is enabled. 

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

### Are cache and persist supported?

Yes cache and persist are supported, but they are not GPU accelerated yet. We are working with
the Spark community on changes that would allow us to accelerate compression when caching data.

### Can I cache data into GPU memory?

No, that is not currently supported. It would require much larger changes to Apache Spark to be able
to support this.

### Is PySpark supported?

Yes

### Are the R APIs for Spark supported?

Yes, but we don't actively test them.

### Are the Java APIs for Spark supported?

Yes, but we don't actively test them.

### Are the Scala APIs for Spark supported?

Yes

### Is the GPU needed on the driver?  Are there any benefits to having a GPU on the driver?

The GPU is not needed on the driver and there is no benefit to having one available on the driver
for the RAPIDS plugin.

### How does the performance compare to Databricks' DeltaEngine?

We have not evaluated the performance yet. DeltaEngine is not open source, so any analysis needs to
be done with Databricks in some form. When DeltaEngine is generally available and the terms of
service allow it, we will look into doing a comparison.

### How many tasks can I run per executor? How many should I run per executor?

There is no limit on the number of tasks per executor that you can run.  Generally we recommend 2 to
6 tasks per executor and 1 GPU per executor. The GPU typically benefits from having 2 tasks run
in [parallel](configs.md#sql.concurrentGpuTasks) on it at a time, assuming your GPU has enough
memory to support that. Having 2 to 3 times as many tasks off of the GPU as on the GPU allows for
I/O to be run in parallel with the processing. If you increase the tasks too high you can overload
the I/O and starting the initial processing can suffer.  But if you have a lot of processing that
cannot be done on the GPU, like complex UDFs, the more tasks you have the more CPU processing you
can throw at it.

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

### Is [Multi-Instance GPU (MIG)](https://docs.nvidia.com/cuda/mig/index.html) supported?

Yes, but it requires support from the underlying cluster manager to isolate the MIG GPU instance
for each executor (e.g.: by setting `CUDA_VISIBLE_DEVICES` or other means).

Note that MIG is not recommended for use with the RAPIDS Accelerator since it significantly
reduces the amount of GPU memory that can be used by the Accelerator for each executor instance.
If the cluster is purpose-built to run Spark with the RAPIDS Accelerator then we recommend running
without MIG. Also note that the UCX-based shuffle plugin will not work as well in this
configuration because
[MIG does not support direct GPU to GPU transfers](https://docs.nvidia.com/datacenter/tesla/mig-user-guide/index.html#app-considerations).

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
[spark.rapids.memory.pinnedPool.size](configs.md#memory.pinnedPool.size) to 0.

### Is speculative execution supported?

Yes, speculative execution in Spark is fine with the RAPIDS Accelerator plugin.

As with all speculative execution, it may or may not be beneficial depending on the nature of why a
particular task is slow and how easily speculation is triggered. You should monitor your Spark jobs
to see how often task speculation occurs and how often the speculating task (i.e.: the one launched
later) finishes before the slow task that triggered speculation. If the speculating task often
finishes first then that's good, it is working as intended. If many tasks are speculating, but the
original task always finishes first then this is a pure loss, the speculation is adding load to
the Spark cluster with no benefit.
