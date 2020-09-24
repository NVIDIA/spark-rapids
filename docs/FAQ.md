---
layout: page
title: Frequently Asked Questions
nav_order: 8
---
# Frequently Asked Questions

### Why does `explain()` show that the GPU will be used even after setting `spark.rapids.sql.enabled` to `false`?

Apache Spark caches what is used to build the output of the `explain()` function. That cache has no
knowledge about configs, so it may return results that are not up to date with the current config
settings. This is true of all configs in Spark. If you changed
`spark.sql.autoBroadcastJoinThreshold` after running `explain()` on a `DataFrame`, the resulting
query would not change to reflect that config and still show a `SortMergeJoin` even though the
new config might have changed to be a `BroadcastHashJoin` instead. When actually running something
like with `collect`, `show` or `write` a new `DataFrame` is constructed causing spark to re-plan the
query. This is why `spark.rapids.sql.enabled` is still respected when running, even if explain
shows stale results.

### What versions of Apache Spark does the RAPIDS Accelerator for Apache Spark support?

The RAPIDS Accelerator for Apache Spark requires version 3.0.0 or 3.0.1 of Apache Spark. Because the
plugin replaces parts of the physical plan that Apache Spark considers to be internal the code for
those plans can change even between bug fix releases. As a part of our process, we try to stay on
top of these changes and release updates as quickly as possible.

### Which distributions are supported?

The RAPIDS Accelerator for Apache Spark officially supports
[Apache Spark](get-started/getting-started-on-prem.md),
[Databricks Runtime 7.0](get-started/getting-started-databricks.md)
and [Google Cloud Dataproc](get-started/getting-started-gcp.md).
Most distributions based off of Apache Spark 3.0.0 should work, but because the plugin replaces
parts of the physical plan that Apache Spark considers to be internal the code for those plans
can change from one distribution to another. We are working with most cloud service providers to
set up testing and validation on their distributions.

### What is the right hardware setup to run GPU accelerated Spark?

Reference architectures should be available around Q4 2020.

### What CUDA versions are supported?

CUDA 10.1, 10.2 and 11.0 are currently supported, but you need to download the cudf jar that 
corresponds to the version you are using. Please look [here][version/stable-release.md] for download 
links for the stable release.

### What parts of Apache Spark are accelerated?

Currently a limited set of SQL and DataFrame operations are supported, please see the
[configs](configs.md) for a more complete list of what is supported. Some of structured streaming
is likely to be accelerated, but it has not been an area of focus right now. Other areas like
MLLib, GraphX or RDDs are not accelerated.

### What is the road-map like?

Please look at the github repository https://github.com/nvidia/spark-rapids It contains
issue tracking and planning for sprints and releases.

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

### Why is the size of my output Parquet/ORC file different?

This can come down to a number of factors.  The GPU version often compresses data in smaller chunks
to get more parallelism and performance. This can result in larger files in some instances. We have
also seen instances where the ordering of the data can have a big impact on the output size of the
files.  Spark tends to prefer sort based joins, and in some cases sort based aggregations, whereas
the GPU versions are all hash based. This means that the resulting data can come out in a different
order for the CPU and the GPU. This is not wrong, but can make the size of the output data
different because of compression. Users can turn on
(spark.rapids.sql.hashOptimizeSort.enabled)[configs.md#sql.hashOptimizeSort.enabled] to have
the GPU try to replicate more closely what the output ordering would have been if sort were used,
like on the CPU.

### How are failures handled?

The RAPIDS accelerator does not change the way failures are normally handled by Apache Spark.

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

### How does the performance compare to DataBricks' DeltaEngine?

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

### How can I run custom expressions/UDFs on the GPU?

We do not currently support this, but we are working on ways to make it possible.

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

Yes, speculative execution in Spark is fine with the RAPIDS accelerator plugin.

As with all speculative execution, it may or may not be beneficial depending on the nature of why a
particular task is slow and how easily speculation is triggered. You should monitor your Spark jobs
to see how often task speculation occurs and how often the speculating task (i.e.: the one launched
later) finishes before the slow task that triggered speculation. If the speculating task often
finishes first then that's good, it is working as intended. If many tasks are speculating, but the
original task always finishes first then this is a pure loss, the speculation is adding load to
the Spark cluster with no benefit.
