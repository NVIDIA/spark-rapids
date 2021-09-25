---
layout: page
title: Getting-Started
nav_order: 2
has_children: true
permalink: /Getting-Started/
---
# Getting Started with the RAPIDS Accelerator for Apache Spark

Apache Spark 3.0+ lets users provide a plugin that can replace the backend for SQL and DataFrame
operations. This requires no API changes from the user. The plugin will replace SQL operations it
supports with GPU accelerated versions. If an operation is not supported it will fall back to using
the Spark CPU version. Note that the plugin cannot accelerate operations that manipulate RDDs
directly.

The accelerator library also provides an implementation of Spark's shuffle that can leverage 
[UCX](https://www.openucx.org/) to optimize GPU data transfers keeping as much data on the GPU as
possible and bypassing the CPU to do GPU to GPU transfers.

The GPU accelerated processing plugin does not require the accelerated shuffle implementation.
However, if accelerated SQL processing is not enabled, the shuffle implementation falls back to the
default `SortShuffleManager`. 

To enable GPU processing acceleration you will need:
- Apache Spark 3.0+
- A spark cluster configured with GPUs that comply with the requirements for the version of 
  [cudf](https://github.com/rapidsai/cudf).
    - One GPU per executor.
- The following jars:
    - A cudf jar that corresponds to the version of CUDA available on your cluster.
    - RAPIDS Spark accelerator plugin jar.
- To set the config `spark.plugins` to `com.nvidia.spark.SQLPlugin`

## Spark GPU Scheduling Overview
Apache Spark 3.0 now supports GPU scheduling as long as you are using a cluster manager that
supports it. You can have Spark request GPUs and assign them to tasks. The exact configs you use
will vary depending on your cluster manager. Here are some example configs:
- Request your executor to have GPUs:
  - `--conf spark.executor.resource.gpu.amount=1`
- Specify the number of GPUs per task:
  - `--conf spark.task.resource.gpu.amount=1`
- Specify a GPU discovery script (required on YARN and K8S):
  - `--conf spark.executor.resource.gpu.discoveryScript=./getGpusResources.sh`

See the deployment specific sections for more details and restrictions. Note that
`spark.task.resource.gpu.amount` can be a decimal amount, so if you want multiple tasks to be run
on an executor at the same time and assigned to the same GPU you can set this to a decimal value
less than 1. You would want this setting to correspond to the `spark.executor.cores` setting.  For
instance, if you have `spark.executor.cores=2` which would allow 2 tasks to run on each executor
and you want those 2 tasks to run on the same GPU then you would set
`spark.task.resource.gpu.amount=0.5`. See the [Tuning Guide](../tuning-guide.md) for more details
on controlling the task concurrency for each executor.

You can also refer to the official Apache Spark documentation.
- [Overview](https://github.com/apache/spark/blob/master/docs/configuration.md#custom-resource-scheduling-and-configuration-overview)
- [Kubernetes specific documentation](https://github.com/apache/spark/blob/master/docs/running-on-kubernetes.md#resource-allocation-and-configuration-overview)
- [Yarn specific documentation](https://github.com/apache/spark/blob/master/docs/running-on-yarn.md#resource-allocation-and-configuration-overview)
- [Standalone specific documentation](https://github.com/apache/spark/blob/master/docs/spark-standalone.md#resource-allocation-and-configuration-overview)
