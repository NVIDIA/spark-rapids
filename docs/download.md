---
layout: page
title: Download
nav_order: 3
---

[RAPIDS Accelerator For Apache Spark](https://github.com/NVIDIA/spark-rapids) provides a set of
plugins for Apache Spark that leverage GPUs to accelerate Dataframe and SQL processing.

The accelerator is built upon the [RAPIDS cuDF project](https://github.com/rapidsai/cudf) and
[UCX](https://github.com/openucx/ucx/).

The RAPIDS Accelerator For Apache Spark requires each worker node in the cluster to have
[CUDA](https://developer.nvidia.com/cuda-toolkit) installed.

The RAPIDS Accelerator For Apache Spark consists of two jars: a plugin jar along with the RAPIDS
cuDF jar, that is either preinstalled in the Spark classpath on all nodes or submitted with each job
that uses the RAPIDS Accelerator For Apache Spark. See the [getting-started
guide](https://nvidia.github.io/spark-rapids/Getting-Started/) for more details.

## Release v22.12.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA P100, V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, Rocky Linux 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.3.0, 3.3.1, Databricks 9.1 ML LTS or 10.4 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Release Notes
New functionality and performance improvements for this release include:
* Deliver z-ordering capability on Databricks
* Enable DPP on Databricks 10.4
* Improve robust for workloads which has high compression ratio input datasets
* Support Delta Lake batch writes on Databricks
* Partially support HiveTableScanExec on GPU
* Support mapInArrow
* Enable tiered projections for more expressions to optimize performance   
* Qualification and Profiling tool:
  * Support cost estimations for Dataproc 1.5 and Dataproc2.x

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Archived releases

As new releases come out, previous ones will still be available in [archived releases](./archive.md). 