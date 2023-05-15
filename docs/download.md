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

## Release v23.04.1
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA P100, V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.3.0, 3.3.1, 3.3.2, Databricks 10.4 ML LTS or 11.3 ML LTS Runtime and GCP Dataproc 2.0, Dataproc 2.1

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v23.04.1
* Download the [RAPIDS
  Accelerator for Apache Spark 23.04.1 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.04.1/rapids-4-spark_2.12-23.04.1.jar)

This package is built against CUDA 11.8 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 23.04.1 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.04.1/rapids-4-spark_2.12-23.04.1.jar)
  and [RAPIDS Accelerator for Apache Spark 23.04.1 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.04.1/rapids-4-spark_2.12-23.04.1.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-23.04.1.jar.asc rapids-4-spark_2.12-23.04.1.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
The 23.04.1 release patches a possible driver OOM which can occur on an executor broadcast.
New functionality and performance improvements for this release include:
* Introduces OOM retry framework for automatic OOM handling in memory-intensive operators, such as: join, aggregates and windows, coalescing, projections and filters.
* Support dynamic repartitioning in large/skewed hash joins
* Optimize the transpilation in `regexp_extract` function
* Support Delta Lake write with auto-optimization and auto-compaction on Databricks platforms
* Qualification and Profiling tool:
  * Add support to recommend cluster shape options on Dataproc and EMR
  * Add support for Databricks local mode with cost savings based on cluster metadata
  * Add TCO calculator to estimate annualized cost savings, including estimated frequency for applications
  * Add support in the qualification tool to generate estimated speed-up for ML functionality in Spark applications
  
  
For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Archived releases

As new releases come out, previous ones will still be available in [archived releases](./archive.md). 