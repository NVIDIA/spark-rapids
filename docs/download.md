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

## Release v22.10.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA P100, V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, Rocky Linux 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.2.2, 3.3.0, Databricks 9.1 ML LTS or 10.4 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v22.10.0
* Download the [RAPIDS
  Accelerator for Apache Spark 22.10.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.10.0/rapids-4-spark_2.12-22.10.0.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 22.10.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.10.0/rapids-4-spark_2.12-22.10.0.jar)
  and [RAPIDS Accelerator for Apache Spark 22.10.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.10.0/rapids-4-spark_2.12-22.10.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-22.10.0.jar.asc rapids-4-spark_2.12-22.10.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Dataproc qualification, profiling, bootstrap and diagnostic tool
* Databricks custom docker container
* AQE support on Databricks
* MultiThreaded Shuffle feature
* Binary write support for parquet
* Cast binary to string  
* Hive parquet table write support
* Qualification and Profiling tool:
  * Print Databricks cluster/job information
  * AutoTuner for Profiling tool

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Archived releases

As new releases come out, previous ones will still be available in [archived releases](./archive.md). 