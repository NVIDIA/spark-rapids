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

## Release v23.08.1
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA P100, V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: 470+

	Python 3.6+, Scala 2.12, Java 8, Java 17

	Supported Spark versions:
		Apache Spark 3.1.1, 3.1.2, 3.1.3 
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2
		Apache Spark 3.4.0, 3.4.1
	
	Supported Databricks runtime versions: 
		Azure/AWS:
			Databricks 10.4 ML LTS (GPU, Scala 2.12, Spark 3.2.1)
			Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
			Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
	
	Supported Dataproc versions:
		GCP Dataproc 2.0
		GCP Dataproc 2.1

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v23.08.1
* Download the [RAPIDS
  Accelerator for Apache Spark 23.08.1 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.08.1/rapids-4-spark_2.12-23.08.1.jar)

This package is built against CUDA 11.8, all CUDA 11.x and 12.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A10, A100, L4 and H100 GPUs with CUDA 11.8-12.0.  For those using other types of GPUs 
which do not have CUDA forward compatibility (for example, GeForce), CUDA 11.8 or later is required.

Note that v23.08.0 is deprecated.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 23.08.1 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.08.1/rapids-4-spark_2.12-23.08.1.jar)
  and [RAPIDS Accelerator for Apache Spark 23.08.1 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.08.1/rapids-4-spark_2.12-23.08.1.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-23.08.1.jar.asc rapids-4-spark_2.12-23.08.1.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Compatibility with Databricks AWS & Azure 12.2 ML LTS.
* Enhanced stability and support for ORC and Parquet.
* Reduction of out-of-memory (OOM) occurrences.
* Corner case evaluation for data formats, operators and expressions
* Qualification and Profiling tool:
  * Profiling tool now supports Azure Databricks and AWS Databricks.
  * Qualification tool can provide advice on unaccelerated operations.
  * Improve user experience through CLI design.
  * Qualification tool provides configuration and migration recommendations for Dataproc and EMR.
  
For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Archived releases

As new releases come out, previous ones will still be available in [archived releases](./archive.md). 