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
guide](https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html) for more details.

## Release v23.12.0
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3
		Apache Spark 3.4.0, 3.4.1
		Apache Spark 3.5.0
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 10.4 ML LTS (GPU, Scala 2.12, Spark 3.2.1)
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
	
	Supported Dataproc versions:
		GCP Dataproc 2.0
		GCP Dataproc 2.1
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v23.12.0
- **Scala 2.12:**
  - [RAPIDS Accelerator for Apache Spark 23.12.0 - Scala 2.12 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.12.0/rapids-4-spark_2.12-23.12.0.jar)
  - [RAPIDS Accelerator for Apache Spark 23.12.0 - Scala 2.12 jar.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.12.0/rapids-4-spark_2.12-23.12.0.jar.asc)

- **Scala 2.13:**
  - [RAPIDS Accelerator for Apache Spark 23.12.0 - Scala 2.13 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/23.12.0/rapids-4-spark_2.13-23.12.0.jar)
  - [RAPIDS Accelerator for Apache Spark 23.12.0 - Scala 2.13 jar.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/23.12.0/rapids-4-spark_2.13-23.12.0.jar.asc)

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-23.12.0.jar.asc rapids-4-spark_2.12-23.12.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-23.12.0.jar.asc rapids-4-spark_2.13-23.12.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Introduced support for chunked reading of ORC files.
* Enhanced support for additional time zones and added stack function support.
* Enhanced performance for join and aggregation operations.
* Kernel optimizations have been implemented to improve Parquet read performance.
* RAPIDS Accelerator also built and tested with Scala 2.13.
* Last version to support Pascal-based Nvidia GPUs; discontinued in the next release.
* Qualification and Profiling tool:
	* Profiling Tool now processes Spark Driver log for GPU runs, enhancing feature analysis.
	* Auto-tuner recommendations include AQE settings for optimized performance.
	* New configurations in Profiler for enabling off-default features: udfCompiler, incompatibleDateFormats, hasExtendedYearValues.

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Archived releases

As new releases come out, previous ones will still be available in [archived releases](./archive.md).
