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

## Release v24.08.0
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
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3
		Apache Spark 3.5.0, 3.5.1
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v24.08.0

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v24.08.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.08.0/rapids-4-spark_2.12-24.08.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.08.0/rapids-4-spark_2.12-24.08.0.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v24.08.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.08.0/rapids-4-spark_2.13-24.08.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.08.0/rapids-4-spark_2.13-24.08.0.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v24.08.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.08.0/rapids-4-spark_2.12-24.08.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.08.0/rapids-4-spark_2.12-24.08.0-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v24.08.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.08.0/rapids-4-spark_2.13-24.08.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.08.0/rapids-4-spark_2.13-24.08.0-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-24.08.0.jar.asc rapids-4-spark_2.12-24.08.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-24.08.0.jar.asc rapids-4-spark_2.13-24.08.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Support timezones with daylight savings shifts
* Improve metrics in Spark UI
* Refactor Parquet decode microkernels and support load balancing RLE runs
* Improve get_json performance
* Support dynamic scan filtering
* Improve UCX shuffle 
* For updates on RAPIDS Accelerator Tools, please visit [this link](https://github.com/NVIDIA/spark-rapids-tools/releases)

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Archived releases

As new releases come out, previous ones will still be available in [archived releases](./archive.md).
