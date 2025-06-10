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

## Release v25.06.0
### Hardware Requirements:

The plugin is designed to work on NVIDIA Volta, Turing, Ampere, Ada Lovelace, Hopper and Blackwell generation datacenter GPUs.  The plugin jar is tested on the following GPUs:

	GPU Models: NVIDIA V100, T4, A10, A100, L4, H100 and B100 GPUs

### Software Requirements:

    OS: Spark RAPIDS is compatible with any Linux distribution with glibc >= 2.28 (Please check ldd --version output).  glibc 2.28 was released August 1, 2018. 
    Tested on Ubuntu 20.04, Ubuntu 22.04, Rocky Linux 8 and Rocky Linux 9

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3, 3.4.4
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4, 3.5.5, 3.5.6
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2

	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 1.2
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v25.06.0

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v25.06.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.06.0/rapids-4-spark_2.12-25.06.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.06.0/rapids-4-spark_2.12-25.06.0.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v25.06.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.06.0/rapids-4-spark_2.13-25.06.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.06.0/rapids-4-spark_2.13-25.06.0.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v25.06.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.06.0/rapids-4-spark_2.12-25.06.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.06.0/rapids-4-spark_2.12-25.06.0-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v25.06.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.06.0/rapids-4-spark_2.13-25.06.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.06.0/rapids-4-spark_2.13-25.06.0-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs with 
CUDA 11.8 and CUDA 12.8.  

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-25.06.0.jar.asc rapids-4-spark_2.12-25.06.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-25.06.0.jar.asc rapids-4-spark_2.13-25.06.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Support functions that have time zones with daylight savings transitions 
* Support Spark Connect for Spark 3.5.6+ (Spark Connect is supported for Spark 3.4-3.5.5 if the plugin jar is built with a single shim)
* Support for Iceberg 1.6.1 on Spark 3.5.x for read with deletions
* Support array_distinct
* Support bit_count
* Support bitwise aggregate functions (bit_and, bit_or and bit_xor) in groupby and reduction
* Support conv
* Support sha1
* Introduce more metrics for troubleshooting (max writers number, memory bookkeepings)
* Fix a GPU Out-of-memory bug when spark.speculation is on
* Fix a get_json_object bug when encounter some special pattern path

Note: There is a known issue in the 25.06.0 release when decompressing gzip files on H100 GPUs.
Please find more details in [issue-16661](https://github.com/rapidsai/cudf/issues/16661).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Archived releases

As new releases come out, previous ones will still be available in [archived releases](./archive.md).
