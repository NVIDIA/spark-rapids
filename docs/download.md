---
layout: page
title: Download
nav_order: 3
---

The [RAPIDS Accelerator for Apache Spark](https://github.com/NVIDIA/spark-rapids) provides a set of
plugins for Apache Spark that leverage GPUs to accelerate Dataframe and SQL processing.

The accelerator is built upon the [RAPIDS cuDF project](https://github.com/rapidsai/cudf) and
[UCX](https://github.com/openucx/ucx/).

RAPIDS Spark requires each worker node in the cluster to have an NVIDIA GPU and the [NVIDIA
driver](https://www.nvidia.com/en-us/drivers/) installed.

RAPIDS Spark consists of the rapids-4-spark plugin jar.  The jar is either preinstalled in the Spark
classpath on all nodes or submitted with each job that uses the RAPIDS Spark. See the
[getting-started
guide](https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html) for
more details.

## Release v26.04.0
### Hardware Requirements:

The plugin is designed to work on NVIDIA Volta, Turing, Ampere, Ada Lovelace, Hopper and Blackwell generation datacenter GPUs.  The plugin jar is tested on the following GPUs:

	GPU Models: NVIDIA V100, T4, A10, A100, L4, H100 and B100 GPUs

### Software Requirements:

    OS: Spark RAPIDS is compatible with any Linux distribution with glibc >= 2.28 (Please check ldd --version output).  glibc 2.28 was released August 1, 2018. 
    Tested on Ubuntu 22.04, Ubuntu 24.04, Rocky Linux 8 and Rocky Linux 9

	NVIDIA Driver*: R525+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3, 3.4.4
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4, 3.5.5, 3.5.6, 3.5.7, 3.5.8
		Apache Spark 4.0.0, 4.0.1, 4.0.2
		Apache Spark 4.1.1
		Scala 2.12: Spark 3.3.0 through 3.5.8
		Scala 2.13: Spark 3.5.0 through 3.5.8, and Spark 4.0.0, 4.0.1, 4.0.2, and 4.1.1
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
		Databricks 17.3 ML LTS (GPU, Scala 2.13, Spark 4.0.0)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
		GCP Dataproc 2.3

	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 1.2
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v26.04.0

#### CUDA 12

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


#### CUDA 13

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


The above packages are built against CUDA 12.9 or CUDA 13.1. They are tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-26.04.0.jar.asc rapids-4-spark_2.12-26.04.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-26.04.0.jar.asc rapids-4-spark_2.13-26.04.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Databricks 17.3 ML LTS (Spark 4.0) support (Delta Lake fallback to CPU); Databricks 12.2 ML LTS no longer supported ([#14518](https://github.com/NVIDIA/spark-rapids/pull/14518))
* Iceberg 1.10.1 support with Spark 4.0.2 ([#14459](https://github.com/NVIDIA/spark-rapids/pull/14459))
* Bloom filter v2 support on Databricks ([#14406](https://github.com/NVIDIA/spark-rapids/pull/14406))
* Parquet GPU reader: defer resource collection until close ([#14490](https://github.com/NVIDIA/spark-rapids/pull/14490)); row-to-column per-batch retry for OOM recovery ([#14428](https://github.com/NVIDIA/spark-rapids/pull/14428))
* Shuffle and spilling: shuffle fanout OOM fix ([#14525](https://github.com/NVIDIA/spark-rapids/pull/14525)), thread-safe shuffle unregister ([#14513](https://github.com/NVIDIA/spark-rapids/pull/14513)), bounded retry logging for shuffle coalesce ([#14537](https://github.com/NVIDIA/spark-rapids/pull/14537))
* Window execution fixes for mixed ROWS windows ([#14533](https://github.com/NVIDIA/spark-rapids/pull/14533))

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

### Known Issues
* PERFILE reader may return incorrect counts for zero-column Delta scans with deletion vectors ([#14574](https://github.com/NVIDIA/spark-rapids/issues/14574)). This can be triggered when all of the following apply: the Delta table has deletion vector files created for the latest snapshot, `spark.rapids.sql.format.parquet.reader.type=PERFILE`, `spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled=true`, and the query reads zero data columns (e.g. `SELECT COUNT(*) ... WHERE <partition predicate>`). Workaround: use the default reader (`AUTO`). 

## Archived releases

As new releases come out, previous ones will still be available in [archived releases](./archive.md).
