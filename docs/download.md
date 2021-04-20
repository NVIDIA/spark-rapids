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


## Release v0.5.0

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

Hardware Requirements: 

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)
	
Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS8
	
	CUDA & Nvidia Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
	
	Apache Hadoop 2.10+ or 3.1.1+ (3.1.1 for nvidia-docker version 2)
	
	Python 3.6+, Scala 2.12, Java 8 

### Download v0.5.0
* Download [RAPIDS Spark Package](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.5.0/rapids-4-spark_2.12-0.5.0.jar)
* Download RAPIDS cuDF 0.19.0 for your system: 
  * [cuDF 11.0 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.19.0/cudf-0.19.0-cuda11.jar)
  * [cuDF 10.2 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.19.0/cudf-0.19.0-cuda10-2.jar)
  * [cuDF 10.1 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.19.0/cudf-0.19.0-cuda10-1.jar)

### Release Notes
New functionality for this release includes:
* Additional support for structs, including casting structs to string, hashing structs, unioning
  structs, and allowing array types and structs to pass through when doing joins
* Support for `get_json_object`, `pivot`, `explode` operators
* Casting string to decimal and decimal to string

Performance improvements for this release include: 
* Optimizing unnecessary columnar->row->columnar transitions with AQE
* Supporting out of core sorts
* Initial support for cost based optimization
* Decimal32 support
* Accelerate data transfer for map Pandas UDF
* Allow spilled buffers to be unspilled


## Release v0.4.2

This is a patch release based on version 0.4.1 with the following change:
* Depend on cuDF 0.18.2

The release is supported on Apache Spark 3.0.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS and
Google Cloud Platform Dataproc 2.0.

The list of all supported operations is provided [here](supported_ops.md).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

Hardware Requirements: 

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)
	
Software Requirements:

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
	
	CUDA & Nvidia Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
	
	Apache Hadoop 2.10+ or 3.1.1+ (3.1.1 for nvidia-docker version 2)
	
	Python 3.6+, Scala 2.12, Java 8 

### Download v0.4.2
* Download [RAPIDS Spark Package](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.4.2/rapids-4-spark_2.12-0.4.2.jar)
* Download RAPIDS cuDF 0.18.2 for your system: 
  * [cuDF 11.0 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.2/cudf-0.18.2-cuda11.jar)
  * [cuDF 10.2 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.2/cudf-0.18.2-cuda10-2.jar)
  * [cuDF 10.1 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.2/cudf-0.18.2-cuda10-1.jar)


## Release v0.4.1
### Download v0.4.1
* Download [RAPIDS Accelerator For Apache Spark v0.4.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.4.1/rapids-4-spark_2.12-0.4.1.jar)
* Download RAPIDS cuDF 0.18.1 for your system:
  * [For CUDA 11.0 & NVIDIA driver 450.36+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda11.jar)
  * [For CUDA 10.2 & NVIDIA driver 440.33+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda10-2.jar)
  * [For CUDA 10.1 & NVIDIA driver 418.87+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda10-1.jar)

### Requirements
Hardware Requirements: 

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)
	
Software Requirements:

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
	
	CUDA & Nvidia Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
	
	Apache Hadoop 2.10+ or 3.1.1+ (3.1.1 for nvidia-docker version 2)
	
	Python 3.6+, Scala 2.12, Java 8 

### Release Notes
This is a patch release based on version 0.4.0 with the following additional fixes:
* Broadcast exchange can fail when job group is set

The release is supported on Apache Spark 3.0.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS and
Google Cloud Platform Dataproc 2.0.

The list of all supported operations is provided [here](supported_ops.md).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md). 


## Release v0.4.0
### Download v0.4.0
* Download [RAPIDS Accelerator For Apache Spark v0.4.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.4.0/rapids-4-spark_2.12-0.4.0.jar)
* Download RAPIDS cuDF 0.18.1 for your system:
  * [For CUDA 11.0 & NVIDIA driver 450.36+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda11.jar)
  * [For CUDA 10.2 & NVIDIA driver 440.33+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda10-2.jar)
  * [For CUDA 10.1 & NVIDIA driver 418.87+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda10-1.jar)

### Requirements
Hardware Requirements: 

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)
	
Software Requirements:

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
	
	CUDA & Nvidia Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
	
	Apache Hadoop 2.10+ or 3.1.1+ (3.1.1 for nvidia-docker version 2)
	
	Python 3.6+, Scala 2.12, Java 8 

### Release Notes
New functionality for the release includes
* Decimal support up to 64 bit, including reading and writing decimal from Parquet (can be enabled
  by setting `spark.rapids.sql.decimalType.enabled` to True)
* Ability for users to provide GPU versions of Scala, Java or Hive UDFs
* Shuffle and sort support for `struct` data types
* `array_contains` for list operations
* `collect_list` and `average` for windowing operations
* Murmur3 `hash` operation 
* Improved performance when reading from DataSource v2 when the source produces data in the Arrow format

This release includes additional performance improvements, including
* RAPIDS Shuffle with UCX performance improvements
* Instructions on how to use [Alluxio caching](get-started/getting-started-alluxio.md) with Spark to
  leverage caching.

The release is supported on Apache Spark 3.0.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS and
Google Cloud Platform Dataproc 2.0.

The list of all supported operations is provided [here](supported_ops.md).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md). 


## Release v0.3.0
### Download v0.3.0
* Download [RAPIDS Accelerator For Apache Spark v0.3.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.3.0/rapids-4-spark_2.12-0.3.0.jar)
* Download RAPIDS cuDF 0.17 for your system:
  * [For CUDA 11.0 & NVIDIA driver 450.36+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.17/cudf-0.17-cuda11.jar)
  * [For CUDA 10.2 & NVIDIA driver 440.33+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.17/cudf-0.17-cuda10-2.jar)
  * [For CUDA 10.1 & NVIDIA driver 418.87+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.17/cudf-0.17-cuda10-1.jar)

### Requirements
Hardware Requirements: 

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)
	
Software Requirements:

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
	
	CUDA & Nvidia Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
	
	Apache Hadoop 2.10+ or 3.1.1+ (3.1.1 for nvidia-docker version 2)
	
	Python 3.6+, Scala 2.12, Java 8 

### Release Notes
This release includes additional performance improvements, including
* Use of per thread default stream to make more efficient use of the GPU
* Further supporting Spark's adaptive query execution, with more rewritten query plans now able to
run on the GPU 
* Performance improvements for reading small Parquet files
* RAPIDS Shuffle with UCX updated to UCX 1.9.0

New functionality for the release includes
* Parquet reading for lists and structs, 
* Lead/lag for windows, and
* Greatest/least operators 

The release is supported on Apache Spark 3.0.0, 3.0.1, Databricks 7.3 ML LTS and Google Cloud
Platform Dataproc 2.0.

The list of all supported operations is provided [here](supported_ops.md).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md). 


## Release v0.2.0
### Download v0.2.0
* Download [RAPIDS Accelerator For Apache Spark v0.2.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.2.0/rapids-4-spark_2.12-0.2.0.jar)
* Download RAPIDS cuDF 0.15 for your system:
  * [For CUDA 11.0 & NVIDIA driver 450.36+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.15/cudf-0.15-cuda11.jar)
  * [For CUDA 10.2 & NVIDIA driver 440.33+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.15/cudf-0.15-cuda10-2.jar)
  * [For CUDA 10.1 & NVIDIA driver 418.87+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.15/cudf-0.15-cuda10-1.jar)

### Requirements
Hardware Requirements: 

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)
	
Software Requirements:

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
	
	CUDA & Nvidia Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1
	
	Apache Hadoop 2.10+ or 3.1.1+ (3.1.1 for nvidia-docker version 2)
	
	Python 3.x, Scala 2.12, Java 8 
	
### Release Notes
This is the second release of the RAPIDS Accelerator for Apache Spark.  Adaptive Query Execution
[SPARK-31412](https://issues.apache.org/jira/browse/SPARK-31412) is a new enhancement that was
included in Spark 3.0 that alters the physical execution plan dynamically to improve the performance
of the query.  The RAPIDS Accelerator v0.2 introduces Adaptive Query Execution (AQE) for GPUs and
leverages columnar processing [SPARK-32332](https://issues.apache.org/jira/browse/SPARK-32332)
starting from Spark 3.0.1.

Another enhancement in v0.2 is improvement in reading small Parquet files.  This feature takes into
account the scenario where input data can be stored across many small files.  By leveraging multiple
CPU threads v0.2 delivers up to 6x performance improvement over the previous release for small
Parquet file reads.

The RAPIDS Accelerator introduces a beta feature that accelerates [Spark shuffle for
GPUs](get-started/getting-started-on-prem.md#enabling-rapidsshufflemanager).  Accelerated
shuffle makes use of high bandwidth transfers between GPUs (NVLink or p2p over PCIe) and leverages
RDMA (RoCE or Infiniband) for remote transfers. 

The list of all supported operations is provided
[here](configs.md#supported-gpu-operators-and-fine-tuning).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md). 


## Release v0.1.0
### Download v0.1.0
* Download [RAPIDS Accelerator For Apache Spark v0.1.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.1.0/rapids-4-spark_2.12-0.1.0.jar)
* Download RAPIDS cuDF 0.14 for your system:
  * [For CUDA 10.2 & NVIDIA driver 440.33+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.14/cudf-0.14-cuda10-2.jar)
  * [For CUDA 10.1 & NVIDIA driver 418.87+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.14/cudf-0.14-cuda10-1.jar)

### Requirements
Hardware Requirements: 
   
    GPU Architecture: NVIDIA Pascal™ or better (Tested on V100 and T4 GPU)

Software Requirements: 

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
    (RHEL 7 support is provided through CentOS 7 builds/installs)

    CUDA & NVIDIA Drivers: 10.1.2 & v418.87+ or 10.2 & v440.33+
    
    Apache Spark 3.0
  
    Apache Hadoop 2.10+ or 3.1.1+ (3.1.1 for nvidia-docker version 2)

    Python 3.x, Scala 2.12, Java 8 





