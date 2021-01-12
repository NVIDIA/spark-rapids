---
layout: page
title: Download
nav_order: 3
---

## Release v0.3.0
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

Hardware Requirements: 

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)
	
Software Requirements:

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
	
	CUDA & Nvidia Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
	
	Apache Hadoop 2.10+ or 3.1.1+ (3.1.1 for nvidia-docker version 2)
	
	Python 3.6+, Scala 2.12, Java 8 

### Download v0.3.0
* [RAPIDS Spark Package](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.3.0/rapids-4-spark_2.12-0.3.0.jar)
* [cuDF 11.0 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.17/cudf-0.17-cuda11.jar)
* [cuDF 10.2 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.17/cudf-0.17-cuda10-2.jar)
* [cuDF 10.1 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.17/cudf-0.17-cuda10-1.jar)

## Release v0.2.0
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

Hardware Requirements: 

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)
	
Software Requirements:

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
	
	CUDA & Nvidia Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1
	
	Apache Hadoop 2.10+ or 3.1.1+ (3.1.1 for nvidia-docker version 2)
	
	Python 3.x, Scala 2.12, Java 8 

### Download v0.2.0
* [RAPIDS Spark Package](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.2.0/rapids-4-spark_2.12-0.2.0.jar)
* [cuDF 11.0 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.15/cudf-0.15-cuda11.jar)
* [cuDF 10.2 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.15/cudf-0.15-cuda10-2.jar)
* [cuDF 10.1 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.15/cudf-0.15-cuda10-1.jar)

## Release v0.1.0

Hardware Requirements: 
   
    GPU Architecture: NVIDIA Pascal™ or better (Tested on V100 and T4 GPU)

Software Requirements: 

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
    (RHEL 7 support is provided through CentOS 7 builds/installs)

    CUDA & NVIDIA Drivers: 10.1.2 & v418.87+ or 10.2 & v440.33+
    
    Apache Spark 3.0
  
    Apache Hadoop 2.10+ or 3.1.1+ (3.1.1 for nvidia-docker version 2)

    Python 3.x, Scala 2.12, Java 8 


### Download v0.1.0
* [RAPIDS Spark Package](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.1.0/rapids-4-spark_2.12-0.1.0.jar)
* [cuDF 10.2 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.14/cudf-0.14-cuda10-2.jar)
* [cuDF 10.1 Package](https://repo1.maven.org/maven2/ai/rapids/cudf/0.14/cudf-0.14-cuda10-1.jar)



