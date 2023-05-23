---
layout: page
title: Archive
nav_order: 15
---
Below are archived releases for RAPIDS Accelerator for Apache Spark.

## Release v23.02.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA P100, V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, Rocky Linux 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.3.0, 3.3.1, Databricks 10.4 ML LTS or 11.3 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v23.02.0
* Download the [RAPIDS
  Accelerator for Apache Spark 23.02.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.02.0/rapids-4-spark_2.12-23.02.0.jar)

This package is built against CUDA 11.8 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 23.02.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.02.0/rapids-4-spark_2.12-23.02.0.jar)
  and [RAPIDS Accelerator for Apache Spark 23.02.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.02.0/rapids-4-spark_2.12-23.02.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-23.02.0.jar.asc rapids-4-spark_2.12-23.02.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Delta Lake MERGE/DELETE/UPDATE (experimental feature, can be enabled with a config flag)
* Function `from_json`
* Hive text table write 
* Databricks 11.3 ML LTS support
* Support batched full join to improve full join's performance
* Qualification and Profiling tool:
  * EMR user tools support for qualification
  * EMR user tools support for bootstrap
  * Updated estimated speedup factors for on-prem, Dataproc, and EMR environments for qualification
  
  
For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

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

### Download v22.12.0
* Download the [RAPIDS
  Accelerator for Apache Spark 22.12.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.12.0/rapids-4-spark_2.12-22.12.0.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 22.12.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.12.0/rapids-4-spark_2.12-22.12.0.jar)
  and [RAPIDS Accelerator for Apache Spark 22.12.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.12.0/rapids-4-spark_2.12-22.12.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-22.12.0.jar.asc rapids-4-spark_2.12-22.12.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Zstandard compression for Parquet and ORC
* Support for Hive text reading
* Improved performance on `like` operations
* Tiered projections for more expressions to optimize performance
* Support for mapInArrow, instr and array_remove operations
* z-ordering capability on Databricks Delta Lake
* Dynamic Partition Pruning (DPP) on Databricks
* Qualification and Profiling tool:
  * Support cost estimations for Dataproc 1.5 and Dataproc2.x
  * Added new Github [repo](https://github.com/NVIDIA/spark-rapids-tools/tree/dev/user_tools) for user tools functionality

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

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

## Release v22.08.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, Rocky Linux 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.2.2, 3.3.0, Databricks 9.1 ML LTS or 10.4 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v22.08.0
* Download the [RAPIDS
  Accelerator for Apache Spark 22.08.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.08.0/rapids-4-spark_2.12-22.08.0.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 22.08.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.08.0/rapids-4-spark_2.12-22.08.0.jar)
  and [RAPIDS Accelerator for Apache Spark 22.08.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.08.0/rapids-4-spark_2.12-22.08.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-22.08.0.jar.asc rapids-4-spark_2.12-22.08.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Rocky Linux 8 support
* Ability to build Spark RAPIDS jars for Java versions 9+
* Zstandard Parquet and ORC read support
* Binary read support from parquet
* Apache Iceberg 0.13 support
* Array function support: array_intersect, array_union, array_except and arrays_overlap
* Support nth_value, first and last in windowing function
* Alluxio auto mount for AWS S3 buckets
* Qualification tool:
  * SQL level qualification
  * Add application details view

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v22.06.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.3.0, Databricks 9.1 ML LTS or 10.4 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v22.06.0
* Download the [RAPIDS
  Accelerator for Apache Spark 22.06.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.06.0/rapids-4-spark_2.12-22.06.0.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 22.06.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.06.0/rapids-4-spark_2.12-22.06.0.jar)
  and [RAPIDS Accelerator for Apache Spark 22.06.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.06.0/rapids-4-spark_2.12-22.06.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-22.06.0.jar.asc rapids-4-spark_2.12-22.06.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Combined cuDF jar and rapids-4-spark jar to a single rapids-4-spark jar.
  The RAPIDS Accelerator jar (rapids-4-spark jar) is the only jar that needs to be passed to Spark.  
  The cuDF jar is now bundled with the rapids-4-spark jar and should not be specified.
* Enable CSV read by default
* Enable regular expression by default
* Enable some float related configurations by default
* Improved ANSI support
* Add a UI for the Qualification tool
* Support function map_filter
* Enable MIG with YARN on Dataproc 2.0
* Changed to ASYNC allocator from ARENA by default

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v22.04.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, Databricks 9.1 ML LTS or 10.4 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v22.04.0
* Download the [RAPIDS
  Accelerator for Apache Spark 22.04.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.04.0/rapids-4-spark_2.12-22.04.0.jar)
* Download the [RAPIDS cuDF 22.04.0 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/22.04.0/cudf-22.04.0-cuda11.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 22.04.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.04.0/rapids-4-spark_2.12-22.04.0.jar)
  and [RAPIDS Accelerator for Apache Spark 22.04.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.04.0/rapids-4-spark_2.12-22.04.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-22.04.0.jar.asc rapids-4-spark_2.12-22.04.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Avro reader for primitive types
* ExistenceJoin support
* ArrayExists support
* GetArrayStructFields support
* Function str_to_map support
* Function percent_rank support
* Regular expression support for function split on string
* Support function approx_percentile in reduction context
* Support function element_at with non-literal index
* Spark cuSpatial UDF

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v22.02.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.0.1, 3.0.2, 3.0.3, 3.1.1, 3.1.2, 3.2.0, 3.2.1, Databricks 7.3 ML LTS or 9.1 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v22.02.0
* Download the [RAPIDS
  Accelerator for Apache Spark 22.02.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.02.0/rapids-4-spark_2.12-22.02.0.jar)
* Download the [RAPIDS cuDF 22.02.0 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/22.02.0/cudf-22.02.0-cuda11.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 22.02.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.02.0/rapids-4-spark_2.12-22.02.0.jar)
  and [RAPIDS Accelerator for Apache Spark 22.02.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.02.0/rapids-4-spark_2.12-22.02.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-22.02.0.jar.asc rapids-4-spark_2.12-22.02.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Parquet reader and writer support for decimal precision up to 38 digits (128-bits)
* Decimal 128-bits casting
    * Casting of decimal 128-bits values in nested types
    * Casting to String from decimal 128-bits
    * Casting from String to decimal 128-bits
* MIG on YARN support
* GPU explain only mode for Spark 3.x and 2.x
* JSON reader support
* Sequence function support
* regexp_extract function support
* Min and max on single-level struct
* CreateMap updates and enable CreateMap by default
* Cast from array to string
* Add regular expression support to regexp_replace function
* Support for conditional joins using libcudf's mixed join feature

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v21.12.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.0.1, 3.0.2, 3.0.3, 3.1.1, 3.1.2, 3.2.0, Databricks 7.3 ML LTS or 9.1 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v21.12.0
* Download the [RAPIDS
  Accelerator for Apache Spark 21.12.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.12.0/rapids-4-spark_2.12-21.12.0.jar)
* Download the [RAPIDS cuDF 21.12.2 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/21.12.2/cudf-21.12.2-cuda11.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 21.12.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.12.0/rapids-4-spark_2.12-21.12.0.jar)
  and [RAPIDS Accelerator for Apache Spark 21.12.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.12.0/rapids-4-spark_2.12-21.12.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-21.12.0.jar.asc rapids-4-spark_2.12-21.12.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Support decimal precision up to 38 digits (128-bits)
* Support stddev on double in window context
* Support CPU row-based UDF
* CreateArray outputs array of struct
* collect_set outputs array of struct
* ORC reader and writer support for decimal precision up to 38 digits (128-bits)
* ORC writer supports array, map, and struct
* Support SampleExec, rlike
* regexp_replace supports more patterns such as replacing null
* ParquetCachedBatchSerializer supports map
* Add function explainPotentialGpuPlan to print GPU query plan in a CPU Spark cluster
* Qualification Tool
    * Detect RDD APIs and JDBC Scan
* Profiling Tool
    * Catch OOM errors and log a hint to increase java heap size
    * Print potential problems

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v21.10.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.0-11.4 & v450.80.02+

	Apache Spark 3.0.1, 3.0.2, 3.0.3, 3.1.1, 3.1.2, 3.2.0, Databricks 7.3 ML LTS or 8.2 ML Runtime, GCP Dataproc 2.0, and Azure Synapse

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v21.10.0
* Download the [RAPIDS
  Accelerator for Apache Spark 21.10.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.10.0/rapids-4-spark_2.12-21.10.0.jar)
* Download the [RAPIDS cuDF 21.10.0 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/21.10.0/cudf-21.10.0-cuda11.jar)

This package is built against CUDA 11.2 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.4.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.2 is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Release Notes
New functionality and performance improvements for this release include:
* Support collect_list and collect_set in group-by aggregation
* Support stddev, percentile_approx in group-by aggregation
* RunningWindow operations on map
* HashAggregate on struct and nested struct
* Sorting on nested structs
* Explode on map, array, struct
* Union-all on map, array and struct of maps
* Parquet writing of map
* ORC reader supports reading map/struct columns
* ORC reader support decimal64
* Qualification Tool
    * Add conjunction and disjunction filters
    * Filtering specific configuration values
    * Filtering user name
    * Reporting nested data types
    * Reporting write data formats
* Profiling Tool
    * Generating structured output format
    * Improved profiling tool performance

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v21.08.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.0-11.4 & v450.80.02+

	Apache Spark 3.0.1, 3.0.2, 3.0.3, 3.1.1, 3.1.2, Databricks 7.3 ML LTS or 8.2 ML Runtime, and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v21.08.0
* Download the [RAPIDS
  Accelerator for Apache Spark 21.08.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.08.0/rapids-4-spark_2.12-21.08.0.jar)
* Download the [RAPIDS cuDF 21.08.2 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/21.08.2/cudf-21.08.2-cuda11.jar)

This package is built against CUDA 11.2 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A30 and A100 GPUs with CUDA 11.0-11.4.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.2 is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Release Notes
New functionality and performance improvements for this release include:
* Handling data sets that spill out of GPU memory for group by and windowing operations
* Running window rank and dense rank operations on the GPU
* Support for the `LEGACY` timestamp
* Unioning of nested structs
* Adoption of UCX 1.11 for improved error handling for RAPIDS Spark Accelerated Shuffle
* Ability to read cached data from the GPU on the supported Databricks runtimes
* Enabling Parquet writing of array data types from the GPU
* Optimized reads for small files for ORC
* Qualification and Profiling Tools
    * Additional filtering capabilities
    * Reporting on data types
    * Reporting on read data formats
    * Ability to run the Qualification tool on Spark 2.x logs
    * Ability to run the tool on Apache Spark 3.x, AWS EMR 6.3.0, Dataproc 2.0, Microsoft Azure, and
      Databricks 7.3 and 8.2 logs
    * Improved Qualification tool performance

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v21.06.2
This is a patch release to address an issue with the plugin in the Databricks 8.2 ML runtime.

Hardware Requirements:

	GPU Models: NVIDIA V100, T4 or A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.0 or 11.2 & v450.80.02+

	Apache Spark 3.0.1, 3.0.2, 3.1.1, 3.1.2, Databricks 7.3 ML LTS or 8.2 ML Runtime, and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v21.06.2
* Download the [RAPIDS
  Accelerator for Apache Spark 21.06.2 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.06.2/rapids-4-spark_2.12-21.06.2.jar)
* Download the [RAPIDS cuDF 21.06.1 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/21.06.1/cudf-21.06.1-cuda11.jar)

This package is built against CUDA 11.2 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on Tesla datacenter GPUs with CUDA 11.0 and 11.2.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.2 is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Release Notes
This release patches the plugin to address a backwards incompatible change to Parquet filters made
by Databricks in the Databricks 8.2 ML runtime.  More information is in [issue
3191](https://github.com/NVIDIA/spark-rapids/issues/3191) in the RAPIDS Spark repository.  See the
[Release v21.06.0](#release-v21060) release notes for more detail about new features in 21.06.

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v21.06.1
This is a patch release to address an issue with the plugin in the Databricks 7.3 ML LTS runtime.

Hardware Requirements:

	GPU Models: NVIDIA V100, T4 or A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.0 or 11.2 & v450.80.02+

	Apache Spark 3.0.1, 3.0.2, 3.1.1, 3.1.2, and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v21.06.1
* Download the [RAPIDS
  Accelerator for Apache Spark 21.06.1 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.06.1/rapids-4-spark_2.12-21.06.1.jar)
* Download the [RAPIDS cuDF 21.06.1 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/21.06.1/cudf-21.06.1-cuda11.jar)

This package is built against CUDA 11.2 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on Tesla datacenter GPUs with CUDA 11.0 and 11.2.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.2 is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Release Notes
This release patches the plugin to address a backwards incompatible change to Parquet filters made
by Databricks in the Databricks 7.3 ML LTS runtime.  More information is in [issue
3098](https://github.com/NVIDIA/spark-rapids/issues/3098) in the RAPIDS Spark repository.  See the
[Release v21.06.0](#release-v21060) release notes for more detail about new features in 21.06.

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v21.06.0
Starting with release 21.06.0, the project is moving to calendar versioning, with the first two
digits representing the year, the second two digits representing the month, and the last digit
representing the patch version of the release.

Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8
	
	CUDA & NVIDIA Drivers*: 11.0 or 11.2 & v450.80.02+
	
	Apache Spark 3.0.1, 3.0.2, 3.1.1, 3.1.2, Databricks 8.2 ML Runtime, and GCP Dataproc 2.0
		
	Python 3.6+, Scala 2.12, Java 8 

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](./FAQ.md#which-distributions-are-supported) section of the FAQ.

### Download v21.06.0
* Download the [RAPIDS
  Accelerator for Apache Spark 21.06.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.06.0/rapids-4-spark_2.12-21.06.0.jar)
* Download the [RAPIDS cuDF 21.06.1 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/21.06.1/cudf-21.06.1-cuda11.jar)

This package is built against CUDA 11.2 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A30 and A100 GPUs with CUDA 11.0 and 11.2.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.2 is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Release Notes
New functionality for this release includes:
* Support for running on Cloudera CDP 7.1.6, CDP 7.1.7 and Databricks 8.2 ML
* New functionality related to arrays:
    * Concatenation of array columns
    * Casting arrays of floats to arrays of doubles
    * Creation of 2D array types
    * Hash partitioning with arrays
    * Explode takes expressions that generate arrays
* New functionality for struct types:
    * Sorting on struct keys
    * Structs with map values
    * Caching of structs
* New windowing functionality:
    * Window lead / lag for arrays
    * Range windows supporting non-timestamp order by expressions
* Enabling large joins that can spill out of memory
* Support for the `concat_ws` operator
* Qualification and Profiling Tools
    * Qualification tool looks at a set of applications to determine if the RAPIDS Accelerator for
      Apache Spark is a good fit
    * Profiling tool to generate information used for debugging and profiling applications

Performance improvements for this release include:
* Moving RAPIDS Shuffle out of beta
* Updates to UCX error handling
* GPUDirect Storage for spilling

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v0.5.0

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

Hardware Requirements:

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS8
	
	CUDA & NVIDIA Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
		
	Python 3.6+, Scala 2.12, Java 8 

### Download v0.5.0
* Download the [RAPIDS Accelerator for Apache Spark v0.5.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.5.0/rapids-4-spark_2.12-0.5.0.jar)
* Download the RAPIDS cuDF 0.19.2 jar for your system:
    * [For CUDA 11.0 & NVIDIA driver 450.36+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.19.2/cudf-0.19.2-cuda11.jar)
    * [For CUDA 10.2 & NVIDIA driver 440.33+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.19.2/cudf-0.19.2-cuda10-2.jar)
    * [For CUDA 10.1 & NVIDIA driver 418.87+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.19.2/cudf-0.19.2-cuda10-1.jar)

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


## Release v0.4.1
### Download v0.4.1
* Download the [RAPIDS Accelerator For Apache Spark v0.4.1 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.4.1/rapids-4-spark_2.12-0.4.1.jar)
* Download the RAPIDS cuDF 0.18.1 jar for your system:
    * [For CUDA 11.0 & NVIDIA driver 450.36+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda11.jar)
    * [For CUDA 10.2 & NVIDIA driver 440.33+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda10-2.jar)
    * [For CUDA 10.1 & NVIDIA driver 418.87+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda10-1.jar)

### Requirements
Hardware Requirements:

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)

Software Requirements:

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
	
	CUDA & NVIDIA Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
		
	Python 3.6+, Scala 2.12, Java 8 

### Release Notes
This is a patch release based on version 0.4.0 with the following additional fixes:
* Broadcast exchange can fail when job group is set

The release is supported on Apache Spark 3.0.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS and
Google Cloud Platform Dataproc 2.0.

The list of all supported operations is provided [here](supported_ops.md).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

**_Note:_** Using NVIDIA driver release 450.80.02, 450.102.04 or 460.32.03 in combination with the
CUDA 10.1 or 10.2 toolkit may result in long read times when reading a file that is snappy
compressed.  In those cases we recommend either running with the CUDA 11.0 toolkit or using a newer
driver.  This issue is resolved in the 0.5.0 and higher releases.

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
	
	CUDA & NVIDIA Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
		
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

**_Note:_** Using NVIDIA driver release 450.80.02, 450.102.04 or 460.32.03 in combination with the
CUDA 10.1 or 10.2 toolkit may result in long read times when reading a file that is snappy
compressed.  In those cases we recommend either running with the CUDA 11.0 toolkit or using a newer
driver.  This issue is resolved in the 0.5.0 and higher releases.

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
	
	CUDA & NVIDIA Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
		
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

**_Note:_** Using NVIDIA driver release 450.80.02, 450.102.04 or 460.32.03 in combination with the
CUDA 10.1 or 10.2 toolkit may result in long read times when reading a file that is snappy
compressed.  In those cases we recommend either running with the CUDA 11.0 toolkit or using a newer
driver.  This issue is resolved in the 0.5.0 and higher releases.

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
	
	CUDA & NVIDIA Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1
		
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

The RAPIDS Accelerator introduces a beta feature that accelerates
[Spark shuffle for GPUs](get-started/getting-started-on-prem.md#enabling-rapids-shuffle-manager).  Accelerated
shuffle makes use of high bandwidth transfers between GPUs (NVLink or p2p over PCIe) and leverages
RDMA (RoCE or Infiniband) for remote transfers.

The list of all supported operations is provided
[here](configs.md#supported-gpu-operators-and-fine-tuning).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

**_Note:_** Using NVIDIA driver release 450.80.02, 450.102.04 or 460.32.03 in combination with the
CUDA 10.1 or 10.2 toolkit may result in long read times when reading a file that is snappy
compressed.  In those cases we recommend either running with the CUDA 11.0 toolkit or using a newer
driver.  This issue is resolved in the 0.5.0 and higher releases.

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

    Python 3.x, Scala 2.12, Java 8



