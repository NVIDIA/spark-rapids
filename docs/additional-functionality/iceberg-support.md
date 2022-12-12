---
layout: page
title: Apache Iceberg Support
parent: Additional Functionality
nav_order: 7
---

# Apache Iceberg Support

The RAPIDS Accelerator for Apache Spark provides limited support for
[Apache Iceberg](https://iceberg.apache.org) tables.
This document details the Apache Iceberg features that are supported.

## Apache Iceberg Versions

The RAPIDS Accelerator supports Apache Iceberg 0.13.x. Earlier versions of Apache Iceberg are
not supported.

> **Note!**
> Apache Iceberg in Databricks is not supported by the RAPIDS Accelerator.

## Reading Tables

### Metadata Queries

Reads of Apache Iceberg metadata, i.e.: the `history`, `snapshots`, and other metadata tables
associated with a table, will not be GPU-accelerated. The CPU will continue to process these
metadata-level queries.

### Row-level Delete and Update Support

Apache Iceberg supports row-level deletions and updates. Tables that are using a configuration of
`write.delete.mode=merge-on-read` are not supported.

### Schema Evolution

Columns that are added and removed at the top level of the table schema are supported. Columns
that are added or removed within struct columns are not supported.

### Data Formats

Apache Iceberg can store data in various formats. Each section below details the levels of support
for each of the underlying data formats.

#### Parquet

Data stored in Parquet is supported with the same limitations for loading data from raw Parquet
files. See the [Input/Output](../supported_ops.md#inputoutput) documentation for details. The
following compression codecs applied to the Parquet data are supported:
- gzip (Apache Iceberg default)
- snappy
- uncompressed
- zstd

#### ORC

The RAPIDS Accelerator does not support Apache Iceberg tables using the ORC data format.

#### Avro

The RAPIDS Accelerator does not support Apache Iceberg tables using the Avro data format.


### Reader Split Size

The maximum number of bytes to pack into a single partition when reading files on Spark is normally
controlled by the config `spark.sql.files.maxPartitionBytes`. But on Iceberg that doesn't apply.
Iceberg has its own configs to control the split size. See the read options in the
 [Iceberg Runtime Configuration](https://iceberg.apache.org/docs/latest/spark-configuration/#runtime-configuration)
documentation for details. One example is to use the `split-size` reader option like:
```scala
spark.read.option("split-size", "24217728").table("someTable")
```

## Writing Tables

The RAPIDS Accelerator for Apache Spark does not accelerate Apache Iceberg writes. Writes
to Iceberg tables will be processed by the CPU.
