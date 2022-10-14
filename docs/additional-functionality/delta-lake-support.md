---
layout: page
title: Delta Lake Support
parent: Additional Functionality
nav_order: 8
---

# Delta Lake Support

The RAPIDS Accelerator for Apache Spark provides limited support for
[Delta Lake](https://delta.io) tables.
This document details the Delta Lake features that are supported.

## Reading Delta Lake Tables

### Data Queries

Delta Lake scans of the underlying Parquet files are presented in the query as normal Parquet
reads, so the Parquet reads will be accelerated in the same way raw Parquet file reads are
accelerated.

### Metadata Queries

Reads of Delta Lake metadata, i.e.: the Delta log detailing the history of snapshots, will not
be GPU accelerated. The CPU will continue to process metadata queries on Delta Lake tables.

## Writing Delta Lake Tables

Delta Lake write acceleration is currently experimental and disabled by default. To enable
acceleration of Delta Lake writes, set
[spark.rapids.sql.format.delta.write.enabled](configs.md#sql.format.delta.write.enabled)=true.

### Delta Lake Versions Supported For Write

The RAPIDS Accelerator supports the following software configurations for accelerating
Delta Lake writes:
- Delta Lake version 2.0.x on Apache Spark 3.2.x
- Delta Lake version 2.1.x on Apache Spark 3.3.x
- Delta Lake on Databricks 10.4 LTS

Delta Lake writes will not be accelerated on Spark 3.1.x or earlier.

### Write Operations Supported

Very limited support is provided for GPU acceleration of table writing. Table writes are only
GPU accelerated if the table is being created via the Spark Catalyst `SaveIntoDataSourceCommand`
operation which is typically triggered via the DataFrame `write` API, e.g.:
`data.write.format("delta").save(...)`.

Table creation from selection, table insertion from SQL, and table merges are not currently
GPU accelerated. These operations will fallback to the CPU.

[Automatic optimization](https://docs.databricks.com/optimizations/auto-optimize.html)
during Delta Lake writes is not supported. Write operations that are configured to
automatically optimize or automatically compact will fallback to the CPU.
