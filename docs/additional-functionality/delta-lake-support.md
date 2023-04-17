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

Delta Lake write acceleration is enabled by default. To disable acceleration of Delta Lake
writes, set spark.rapids.sql.format.delta.write.enabled=false.

### Delta Lake Versions Supported For Write

The RAPIDS Accelerator supports the following software configurations for accelerating
Delta Lake writes:
- Delta Lake version 2.0.1 on Apache Spark 3.2.x
- Delta Lake version 2.1.1 and 2.2.0 on Apache Spark 3.3.x
- Delta Lake on Databricks 10.4 LTS
- Delta Lake on Databricks 11.3 LTS

Delta Lake writes will not be accelerated on Spark 3.1.x or earlier.

### Write Operations Supported

Very limited support is provided for GPU acceleration of table writing. Table writes are only
GPU accelerated if the table is being created via the Spark Catalyst `SaveIntoDataSourceCommand`
operation which is typically triggered via the DataFrame `write` API, e.g.:
`data.write.format("delta").save(...)`.

Table creation from selection, table insertion from SQL, and table merges are not currently
GPU accelerated. These operations will fallback to the CPU.

#### Automatic Optimization of Writes

Delta Lake on Databricks has
[automatic optimization](https://docs.databricks.com/optimizations/auto-optimize.html)
features for optimized writes and automatic compaction.

Optimized writes are supported only on Databricks platforms. The algorithm used is similar but
not identical to the Databricks version. The following table describes configuration settings
that control the operation of the optimized write.

| Configuration                                               | Default | Description                                                                                |
|-------------------------------------------------------------|---------|--------------------------------------------------------------------------------------------|
| spark.databricks.delta.optimizeWrite.binSize                | 512     | Target uncompressed partition size in megabytes                                            |
| spark.databricks.delta.optimizeWrite.smallPartitionFactor   | 0.5     | Merge partitions smaller than this factor multiplied by the target partition size          |
| spark.databricks.delta.optimizeWrite.mergedPartitionFactor  | 1.2     | Avoid combining partitions larger than this factor multiplied by the target partition size |

Automatic compaction is supported only on Databricks platforms. The algorithm is similar but 
not identical to the Databricks version. The following table describes configuration settings
that control the operation of automatic compaction.

| Configuration                                                       | Default | Description                                                                                            |
|---------------------------------------------------------------------|---------|--------------------------------------------------------------------------------------------------------|
| spark.databricks.delta.autoCompact.enabled                          | false   | Enable/disable auto compaction for writes to Delta directories                                         |
| spark.databricks.delta.properties.defaults.autoOptimize.autoCompact | false   | Whether to enable auto compaction by default, if spark.databricks.delta.autoCompact.enabled is not set |
| spark.databricks.delta.autoCompact.minNumFiles                      | 50      | Minimum number of files in the Delta directory before which auto optimize does not begin compaction    |

Note that optimized write support requires round-robin partitioning of the data, and round-robin
partitioning requires sorting across all columns for deterministic operation. If the GPU cannot
support sorting a particular column type in order to support the round-robin partitioning, the
Delta Lake write will fallback to the CPU.

### RapidsDeltaWrite Node in Query Plans

A side-effect of performing a GPU accelerated Delta Lake write is a new node will appear in the
query plan, RapidsDeltaWrite. Normally the writing of Delta Lake files is not represented by a
dedicated node in query plans, as it is implicitly covered by higher-level operations such as
SaveIntoDataSourceCommand that wrap the entire query along with the write operation afterwards.
The RAPIDS Accelerator places a node in the plan being written to mark the point at which the
write occurs and adds statistics showing the time spent performing the low-level write operation.

## Merging Into Delta Lake Tables

Delta Lake merge acceleration is experimental and is disabled by default. To enable acceleration
of Delta Lake merge operations, set spark.rapids.sql.command.MergeIntoCommand=true and also set
spark.rapids.sql.command.MergeIntoCommandEdge=true on Databricks platforms.

Merging into Delta Lake tables via the SQL `MERGE INTO` statement or via the DeltaTable `merge`
API on non-Databricks platforms is supported.

### Limitations with DeltaTable `merge` API on non-Databricks Platforms

For non-Databricks platforms, the DeltaTable `merge` API directly instantiates a CPU
`MergeIntoCommand` instance and invokes it. This does not go through the normal Spark Catalyst
optimizer, and the merge operation will not be visible in the Spark SQL UI on these platforms.
Since the Catalyst optimizer is bypassed, the RAPIDS Accelerator cannot replace the operation
with a GPU accelerated version. As a result, DeltaTable `merge` operations on non-Databricks
platforms will not be GPU accelerated. In those cases the query will need to be modified to use
a SQL `MERGE INTO` statement instead.

### RapidsProcessDeltaMergeJoin Node in Query Plans

A side-effect of performing GPU accelerated Delta Lake merge operations is a new node will appear
in the query plan, RapidsProcessDeltaMergeJoin. Normally the Delta Lake merge is performed via
a join and then post-processing of the join via a MapPartitions node. Instead the GPU performs
the join post-processing via this new RapidsProcessDeltaMergeJoin node.

## Delete Operations on Delta Lake Tables

Delta Lake delete acceleration is experimental and is disabled by default. To enable acceleration
of Delta Lake delete operations, set spark.rapids.sql.command.DeleteCommand=true and also set
spark.rapids.sql.command.DeleteCommandEdge=true on Databricks platforms.

Deleting data from Delta Lake tables via the SQL `DELETE FROM` statement or via the DeltaTable
`delete` API is supported.

### num_affected_rows Difference with Databricks

The Delta Lake delete command returns a single row result with a `num_affected_rows` column.
When entire partition files in the table are deleted, the open source Delta Lake and RAPIDS
Acclerator implementations of delete can return -1 for `num_affected_rows` since it could be
expensive to open the files and produce an accurate row count. Databricks changed the behavior
of delete operations that delete entire partition files to return the actual row count.
This is only a difference in the statistics of the operation, and the table contents will still
be accurately deleted with the RAPIDS Accelerator.

## Update Operations on Delta Lake Tables

Delta Lake update acceleration is experimental and is disabled by default. To enable acceleration
of Delta Lake update operations, set spark.rapids.sql.command.Updatecommand=true and also set
spark.rapids.sql.command.UpdateCommandEdge=true on Databricks platforms.

Updating data from Delta Lake tables via the SQL `UPDATE` statement or via the DeltaTable
`update` API is supported.
