---
layout: page
title: Iceberg per-table session-level scan options
parent: Additional Functionality
nav_order: 11
---

# Iceberg per-table session-level scan options

Iceberg's split-planning knobs (`read.split.target-size`,
`read.split.planning-lookback`, `read.split.open-file-cost`) are normally tuned
either by editing the table properties at write time or by passing
`.option(...)` at every DataFrame read site. Both are awkward when an admin
wants to tune scan behavior across an existing Spark session — for example
before a benchmark run, or to try different split sizes against existing tables
without modifying them.

The RAPIDS Accelerator ships a thin Iceberg session-catalog wrapper that lets
these options be set per-table via session conf. The wrapper is opt-in and a
pure pass-through for any table whose conf is not set, so enabling it does not
change behavior for tables you do not explicitly tune.

## When to use it

Use this when you want to:

- override Iceberg scan options for a specific table without modifying the
  table itself or recompiling the application; or
- A/B test split-size / lookback tunings across runs by toggling a session
  conf instead of rewriting tables.

If you are happy with the iceberg defaults, or you set scan options at the
DataFrame level via `.option(...)`, you do not need this feature.

## Enabling

Replace Iceberg's session catalog with the rapids drop-in. For the default
catalog (`spark_catalog`):

```
--conf spark.sql.catalog.spark_catalog=com.nvidia.spark.rapids.iceberg.spark.RapidsSparkSessionCatalog
--conf spark.sql.catalog.spark_catalog.type=hadoop
--conf spark.sql.catalog.spark_catalog.warehouse=s3://my-bucket/warehouse/
--conf spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

The `io-impl=org.apache.iceberg.aws.s3.S3FileIO` line is required when the
warehouse lives on S3 — without it, iceberg falls back to the hadoop
`FileSystem` API and you have to deal with `s3a://` URI rewrites yourself.
Drop the line for non-S3 warehouses.

For a non-default Iceberg catalog (e.g. one you have configured under
`spark.sql.catalog.<name>`), replace the catalog class with
`com.nvidia.spark.rapids.iceberg.spark.RapidsSparkCatalog` instead:

```
--conf spark.sql.catalog.my_catalog=com.nvidia.spark.rapids.iceberg.spark.RapidsSparkCatalog
--conf spark.sql.catalog.my_catalog.type=hadoop
--conf spark.sql.catalog.my_catalog.warehouse=s3://my-bucket/warehouse/
--conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

## Setting per-table options

Per-table overrides use the prefix
`spark.rapids.iceberg.<catalog>.<namespace>.<table>.<suffix>`. Three suffixes
are recognized:

| Session-conf suffix             | Iceberg read option                    | Iceberg `TableProperties` key   | Type        |
| ------------------------------- | -------------------------------------- | ------------------------------- | ----------- |
| `read-split-target-size`        | `SparkReadOptions.SPLIT_SIZE`          | `read.split.target-size`        | bytes (long)|
| `read-split-planning-lookback`  | `SparkReadOptions.LOOKBACK`            | `read.split.planning-lookback`  | int         |
| `read-split-open-file-cost`     | `SparkReadOptions.FILE_OPEN_COST`      | `read.split.open-file-cost`     | bytes (long)|

The suffix names mirror the iceberg `TableProperties` keys (the token after
`read.split.`) with `-` instead of `.`, so the dot-separated session-conf
path remains unambiguous.

`<catalog>` is the Spark catalog name, `<namespace>` is the iceberg namespace
(`default` for hadoop catalogs that have not been organized into namespaces),
and `<table>` is the iceberg table name.

### Example

Tune split sizing for the 6 NDS fact tables under the default catalog:

```
--conf spark.rapids.iceberg.spark_catalog.default.store_sales.read-split-target-size=2147483648
--conf spark.rapids.iceberg.spark_catalog.default.store_sales.read-split-planning-lookback=1000
--conf spark.rapids.iceberg.spark_catalog.default.store_returns.read-split-target-size=2147483648
--conf spark.rapids.iceberg.spark_catalog.default.store_returns.read-split-planning-lookback=1000
--conf spark.rapids.iceberg.spark_catalog.default.catalog_sales.read-split-target-size=2147483648
--conf spark.rapids.iceberg.spark_catalog.default.catalog_sales.read-split-planning-lookback=1000
--conf spark.rapids.iceberg.spark_catalog.default.catalog_returns.read-split-target-size=2147483648
--conf spark.rapids.iceberg.spark_catalog.default.catalog_returns.read-split-planning-lookback=1000
--conf spark.rapids.iceberg.spark_catalog.default.web_sales.read-split-target-size=2147483648
--conf spark.rapids.iceberg.spark_catalog.default.web_sales.read-split-planning-lookback=1000
--conf spark.rapids.iceberg.spark_catalog.default.web_returns.read-split-target-size=2147483648
--conf spark.rapids.iceberg.spark_catalog.default.web_returns.read-split-planning-lookback=1000
```

## Precedence

Explicit DataFrame `.option(...)` calls always win over session-conf
overrides:

```scala
spark.read.format("iceberg")
  .option(SparkReadOptions.SPLIT_SIZE, 268435456L)  // wins; 256 MiB
  .load("default.store_sales")
```

Tables for which no `spark.rapids.iceberg.<…>.*` conf is set behave exactly
as if the rapids catalog wrapper were not in use.

## Caveats

- Suffixes other than the three listed above are rejected with an
  `IllegalArgumentException` on the first scan of the matching table.
  Mistyping a key (e.g. `read.split.target-size` with dots, or `target-size`
  without the `read-split-` prefix) is therefore caught loudly rather than
  being a silent no-op.
- The catalog name, namespace components, and table name are joined with `.`
  to form the conf key prefix, so a `.` inside any of those identifiers makes
  the prefix ambiguous (`catalog=hadoop.prod` + `namespace=[ns]` + `table=tbl`
  produces the same prefix as `catalog=hadoop` + `namespace=[prod, ns]` +
  `table=tbl`). Tables with such identifiers stay as a pure pass-through as
  long as no `spark.rapids.iceberg.<…>.*` conf is set for them. If a matching
  conf is set, the wrapper throws an `IllegalArgumentException` from
  `RapidsSparkTable` rather than silently picking the wrong override; in
  that case, either rename the identifier or set the iceberg `read.split.*`
  table property directly.
- The wrapper applies to scans only. Writes go through the underlying iceberg
  `SparkTable.newWriteBuilder` unchanged.
- The session-conf lookup happens once per `newScanBuilder(options)` call (a
  single `SparkSession.conf` read per option key) and is cheap; setting many
  per-table overrides has no measurable cost on the scan path.
