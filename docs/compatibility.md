# RAPIDS Accelerator for Apache Spark Compatibility with Apache Spark

The SQL plugin tries to produce results that are bit for bit identical with Apache Spark.
There are a number of cases where there are some differences. In most cases operators
that produce different results are off by default, and you can look at the
[configs](configs.md) for more information on how to enable them.  In some cases
we felt that enabling the incompatibility by default was worth the performance gain. All
of those operators can be disabled through configs if it becomes a problem. Please also look
at the current list of
[bugs](https://github.com/NVIDIA/spark-rapids/issues?q=is%3Aopen+is%3Aissue+label%3Abug)
which are typically incompatibilities that we have not yet addressed.

## Ordering of Output

There are some operators where Spark does not guarantee the order of the output.
These are typically things like aggregates and joins that may use a hash to distribute the work
load among downstream tasks. In these cases the plugin does not guarantee that it will
produce the same output order that Spark does. In cases such as an `order by` operation
where the ordering is explicit the plugin will produce an ordering that is compatible with
Spark's guarantee. It may not be 100% identical if the ordering is ambiguous.

The one known issue with this is [a bug](https://github.com/NVIDIA/spark-rapids/issues/84) where
`-0.0` and `0.0` compare as equal with the GPU plugin enabled but on the CPU `-0.0 < 0.0`.

## Floating Point

For most basic floating point operations like addition, subtraction, multiplication, and division
the plugin will produce a bit for bit identical result as Spark does. For other functions like `sin`,
`cos`, etc. the output may be different, but within the rounding error inherent in floating point
calculations. The ordering of operations to calculate the value may differ between
the underlying JVM implementation used by the CPU and the C++ standard library implementation
used by the GPU.

For aggregations the underlying implementation is doing the aggregations in parallel and due to
race conditions within the computation itself the result may not be the same each time the query is
run. This is inherent in how the plugin speeds up the calculations and cannot be "fixed." If
a query joins on a floating point value, which is not wise to do anyways,
and the value is the result of a floating point aggregation then the join may fail to work
properly with the plugin but would have worked with plain Spark. Because of this most
floating point aggregations are off by default but can be enabled with the config
[`spark.rapids.sql.variableFloatAgg.enabled`](configs.md#sql.variableFloatAgg.enabled).

Additionally, some aggregations on floating point columns that contain NaNs can produce
incorrect results. More details on this behavior can be found
[here](https://github.com/NVIDIA/spark-rapids/issues/87)
and in this cudf [feature request](https://github.com/rapidsai/cudf/issues/4753).
If it is known with certainty that the floating point columns do not contain NaNs,
set `spark.rapids.sql.hasNans` to `false` to run GPU enabled aggregations on them.

## Unicode

Spark delegates Unicode operations to the underlying JVM. Each version of Java complies with a
specific version of the Unicode standard. The SQL plugin does not use the JVM for Unicode support
and is compatible with Unicode version 12.1. Because of this there may be corner cases where
Spark will produce a different result compared to the plugin.

## CSV Reading

Spark is very strict when reading CSV and if the data does not conform with the expected format
exactly it will result in a `null` value. The underlying parser that the SQL plugin uses is much
more lenient. If you have badly formatted CSV data you may get data back instead of nulls.
If this is a problem you can disable the CSV reader by setting the config 
[`spark.rapids.sql.input.CSVScan`](configs.md#sql.input.CSVScan) to `false`. Because the speed up
is so large and the issues only show up in error conditions we felt it was worth having the CSV
reader enabled by default.

## Timestamps

Spark stores timestamps internally relative to the JVM time zone.  Converting an
arbitrary timestamp between time zones is not currently supported on the GPU. Therefore operations
involving timestamps will only be GPU-accelerated if the time zone used by the JVM is UTC.

Apache Spark 3.0 defaults to writing Parquet timestamps in the deprecated INT96 format. The plugin
does not support writing timestamps in the INT96 format, so by default writing timestamp columns to
Parquet will not be GPU-accelerated. If the INT96 timestamp format is not required for
compatibility with other tools then set `spark.sql.parquet.outputTimestampType` to
`TIMESTAMP_MICROS`.