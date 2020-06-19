---
layout: page
title: Compatability
nav_order: 4
---

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
set [`spark.rapids.sql.hasNans`](configs.md#sql.hasNans) to `false` to run GPU enabled
aggregations on them.

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
[`spark.rapids.sql.format.csv.read.enabled`](configs.md#sql.format.csv.read.enabled) to `false`.
Because the speed up is so large and the issues typically only show up in error conditions we felt
it was worth having the CSV reader enabled by default.

There are also discrepancies/issues with specific types that are detailed below.

### CSV Strings
Writing strings to a CSV file in general for Spark can be problematic unless you can ensure
that your data does not have any line deliminators in it. The GPU accelerated CSV parser
handles quoted line deliminators similar to `multiLine` mode.  But there are still a number
of issues surrounding it and they should be avoided. 

Escaped quote characters `'\"'` are not supported well as described by this
[issue](https://github.com/NVIDIA/spark-rapids/issues/129).

Null values are not respected as described
[here](https://github.com/NVIDIA/spark-rapids/issues/127) even though they are
supported for other types.

### CSV Dates
Parsing a `timestamp` as a `date` does not work. The details are documented in this
[issue](https://github.com/NVIDIA/spark-rapids/issues/122).

Only a limited set of formats are supported when parsing dates.

* `"yyyy-MM-dd"`
* `"yyyy/MM/dd"`
* `"yyyy-MM"`
* `"yyyy/MM"`
* `"MM-yyyy"`
* `"MM/yyyy"`
* `"MM-dd-yyyy"`
* `"MM/dd/yyyy"`

The reality is that all of these formats are supported at the same time. The plugin
will only disable itself if you set a format that it does not support.

### CSV Timestamps
The CSV parser only works for timestamps that are after 1902 and before the end of 2038.
This is documented in this [issue](https://github.com/NVIDIA/spark-rapids/issues/122).

The CSV parser does not support time zones.  It will ignore any trailing time zone
information, despite the format asking for a `XXX` or `[XXX]`. As such it is off by
default and you can enable it by setting 
[`spark.rapids.sql.csvTimestamps.enabled`](configs.md#sql.csvTimestamps.enabled) to `true`. 

The formats supported for timestamps are limited similar to dates.  The first part of
the format must be a supported date format.  The second part must start with a `'T'`
to separate the time portion followed by one of the following formats:

* `HH:mm:ss.SSSXXX`
* `HH:mm:ss[.SSS][XXX]`
* `HH:mm`
* `HH:mm:ss`
* `HH:mm[:ss]`
* `HH:mm:ss.SSS`
* `HH:mm:ss[.SSS]`

Just like with dates all timestamp formats are actually supported at the same time.
The plugin will disable itself if it sees a format it cannot support.

### CSV Floating Point

The CSV parser is not able to parse `Infinity`, `-Infinity`, or `NaN` values.  All of
these are likely to be turned into null values, as described in this
[issue](https://github.com/NVIDIA/spark-rapids/issues/125).

Some floating-point values also appear to overflow but do not for the CPU as described
in this [issue](https://github.com/NVIDIA/spark-rapids/issues/124).

Any number that overflows will not be turned into a null value.

### CSV Integer

Any number that overflows will not be turned into a null value.

## ORC

The ORC format has fairly complete support for both reads and writes. There are only a few known
issues. The first is for reading timestamps and dates around the transition between Julian and
Gregorian calendars as described [here](https://github.com/NVIDIA/spark-rapids/issues/131). A
similar issue exists for writing dates as described
[here](https://github.com/NVIDIA/spark-rapids/issues/139). Writing timestamps, however only
appears to work for dates after the epoch as described
[here](https://github.com/NVIDIA/spark-rapids/issues/140). 

## Parquet

The Parquet format has more configs because there are multiple versions with some compatibility
issues between them. Dates and timestamps are where the known issues exist. 
For reads when `spark.sql.legacy.parquet.datetimeRebaseModeInWrite` is set to `CORRECTED`
[timestamps](https://github.com/NVIDIA/spark-rapids/issues/132) before the transition
between the Julian and Gregorian calendars are wrong, but dates are fine. When 
`spark.sql.legacy.parquet.datetimeRebaseModeInWrite` is set to `LEGACY`, however both dates and
timestamps are read incorrectly before the Gregorian calendar transition as described
[here]('https://github.com/NVIDIA/spark-rapids/issues/133).

When writing `spark.sql.legacy.parquet.datetimeRebaseModeInWrite` is currently ignored as described
[here](https://github.com/NVIDIA/spark-rapids/issues/144).

Apache Spark 3.0 defaults to writing Parquet timestamps in the deprecated INT96 format. The plugin
does not support writing timestamps in the INT96 format, so by default writing timestamp columns to
Parquet will not be GPU-accelerated. If the INT96 timestamp format is not required for
compatibility with other tools then set `spark.sql.parquet.outputTimestampType` to
`TIMESTAMP_MICROS`.

## Timestamps

Spark stores timestamps internally relative to the JVM time zone.  Converting an
arbitrary timestamp between time zones is not currently supported on the GPU. Therefore operations
involving timestamps will only be GPU-accelerated if the time zone used by the JVM is UTC.

## Casting between types

In general, performing `cast` and `ansi_cast` operations on the GPU is compatible with the same operations on the CPU. However, there are some exceptions. For this reason, certain casts are disabled on the GPU by default and require configuration options to be specified to enable them. 

### Float to String

The GPU will use different precision than Java's toString method when converting floating-point data types to strings and this can produce results that differ from the default behavior in Spark. 

To enable this operation on the GPU, set
[`spark.rapids.sql.castFloatToString.enabled`](configs.md#sql.castFloatToString.enabled) to `true`.

### String to Float

Casting from string to floating-point types on the GPU returns incorrect results when the string represents any number in the following ranges. In both cases the GPU returns `Double.MaxValue`. The default behavior in Apache Spark is to return `+Infinity` and `-Infinity`, respectively.

- `1.7976931348623158E308 <= x < 1.7976931348623159E308`
- `-1.7976931348623159E308 < x <= -1.7976931348623158E308` 

Also, the GPU does not support casting from strings containing hex values.

To enable this operation on the GPU, set 
[`spark.rapids.sql.castStringToFloat.enabled`](configs.md#sql.castStringToFloat.enabled) to `true`.
       
### String to Integral Types

The GPU will return incorrect results for strings representing values greater than Long.MaxValue or less than Long.MinValue. The correct behavior would be to return null for these values, but the GPU currently overflows and returns an incorrect integer value.

To enable this operation on the GPU, set
[`spark.rapids.sql.castStringToInteger.enabled`](configs.md#sql.castStringToInteger.enabled) to `true`.

### String to Date

The following formats/patterns are supported on the GPU. Timezone of UTC is assumed.

| Format or Pattern     | Supported on GPU? |
| --------------------- | ----------------- |
| `"yyyy"`              | Yes.              |
| `"yyyy-[M]M"`         | Yes.              |
| `"yyyy-[M]M "`        | Yes.              |
| `"yyyy-[M]M-[d]d"`    | Yes.              |
| `"yyyy-[M]M-[d]d "`   | Yes.              |
| `"yyyy-[M]M-[d]d *"`  | Yes.              |
| `"yyyy-[M]M-[d]d T*"` | Yes.              |
| `"epoch"`             | Yes.              |
| `"now"`               | Yes.              |
| `"today"`             | Yes.              |
| `"tomorrow"`          | Yes.              |
| `"yesterday"`         | Yes.              |

## String to Timestamp

To allow casts from string to timestamp on the GPU, enable the configuration property 
[`spark.rapids.sql.castStringToTimestamp.enabled`](configs.md#sql.castStringToTimestamp.enabled).

Casting from string to timestamp currently has the following limitations. 

| Format or Pattern                                                   | Supported on GPU? |
| ------------------------------------------------------------------- | ------------------|
| `"yyyy"`                                                            | Yes.              |
| `"yyyy-[M]M"`                                                       | Yes.              |
| `"yyyy-[M]M "`                                                      | Yes.              |
| `"yyyy-[M]M-[d]d"`                                                  | Yes.              |
| `"yyyy-[M]M-[d]d "`                                                 | Yes.              |
| `"yyyy-[M]M-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]"` | Partial [1].      |
| `"yyyy-[M]M-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]"` | Partial [1].      |
| `"[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]"`                | Partial [1].      |
| `"T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]"`               | Partial [1].      |
| `"epoch"`                                                           | Yes.              |
| `"now"`                                                             | Yes.              |
| `"today"`                                                           | Yes.              |
| `"tomorrow"`                                                        | Yes.              |
| `"yesterday"`                                                       | Yes.              |

- [1] The timestamp portion must be complete in terms of hours, minutes, seconds, and
 milliseconds, with 2 digits each for hours, minutes, and seconds, and 6 digits for milliseconds. 
 Only timezone 'Z' (UTC) is supported. Casting unsupported formats will result in null values. 
 
