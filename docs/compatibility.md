---
layout: page
title: Compatibility
nav_order: 5
---

# RAPIDS Accelerator for Apache Spark Compatibility with Apache Spark

The SQL plugin tries to produce results that are bit for bit identical with Apache Spark.  There are
a number of cases where there are some differences. In most cases operators that produce different
results are off by default, and you can look at the [configs](configs.md) for more information on
how to enable them.  In some cases we felt that enabling the incompatibility by default was worth
the performance gain. All of those operators can be disabled through configs if it becomes a
problem. Please also look at the current list of
[bugs](https://github.com/NVIDIA/spark-rapids/issues?q=is%3Aopen+is%3Aissue+label%3Abug) which are
typically incompatibilities that we have not yet addressed.

## Ordering of Output

There are some operators where Spark does not guarantee the order of the output.
These are typically things like aggregates and joins that may use a hash to distribute the work
load among downstream tasks. In these cases the plugin does not guarantee that it will
produce the same output order that Spark does. In cases such as an `order by` operation
where the ordering is explicit the plugin will produce an ordering that is compatible with
Spark's guarantee. It may not be 100% identical if the ordering is ambiguous.

In versions of Spark prior to 3.1.0 `-0.0` is always < `0.0` but in 3.1.0 and above this is
not true for sorting. For all versions of the plugin `-0.0` == `0.0` for sorting.

Spark's sorting is typically a [stable](https://en.wikipedia.org/wiki/Sorting_algorithm#Stability)
sort. Sort stability cannot be guaranteed in distributed work loads because the order in which
upstream data arrives to a task is not guaranteed. Sort stability is only
guaranteed in one situation which is reading and sorting data from a file using a single
task/partition. The RAPIDS Accelerator does an unstable
[out of core](https://en.wikipedia.org/wiki/External_memory_algorithm) sort by default. This
simply means that the sort algorithm allows for spilling parts of the data if it is larger than
can fit in the GPU's memory, but it does not guarantee ordering of rows when the ordering of the
keys is ambiguous. If you do rely on a stable sort in your processing you can request this by
setting [spark.rapids.sql.stableSort.enabled](additional-functionality/advanced_configs.md#sql.stableSort.enabled) to `true` and
RAPIDS will try to sort all the data for a given task/partition at once on the GPU. This may change
in the future to allow for a spillable stable sort.

## Floating Point

For most basic floating-point operations like addition, subtraction, multiplication, and division
the plugin will produce a bit for bit identical result as Spark does. For other functions like
`sin`, `cos`, etc. the output may be different, but within the rounding error inherent in
floating-point calculations. The ordering of operations to calculate the value may differ between the
underlying JVM implementation used by the CPU and the C++ standard library implementation used by
the GPU.

In the case of `round` and `bround` the results can be off by more because they can enlarge the
difference. This happens in cases where a binary floating-point representation cannot exactly
capture a decimal value. For example `1.025` cannot exactly be represented and ends up being closer
to `1.02499`. The Spark implementation of `round` converts it first to a decimal value with complex
logic to make it `1.025` and then does the rounding.  This results in `round(1.025, 2)` under pure
Spark getting a value of `1.03` but under the RAPIDS accelerator it produces `1.02`. As a side note
Python will produce `1.02`, Java does not have the ability to do a round like this built in, but if
you do the simple operation of `Math.round(1.025 * 100.0)/100.0` you also get `1.02`.

For the `degrees` functions, Spark's implementation relies on Java JDK's built-in functions `Math.toDegrees`. It is `angrad * 180.0 / PI` in Java 8 while `angrad * (180d / PI)` in Java 9+. So their results will differ depending on the JDK runtime versions when considering overflow. The RAPIDS Accelerator follows the behavior of Java 9+. Therefore, with JDK 8 or below, the `degrees` on GPU will not overflow on some very large numbers while the CPU version does.

For aggregations the underlying implementation is doing the aggregations in parallel and due to race
conditions within the computation itself the result may not be the same each time the query is
run. This is inherent in how the plugin speeds up the calculations and cannot be "fixed." If a query
joins on a floating point value, which is not wise to do anyways, and the value is the result of a
floating point aggregation then the join may fail to work properly with the plugin but would have
worked with plain Spark. This is behavior is enabled by default but can be disabled with the config
[`spark.rapids.sql.variableFloatAgg.enabled`](additional-functionality/advanced_configs.md#sql.variableFloatAgg.enabled).

### `0.0` vs `-0.0`

Floating point allows zero to be encoded as `0.0` and `-0.0`, but the IEEE standard says that they
should be interpreted as the same. Most databases normalize these values to always be `0.0`. Spark
does this in some cases but not all as is documented
[here](https://issues.apache.org/jira/browse/SPARK-32110). The underlying implementation of this
plugin treats them as the same for essentially all processing. This can result in some differences
with Spark for operations, prior to Spark 3.1.0, like sorting, and distinct count.  There are still
differences with [joins, and comparisons](https://github.com/NVIDIA/spark-rapids/issues/294) even
after Spark 3.1.0.

We do not disable operations that produce different results due to `-0.0` in the data because it is
considered to be a rare occurrence.

### `NaN` vs `NaN`

Apache Spark does not have a consistent way to handle `NaN` comparison. Sometimes, all `NaN` are
considered as one unique value while other times they can be treated as different. The outcome of
`NaN` comparison can differ in various operations and also changed between Spark versions.
The RAPIDS Accelerator tries to match its output with Apache Spark except for a few operation(s) listed below:
 - `IN` SQL expression: `NaN` can be treated as different values in Spark 3.1.2 and
 prior versions, see [SPARK-36792](https://issues.apache.org/jira/browse/SPARK-36792) for more details.
The RAPIDS Accelerator compares `NaN` values as equal for this operation which matches
the behavior of Apache Spark 3.1.3 and later versions.


## Decimal Support

Apache Spark supports decimal values with a precision up to 38. This equates to 128-bits.
When processing the data, in most cases, it is temporarily converted to Java's `BigDecimal` type
which allows for effectively unlimited precision. Overflows will be detected whenever the
`BigDecimal` value is converted back into the Spark decimal type.

The RAPIDS Accelerator does not implement a GPU equivalent of `BigDecimal`, but it does implement
computation on 256-bit values to allow the detection of overflows. The points at which overflows
are detected may differ between the CPU and GPU. Spark gives no guarantees that overflows are
detected if an intermediate value could overflow the original decimal type during computation
but the final value does not (e.g.: a sum of values with many large positive values followed by
many large negative values). Spark injects overflow detection at various points during aggregation,
and these points can fluctuate depending on cluster shape and number of shuffle partitions.

## Unicode

Spark delegates Unicode operations to the underlying JVM. Each version of Java complies with a
specific version of the Unicode standard. The SQL plugin does not use the JVM for Unicode support
and is compatible with Unicode version 12.1. Because of this there may be corner cases where Spark
will produce a different result compared to the plugin.

## CSV Reading

Spark allows for stripping leading and trailing white space using various options that are off by
default. The plugin will strip leading and trailing space for all values except strings.

There are also discrepancies/issues with specific types that are detailed below.

### CSV Strings
Writing strings to a CSV file in general for Spark can be problematic unless you can ensure that
your data does not have any line deliminator in it. The GPU accelerated CSV parser handles quoted
line deliminators similar to `multiLine` mode.  But there are still a number of issues surrounding
it and they should be avoided.

Escaped quote characters `'\"'` are not supported well as described by this
[issue](https://github.com/NVIDIA/spark-rapids/issues/129).

The GPU accelerated CSV parser does not replace invalid UTF-8 characters with the Unicode
replacement character �.  Instead it just passes them through as described in this
[issue](https://github.com/NVIDIA/spark-rapids/issues/9560).

### CSV Dates

Only a limited set of formats are supported when parsing dates.

* `"yyyy-MM-dd"`
* `"yyyy/MM/dd"`
* `"yyyy-MM"`
* `"yyyy/MM"`
* `"MM-yyyy"`
* `"MM/yyyy"`
* `"MM-dd-yyyy"`
* `"MM/dd/yyyy"`
* `"dd-MM-yyyy"`
* `"dd/MM/yyyy"`

### CSV Timestamps
The CSV parser does not support time zones.  It will ignore any trailing time zone information,
despite the format asking for a `XXX` or `[XXX]`. The CSV parser does not support the `TimestampNTZ`
type and will fall back to CPU if `spark.sql.timestampType` is set to `TIMESTAMP_NTZ` or if an 
explicit schema is provided that contains the `TimestampNTZ` type.

The formats supported for timestamps are limited similar to dates.  The first part of the format
must be a supported date format.  The second part must start with a `'T'` to separate the time
portion followed by one of the following formats:

* `HH:mm:ss.SSSXXX`
* `HH:mm:ss[.SSS][XXX]`
* `HH:mm:ss[.SSSXXX]`
* `HH:mm`
* `HH:mm:ss`
* `HH:mm[:ss]`
* `HH:mm:ss.SSS`
* `HH:mm:ss[.SSS]`

Just like with dates all timestamp formats are actually supported at the same time.  The plugin will
disable itself if it sees a format it cannot support.

Invalid timestamps in Spark, ones that have the correct format, but the numbers produce invalid
dates or times, can result in an exception by default and how they are parsed can be controlled
through a config. The RAPIDS Accelerator does not support any of this and will produce an incorrect
date. Typically, one that overflowed.

### CSV Floating Point

Parsing floating-point values has the same limitations as [casting from string to float](#string-to-float).

Also parsing of some values will not produce bit for bit identical results to what the CPU does.
They are within round-off errors except when they are close enough to overflow to Inf or -Inf which
then results in a number being returned when the CPU would have returned null.

### CSV ANSI day time interval
This type was added in as a part of Spark 3.3.0, and it's not supported on Spark versions before 3.3.0.
Apache Spark can [overflow](https://issues.apache.org/jira/browse/SPARK-38520) when reading ANSI day time interval values.
The RAPIDS Accelerator does not overflow and as such is not bug for bug compatible with Spark in this case.

Interval string in csv|Spark reads to|The RAPIDS Accelerator reads to|Comments|
-----|-------------------------|-----------------|-----------|
interval '106751992' day| INTERVAL '-106751990' DAY | NULL| Spark issue|
interval '2562047789' hour| INTERVAL '-2562047787' HOUR | NULL| Spark issue|

There are two valid textual representations in CSV: the ANSI style and the HIVE style, e.g:

SQL Type|An instance of ANSI style|An instance of HIVE style|
-----|-------------------------------------------|-----------------|
INTERVAL DAY | INTERVAL '100' DAY TO SECOND              | 100|
INTERVAL DAY TO HOUR | INTERVAL '100 10' DAY TO HOUR             | 100 10|
INTERVAL DAY TO MINUTE | INTERVAL '100 10:30' DAY TO MINUTE        | 100 10:30|
INTERVAL DAY TO SECOND | INTERVAL '100 10:30:40.999999' DAY TO SECOND | 100 10:30:40.999999|
INTERVAL HOUR | INTERVAL '10' HOUR                        | 10|
INTERVAL HOUR TO MINUTE | INTERVAL '10:30' HOUR TO MINUTE           | 10:30|
INTERVAL HOUR TO SECOND | INTERVAL '10:30:40.999999' HOUR TO SECOND | 10:30:40.999999|
INTERVAL MINUTE | INTERVAL '30' MINUTE                      | 30|
INTERVAL MINUTE TO SECOND | INTERVAL '30:40.999999' MINUTE TO SECOND  | 30:40.999999|
INTERVAL SECOND | INTERVAL '40.999999' SECOND               | 40.999999|

Currently, the RAPIDS Accelerator only supports the ANSI style.

## Hive Text File

Hive text files are very similar to CSV, but not exactly the same.

### Hive Text File Floating Point

Parsing floating-point values has the same limitations as [casting from string to float](#string-to-float).

Also parsing of some values will not produce bit for bit identical results to what the CPU does.
They are within round-off errors except when they are close enough to overflow to Inf or -Inf which
then results in a number being returned when the CPU would have returned null.

### Hive Text File Decimal

Hive has some limitations in what decimal values it can parse. The GPU kernels that we use
to parse decimal values do not have the same limitations. This means that there are times
when the CPU version would return a null for an input value, but the GPU version will
return a value. This typically happens for numbers with large negative exponents where
the GPU will return `0` and Hive will return `null`.
See https://github.com/NVIDIA/spark-rapids/issues/7246

## ORC

The ORC format has fairly complete support for both reads and writes. There are only a few known
issues. The first is for reading timestamps and dates around the transition between Julian and
Gregorian calendars as described [here](https://github.com/NVIDIA/spark-rapids/issues/131). A
similar issue exists for writing dates as described
[here](https://github.com/NVIDIA/spark-rapids/issues/139). Writing timestamps, however only appears
to work for dates after the epoch as described
[here](https://github.com/NVIDIA/spark-rapids/issues/140).

The plugin supports reading `uncompressed`, `snappy`, `zlib` and `zstd` ORC files and writing
 `uncompressed`, `snappy` and `zstd` ORC files.  At this point, the plugin does not have the 
ability to 
fall
 back to the CPU when reading an unsupported compression format, and will error out in that case.

### Push Down Aggregates for ORC

Spark-3.3.0+ pushes down certain aggregations (`MIN`/`MAX`/`COUNT`) into ORC when the user-config
`spark.sql.orc.aggregatePushdown` is set to true.
By enabling this feature, aggregate query performance will improve as it takes advantage of the
statistics information.

**Caution**

Spark ORC reader/writer assumes that all ORC files must have valid column statistics. This assumption
deviates from the [ORC-specification](https://orc.apache.org/specification) which states that statistics
are optional.
When a Spark-3.3.0+ job reads an ORC file with empty file-statistics, it fails while throwing the following
runtime exception:

```bash
org.apache.spark.SparkException: Cannot read columns statistics in file: /PATH_TO_ORC_FILE
E    Caused by: java.util.NoSuchElementException
E        at java.util.LinkedList.removeFirst(LinkedList.java:270)
E        at java.util.LinkedList.remove(LinkedList.java:685)
E        at org.apache.spark.sql.execution.datasources.orc.OrcFooterReader.convertStatistics(OrcFooterReader.java:54)
E        at org.apache.spark.sql.execution.datasources.orc.OrcFooterReader.readStatistics(OrcFooterReader.java:45)
E        at org.apache.spark.sql.execution.datasources.orc.OrcUtils$.createAggInternalRowFromFooter(OrcUtils.scala:428)
```

The Spark community is planning to work on a runtime fallback to read from actual rows when ORC
file-statistics are missing (see [SPARK-34960 discussion](https://issues.apache.org/jira/browse/SPARK-34960)).

*Writing ORC Files*

There are issues writing ORC files with dates or timestamps that fall within the lost days during
the switch from the Julian to Gregorian calendar, i.e.: between October 3rd, 1582 and October 15th,
1582. Dates or timestamps that fall within the range of lost days will not always be written
properly by the GPU to the ORC file. The values read by the CPU and the GPU may differ with the
CPU often rounding the day up to October 15th, 1582 whereas the GPU does not.

Note that the CPU rounds up dates or timestamps in the lost days range to October 15th, 1582
_before_ writing to the ORC file. If the CPU writes these problematic dates or timestamps to an
ORC file, they will be read properly by both the CPU and the GPU.

*Reading ORC Files*

To take advantage of the aggregate query optimization, where only the ORC metadata is read to
satisfy the query, the ORC read falls back to the CPU as it is a metadata-only query.

## Parquet

The Parquet format has more configs because there are multiple versions with some compatibility
issues between them. Dates and timestamps are where the known issues exist.  For reads when
`spark.sql.legacy.parquet.datetimeRebaseModeInWrite` is set to `CORRECTED`
[timestamps](https://github.com/NVIDIA/spark-rapids/issues/132) before the transition between the
Julian and Gregorian calendars are wrong, but dates are fine. When
`spark.sql.legacy.parquet.datetimeRebaseModeInWrite` is set to `LEGACY`, the read may fail for
values occurring before the transition between the Julian and Gregorian calendars, i.e.: date <= 1582-10-04.

When writing `spark.sql.legacy.parquet.datetimeRebaseModeInWrite` is currently ignored as described
[here](https://github.com/NVIDIA/spark-rapids/issues/144).

The plugin supports reading `uncompressed`, `snappy`, `gzip` and `zstd` Parquet files and writing
`uncompressed`, `snappy` and `zstd` Parquet files.  At this point, the plugin does not have the 
ability to
fall back to the CPU when reading an unsupported compression format, and will error out in that
case.

## JSON

The JSON format read is an experimental feature which is expected to have some issues, so we disable
it by default. If you would like to test it, you need to enable `spark.rapids.sql.format.json.enabled` and
`spark.rapids.sql.format.json.read.enabled`.

### Invalid JSON

In Apache Spark on the CPU if a line in the JSON file is invalid the entire row is considered
invalid and will result in nulls being returned for all columns. It is considered invalid if it
violates the JSON specification, but with a few extensions.

  * Single quotes are allowed to quote strings and keys
  * Unquoted values like NaN and Infinity can be parsed as floating point values
  * Control characters do not need to be replaced with the corresponding escape sequences in a 
    quoted string.
  * Garbage at the end of a row, if there is valid JSON at the beginning of the row, is ignored.

The GPU implementation does the same kinds of validations, but many of them are done on a per-column
basis, which, for example, means if a number is formatted incorrectly, it is likely only that value
will be considered invalid and return a null instead of nulls for the entire row.  

There are options that can be used to enable and disable many of these features which are mostly
listed below.

### JSON options

Spark supports passing options to the JSON parser when reading a dataset.  In most cases if the RAPIDS Accelerator
sees one of these options that it does not support it will fall back to the CPU. In some cases we do not. The
following options are documented below.

- `allowNumericLeadingZeros`  - Allows leading zeros in numbers (e.g. 00012). By default this is set to false.
  When it is false Spark considers the JSON invalid if it encounters this type of number. The RAPIDS
  Accelerator supports validating columns that are returned to the user with this option on or off. 

- `allowUnquotedControlChars` - Allows JSON Strings to contain unquoted control characters (ASCII characters with
  value less than 32, including tab and line feed characters) or not. By default this is set to false. If the schema
  is provided while reading JSON file, then this flag has no impact on the RAPIDS Accelerator as it always allows
  unquoted control characters but Spark sees these are invalid are returns nulls. However, if the schema is not provided
  and this option is false, then RAPIDS Accelerator's behavior is same as Spark where an exception is thrown
  as discussed in `JSON Schema discovery` section.

- `allowNonNumericNumbers` - Allows `NaN` and `Infinity` values to be parsed (note that these are not valid numeric
  values in the [JSON specification](https://json.org)). Spark versions prior to 3.3.0 have inconsistent behavior and will
  parse some variants of `NaN` and `Infinity` even when this option is disabled
  ([SPARK-38060](https://issues.apache.org/jira/browse/SPARK-38060)). The RAPIDS Accelerator behavior is consistent with
  Spark version 3.3.0 and later.

### Nesting
In versions of Spark before 3.5.0 there is no maximum to how deeply nested JSON can be.  After 
3.5.0 this was updated to be 1000 by default. The current GPU implementation limits this to 254 
no matter what version of Spark is used. If the nesting level is over this the JSON is considered
invalid and all values will be returned as nulls.

Only structs are supported for nested types. There are also some issues with arrays of structs. If
your data includes this, even if you are not reading it, you might get an exception. You can
try to set `spark.rapids.sql.json.read.mixedTypesAsString.enabled` to true to work around this,
but it also has some issues with it.

Dates and Timestamps have some issues and may return values for technically invalid inputs.

Floating point numbers have issues generally like with the rest of Spark, and we can parse them into
a valid floating point number, but it might not match 100% with the way Spark does it.

Strings are supported, but the data returned might not be normalized in the same way as the CPU
implementation. Generally this comes down to the GPU not modifying the input, whereas Spark will
do things like remove extra white space and parse numbers before turning them back into a string.

### JSON Floating Point

Parsing floating-point values has the same limitations as [casting from string to float](#string-to-float).

Prior to Spark 3.3.0, reading JSON strings such as `"+Infinity"` when specifying that the data type is `FloatType`
or `DoubleType` caused these values to be parsed even when `allowNonNumericNumbers` is set to false. Also, Spark
versions prior to 3.3.0 only supported the `"Infinity"` and `"-Infinity"` representations of infinity and did not
support `"+INF"`, `"-INF"`, or `"+Infinity"`, which Spark considers valid when unquoted. The GPU JSON reader is
consistent with the behavior in Spark 3.3.0 and later.

Another limitation of the GPU JSON reader is that it will parse strings containing non-string boolean or numeric values where
Spark will treat them as invalid inputs and will just return `null`.

### JSON Timestamps/Dates

The JSON parser does not support the `TimestampNTZ` type and will fall back to CPU if `spark.sql.timestampType` is
set to `TIMESTAMP_NTZ` or if an explicit schema is provided that contains the `TimestampNTZ` type.

There is currently no support for reading numeric values as timestamps and null values are returned instead
([#4940](https://github.com/NVIDIA/spark-rapids/issues/4940)). A workaround would be to read as longs and then cast
to timestamp.

### JSON Schema discovery

Spark SQL can automatically infer the schema of a JSON dataset if schema is not provided explicitly. The CPU
handles schema discovery and there is no GPU acceleration of this. By default Spark will read/parse the entire
dataset to determine the schema. This means that some options/errors which are ignored by the GPU may still
result in an exception if used with schema discovery.

### `from_json` function

`JsonToStructs` of `from_json` is based on the same code as reading a JSON lines file.  There are
a few differences with it.

The `from_json` function is disabled by default because it is experimental and has some known
incompatibilities with Spark, and can be enabled by setting 
`spark.rapids.sql.expression.JsonToStructs=true`. You don't need to set 
`spark.rapids.sql.format.json.enabled` and`spark.rapids.sql.format.json.read.enabled` to true.

There is no schema discovery as a schema is required as input to `from_json`

In addition to `structs`, a top level `map` type is supported, but only if the key and value are
strings.

### `to_json` function

The `to_json` function is disabled by default because it is experimental and has some known incompatibilities 
with Spark, and can be enabled by setting `spark.rapids.sql.expression.StructsToJson=true`.

Known issues are:

- There can be rounding differences when formatting floating-point numbers as strings. For example, Spark may
  produce `-4.1243574E26` but the GPU may produce `-4.124357351E26`.
- Not all JSON options are respected

### get_json_object

Known issue:
- [Non-string output is not normalized](https://github.com/NVIDIA/spark-rapids/issues/10218)
  When returning a result for things other than strings, a number of things are normalized by
  Apache Spark, but are not normalized by the GPU, like removing unnecessary white space,
  parsing and then serializing floating point numbers, turning single quotes to double quotes,
  and removing unneeded escapes for single quotes.

## Avro

The Avro format read is a very experimental feature which is expected to have some issues, so we disable
it by default. If you would like to test it, you need to enable `spark.rapids.sql.format.avro.enabled` and
`spark.rapids.sql.format.avro.read.enabled`.

Currently, the GPU accelerated Avro reader doesn't support reading the Avro version 1.2 files.

### Supported types

The boolean, byte, short, int, long, float, double, string are supported in current version.

## Regular Expressions

The following Apache Spark regular expression functions and expressions are supported on the GPU:

- `RLIKE`
- `regexp`
- `regexp_extract`
- `regexp_extract_all`
- `regexp_like`
- `regexp_replace`
- `string_split`
- `str_to_map`

Regular expression evaluation on the GPU is enabled by default when the UTF-8 character set is used
by the current locale. Execution will fall back to the CPU for regular expressions that are not yet
supported on the GPU, and in environments where the locale does not use UTF-8. However, there are
some edge cases that will still execute on the GPU and produce different results to the CPU. To
disable regular expressions on the GPU, set `spark.rapids.sql.regexp.enabled=false`.

These are the known edge cases where running on the GPU will produce different results to the CPU:

- Regular expressions that contain an end of line anchor '$' or end of string anchor '\Z' immediately
 next to a newline or a repetition that produces zero or more results
 ([#5610](https://github.com/NVIDIA/spark-rapids/pull/5610))`
- Word and non-word boundaries, `\b` and `\B`
- Line anchor `$` will incorrectly match any of the unicode characters `\u0085`, `\u2028`, or `\u2029` followed by
  another line-terminator, such as `\n`. For example, the pattern `TEST$` will match `TEST\u0085\n` on the GPU but
  not on the CPU ([#7585](https://github.com/NVIDIA/spark-rapids/issues/7585)).

The following regular expression patterns are not yet supported on the GPU and will fall back to the CPU.

- Line anchors `^` and `$` are not supported in some contexts, such as when combined with a choice (`^|a` or `$|a`).
- String anchor `\Z` is not supported by `regexp_replace`, and in some rare contexts.
- String anchor `\z` is not supported
- Patterns containing an end of line or string anchor immediately next to a newline or repetition that produces zero
  or more results
- Line anchor `$` and string anchors `\Z` are not supported in patterns containing `\W` or `\D`
- Line and string anchors are not supported by `string_split` and `str_to_map`
- Lazy quantifiers within a choice block such as `(2|\u2029??)+` 
- Possessive quantifiers, such as `a*+`
- Character classes that use union, intersection, or subtraction semantics, such as `[a-d[m-p]]`, `[a-z&&[def]]`,
  or `[a-z&&[^bc]]`
- Empty groups: `()`

Work is ongoing to increase the range of regular expressions that can run on the GPU.

## URL Parsing

`parse_url` QUERY with a column key could produce different results on CPU and GPU. In Spark, the `key` in `parse_url` could act like a regex, but GPU will match the key exactly. If key is literal, GPU will check if key contains regex special characters and fallback to CPU if it does, but if key is column, it will not be able to fallback. For example, `parse_url("http://foo/bar?abc=BAD&a.c=GOOD", QUERY, "a.c")` will return "BAD" on CPU, but "GOOD" on GPU. See the Spark issue: https://issues.apache.org/jira/browse/SPARK-44500

## Timestamps

Spark stores timestamps internally relative to the JVM time zone.  Converting an arbitrary timestamp
between time zones is not currently supported on the GPU. Therefore operations involving timestamps
will only be GPU-accelerated if the time zone used by the JVM is UTC.

## Windowing

### Window Functions

Because of ordering differences between the CPU and the GPU window functions especially row based
window functions like `row_number`, `lead`, and `lag` can produce different results if the ordering
includes both `-0.0` and `0.0`, or if the ordering is ambiguous. Spark can produce different results
from one run to another if the ordering is ambiguous on a window function too.

### Range Window

When the order-by column of a range based window is numeric type like `byte/short/int/long` and
the range boundary calculated for a value has overflow, CPU and GPU will get different results.

For example, consider the following dataset:

``` console
+------+---------+
| id   | dollars |
+------+---------+
|    1 |    NULL |
|    1 |      13 |
|    1 |      14 |
|    1 |      15 |
|    1 |      15 |
|    1 |      17 |
|    1 |      18 |
|    1 |      52 |
|    1 |      53 |
|    1 |      61 |
|    1 |      65 |
|    1 |      72 |
|    1 |      73 |
|    1 |      75 |
|    1 |      78 |
|    1 |      84 |
|    1 |      85 |
|    1 |      86 |
|    1 |      92 |
|    1 |      98 |
+------+---------+
```

After executing the SQL statement:

``` sql
SELECT
 COUNT(dollars) over
    (PARTITION BY id
    ORDER BY CAST (dollars AS Byte) ASC
    RANGE BETWEEN 127 PRECEDING AND 127 FOLLOWING)
FROM table
```

The results will differ between the CPU and GPU due to overflow handling.

``` console
CPU: WrappedArray([0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0])
GPU: WrappedArray([0], [19], [19], [19], [19], [19], [19], [19], [19], [19], [19], [19], [19], [19], [19], [19], [19], [19], [19], [19])
```

To enable byte-range windowing on the GPU, set
[`spark.rapids.sql.window.range.byte.enabled`](additional-functionality/advanced_configs.md#sql.window.range.byte.enabled) to true.

We also provide configurations for other integral range types:

- [`spark.rapids.sql.window.range.short.enabled`](additional-functionality/advanced_configs.md#sql.window.range.short.enabled)
- [`spark.rapids.sql.window.range.int.enabled`](additional-functionality/advanced_configs.md#sql.window.range.int.enabled)
- [`spark.rapids.sql.window.range.long.enabled`](additional-functionality/advanced_configs.md#sql.window.range.long.enabled)

The reason why we default the configurations to false for byte/short and to true for int/long is that
we think the most real-world queries are based on int or long.

## Parsing strings as dates or timestamps

When converting strings to dates or timestamps using functions like `to_date` and `unix_timestamp`,
the specified format string will fall into one of three categories:

- Supported on GPU and 100% compatible with Spark
- Supported on GPU but may produce different results to Spark
- Unsupported on GPU

The formats which are supported on GPU vary depending on the setting for `timeParserPolicy`.

### CORRECTED and EXCEPTION timeParserPolicy

With timeParserPolicy set to `CORRECTED` or `EXCEPTION` (the default), the following formats are supported
on the GPU without requiring any additional settings.

- `yyyy-MM-dd`
- `yyyy/MM/dd`
- `yyyy-MM`
- `yyyy/MM`
- `dd/MM/yyyy`
- `yyyy-MM-dd HH:mm:ss`
- `MM-dd`
- `MM/dd`
- `dd-MM`
- `dd/MM`
- `MM/yyyy`
- `MM-yyyy`
- `MM/dd/yyyy`
- `MM-dd-yyyy`
- `MMyyyy`

Valid Spark date/time formats that do not appear in the list above may also be supported but have not been
extensively tested and may produce different results compared to the CPU. Known issues include:

- Valid dates and timestamps followed by trailing characters (including whitespace) may be parsed to non-null
  values on GPU where Spark would treat the data as invalid and return null

To attempt to use other formats on the GPU, set
[`spark.rapids.sql.incompatibleDateFormats.enabled`](additional-functionality/advanced_configs.md#sql.incompatibleDateFormats.enabled)
to `true`.

Formats that contain any of the following characters are unsupported and will fall back to CPU:

```
'k', 'K','z', 'V', 'c', 'F', 'W', 'Q', 'q', 'G', 'A', 'n', 'N',
'O', 'X', 'p', '\'', '[', ']', '#', '{', '}', 'Z', 'w', 'e', 'E', 'x', 'Z', 'Y'
```

Formats that contain any of the following words are unsupported and will fall back to CPU:

```
"u", "uu", "uuu", "uuuu", "uuuuu", "uuuuuu", "uuuuuuu", "uuuuuuuu", "uuuuuuuuu", "uuuuuuuuuu",
"y", "yy", yyy", "yyyyy", "yyyyyy", "yyyyyyy", "yyyyyyyy", "yyyyyyyyy", "yyyyyyyyyy",
"D", "DD", "DDD", "s", "m", "H", "h", "M", "MMM", "MMMM", "MMMMM", "L", "LLL", "LLLL", "LLLLL",
"d", "S", "SS", "SSS", "SSSS", "SSSSS", "SSSSSSSSS", "SSSSSSS", "SSSSSSSS"
```

### LEGACY timeParserPolicy

With timeParserPolicy set to `LEGACY` and
[`spark.rapids.sql.incompatibleDateFormats.enabled`](additional-functionality/advanced_configs.md#sql.incompatibleDateFormats.enabled)
set to `true`, and `spark.sql.ansi.enabled` set to `false`, the following formats are supported but not
guaranteed to produce the same results as the CPU:

- `dd-MM-yyyy`
- `dd/MM/yyyy`
- `yyyy/MM/dd`
- `yyyy-MM-dd`
- `yyyy/MM/dd HH:mm:ss`
- `yyyy-MM-dd HH:mm:ss`

LEGACY timeParserPolicy support has the following limitations when running on the GPU:

- Only 4 digit years are supported
- The proleptic Gregorian calendar is used instead of the hybrid Julian+Gregorian calendar
  that Spark uses in legacy mode

## Formatting dates and timestamps as strings

When formatting dates and timestamps as strings using functions such as `from_unixtime`, only a
subset of valid format strings are supported on the GPU.

Formats that contain any of the following characters are unsupported and will fall back to CPU:

```
'k', 'K','z', 'V', 'c', 'F', 'W', 'Q', 'q', 'G', 'A', 'n', 'N',
'O', 'X', 'p', '\'', '[', ']', '#', '{', '}', 'Z', 'w', 'e', 'E', 'x', 'Z', 'Y'
```

Formats that contain any of the following words are unsupported and will fall back to CPU:

```
"u", "uu", "uuu", "uuuu", "uuuuu", "uuuuuu", "uuuuuuu", "uuuuuuuu", "uuuuuuuuu", "uuuuuuuuuu",
"y", yyy", "yyyyy", "yyyyyy", "yyyyyyy", "yyyyyyyy", "yyyyyyyyy", "yyyyyyyyyy",
"D", "DD", "DDD", "s", "m", "H", "h", "M", "MMM", "MMMM", "MMMMM", "L", "LLL", "LLLL", "LLLLL",
"d", "S", "SS", "SSSS", "SSSSS", "SSSSSSSSS", "SSSSSSS", "SSSSSSSS"
```

Note that this list differs very slightly from the list given in the previous section for parsing
strings to dates because the two-digit year format `"yy"` is supported when formatting dates as
strings but not when parsing strings to dates.

## Casting between types

In general, performing `cast` and `ansi_cast` operations on the GPU is compatible with the same
operations on the CPU. However, there are some exceptions. For this reason, certain casts are
disabled on the GPU by default and require configuration options to be specified to enable them.

### Float to Decimal

The GPU will use a different strategy from Java's BigDecimal to handle/store decimal values, which
leads to restrictions:
* Float values cannot be larger than `1e18` or smaller than `-1e18` after conversion.
* The results produced by GPU slightly differ from the default results of Spark.

This configuration is enabled by default. To disable this operation on the GPU set
[`spark.rapids.sql.castFloatToDecimal.enabled`](additional-functionality/advanced_configs.md#sql.castFloatToDecimal.enabled) to `false`

### Float to Integral Types

With both `cast` and `ansi_cast`, Spark uses the expression `Math.floor(x) <= MAX && Math.ceil(x) >=
MIN` to determine whether a floating-point value can be converted to an integral type. Prior to
Spark 3.1.0 the MIN and MAX values were floating-point values such as `Int.MaxValue.toFloat` but
starting with 3.1.0 these are now integral types such as `Int.MaxValue` so this has slightly
affected the valid range of values and now differs slightly from the behavior on GPU in some cases.

This configuration is enabled by default. To disable this operation on the GPU set
[`spark.rapids.sql.castFloatToIntegralTypes.enabled`](additional-functionality/advanced_configs.md#sql.castFloatToIntegralTypes.enabled)
to `false`.

### Float to String

The Rapids Accelerator for Apache Spark uses uses a method based on [ryu](https://github.com/ulfjack/ryu) when converting floating point data type to string. As a result the computed string can differ from the output of Spark in some cases: sometimes the output is shorter (which is arguably more accurate) and sometimes the output may differ in the precise digits output.

This configuration is enabled by default. To disable this operation on the GPU set
[`spark.rapids.sql.castFloatToString.enabled`](additional-functionality/advanced_configs.md#sql.castFloatToString.enabled) to `false`.

The `format_number` function also uses [ryu](https://github.com/ulfjack/ryu) as the solution when formatting floating-point data types to 
strings, so results may differ from Spark in the same way. To disable this on the GPU, set 
[`spark.rapids.sql.formatNumberFloat.enabled`](additional-functionality/advanced_configs.md#sql.formatNumberFloat.enabled) to `false`.

### String to Float

Casting from string to double on the GPU returns incorrect results when the string represents any 
number in the following ranges. In both cases the GPU returns `Double.MaxValue`. The default behavior 
in Apache Spark is to return `+Infinity` and `-Infinity`, respectively.

- `1.7976931348623158E308 <= x < 1.7976931348623159E308`
- `-1.7976931348623159E308 < x <= -1.7976931348623158E308`

Casting from string to double on the GPU could also sometimes return incorrect results if the string 
contains high precision values. Apache Spark rounds the values to the nearest double, while the GPU 
truncates the values directly.

Also, the GPU does not support casting from strings containing hex values to floating-point types.

This configuration is enabled by default. To disable this operation on the GPU set
[`spark.rapids.sql.castStringToFloat.enabled`](additional-functionality/advanced_configs.md#sql.castStringToFloat.enabled) to `false`.

### String to Date

The following formats/patterns are supported on the GPU. Timezone of UTC is assumed.

| Format or Pattern     | Supported on GPU? |
| --------------------- | ----------------- |
| `"yyyy"`              | Yes               |
| `"yyyy-[M]M"`         | Yes               |
| `"yyyy-[M]M "`        | Yes               |
| `"yyyy-[M]M-[d]d"`    | Yes               |
| `"yyyy-[M]M-[d]d "`   | Yes               |
| `"yyyy-[M]M-[d]d *"`  | Yes               |
| `"yyyy-[M]M-[d]d T*"` | Yes               |
| `"epoch"`             | Yes               |
| `"now"`               | Yes               |
| `"today"`             | Yes               |
| `"tomorrow"`          | Yes               |
| `"yesterday"`         | Yes               |

### String to Timestamp

To allow casts from string to timestamp on the GPU, enable the configuration property
[`spark.rapids.sql.castStringToTimestamp.enabled`](additional-functionality/advanced_configs.md#sql.castStringToTimestamp.enabled).

Casting from string to timestamp currently has the following limitations.

| Format or Pattern                                                   | Supported on GPU? |
| ------------------------------------------------------------------- | ------------------|
| `"yyyy"`                                                            | Yes               |
| `"yyyy-[M]M"`                                                       | Yes               |
| `"yyyy-[M]M "`                                                      | Yes               |
| `"yyyy-[M]M-[d]d"`                                                  | Yes               |
| `"yyyy-[M]M-[d]d "`                                                 | Yes               |
| `"yyyy-[M]M-[d]dT[h]h:[m]m:[s]s[zone_id]"` | Partial [\[1\]](#Footnote1)       |
| `"yyyy-[M]M-[d]d [h]h:[m]m:[s]s[zone_id]"` | Partial [\[1\]](#Footnote1)       |
| `"yyyy-[M]M-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]"` | Partial [\[1\]](#Footnote1)       |
| `"yyyy-[M]M-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]"` | Partial [\[1\]](#Footnote1)       |
| `"[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]"`                | Partial [\[1\]](#Footnote1)       |
| `"T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]"`               | Partial [\[1\]](#Footnote1)       |
| `"epoch"`                                                           | Yes               |
| `"now"`                                                             | Yes               |
| `"today"`                                                           | Yes               |
| `"tomorrow"`                                                        | Yes               |
| `"yesterday"`                                                       | Yes               |

- <a name="Footnote1"></a>[1] Leap seconds are not supported. If a zone_id is provided then only
 timezone 'Z' (UTC) is supported. Casting unsupported formats will result in null values.

Spark is very lenient when casting from string to timestamp because all date and time components
are optional, meaning that input values such as `T`, `T2`, `:`, `::`, `1:`, `:1`, and `::1`
are considered valid timestamps. The GPU will treat these values as invalid and cast them to null
values.

### Constant Folding

ConstantFolding is an operator optimization rule in Catalyst that replaces expressions that can
be statically evaluated with their equivalent literal values. The RAPIDS Accelerator relies
on constant folding and parts of the query will not be accelerated if
`org.apache.spark.sql.catalyst.optimizer.ConstantFolding` is excluded as a rule.

### long/double to Timestamp
Spark 330+ has an issue when casting a big enough long/double as timestamp, refer to https://issues.apache.org/jira/browse/SPARK-39209.
Spark 330+ throws errors while the RAPIDS Accelerator can handle correctly when casting a big enough long/double as timestamp.

## JSON string handling
The 0.5 release introduces the `get_json_object` operation.  The JSON specification only allows
double quotes around strings in JSON data, whereas Spark allows single quotes around strings in JSON
data.  The RAPIDS Spark `get_json_object` operation on the GPU will return `None` in PySpark or
`Null` in Scala when trying to match a string surrounded by single quotes.  This behavior will be
updated in a future release to more closely match Spark.

If the JSON has a single quote `'` in the path, the GPU query may fail with `ai.rapids.cudf.CudfException`.
More examples are in [issue-12483](https://github.com/rapidsai/cudf/issues/12483).

## Approximate Percentile

The GPU implementation of `approximate_percentile` uses
[t-Digests](https://arxiv.org/abs/1902.04023) which have high accuracy, particularly near the tails of a
distribution. The results are not bit-for-bit identical with the Apache Spark implementation of
`approximate_percentile`. This feature is enabled by default and can be disabled by setting
`spark.rapids.sql.expression.ApproximatePercentile=false`.

## Conditionals and operations with side effects (ANSI mode)

In Apache Spark condition operations like `if`, `coalesce`, and `case/when` lazily evaluate
their parameters on a row by row basis. On the GPU it is generally more efficient to
evaluate the parameters regardless of the condition and then select which result to return
based on the condition. This is fine so long as there are no side effects caused by evaluating
a parameter. For most expressions in Spark this is true, but in ANSI mode many expressions can
throw exceptions, like for the `Add` expression if an overflow happens. This is also true of
UDFs, because by their nature they are user defined and can have side effects like throwing
exceptions.

Currently, the RAPIDS Accelerator
[assumes that there are no side effects](https://github.com/NVIDIA/spark-rapids/issues/3849).
This can result it situations, specifically in ANSI mode, where the RAPIDS Accelerator will
always throw an exception, but Spark on the CPU will not.  For example:

```scala
spark.conf.set("spark.sql.ansi.enabled", "true")

Seq(0L, Long.MaxValue).toDF("val")
    .repartition(1) // The repartition makes Spark not optimize selectExpr away
    .selectExpr("IF(val > 1000, null, val + 1) as ret")
    .show()
```

If the above example is run on the CPU you will get a result like.
```
+----+
| ret|
+----+
|   1|
|null|
+----+
```

But if it is run on the GPU an overflow exception is thrown. As was explained before this
is because the RAPIDS Accelerator will evaluate both `val + 1` and `null` regardless of
the result of the condition. In some cases you can work around this. The above example
could be re-written so the `if` happens before the `Add` operation.

```scala
Seq(0L, Long.MaxValue).toDF("val")
    .repartition(1) // The repartition makes Spark not optimize selectExpr away
    .selectExpr("IF(val > 1000, null, val) + 1 as ret")
    .show()
```

But this is not something that can be done generically and requires inner knowledge about
what can trigger a side effect.
