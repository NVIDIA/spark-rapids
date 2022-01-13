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
setting [spark.rapids.sql.stableSort.enabled](configs.md#sql.stableSort.enabled) to `true` and
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

For aggregations the underlying implementation is doing the aggregations in parallel and due to race
conditions within the computation itself the result may not be the same each time the query is
run. This is inherent in how the plugin speeds up the calculations and cannot be "fixed." If a query
joins on a floating point value, which is not wise to do anyways, and the value is the result of a
floating point aggregation then the join may fail to work properly with the plugin but would have
worked with plain Spark. Because of this most floating point aggregations are off by default but can
be enabled with the config
[`spark.rapids.sql.variableFloatAgg.enabled`](configs.md#sql.variableFloatAgg.enabled).

Additionally, some aggregations on floating point columns that contain `NaN` can produce results
different from Spark in versions prior to Spark 3.1.0.  If it is known with certainty that the
floating point columns do not contain `NaN`, set
[`spark.rapids.sql.hasNans`](configs.md#sql.hasNans) to `false` to run GPU enabled aggregations on
them.

In the case of a distinct count on `NaN` values, prior to Spark 3.1.0, the issue only shows up if
 you have different `NaN` values. There are several different binary values that are all considered
 to be `NaN` by floating point. The plugin treats all of these as the same value, where as Spark
 treats them all as different values. Because this is considered to be rare we do not disable
 distinct count for floating point values even if
 [`spark.rapids.sql.hasNans`](configs.md#sql.hasNans) is `true`.

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

## Decimal Support

Apache Spark supports decimal values with a precision up to 38. This equates to 128-bits.
However, when actually processing the data, in most cases, it is temporarily converted to 
Java's `BigDecimal` type which allows for effectively unlimited precision. This lets Spark do
complicated calculations without the risk of missing an overflow and causing data corruption.
It also lets Spark support some operations that require intermediate values that are larger than
a 128-bit representation can support.

The RAPIDS Accelerator currently is limited to a maximum of 128-bits for storing or processing
decimal values. This allows us to fully support the majority of decimal operations. But there are
a few operations that we cannot support to the same degree as Spark can on the CPU.

### Decimal Sum Aggregation

A number of fixes for overflow detection went into Spark 3.1.0. Please see
[SPARK-28067](https://issues.apache.org/jira/browse/SPARK-28067) and
[SPARK-32018](https://issues.apache.org/jira/browse/SPARK-32018) for more detailed information.
Some of these fixes we were able to back port, but some of them require Spark 3.1.0 or above to
fully be able to detect overflow in all cases. As such on versions of Spark older than 3.1.0 for
large decimal values there is the possibility of data corruption in some corner cases. 
This is true for both the CPU and GPU implementations, but there are fewer of these cases for the 
GPU. If this concerns you, you should upgrade to Spark 3.1.0 or above. 

When Apache Spark does a sum aggregation on decimal values it will store the result in a value
with a precision that is the input precision + 10, but with a maximum precision of 38.
For an input precision of 9 and above, Spark will do the aggregations as a Java `BigDecimal`
value which is slow, but guarantees that any overflow can be detected because it can work with
effectively unlimited precision. For inputs with a precision of 8 or below Spark will internally do
the calculations as a long value, 64-bits. When the precision is 8, you would need at least 
174,467,442,482 values/rows contributing to a single aggregation result before the overflow is no
longer detected. Even then all the values would need to be either the largest or the smallest value
possible to be stored in the type for the overflow to cause data corruption.

For the RAPIDS Accelerator we don't have direct access to unlimited precision for our calculations
like the CPU does. For input values with a precision of 8 and below we follow Spark and process the
data the same way, as a 64-bit value. For larger values we will do extra calculations looking at the
higher order digits to be able to detect overflow in all cases. But because of this you may see
some performance differences depending on the input precision used. The differences will show up
when going from an input precision of 8 to 9 and again when going from an input precision of 28 to 29.

### Decimal Average

Average is effectively doing a `sum(input)/count(input)`, except the scale of the output type is
the scale of the input + 4. As such it inherits some of the same issues that both sum and divide
have. It also inherits some issues from Spark itself. See
https://issues.apache.org/jira/browse/SPARK-37024 for a detailed description of some issues
with average in Spark.

In order to be able to guarantee doing the divide with half up rounding at the end we only support
average on input values with a precision of 23 or below. This is 38 - 10 for the sum guarantees
and then 5 less to be able to shift the left-hand side of the divide enough to get a correct
answer that can be rounded to the result that Spark would produce.

### Divide and Multiply

Division and multiplication of decimal types is a little complicated in Apache Spark. For 
some arbitrary reason divide and multiply in Spark require that the precision and scale of 
the left-hand side and the right-hand side match. As such when planning a divide or multiply 
Spark will look at the original inputs to calculate the output precision and scale. Then it will
cast the inputs to a common wider value where the scale is the max of the two input scales,
and the precision is max of the two input non-scale portions (precision - scale) + the new
scale. Then it will do the divide or multiply as a `BigDecimal` value, and return the result as
a `BigDecimal` but lie about the precision and scale of the return type. Finally, Spark will
insert a `CheckOverflow` expression that will round the scale of the BigDecimal value to that
of the desired output type and check that the final precision will fit in the precision of the
desired output type. 

In order to match exactly with what Spark is doing the RAPIDS Accelerator would need at least
256-bit decimal values. We might implement that at some point, but until then we try to cover as
much of division and multiplication as possible.

To combat this we look at the query plan and try to determine what is the smallest precision
and scale for each parameter that would let us still produce the exact same answer as Apache
Spark. We effectively try to undo what Spark did when widening the types to make them common.

#### Division

In Spark the output of a division operation is

```scala
val precision = p1 - s1 + s2 + max(6, s1 + p2 + 1)
val scale = max(6, s1 + p2 + 1)
```

Where `p1` and `s1` are the precision and scale of the left-hand side of the operation and
`p2` and `s2` are the precision and scale of the right-hand side of the operation. But decimal
divide inherently produces a result where the output scale is `s1 - s2`. In addition to this 
Spark will round the result to the given scale, and not just truncate it. This means that to
produce the same result as Apache Spark we have to increase the scale of the left-hand side
operation to be at least `output_scale + s2 + 1`. The `+ 1` is so the output is large enough that
we can round it to the desired result.  If this causes the precision of the left-hand side
to go above 38, the maximum precision that 128-bits can hold, then we have to fall back to the
CPU. Unfortunately the math is a bit complicated so there is no simple rule of thumb for this.

#### Multiplication

In Spark the output of a multiplication operation is

```scala
val precision = p1 + p2 + 1
val scale = s1 + s2
```

Where `p1` and `s1` are the precision and scale of the left-hand side of the operation and
`p2` and `s2` are the precision and scale of the right-hand side of the operation. Fortunately,
decimal multiply inherently produces the same scale, but Spark will round the result. As such,
the RAPIDS Accelerator must add an extra decimal place to the scale and the precision, so we can
round correctly. This means that if `p1 + p2 > 36` we will fall back to the CPU to do processing. 

### How to get more decimal operations on the GPU?

Spark is very conservative in calculating the output types for decimal operations. It does this
to avoid overflow in the worst case scenario, but generally will end up using a much larger type
than is needed to store the final result. This means that over the course of a large query the
precision and scale can grow to a size that would force the RAPIDS Accelerator to fall back
to the CPU out of an abundance of caution. If you find yourself in this situation you can often
cast the results to something smaller and still get the same answer. These casts should be done 
with some knowledge about the data being processed.

For example if we had a query like

```sql
SELECT SUM(cs_wholesale_cost * cs_quantity)/
       SUM(cs_sales_price * cs_quantity) cost_to_sale
  FROM catalog_sales
  GROUP BY cs_sold_date_sk
  ORDER BY cs_sold_date_sk
```

where `cs_wholesale_cost` and `cs_sale_price` are both decimal values with a precision of 7 
and a scale of 2, `Decimal(7, 2)`, and `cs_quantity` is a 32-bit integer. Only the first half 
of the query will be on the GPU. The following explanation is a bit complicated but tries to
break down the processing into the distinct steps that Spark takes.

  1. Multiplying a `Decimal(7, 2)` by an integer produces a `Decimal(18, 2)` value. This is the
     same for both multiply operations in the query.
  2. The `sum` operation on the resulting `Decimal(18, 2)` column produces a `Decimal(28, 2)`.
     This also is the same for both sum aggregations in the query.
  3. The final divide operation is dividing a `Decimal(28, 2)` by another `Decimal(28, 2)` and
     produces a `Decimal(38, 10)`.

We cannot guarantee that on the GPU the divide will produce the exact same result as
the CPU for all possible inputs. But we know that we have at most 1,000,000 line items
for each `cs_sold_date_sk`, and the average price/cost is no where close to the maximum
value that `Decimal(7, 2)` can hold. So we can cast the result of the sums to a more
reasonable `Decimal(14, 2)` and still produce an equivalent result, but totally on the GPU.

```sql
SELECT CAST(SUM(cs_wholesale_cost * cs_quantity) AS Decimal(14,2))/
       CAST(SUM(cs_sales_price * cs_quantity) AS Decimal(14,2)) cost_to_sale
  FROM catalog_sales
  GROUP BY cs_sold_date_sk
  ORDER BY cs_sold_date_sk
```

This should be done with some caution as it does reduce the range of values that the query could
process before overflowing. It also can produce different result types. In this case instead of
producing a `Decimal(38, 10)` the result is a `Decimal(31, 17)`. If you really want the exact
same result type you can cast the result back to a `Decimal(38, 10)`, and the result will be
identical to before. But, it can have a positive impact to performance.

If you have made it this far in the documentation then you probably know what you are doing
and will use the following power only for good. It can often be difficult to
determine if adding casts to put some processing on the GPU would improve performance or not.
It can also be difficult to detect if a query might produce incorrect results because of a cast.
To help answer some of these questions we provide
`spark.rapids.sql.decimalOverflowGuarantees` that if set to false will disable guarantees for
overflow checking and run all decimal operations on the GPU, even if it cannot guarantee that
it will produce the exact same result as Spark. This should **never** be set to false in
production because it disables all guarantees, and if your data does overflow, it might produce
either a `null` value or worse an incorrect decimal value. But, it should give you more
information about what the performance impact might be if you tuned it with casting. If
you compare the results to GPU results with the guarantees still in place it should give you 
an idea if casting would still produce a correct answer. Even with this you should go through
the query and your data and see what level of guarantees for outputs you are comfortable with.

## Unicode

Spark delegates Unicode operations to the underlying JVM. Each version of Java complies with a
specific version of the Unicode standard. The SQL plugin does not use the JVM for Unicode support
and is compatible with Unicode version 12.1. Because of this there may be corner cases where Spark
will produce a different result compared to the plugin.

## CSV Reading

Due to inconsistencies between how CSV data is parsed CSV parsing is off by default.
Each data type can be enabled or disabled independently using the following configs.

 * [spark.rapids.sql.csv.read.bool.enabled](configs.md#sql.csv.read.bool.enabled)
 * [spark.rapids.sql.csv.read.byte.enabled](configs.md#sql.csv.read.byte.enabled)
 * [spark.rapids.sql.csv.read.date.enabled](configs.md#sql.csv.read.date.enabled)
 * [spark.rapids.sql.csv.read.double.enabled](configs.md#sql.csv.read.double.enabled)
 * [spark.rapids.sql.csv.read.float.enabled](configs.md#sql.csv.read.float.enabled)
 * [spark.rapids.sql.csv.read.integer.enabled](configs.md#sql.csv.read.integer.enabled)
 * [spark.rapids.sql.csv.read.long.enabled](configs.md#sql.csv.read.long.enabled)
 * [spark.rapids.sql.csv.read.short.enabled](configs.md#sql.csv.read.short.enabled)
 * [spark.rapids.sql.csvTimestamps.enabled](configs.md#sql.csvTimestamps.enabled)

If you know that your particular data type will be parsed correctly enough, you may enable each
type you expect to use. Often the performance improvement is so good that it is worth
checking if it is parsed correctly.

Spark is generally very strict when reading CSV and if the data does not conform with the 
expected format exactly it will result in a `null` value. The underlying parser that the RAPIDS Accelerator
uses is much more lenient. If you have badly formatted CSV data you may get data back instead of
nulls.

Spark allows for stripping leading and trailing white space using various options that are off by
default. The plugin will strip leading and trailing space for all values except strings.

There are also discrepancies/issues with specific types that are detailed below.

### CSV Boolean

Invalid values like `BAD` show up as `true` as described by this 
[issue](https://github.com/NVIDIA/spark-rapids/issues/2071)

This is the same for all other types, but because that is the only issue with boolean parsing
we have called it out specifically here.

### CSV Strings
Writing strings to a CSV file in general for Spark can be problematic unless you can ensure that
your data does not have any line deliminators in it. The GPU accelerated CSV parser handles quoted
line deliminators similar to `multiLine` mode.  But there are still a number of issues surrounding
it and they should be avoided.

Escaped quote characters `'\"'` are not supported well as described by this
[issue](https://github.com/NVIDIA/spark-rapids/issues/129).

### CSV Dates
Parsing a `timestamp` as a `date` does not work. The details are documented in this
[issue](https://github.com/NVIDIA/spark-rapids/issues/869).

Only a limited set of formats are supported when parsing dates.

* `"yyyy-MM-dd"`
* `"yyyy/MM/dd"`
* `"yyyy-MM"`
* `"yyyy/MM"`
* `"MM-yyyy"`
* `"MM/yyyy"`
* `"MM-dd-yyyy"`
* `"MM/dd/yyyy"`

The reality is that all of these formats are supported at the same time. The plugin will only
disable itself if you set a format that it does not support.

As a workaround you can parse the column as a timestamp and then cast it to a date.

Invalid dates in Spark, values that have the correct format, but the numbers produce invalid dates,
can result in an exception by default, and how they are parsed can be controlled through a config.
The RAPIDS Accelerator does not support any of this and will produce an incorrect date. Typically,
one that overflowed.

### CSV Timestamps
The CSV parser does not support time zones.  It will ignore any trailing time zone information,
despite the format asking for a `XXX` or `[XXX]`. As such it is off by default and you can enable it
by setting [`spark.rapids.sql.csvTimestamps.enabled`](configs.md#sql.csvTimestamps.enabled) to
`true`.

The formats supported for timestamps are limited similar to dates.  The first part of the format
must be a supported date format.  The second part must start with a `'T'` to separate the time
portion followed by one of the following formats:

* `HH:mm:ss.SSSXXX`
* `HH:mm:ss[.SSS][XXX]`
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

The CSV parser is not able to parse `NaN` values.  These are
likely to be turned into null values, as described in this
[issue](https://github.com/NVIDIA/spark-rapids/issues/125).

Some floating-point values also appear to overflow but do not for the CPU as described in this
[issue](https://github.com/NVIDIA/spark-rapids/issues/124).

Any number that overflows will not be turned into a null value.

Also parsing of some values will not produce bit for bit identical results to what the CPU does.
They are within round-off errors except when they are close enough to overflow to Inf or -Inf which
then results in a number being returned when the CPU would have returned null.

### CSV Integer

Any number that overflows will not be turned into a null value.

## ORC

The ORC format has fairly complete support for both reads and writes. There are only a few known
issues. The first is for reading timestamps and dates around the transition between Julian and
Gregorian calendars as described [here](https://github.com/NVIDIA/spark-rapids/issues/131). A
similar issue exists for writing dates as described
[here](https://github.com/NVIDIA/spark-rapids/issues/139). Writing timestamps, however only appears
to work for dates after the epoch as described
[here](https://github.com/NVIDIA/spark-rapids/issues/140).

The plugin supports reading `uncompressed`, `snappy` and `zlib` ORC files and writing `uncompressed`
 and `snappy` ORC files.  At this point, the plugin does not have the ability to fall back to the
 CPU when reading an unsupported compression format, and will error out in that case.

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

When `spark.sql.parquet.outputTimestampType` is set to `INT96`, the timestamps will overflow and 
result in an `IllegalArgumentException` thrown, if any value is before 
September 21, 1677 12:12:43 AM or it is after April 11, 2262 11:47:17 PM. To get around this
issue, turn off the ParquetWriter acceleration for timestamp columns by either setting 
`spark.rapids.sql.format.parquet.writer.int96.enabled` to false or 
set `spark.sql.parquet.outputTimestampType` to `TIMESTAMP_MICROS` or `TIMESTAMP_MILLIS` to by
-pass the issue entirely.

The plugin supports reading `uncompressed`, `snappy` and `gzip` Parquet files and writing
`uncompressed` and `snappy` Parquet files.  At this point, the plugin does not have the ability to
fall back to the CPU when reading an unsupported compression format, and will error out in that
case.

## JSON

The JSON format read is a very experimental feature which is expected to have some issues, so we disable 
it by default. If you would like to test it, you need to enable `spark.rapids.sql.format.json.enabled` and 
`spark.rapids.sql.format.json.read.enabled`.

Currently, the GPU accelerated JSON reader doesn't support column pruning, which will likely make 
this difficult to use or even test. The user must specify the full schema or just let Spark infer 
the schema from the JSON file. eg,

We have a `people.json` file with below content

``` console
{"name":"Michael"}
{"name":"Andy", "age":30}
{"name":"Justin", "age":19}
```

Both below ways will work

- Inferring the schema

  ``` scala
  val df = spark.read.json("people.json")
  ```

- Specifying the full schema

  ``` scala
  val schema = StructType(Seq(StructField("name", StringType), StructField("age", IntegerType)))
  val df = spark.read.schema(schema).json("people.json")
  ```

While the below code will not work in the current version,

``` scala
val schema = StructType(Seq(StructField("name", StringType)))
val df = spark.read.schema(schema).json("people.json")
```

### JSON supporting types

The nested types(array, map and struct), date and timestamp are not supported yet in current version.

### JSON Floating Point

Like the CSV reader, the JSON reader has the same floating point issue. Please refer to [CSV Floating Point](#csv-floating-point) section.

## LIKE

If a null char '\0' is in a string that is being matched by a regular expression, `LIKE` sees it as
the end of the string.  This will be fixed in a future release. The issue is
[here](https://github.com/NVIDIA/spark-rapids/issues/119).

## Regular Expressions

The following Apache Spark regular expression functions and expressions are supported on the GPU:

- `RLIKE`
- `regexp`
- `regexp_extract`
- `regexp_like`
- `regexp_replace`

These operations are disabled by default because of known incompatibilities between the Java regular expression 
engine that Spark uses and the cuDF regular expression engine on the GPU, and also because the regular expression 
kernels can potentially have high memory overhead.

These operations can be enabled on the GPU with the following configuration settings:

- `spark.rapids.sql.expression.RLike=true` (for `RLIKE`, `regexp`, and `regexp_like`)
- `spark.rapids.sql.expression.RegExpReplace=true` for `regexp_replace`
- `spark.rapids.sql.expression.RegExpExtract=true` for `regexp_extract`

Even when these expressions are enabled, there are instances where regular expression operations will fall back to 
CPU when the RAPIDS Accelerator determines that a pattern is either unsupported or would produce incorrect results on the GPU.

Here are some examples of regular expression patterns that are not supported on the GPU and will fall back to the CPU.

- Lazy quantifiers, such as `a*?`
- Possessive quantifiers, such as `a*+`
- Character classes that use union, intersection, or subtraction semantics, such as `[a-d[m-p]]`, `[a-z&&[def]]`, 
  or `[a-z&&[^bc]]`
- Word and non-word boundaries, `\b` and `\B`
- Empty groups: `()`
- Regular expressions containing null characters (unless the pattern is a simple literal string)
- Beginning-of-line and end-of-line anchors (`^` and `$`) are not supported in some contexts, such as when combined 
- with a choice (`^|a`).

In addition to these cases that can be detected, there are also known issues that can cause incorrect results:

- Character classes for negative matches have different behavior between CPU and GPU for multiline
  strings. The pattern `[^a]` will match line-terminators on CPU but not on GPU.

Work is ongoing to increase the range of regular expressions that can run on the GPU.

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
[`spark.rapids.sql.window.range.byte.enabled`](configs.md#sql.window.range.byte.enabled) to true.

We also provide configurations for other integral range types:

- [`spark.rapids.sql.window.range.short.enabled`](configs.md#sql.window.range.short.enabled)
- [`spark.rapids.sql.window.range.int.enabled`](configs.md#sql.window.range.int.enabled)
- [`spark.rapids.sql.window.range.long.enabled`](configs.md#sql.window.range.short.enabled)

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

- `dd/MM/yyyy`
- `yyyy/MM`
- `yyyy/MM/dd`
- `yyyy-MM`
- `yyyy-MM-dd`
- `yyyy-MM-dd HH:mm:ss`
- `MM-dd`
- `MM/dd`
- `dd-MM`
- `dd/MM`

Valid Spark date/time formats that do not appear in the list above may also be supported but have not been 
extensively tested and may produce different results compared to the CPU. Known issues include:

- Valid dates and timestamps followed by trailing characters (including whitespace) may be parsed to non-null 
  values on GPU where Spark would treat the data as invalid and return null

To attempt to use other formats on the GPU, set
[`spark.rapids.sql.incompatibleDateFormats.enabled`](configs.md#sql.incompatibleDateFormats.enabled)
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
[`spark.rapids.sql.incompatibleDateFormats.enabled`](configs.md#sql.incompatibleDateFormats.enabled)
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
"d", "S", "SS", "SSS", "SSSS", "SSSSS", "SSSSSSSSS", "SSSSSSS", "SSSSSSSS"
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
* It is only available when `ansiMode` is on.
* Float values cannot be larger than `1e18` or smaller than `-1e18` after conversion.
* The results produced by GPU slightly differ from the default results of Spark.

To enable this operation on the GPU, set
[`spark.rapids.sql.castFloatToDecimal.enabled`](configs.md#sql.castFloatToDecimal.enabled) to `true`
and set `spark.sql.ansi.enabled` to `true`.

### Float to Integral Types

With both `cast` and `ansi_cast`, Spark uses the expression `Math.floor(x) <= MAX && Math.ceil(x) >=
MIN` to determine whether a floating-point value can be converted to an integral type. Prior to
Spark 3.1.0 the MIN and MAX values were floating-point values such as `Int.MaxValue.toFloat` but
starting with 3.1.0 these are now integral types such as `Int.MaxValue` so this has slightly
affected the valid range of values and now differs slightly from the behavior on GPU in some cases.

To enable this operation on the GPU when using Spark 3.1.0 or later, set
[`spark.rapids.sql.castFloatToIntegralTypes.enabled`](configs.md#sql.castFloatToIntegralTypes.enabled)
to `true`.

This configuration setting is ignored when using Spark versions prior to 3.1.0.

### Float to String

The GPU will use different precision than Java's toString method when converting floating-point data
types to strings. The GPU uses a lowercase `e` prefix for an exponent while Spark uses uppercase
`E`. As a result the computed string can differ from the default behavior in Spark.

To enable this operation on the GPU, set
[`spark.rapids.sql.castFloatToString.enabled`](configs.md#sql.castFloatToString.enabled) to `true`.

### String to Float

Casting from string to floating-point types on the GPU returns incorrect results when the string
represents any number in the following ranges. In both cases the GPU returns `Double.MaxValue`. The
default behavior in Apache Spark is to return `+Infinity` and `-Infinity`, respectively.

- `1.7976931348623158E308 <= x < 1.7976931348623159E308`
- `-1.7976931348623159E308 < x <= -1.7976931348623158E308` 

Also, the GPU does not support casting from strings containing hex values.

To enable this operation on the GPU, set
[`spark.rapids.sql.castStringToFloat.enabled`](configs.md#sql.castStringToFloat.enabled) to `true`.

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
[`spark.rapids.sql.castStringToTimestamp.enabled`](configs.md#sql.castStringToTimestamp.enabled).

Casting from string to timestamp currently has the following limitations. 

| Format or Pattern                                                   | Supported on GPU? |
| ------------------------------------------------------------------- | ------------------|
| `"yyyy"`                                                            | Yes               |
| `"yyyy-[M]M"`                                                       | Yes               |
| `"yyyy-[M]M "`                                                      | Yes               |
| `"yyyy-[M]M-[d]d"`                                                  | Yes               |
| `"yyyy-[M]M-[d]d "`                                                 | Yes               |
| `"yyyy-[M]M-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]"` | Partial [\[1\]](#Footnote1)       |
| `"yyyy-[M]M-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]"` | Partial [\[1\]](#Footnote1)       |
| `"[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]"`                | Partial [\[1\]](#Footnote1)       |
| `"T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]"`               | Partial [\[1\]](#Footnote1)       |
| `"epoch"`                                                           | Yes               |
| `"now"`                                                             | Yes               |
| `"today"`                                                           | Yes               |
| `"tomorrow"`                                                        | Yes               |
| `"yesterday"`                                                       | Yes               |

- <a name="Footnote1"></a>[1] The timestamp portion must have 6 digits for milliseconds.
 Only timezone 'Z' (UTC) is supported. Casting unsupported formats will result in null values.

Spark is very lenient when casting from string to timestamp because all date and time components
are optional, meaning that input values such as `T`, `T2`, `:`, `::`, `1:`, `:1`, and `::1`
are considered valid timestamps. The GPU will treat these values as invalid and cast them to null
values.

### Constant Folding

ConstantFolding is an operator optimization rule in Catalyst that replaces expressions that can
be statically evaluated with their equivalent literal values. The RAPIDS Accelerator relies
on constant folding and parts of the query will not be accelerated if 
`org.apache.spark.sql.catalyst.optimizer.ConstantFolding` is excluded as a rule.

## JSON string handling
The 0.5 release introduces the `get_json_object` operation.  The JSON specification only allows
double quotes around strings in JSON data, whereas Spark allows single quotes around strings in JSON
data.  The RAPIDS Spark `get_json_object` operation on the GPU will return `None` in PySpark or
`Null` in Scala when trying to match a string surrounded by single quotes.  This behavior will be
updated in a future release to more closely match Spark.

## Approximate Percentile

The GPU implementation of `approximate_percentile` uses
[t-Digests](https://arxiv.org/abs/1902.04023) which have high accuracy, particularly near the tails of a
distribution. Because the results are not bit-for-bit identical with the Apache Spark implementation of
`approximate_percentile`, this feature is disabled by default and can be enabled by setting
`spark.rapids.sql.expression.ApproximatePercentile=true`.

There is also a known issue ([issue #4060](https://github.com/NVIDIA/spark-rapids/issues/4060)) where
incorrect results are produced intermittently.

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

