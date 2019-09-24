# Rapids Plugin 4 Spark Configuration
The following is the list of options that `rapids-plugin-4-spark` supports.

On startup use: `--conf [conf key]=[conf value]`. For example:

```
${SPARK_HOME}/bin/spark --jars 'rapids-4-spark-0.1-SNAPSHOT.jar,cudf-0.10-SNAPSHOT-cuda10.jar' \
--conf spark.sql.extensions=ai.rapids.spark.Plugin \
--conf spark.rapids.sql.incompatible_ops=true
```

At runtime use: `spark.conf.set("[conf key]", [conf value])`. For example:

```
scala> spark.conf.set("spark.rapids.sql.incompatible_ops", true)
```

## General Configuration
Name | Description | Default Value
-----|-------------|--------------
spark.rapids.memory_debug|If memory management is enabled and this is true GPU memory allocations are tracked and printed out when the process exits.  This should not be used in production.|false
spark.rapids.sql.allowIncompatUTF8Strings|Config to allow GPU operations that are incompatible for UTF8 strings. Only turn to true if your data is ASCII compatible. If you do have UTF8 strings in your data and you set this to true, it can cause data corruption/loss if doing a sort merge join.|false
spark.rapids.sql.allowVariableFloatAgg|Spark assumes that all operations produce the exact same result each time. This is not true for some floating point aggregations, which can produce slightly different results on the GPU as the aggregation is done in parallel.  This can enable those operations if you know the query is only computing it once.|false
spark.rapids.sql.batchSizeRows|Set the target number of rows for a GPU batch. Splits sizes for input data is covered by separate configs.|1000000
spark.rapids.sql.enableReplaceSortMergeJoin|Allow replacing sortMergeJoin with HashJoin|true
spark.rapids.sql.enableStringHashGroupBy|Config to allow grouping by strings using the GPU in the hash aggregate. Currently they are really slow|false
spark.rapids.sql.enableTotalOrderSort|Allow for total ordering sort where the partitioning runs on CPU and sort runs on GPU.|false
spark.rapids.sql.enabled|Enable (true) or disable (false) sql operations on the GPU|true
spark.rapids.sql.explain|Explain why some parts of a query were not placed on a GPU|false
spark.rapids.sql.hasNans|Config to indicate if your data has NaN's. Cudf doesn't currently support NaN's properly so you can get corrupt data if you have NaN's in your data and it runs on the GPU.|true
spark.rapids.sql.incompatible_ops|For operations that work, but are not 100% compatible with the Spark equivalent   set if they should be enabled by default or disabled by default.|false
spark.rapids.sql.maxReaderBatchSize|Maximum number of rows the reader reads at a time|2147483647

## Fine Tunning
_Rapids Plugin 4 Spark_ can be further configured to enable or disable specific
expressions and to control what parts of the query execute using the GPU or
the CPU.

Please leverage the `spark.rapids.sql.explain` setting to get feeback from the
plugin as to why parts of a query may not be executing in the GPU.

**NOTE:** Setting `spark.rapids.sql.incompatible_ops=true` will enable all
the settings in the table below which are not enabled by default due to
incompatibilities.

### Expressions
Name | Description | Default Value | Incompatibilities
-----|-------------|---------------|------------------
org.apache.spark.sql.catalyst.expressions.Abs|absolute value|true|None|
org.apache.spark.sql.catalyst.expressions.Acos|inverse cosine|true|None|
org.apache.spark.sql.catalyst.expressions.Add|addition|true|None|
org.apache.spark.sql.catalyst.expressions.Alias|gives a column a name|true|None|
org.apache.spark.sql.catalyst.expressions.And|logical and|true|None|
org.apache.spark.sql.catalyst.expressions.Asin|inverse sine|true|None|
org.apache.spark.sql.catalyst.expressions.Atan|inverse tangent|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
org.apache.spark.sql.catalyst.expressions.AttributeReference|references an input column|true|None|
org.apache.spark.sql.catalyst.expressions.Cast|convert a column of one type of data into another type|true|None|
org.apache.spark.sql.catalyst.expressions.Ceil|ceiling of a number|true|None|
org.apache.spark.sql.catalyst.expressions.Cos|cosine|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
org.apache.spark.sql.catalyst.expressions.DayOfMonth|get the day of the month from a date or timestamp|true|None|
org.apache.spark.sql.catalyst.expressions.Divide|division|false|This is not 100% compatible with the Spark version because divide by 0 does not result in null|
org.apache.spark.sql.catalyst.expressions.EqualTo|check if the values are equal|true|None|
org.apache.spark.sql.catalyst.expressions.Exp|Euler's number e raised to a power|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
org.apache.spark.sql.catalyst.expressions.Floor|floor of a number|true|None|
org.apache.spark.sql.catalyst.expressions.GreaterThan|> operator|true|None|
org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual|>= operator|true|None|
org.apache.spark.sql.catalyst.expressions.IntegralDivide|division with a integer result|false|This is not 100% compatible with the Spark version because divide by 0 does not result in null|
org.apache.spark.sql.catalyst.expressions.IsNotNull|checks if a value is not null|true|None|
org.apache.spark.sql.catalyst.expressions.IsNull|checks if a value is null|true|None|
org.apache.spark.sql.catalyst.expressions.KnownFloatingPointNormalized|tag to prevent redundant normalization|false|This is not 100% compatible with the Spark version because when enabling these, there may be extra groups produced for floating point grouping keys (e.g. -0.0, and 0.0)|
org.apache.spark.sql.catalyst.expressions.LessThan|< operator|true|None|
org.apache.spark.sql.catalyst.expressions.LessThanOrEqual|<= operator|true|None|
org.apache.spark.sql.catalyst.expressions.Literal|holds a static value from the query|true|None|
org.apache.spark.sql.catalyst.expressions.Log|natural log|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
org.apache.spark.sql.catalyst.expressions.Month|get the month from a date or timestamp|true|None|
org.apache.spark.sql.catalyst.expressions.Multiply|multiplication|true|None|
org.apache.spark.sql.catalyst.expressions.Not|boolean not operator|true|None|
org.apache.spark.sql.catalyst.expressions.Or|logical or|true|None|
org.apache.spark.sql.catalyst.expressions.Pow|lhs ^ rhs|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
org.apache.spark.sql.catalyst.expressions.Remainder|remainder or modulo|false|This is not 100% compatible with the Spark version because divide by 0 does not result in null|
org.apache.spark.sql.catalyst.expressions.Sin|sine|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
org.apache.spark.sql.catalyst.expressions.SortOrder|sort order|true|None|
org.apache.spark.sql.catalyst.expressions.Sqrt|square root|true|None|
org.apache.spark.sql.catalyst.expressions.Subtract|subtraction|true|None|
org.apache.spark.sql.catalyst.expressions.Tan|tangent|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
org.apache.spark.sql.catalyst.expressions.UnaryMinus|negate a numeric value|true|None|
org.apache.spark.sql.catalyst.expressions.UnaryPositive|a numeric value with a + in front of it|true|None|
org.apache.spark.sql.catalyst.expressions.Year|get the year from a date or timestamp|true|None|
org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression|aggregate expression|true|None|
org.apache.spark.sql.catalyst.expressions.aggregate.Average|average aggregate operator|true|None|
org.apache.spark.sql.catalyst.expressions.aggregate.Count|count aggregate operator|true|None|
org.apache.spark.sql.catalyst.expressions.aggregate.First|first aggregate operator|true|None|
org.apache.spark.sql.catalyst.expressions.aggregate.Last|last aggregate operator|true|None|
org.apache.spark.sql.catalyst.expressions.aggregate.Max|max aggregate operator|true|None|
org.apache.spark.sql.catalyst.expressions.aggregate.Min|min aggregate operator|true|None|
org.apache.spark.sql.catalyst.expressions.aggregate.Sum|sum aggregate operator|true|None|
org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero|normalize nan and zero|false|This is not 100% compatible with the Spark version because when enabling these, there may be extra groups produced for floating point grouping keys (e.g. -0.0, and 0.0)|

### Execution
Name | Description | Default Value | Incompatibilities
-----|-------------|---------------|------------------
org.apache.spark.sql.execution.FilterExec|The backend for most filter statements|true|None|
org.apache.spark.sql.execution.ProjectExec|The backend for most select, withColumn and dropColumn statements|true|None|
org.apache.spark.sql.execution.SortExec|The backend for the sort operator|true|None|
org.apache.spark.sql.execution.UnionExec|The backend for the union operator|true|None|
org.apache.spark.sql.execution.aggregate.HashAggregateExec|The backend for hash based aggregations|true|None|
org.apache.spark.sql.execution.datasources.v2.BatchScanExec|The backend for most file input|true|None|
org.apache.spark.sql.execution.exchange.BroadcastExchangeExec|The backend for broadcast exchange of data|true|None|
org.apache.spark.sql.execution.exchange.ShuffleExchangeExec|The backend for most data being exchanged between processes|true|None|
org.apache.spark.sql.execution.joins.BroadcastHashJoinExec|Implementation of join using broadcast data|false|This is not 100% compatible with the Spark version because GPU required on the driver|
org.apache.spark.sql.execution.joins.ShuffledHashJoinExec|Implementation of join using hashed shuffled data|true|None|
org.apache.spark.sql.execution.joins.SortMergeJoinExec|Sort merge join, replacing with shuffled hash join|true|None|

### Scans
Name | Description | Default Value | Incompatibilities
-----|-------------|---------------|------------------
org.apache.spark.sql.execution.datasources.v2.csv.CSVScan|CSV parsing|true|None|
org.apache.spark.sql.execution.datasources.v2.orc.OrcScan|ORC parsing|true|None|
org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan|Parquet parsing|true|None|

### Partitioning
Name | Description | Default Value | Incompatibilities
-----|-------------|---------------|------------------
org.apache.spark.sql.catalyst.plans.physical.HashPartitioning|Hash based partitioning|true|None|
