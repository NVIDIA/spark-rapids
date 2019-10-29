# Rapids Plugin 4 Spark Configuration
The following is the list of options that `rapids-plugin-4-spark` supports.

On startup use: `--conf [conf key]=[conf value]`. For example:

```
${SPARK_HOME}/bin/spark --jars 'rapids-4-spark-0.1-SNAPSHOT.jar,cudf-0.10-SNAPSHOT-cuda10.jar' \
--conf spark.sql.extensions=ai.rapids.spark.Plugin \
--conf spark.rapids.sql.incompatibleOps.enabled=true
```

At runtime use: `spark.conf.set("[conf key]", [conf value])`. For example:

```
scala> spark.conf.set("spark.rapids.sql.incompatibleOps.enabled", true)
```

## General Configuration
Name | Description | Default Value
-----|-------------|--------------
spark.rapids.memory.gpu.debug|If memory management is enabled and this is true GPU memory allocations are tracked and printed out when the process exits.  This should not be used in production.|false
spark.rapids.memory.pinnedPool.size|The size of the pinned memory pool in bytes unless otherwise specified. Use 0 to disable the pool.|0
spark.rapids.sql.batchSizeRows|Set the target number of rows for a GPU batch. Splits sizes for input data is covered by separate configs.|1000000
spark.rapids.sql.enabled|Enable (true) or disable (false) sql operations on the GPU|true
spark.rapids.sql.explain|Explain why some parts of a query were not placed on a GPU or not. Possible values are ALL: print everything, NONE: print nothing, NOT_ON_GPU: print only did not go on the GPU|NONE
spark.rapids.sql.hasNans|Config to indicate if your data has NaN's. Cudf doesn't currently support NaN's properly so you can get corrupt data if you have NaN's in your data and it runs on the GPU.|true
spark.rapids.sql.incompatibleOps.enabled|For operations that work, but are not 100% compatible with the Spark equivalent set if they should be enabled by default or disabled by default.|false
spark.rapids.sql.reader.batchSizeRows|Maximum number of rows the reader reads at a time|2147483647
spark.rapids.sql.replaceSortMergeJoin.enabled|Allow replacing sortMergeJoin with HashJoin|true
spark.rapids.sql.stringHashGroupBy.enabled|Config to allow grouping by strings using the GPU in the hash aggregate. Currently they are really slow|false
spark.rapids.sql.totalOrderSort.enabled|Allow for total ordering sort where the partitioning runs on CPU and sort runs on GPU.|false
spark.rapids.sql.variableFloatAgg.enabled|Spark assumes that all operations produce the exact same result each time. This is not true for some floating point aggregations, which can produce slightly different results on the GPU as the aggregation is done in parallel.  This can enable those operations if you know the query is only computing it once.|false

## Fine Tunning
_Rapids Plugin 4 Spark_ can be further configured to enable or disable specific
expressions and to control what parts of the query execute using the GPU or
the CPU.

Please leverage the `spark.rapids.sql.explain` setting to get feeback from the
plugin as to why parts of a query may not be executing in the GPU.

**NOTE:** Setting `spark.rapids.sql.incompatibleOps.enabled=true` will enable all
the settings in the table below which are not enabled by default due to
incompatibilities.

### Expressions
Name | Description | Default Value | Incompatibilities
-----|-------------|---------------|------------------
spark.rapids.sql.expression.Abs|absolute value|true|None|
spark.rapids.sql.expression.Acos|inverse cosine|true|None|
spark.rapids.sql.expression.Add|addition|true|None|
spark.rapids.sql.expression.Alias|gives a column a name|true|None|
spark.rapids.sql.expression.And|logical and|true|None|
spark.rapids.sql.expression.Asin|inverse sine|true|None|
spark.rapids.sql.expression.Atan|inverse tangent|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
spark.rapids.sql.expression.AttributeReference|references an input column|true|None|
spark.rapids.sql.expression.Cast|convert a column of one type of data into another type|true|None|
spark.rapids.sql.expression.Ceil|ceiling of a number|true|None|
spark.rapids.sql.expression.Cos|cosine|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
spark.rapids.sql.expression.DayOfMonth|get the day of the month from a date or timestamp|true|None|
spark.rapids.sql.expression.Divide|division|false|This is not 100% compatible with the Spark version because divide by 0 does not result in null|
spark.rapids.sql.expression.EqualTo|check if the values are equal|true|None|
spark.rapids.sql.expression.Exp|Euler's number e raised to a power|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
spark.rapids.sql.expression.Floor|floor of a number|true|None|
spark.rapids.sql.expression.GreaterThan|> operator|true|None|
spark.rapids.sql.expression.GreaterThanOrEqual|>= operator|true|None|
spark.rapids.sql.expression.IntegralDivide|division with a integer result|false|This is not 100% compatible with the Spark version because divide by 0 does not result in null|
spark.rapids.sql.expression.IsNotNull|checks if a value is not null|true|None|
spark.rapids.sql.expression.IsNull|checks if a value is null|true|None|
spark.rapids.sql.expression.KnownFloatingPointNormalized|tag to prevent redundant normalization|false|This is not 100% compatible with the Spark version because when enabling these, there may be extra groups produced for floating point grouping keys (e.g. -0.0, and 0.0)|
spark.rapids.sql.expression.LessThan|< operator|true|None|
spark.rapids.sql.expression.LessThanOrEqual|<= operator|true|None|
spark.rapids.sql.expression.Literal|holds a static value from the query|true|None|
spark.rapids.sql.expression.Log|natural log|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
spark.rapids.sql.expression.Month|get the month from a date or timestamp|true|None|
spark.rapids.sql.expression.Multiply|multiplication|true|None|
spark.rapids.sql.expression.Not|boolean not operator|true|None|
spark.rapids.sql.expression.Or|logical or|true|None|
spark.rapids.sql.expression.Pow|lhs ^ rhs|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
spark.rapids.sql.expression.Remainder|remainder or modulo|false|This is not 100% compatible with the Spark version because divide by 0 does not result in null|
spark.rapids.sql.expression.Sin|sine|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
spark.rapids.sql.expression.SortOrder|sort order|true|None|
spark.rapids.sql.expression.Sqrt|square root|true|None|
spark.rapids.sql.expression.Subtract|subtraction|true|None|
spark.rapids.sql.expression.Tan|tangent|false|This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount|
spark.rapids.sql.expression.UnaryMinus|negate a numeric value|true|None|
spark.rapids.sql.expression.UnaryPositive|a numeric value with a + in front of it|true|None|
spark.rapids.sql.expression.Year|get the year from a date or timestamp|true|None|
spark.rapids.sql.expression.AggregateExpression|aggregate expression|true|None|
spark.rapids.sql.expression.Average|average aggregate operator|true|None|
spark.rapids.sql.expression.Count|count aggregate operator|true|None|
spark.rapids.sql.expression.First|first aggregate operator|true|None|
spark.rapids.sql.expression.Last|last aggregate operator|true|None|
spark.rapids.sql.expression.Max|max aggregate operator|true|None|
spark.rapids.sql.expression.Min|min aggregate operator|true|None|
spark.rapids.sql.expression.Sum|sum aggregate operator|true|None|
spark.rapids.sql.expression.NormalizeNaNAndZero|normalize nan and zero|false|This is not 100% compatible with the Spark version because when enabling these, there may be extra groups produced for floating point grouping keys (e.g. -0.0, and 0.0)|

### Execution
Name | Description | Default Value | Incompatibilities
-----|-------------|---------------|------------------
spark.rapids.sql.exec.FilterExec|The backend for most filter statements|true|None|
spark.rapids.sql.exec.ProjectExec|The backend for most select, withColumn and dropColumn statements|true|None|
spark.rapids.sql.exec.SortExec|The backend for the sort operator|true|None|
spark.rapids.sql.exec.UnionExec|The backend for the union operator|true|None|
spark.rapids.sql.exec.HashAggregateExec|The backend for hash based aggregations|true|None|
spark.rapids.sql.exec.BatchScanExec|The backend for most file input|true|None|
spark.rapids.sql.exec.BroadcastExchangeExec|The backend for broadcast exchange of data|true|None|
spark.rapids.sql.exec.ShuffleExchangeExec|The backend for most data being exchanged between processes|true|None|
spark.rapids.sql.exec.BroadcastHashJoinExec|Implementation of join using broadcast data|true|None|
spark.rapids.sql.exec.ShuffledHashJoinExec|Implementation of join using hashed shuffled data|true|None|
spark.rapids.sql.exec.SortMergeJoinExec|Sort merge join, replacing with shuffled hash join|true|None|

### Scans
Name | Description | Default Value | Incompatibilities
-----|-------------|---------------|------------------
spark.rapids.sql.input.CSVScan|CSV parsing|true|None|
spark.rapids.sql.input.OrcScan|ORC parsing|true|None|
spark.rapids.sql.input.ParquetScan|Parquet parsing|true|None|

### Partitioning
Name | Description | Default Value | Incompatibilities
-----|-------------|---------------|------------------
spark.rapids.sql.partitioning.HashPartitioning|Hash based partitioning|true|None|
