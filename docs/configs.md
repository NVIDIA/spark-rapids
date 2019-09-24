Rapids Configs:
spark.rapids.sql.enabled:
	Enable (true) or disable (false) sql operations on the GPU
	default true

spark.rapids.sql.incompatible_ops:
	For operations that work, but are not 100% compatible with the Spark equivalent   set if they should be enabled by default or disabled by default.
	default false

spark.rapids.sql.hasNans:
	Config to indicate if your data has NaN's. Cudf doesn't currently support NaN's properly so you can get corrupt data if you have NaN's in your data and it runs on the GPU.
	default true

spark.rapids.sql.batchSizeRows:
	Set the target number of rows for a GPU batch. Splits sizes for input data is covered by separate configs.
	default 1000000

spark.rapids.sql.allowIncompatUTF8Strings:
	Config to allow GPU operations that are incompatible for UTF8 strings. Only turn to true if your data is ASCII compatible. If you do have UTF8 strings in your data and you set this to true, it can cause data corruption/loss if doing a sort merge join.
	default false

spark.rapids.sql.allowVariableFloatAgg:
	Spark assumes that all operations produce the exact same result each time. This is not true for some floating point aggregations, which can produce slightly different results on the GPU as the aggregation is done in parallel.  This can enable those operations if you know the query is only computing it once.
	default false

spark.rapids.sql.enableStringHashGroupBy:
	Config to allow grouping by strings using the GPU in the hash aggregate. Currently they are really slow
	default false

spark.rapids.sql.explain:
	Explain why some parts of a query were not placed on a GPU
	default false

spark.rapids.memory_debug:
	If memory management is enabled and this is true GPU memory allocations are tracked and printed out when the process exits.  This should not be used in production.
	default false

spark.rapids.sql.maxReaderBatchSize:
	Maximum number of rows the reader reads at a time
	default 2147483647

spark.rapids.sql.enableTotalOrderSort:
	Allow for total ordering sort where the partitioning runs on CPU and sort runs on GPU.
	default false

spark.rapids.sql.enableReplaceSortMergeJoin:
	Allow replacing sortMergeJoin with HashJoin
	default true

spark.rapids.sql.expression.Not:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Not Expression.
	boolean not operator
	default: true

spark.rapids.sql.expression.GreaterThanOrEqual:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual Expression.
	>= operator
	default: true

spark.rapids.sql.expression.Literal:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Literal Expression.
	holds a static value from the query
	default: true

spark.rapids.sql.expression.Year:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Year Expression.
	get the year from a date or timestamp
	default: true

spark.rapids.sql.expression.UnaryPositive:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.UnaryPositive Expression.
	a numeric value with a + in front of it
	default: true

spark.rapids.sql.expression.Log:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Log Expression.
	natural log
	This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount
	default: false

spark.rapids.sql.expression.Sqrt:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Sqrt Expression.
	square root
	default: true

spark.rapids.sql.expression.Last:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.aggregate.Last Expression.
	last aggregate operator
	default: true

spark.rapids.sql.expression.Exp:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Exp Expression.
	Euler's number e raised to a power
	This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount
	default: false

spark.rapids.sql.expression.Sum:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.aggregate.Sum Expression.
	sum aggregate operator
	default: true

spark.rapids.sql.expression.LessThan:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.LessThan Expression.
	< operator
	default: true

spark.rapids.sql.expression.Max:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.aggregate.Max Expression.
	max aggregate operator
	default: true

spark.rapids.sql.expression.Tan:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Tan Expression.
	tangent
	This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount
	default: false

spark.rapids.sql.expression.Subtract:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Subtract Expression.
	subtraction
	default: true

spark.rapids.sql.expression.AggregateExpression:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression Expression.
	aggregate expression
	default: true

spark.rapids.sql.expression.Average:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.aggregate.Average Expression.
	average aggregate operator
	default: true

spark.rapids.sql.expression.Cos:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Cos Expression.
	cosine
	This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount
	default: false

spark.rapids.sql.expression.KnownFloatingPointNormalized:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.KnownFloatingPointNormalized Expression.
	tag to prevent redundant normalization
	This is not 100% compatible with the Spark version because when enabling these, there may be extra groups produced for floating point grouping keys (e.g. -0.0, and 0.0)
	default: false

spark.rapids.sql.expression.Month:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Month Expression.
	get the month from a date or timestamp
	default: true

spark.rapids.sql.expression.SortOrder:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.SortOrder Expression.
	sort order
	default: true

spark.rapids.sql.expression.Sin:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Sin Expression.
	sine
	This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount
	default: false

spark.rapids.sql.expression.IsNotNull:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.IsNotNull Expression.
	checks if a value is not null
	default: true

spark.rapids.sql.expression.Acos:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Acos Expression.
	inverse cosine
	default: true

spark.rapids.sql.expression.IntegralDivide:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.IntegralDivide Expression.
	division with a integer result
	This is not 100% compatible with the Spark version because divide by 0 does not result in null
	default: false

spark.rapids.sql.expression.Floor:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Floor Expression.
	floor of a number
	default: true

spark.rapids.sql.expression.And:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.And Expression.
	logical and
	default: true

spark.rapids.sql.expression.Asin:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Asin Expression.
	inverse sine
	default: true

spark.rapids.sql.expression.Cast:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Cast Expression.
	convert a column of one type of data into another type
	default: true

spark.rapids.sql.expression.UnaryMinus:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.UnaryMinus Expression.
	negate a numeric value
	default: true

spark.rapids.sql.expression.LessThanOrEqual:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.LessThanOrEqual Expression.
	<= operator
	default: true

spark.rapids.sql.expression.Min:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.aggregate.Min Expression.
	min aggregate operator
	default: true

spark.rapids.sql.expression.Divide:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Divide Expression.
	division
	This is not 100% compatible with the Spark version because divide by 0 does not result in null
	default: false

spark.rapids.sql.expression.Count:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.aggregate.Count Expression.
	count aggregate operator
	default: true

spark.rapids.sql.expression.Alias:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Alias Expression.
	gives a column a name
	default: true

spark.rapids.sql.expression.First:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.aggregate.First Expression.
	first aggregate operator
	default: true

spark.rapids.sql.expression.Multiply:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Multiply Expression.
	multiplication
	default: true

spark.rapids.sql.expression.GreaterThan:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.GreaterThan Expression.
	> operator
	default: true

spark.rapids.sql.expression.Abs:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Abs Expression.
	absolute value
	default: true

spark.rapids.sql.expression.EqualTo:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.EqualTo Expression.
	check if the values are equal
	default: true

spark.rapids.sql.expression.Add:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Add Expression.
	addition
	default: true

spark.rapids.sql.expression.Ceil:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Ceil Expression.
	ceiling of a number
	default: true

spark.rapids.sql.expression.Remainder:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Remainder Expression.
	remainder or modulo
	This is not 100% compatible with the Spark version because divide by 0 does not result in null
	default: false

spark.rapids.sql.expression.Or:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Or Expression.
	logical or
	default: true

spark.rapids.sql.expression.NormalizeNaNAndZero:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero Expression.
	normalize nan and zero
	This is not 100% compatible with the Spark version because when enabling these, there may be extra groups produced for floating point grouping keys (e.g. -0.0, and 0.0)
	default: false

spark.rapids.sql.expression.AttributeReference:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.AttributeReference Expression.
	references an input column
	default: true

spark.rapids.sql.expression.IsNull:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.IsNull Expression.
	checks if a value is null
	default: true

spark.rapids.sql.expression.Atan:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Atan Expression.
	inverse tangent
	This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount
	default: false

spark.rapids.sql.expression.DayOfMonth:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.DayOfMonth Expression.
	get the day of the month from a date or timestamp
	default: true

spark.rapids.sql.expression.Pow:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.expressions.Pow Expression.
	lhs ^ rhs
	This is not 100% compatible with the Spark version because floating point results in some cases may differ with the JVM version by a small amount
	default: false

spark.rapids.sql.exec.SortMergeJoinExec:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.joins.SortMergeJoinExec Exec.
	Sort merge join, replacing with shuffled hash join
	default: true

spark.rapids.sql.exec.HashAggregateExec:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.aggregate.HashAggregateExec Exec.
	The backend for hash based aggregations
	default: true

spark.rapids.sql.exec.BatchScanExec:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.datasources.v2.BatchScanExec Exec.
	The backend for most file input
	default: true

spark.rapids.sql.exec.ShuffledHashJoinExec:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.joins.ShuffledHashJoinExec Exec.
	Implementation of join using hashed shuffled data
	default: true

spark.rapids.sql.exec.ProjectExec:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.ProjectExec Exec.
	The backend for most select, withColumn and dropColumn statements
	default: true

spark.rapids.sql.exec.UnionExec:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.UnionExec Exec.
	The backend for the union operator
	default: true

spark.rapids.sql.exec.SortExec:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.SortExec Exec.
	The backend for the sort operator
	default: true

spark.rapids.sql.exec.FilterExec:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.FilterExec Exec.
	The backend for most filter statements
	default: true

spark.rapids.sql.exec.BroadcastExchangeExec:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.exchange.BroadcastExchangeExec Exec.
	The backend for broadcast exchange of data
	default: true

spark.rapids.sql.exec.BroadcastHashJoinExec:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.joins.BroadcastHashJoinExec Exec.
	Implementation of join using broadcast data
	This is not 100% compatible with the Spark version because GPU required on the driver
	default: false

spark.rapids.sql.exec.ShuffleExchangeExec:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.exchange.ShuffleExchangeExec Exec.
	The backend for most data being exchanged between processes
	default: true

spark.rapids.sql.input.CSVScan:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.datasources.v2.csv.CSVScan Input.
	CSV parsing
	default: true

spark.rapids.sql.input.ParquetScan:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan Input.
	Parquet parsing
	default: true

spark.rapids.sql.input.OrcScan:
	Enable (true) or disable (false) the org.apache.spark.sql.execution.datasources.v2.orc.OrcScan Input.
	ORC parsing
	default: true

spark.rapids.sql.partitioning.HashPartitioning:
	Enable (true) or disable (false) the org.apache.spark.sql.catalyst.plans.physical.HashPartitioning Partitioning.
	Hash based partitioning
	default: true

