# RAPIDS Plugin For Apache Spark

Plugin for [Apache Spark](https://spark.apache.org) that leverages GPUs to accelerate processing
via the [RAPIDS](https://rapids.ai) libraries.

As of Apache Spark release 3.0 users can schedule GPU resources and can replace the backend for 
many SQL and dataframe operations so that they are accelerated using GPUs. The plugin does not work with
RDD operations. The plugin requires no API changes from the user, and it will replace SQL operations 
it supports with GPU operations. If the plugin doesn't support an operation,
it will fall back to using the Spark CPU version.  The plugin currently does not support spilling, so any
operation that runs on the GPU requires that each task's data fit into GPU memory. If it doesn't fit it will
error out, and the user will have to repartition data or change the parallelism configs such that it will fit.

To enable this GPU acceleration you will need:
  * Apache Spark 3.0+
  * Running on cluster that has nodes that comply with the requirements for [CUDF](https://github.com/rapidsai/cudf).
  * Ship the `rapids-4-spark` and `cudf` jars with your job
  * Set the config `spark.sql.extensions` to `ai.rapids.spark.Plugin`.

```
> spark-shell --jars 'rapids-4-spark-0.8-SNAPSHOT.jar,cudf-0.8-SNAPSHOT-cuda10.jar' --conf spark.sql.extensions=ai.rapids.spark.Plugin --conf spark.plugins=ai.rapids.spark.RapidsSparkPlugin
```

## <a name="MEMORY"></a>Memory

One of the slowest parts of processing data on the GPU is moving the data from host memory to GPU
memory and allocating GPU memory. To improve the performance of these operations Rapids uses RMM to
as a user level GPU memory allocator/cache and this plugin will allocate and reuse pinned memory
where possible to speed up CPU to GPU and GPU to CPU data transfers.

To enable RMM you need to set the config `spark.rapids.memory.gpu.pooling.enabled` to
`true` when launching your cluster.  To enable pinned memory you also
need to set `spark.rapids.memory.pinnedPool.size` to the amount of pinned memory you want to use.
Because this is used for data transfers it typically should be about 1/4 to 1/2 of the amount of
GPU memory you have. You also need to enable it by setting the java System property 
`ai.rapids.cudf.prefer-pinned` to `true`.

If you are running Spark under YARN or kubernetes you need to be sure that you are adding in the
overhead for this memory in addition to what you normally would ask for as this memory is not
currently tracked by spark.

## Configuration

The Plugin has a set of configs that controls the behavior.
Some of these are due to CUDF not being completely compatible with the Spark behavior and some of these are
just adding the ability to turn on and off different operators.

Pooled GPU memory allocation can be enabled to improve performance, but this should not be used
if you want to use operators that also use GPU memory like XGBoost or Tensorflow, as the pool
it allocates cannot be used by other tools.
To enable pool GPU memory allocation set config `spark.rapids.memory.gpu.pooling.enabled` to `true`.

See documentation for [configs](docs/configs.md).

## Releases

| Version | Description |
|---------|-------------|

## Downloading

You can get the [rapids-4-spark](https://gpuwa.nvidia.com/artifactory/sw-spark-maven-local/ai/rapids/rapids-4-spark/) and [cudf](http://gpuwa.nvidia.com/artifactory/sw-spark-maven-local/ai/rapids/cudf/) jars in artifactory.

## Monitoring

Since the plugin runs without any API changes, the easiest way to see what is running on the GPU is to look at the "SQL" tab in the Spark Web UI. The SQL tab only shows up once you have actually executed a query. Go to the SQL tab in the UI, click on the query you are interested in an it shows a DAG picture with details. You can also scroll down and twisty the "Details" section to see the text representation.

If you want to look at the Spark plan via the code you can use the `explain()` function call. For example: query.explain() will print the physical plan from Spark and you can see what nodes were replaced with GPU calls. 

To see why some parts of your query did not run on the GPU set the config `spark.rapids.sql.explain` to `true`. The output will will be logged to the driver's log or to the screen in interactive mode.

## Debugging

For now, the best way to debug is how you would normally do it on Spark. Look at the UI and log files to see what failed. If you got a seg fault from the GPU find the hs_err_pid.log file. To make sure your hs_err_pid.log file goes into the YARN application log dir you can add in the config: `--conf spark.executor.extraJavaOptions="-XX:ErrorFile=<LOG_DIR>/hs_err_pid_%p.log"`

## Issues

Please file NVBugs for any issues you have in the [Machine Learning - Spark](https://nvbugswb.nvidia.com/NvBugs5/SWBug.aspx) Module.

## Spark 3.0

For internal Nvidia use, we have a Spark 3.0 distribution available since Spark 3.0 hasn't been released yet.
You can find these releases available in [artifactory](http://gpuwa.nvidia.com/artifactory/sw-spark-maven-local/org/apache/spark/3.0.0-SNAPSHOT/)

## Notes on Building

The build requires Apache Spark 3.0+ built against the nohive ORC classifier.  Building with a
version of Spark built without the nohive support (e.g.: the Apache Spark hadoop-3.2 profile)
will cause errors during build and test due to the conflicting ORC jars.

## Tests

We have several tests that you can run in the tests subdirectory/jar.

One set is based off of the mortgage dataset you can download at https://rapidsai.github.io/demos/datasets/mortgage-data or http://www.fanniemae.com/portal/funding-the-market/data/loan-performance-data.html
and are in the ai.rapids.sparkexamples.mortgage package.

The other is based off of TPCH. You can use the TPCH `dbgen` tool to generate data for them.  They are
in the ai.rapids.sparkexamples.tpch package. dbgen has various options, one way to generate the data is like
`dbgen -b dists.dss -s 10`.

You can include the test jar (`rapids-plugin-4-spark/tests/target/rapids-4-spark-tests-0.9-SNAPSHOT.jar`) with the
Spark --jars option to get the Tpch tests. To setup for the queries you can run `TpchLikeSpark.setupAllCSV`
for CSV formatted data or `TpchLikeSpark.setupAllParquet` for parquet formatted data.  Both of those take
 the spark session and a path to the dbgen generated data.  After that each query has its own object.
So you can call like:
```
import ai.rapids.sparkexamples.tpch._
val pathTodbgenoutput = SPECIFY PATH
TpchLikeSpark.setupAllCSV(spark, pathTodbgenoutput)
Q1Like(spark).count()
```
They generally follow TPCH but are not guaranteed to be the same.
`Q1Like(spark)` will return a dataframe that can be executed to run the corresponding query.

## Advanced Operations

### Zero Copy Exporting of GPU data.
There are cases where you may want to get access to the raw data on the GPU, preferably without
copying it. One use case for this is exporting the data to an ML framework after doing feature
extraction. To do this we provide a simple Scala utility `ai.rapids.spark.ColumnarRdd` that can
be used to convert a `DataFrame` to an `RDD[ai.rapids.cudf.Table]`. Each Table will have the same
schema as the `DataFrame` passed in.

`Table` is not a typical thing in an `RDD` so special care needs to be taken when working with it.
By default it is not serializable so repartitioning the `RDD` or any other operator that involves
a shuffle will not work. This is because it is relatively expensive to serialize and
deserialize GPU data using a conventional spark shuffle. In addition most of the memory associated
with the Table is on the GPU itself, so each table must be closed when it is no longer needed to
avoid running out of GPU memory. By convention it is the responsibility of the one consuming the
data to close it when they no longer need it.

```scala
val df = spark.sql("""select my_column from my_table""")
val rdd: RDD[Table] = ColumnarRdd(df)
// Compute the max of the first column
val maxValue = rdd.map(table => {
  val max = table.getColumn(0).max().getLong
  // Close the table to avoid leaks
  table.close()
  max
}).max()
```

You may need to disable [RMM](#memory) caching when exporting data to an ML library as that library
will likely want to use all of the GPU's memory and if it is not aware of RMM it will not have
access to any of the memory that RMM is holding. 