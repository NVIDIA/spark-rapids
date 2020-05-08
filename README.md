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
  * Set the config `spark.plugins` to `ai.rapids.spark.SQLPlugin`.

```
> spark-shell --jars 'rapids-4-spark_2.12-0.1-SNAPSHOT.jar,cudf-0.14-SNAPSHOT-cuda10.jar' --conf spark.plugins=ai.rapids.spark.SQLPlugin
```

Note if you are using the KryoSerializer with Spark (`--conf spark.serializer=org.apache.spark.serializer.KryoSerializer`) you will have to register the GpuKryoRegistrator class: `--conf spark.kryo.registrator=ai.rapids.spark.GpuKryoRegistrator`.

## Compatibility

The SQL plugin tries to produce results that are bit for bit identical with Apache Spark.
There are a number of cases where there are some differences. In most cases operators
that produce different results are off by default, and you can look at the
[configs](docs/configs.md) for more information on how to enable them.  In some cases
we felt that enabling the incompatibility by default was worth the performance gain. All
of those operators can be disabled through configs if it becomes a problem.

### Ordering of Output

There are some operators where Spark does not guarantee the order of the output.
These are typically things like aggregates and joins that may use a hash to distribute the work
load among downstream tasks. In these cases the plugin does not guarantee that it will
produce the same output order as Spark does. In cases such as an `order by` operation
where the ordering is explicit the plugin will produce an ordering that is compatible with
Spark's guarantee. It may not be 100% identical if the ordering is ambiguous.

### Floating Point

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
`spark.rapids.sql.variableFloatAgg.enabled`.

### Unicode

Spark delegates Unicode operations to the underlying JVM. Each version of Java compies with a
specific version of the Unicode standard. The SQL plugin does not use the JVM for Unicode support
and is compatible with Unicode version 12.1. Because of this there may be corner cases where
Spark will produce a different result compared to the plugin.

### CSV Reading

Spark is very strict when reading CSV and if the data does not conform with the expected format
exactly it will result in a `null` value. The underlying parser that the SQL plugin uses is much
more lenient. If you have badly formatted CSV data you may get data back instead of nulls.
If this is a problem you can disable the CSV reader by setting the config 
`spark.rapids.sql.input.CSVScan` to `false`. Because the speed up is so large and the issues only
show up in error conditions we felt it was worth having the CSV reader enabled by default.

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
currently tracked by Spark.

## Configuration

The Plugin has a set of configs that controls the behavior.
Some of these are due to CUDF not being completely compatible with the Spark behavior and some of these are
just adding the ability to turn on and off different operators.

Pooled GPU memory allocation can be enabled to improve performance, but this should not be used
if you want to use operators that also use GPU memory like XGBoost or Tensorflow, as the pool
it allocates cannot be used by other tools.
To enable pool GPU memory allocation set config `spark.rapids.memory.gpu.pooling.enabled` to `true`.

See documentation for [configs](docs/configs.md).

## GPU Scheduling

Spark 3.0 adds support for scheduling GPUs. The exact configs vary depending on the cluster manager you
are running - YARN, Kubernetes, Standalone. The scheduling features and scripts that come with Spark
rely on the executors running in an isolated environment (e.g.: Docker that ensures only your container sees 
the assigned GPU) on YARN and Kubernetes.
If you are running in Standalone mode or in an isolated environment you can use GPU scheduling by
adding configs like below. See the Spark 3.0 documentation for more specifics.

```
--conf spark.executor.resource.gpu.amount=1
--conf spark.task.resource.gpu.amount=1 
--conf spark.executor.resource.gpu.discoveryScript=./getGpusResources.sh
--conf spark.executor.resource.gpu.vendor=nvidia.com // only needed on Kubernetes
```
Note that the discoveryScript above has to be shipped with your job or present on the node the
executors run on. There is a sample script in the Spark code base.

If the cluster you are running on does not support running in isolated environment then you need
a different way to discover the GPUs. One solution for this is to put all the GPUs on the node
into process exclusive mode and use ai.rapids.spark.ExclusiveModeGpuDiscoveryPlugin as the discovery
plugin with Spark. This class will iterate through all the GPUs on the node and allocate one that
is not being used by another process.

To enable it, add the following config to your Spark configurations:

```
--conf spark.resourceDiscovery.plugin=ai.rapids.spark.ExclusiveModeGpuDiscoveryPlugin
```

## JIT Kernel Cache Path

CUDF can compile GPU kernels at runtime using a just-in-time (JIT) compiler. The resulting kernels
are cached on the filesystem. The default location for this cache is under the `.cudf` directory in
the user's home directory. When running in an environment where the user's home directory cannot be
written, such as running in a container environment on a cluster, the JIT cache path will need to be
specified explicitly with the `LIBCUDF_KERNEL_CACHE_PATH` environment variable.

The specified kernel cache path should be specific to the user to avoid conflicts with others
running on the same host. For example, the following would specify the path to a user-specific
location under `/tmp`:

```
--conf spark.executorEnv.LIBCUDF_KERNEL_CACHE_PATH="/tmp/cudf-$USER"
```

## Releases

| Version | Description |
|---------|-------------|

## Downloading

You can get the [rapids-4-spark](https://gpuwa.nvidia.com/artifactory/sw-spark-maven-local/ai/rapids/rapids-4-spark_2.12/) and [cudf](http://gpuwa.nvidia.com/artifactory/sw-spark-maven-local/ai/rapids/cudf/) jars in artifactory.

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

You can include the test jar (`rapids-plugin-4-spark/tests/target/rapids-4-spark_2.12-tests-0.9-SNAPSHOT.jar`) with the
Spark --jars option to get the Tpch tests. To setup for the queries you can run `TpchLikeSpark.setupAllCSV`
for CSV formatted data or `TpchLikeSpark.setupAllParquet` for parquet formatted data.  Both of those take
 the Spark session and a path to the dbgen generated data.  After that each query has its own object.
So you can call like:
```
import ai.rapids.sparkexamples.tpch._
val pathTodbgenoutput = SPECIFY PATH
TpchLikeSpark.setupAllCSV(spark, pathTodbgenoutput)
Q1Like(spark).count()
```
They generally follow TPCH but are not guaranteed to be the same.
`Q1Like(spark)` will return a dataframe that can be executed to run the corresponding query.

Unicode operation results may differ when compared against CPU-generated data from different versions of Java (different versions of Java support different Unicode standards : http://www.herongyang.com/Unicode/Java-Unicode-Version-Supported-in-Java-History.html 
https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8221431).
Character differences between Unicode versions can be found at http://www.unicode.org/versions/

### Integration Tests

There is an integration test framework based off of pytest and pyspark in the integration_tests directory.
The tests will run as a part of the build if you have the env variable `SPARK_HOME` set.  If you have
`SPARK_CONF_DIR` also set the tests will try to use whatever cluster you have configured.

To run the tests separate from the build go to the integration_tests directory and submit `run_tests.py`
through `spark-submit`.  Be sure to include the necessary jars for the RAPIDS plugin either with
`spark-submit` or with the cluster when it is launched. The command line arguments to `run_tests.py` are
the same as for [pytest](https://docs.pytest.org/en/latest/usage.html).

We also have a large number of integration tests that currently run as a part of the unit tests for the sql-plugin.
We plan to migrate these to the integration_tests package so they can be run against any cluster to verify that 
the plugin works as expected against different distributions based off of Apache Spark.  For now you can run
these tests manually by building the sql plugin test jar.

```
mvn package jar:test-jar
```

After this you can launch a cluster with the plugin jars on the classpath. The tests will
enable and disable the plugin as they run.

Now you need to copy over some test files to whatever distributed file system you are using.  The test files are
everything under `./sql-plugin/src/test/resources/`  Be sure to note where you placed them because you will need
to tell the tests where they are.

Once you have that ready you can launch a `spark-shell` and add in the test jar, scala-test and scalactic.  You can find scala-test and scalactic under `~/.m2/repository`.

```
spark-shell --jars ./rapids-4-spark_2.12-0.1-SNAPSHOT-tests.jar,scalatest_2.12-3.0.5.jar,scalactic_2.12-3.0.5.jar
```

Once in the shell you can run scala tests using the ScalaTest shell http://www.scalatest.org/user_guide/using_the_scalatest_shell

First you import the scalatest_shell and tell the tests where they can find the test files you just copied over.

```
import org.scalatest._
ai.rapids.spark.TestResourceFinder.setPrefix(PATH_TO_TEST_FILES)
```

Next you can start to run the tests.

```
durations.run(new ai.rapids.spark.ArithmeticOperatorsSuite)
durations.run(new ai.rapids.spark.BitwiseOperatorsSuite)
durations.run(new ai.rapids.spark.CastOpSuite)
durations.run(new ai.rapids.spark.CoalesceExecSuite)
durations.run(new ai.rapids.spark.CsvScanSuite)
durations.run(new ai.rapids.spark.FileSourceScanExecSuite)
durations.run(new ai.rapids.spark.FilterExprSuite)
durations.run(new ai.rapids.spark.GenerateExprSuite)
durations.run(new ai.rapids.spark.GpuCoalesceBatchesSuite)
durations.run(new ai.rapids.spark.HashAggregatesSuite)
durations.run(new ai.rapids.spark.JoinsSuite)
durations.run(new ai.rapids.spark.LimitExecSuite)
durations.run(new ai.rapids.spark.LogicalOpsSuite)
durations.run(new ai.rapids.spark.LogOperatorsSuite)
durations.run(new ai.rapids.spark.LogOperatorUnitTestSuite)
durations.run(new ai.rapids.spark.OrcScanSuite)
durations.run(new ai.rapids.spark.OrcWriterSuite)
durations.run(new ai.rapids.spark.ParquetScanSuite)
durations.run(new ai.rapids.spark.ParquetWriterSuite)
durations.run(new ai.rapids.spark.ProjectExprSuite)
durations.run(new ai.rapids.spark.SortExecSuite)
durations.run(new ai.rapids.spark.StringOperatorsSuite)
durations.run(new ai.rapids.spark.TimeOperatorsSuite)
durations.run(new ai.rapids.spark.UnaryOperatorsSuite)
durations.run(new ai.rapids.spark.UnionExprSuite)
```

Please note that not all of these tests run perfectly on all clusters so if you
have failures you will need to dig in and see if they are caused by issues in
the tests or issues with the plugin.

Some tests are known to be flaky if the cluster is larger than one single executor task including 
`ParquetWriterSuite` and `OrcWriterSuite`  These may show up as issues with the correct data but in
the wrong order.

There are some tests that assume you are on a local file system and will fail on a distributed one.
These also includes the `*WriterSuite` tests.

There is one test that is known to fail if your cluster is not configured for UTC time.
`OrcScanSuite "Test ORC msec timestamps and dates"`

There may be others that have issues too so you will need to dig into each failure to see what is happening.

Also because the tests launch a new application each time this results in a lot of startup costs and the
tests can take a very long time to run.

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
deserialize GPU data using a conventional Spark shuffle. In addition most of the memory associated
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
