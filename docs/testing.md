# RAPIDS Accelerator for Apache Spark Testing

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
