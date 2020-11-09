---
layout: page
title: Testing
nav_order: 1
parent: Developer Overview
---
# RAPIDS Accelerator for Apache Spark Testing

We have several stand alone examples that you can run in the integration_tests subdirectory/jar.

One set is based off of the mortgage dataset you can download 
[here](http://www.fanniemae.com/portal/funding-the-market/data/loan-performance-data.html)
and are in the `com.nvidia.spark.rapids.tests.mortgage` package.

The other is based off of TPCH. You can use the TPCH `dbgen` tool to generate data for them.  They
are in the `com.nvidia.spark.rapids.tests.tpch` package. `dbgen` has various options to
generate the data. Please refer to the documentation that comes with dbgen on how to use it, but
we typically run with the default options and only increase the scale factor depending on the test.
```shell 
dbgen -b dists.dss -s 10
```

You can include the test jar `rapids-4-spark-integration-tests_2.12-0.3.0-SNAPSHOT.jar` with the
Spark --jars option to get the TPCH tests. To setup for the queries you can run 
`TpchLikeSpark.setupAllCSV` for CSV formatted data or `TpchLikeSpark.setupAllParquet`
for parquet formatted data.  Both of those take the Spark session, and a path to the dbgen
generated data.  After that each query has its own object.

So you can make a call like:
```scala
import com.nvidia.spark.rapids.tests.tpch._
val pathTodbgenoutput = SPECIFY PATH
TpchLikeSpark.setupAllCSV(spark, pathTodbgenoutput)
Q1Like(spark).count()
```

They generally follow TPCH but are not guaranteed to be the same.
`Q1Like(spark)` will return a DataFrame that can be executed to run the corresponding query.

## Unit tests

Unit tests exist in the tests directory. This is unconventional and is done so we can run the tests
on the final shaded version of the plugin. It also helps with how we collect code coverage.
You can run the unit tests against different versions of Spark using the different profiles. The
default version runs again Spark 3.0.0, to run against other version use one of the following profiles:
   - `-Pspark301tests` (Spark 3.0.1)
   - `-Pspark302tests` (Spark 3.0.2)
   - `-Pspark310tests` (Spark 3.1.0)

## Integration tests

Integration tests are stored in the [integration_tests](../integration_tests/README.md) directory.
There are two frameworks used for testing. One is based off of pytest and pyspark in the 
`src/main/python` directory. These tests will run as a part of the maven build if you have the environment
variable `SPARK_HOME` set.

By default the tests try to use the python packages `pytest-xdist` and `findspark` to oversubscribe
your GPU and run the tests in Spark local mode. This can speed up these tests significantly as all
of the tests that run by default process relatively small amounts of data. Be careful because if
you have `SPARK_CONF_DIR` also set the tests will try to use whatever cluster you have configured.
If you do want to run the tests in parallel on an existing cluster it is recommended that you set
`-Dpytest.TEST_PARALLEL` to one less than the number of worker applications that will be
running on the cluster.  This is because `pytest-xdist` will launch one control application that
is not included in that number. All it does is farm out work to the other applications, but because
it needs to know about the Spark cluster to determine which tests to run and how it still shows up
as a Spark application.

To run the tests separate from the build go to the `integration_tests` directory. You can submit
`runtests.py` through `spark-submit`, but if you want to run the tests in parallel with
`pytest-xdist` you will need to submit it as a regular python application and have `findspark`
installed.  Be sure to include the necessary jars for the RAPIDS plugin either with
`spark-submit` or with the cluster when it is 
[setup](get-started/getting-started-on-prem.md).
The command line arguments to `runtests.py` are the same as for 
[pytest](https://docs.pytest.org/en/latest/usage.html). The only reason we have a separate script
is that `spark-submit` uses python if the file name ends with `.py`.

If you want to configure the Spark cluster you may also set environment variables for the tests.
The name of the env var should be in the form `"PYSP_TEST_" + conf_key.replace('.', '_')`. Linux
does not allow '.' in the name of an environment variable so we replace it with an underscore. As
Spark configs avoid this character we have no other special processing.

We also have a large number of integration tests that currently run as a part of the unit tests
using scala test. Those are in the `src/test/scala` sub-directory and depend on the testing
framework from the `rapids-4-spark-tests_2.12` test jar.

You can run these tests against a cluster similar to how you can run `pytests` against an
existing cluster. To do this you need to launch a cluster with the plugin jars on the
classpath. The tests will enable and disable the plugin as they run.

Next you need to copy over some test files to whatever distributed file system you are using.
The test files are everything under `./integration_tests/src/test/resources/`  Be sure to note
where you placed them because you will need to tell the tests where they are.

When running these tests you will need to include the test jar, the integration test jar,
scala-test and scalactic. You can find scala-test and scalactic under `~/.m2/repository`.

It is recommended that you use `spark-shell` and the scalatest shell to run each test
individually, so you don't risk running unit tests along with the integration tests.
http://www.scalatest.org/user_guide/using_the_scalatest_shell

```shell 
spark-shell --jars rapids-4-spark-tests_2.12-0.3.0-SNAPSHOT-tests.jar,rapids-4-spark-integration-tests_2.12-0.3.0-SNAPSHOT-tests.jar,scalatest_2.12-3.0.5.jar,scalactic_2.12-3.0.5.jar
```

First you import the `scalatest_shell` and tell the tests where they can find the test files you
just copied over.

```scala
import org.scalatest._
com.nvidia.spark.rapids.TestResourceFinder.setPrefix(PATH_TO_TEST_FILES)
```

Next you can start to run the tests.

```scala
durations.run(new com.nvidia.spark.rapids.JoinsSuite)
...
```
