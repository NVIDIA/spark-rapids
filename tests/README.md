---
layout: page
title: Testing
nav_order: 1
parent: Developer Overview
---
# RAPIDS Accelerator for Apache Spark Testing

We have several stand alone examples that you can run in the [integration tests](../integration_tests).

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

You can include the test jar `rapids-4-spark-integration-tests_2.12-0.4.0-SNAPSHOT.jar` with the
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

## Unit Tests

Unit tests exist in the [tests]() directory. This is unconventional and is done so we can run the 
tests on the final shaded version of the plugin. It also helps with how we collect code coverage. 

Use Maven to run the unit tests via `mvn test`.

To run targeted Scala tests append `-DwildcardSuites=<comma separated list of wildcard suite
 names to execute>` to the above command. 
 
For more information about using scalatest with Maven please refer to the
[scalatest documentation](https://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin).
    
#### Running Unit Tests Against Specific Apache Spark Versions
You can run the unit tests against different versions of Spark using the different profiles. The
default version runs against Spark 3.0.0, to run against other versions use one of the following
 profiles:
   - `-Pspark301tests` (spark 3.0.1)
   - `-Pspark302tests` (spark 3.0.2)
   - `-Pspark311tests` (spark 3.1.1)

Please refer to the [tests project POM](pom.xml) to see the list of test profiles supported.
Apache Spark specific configurations can be passed in by setting the `SPARK_CONF` environment
variable.

Examples: 
- To run tests against Apache Spark 3.1.0, 
 `mvn -P spark311tests test` 
- To pass Apache Spark configs `--conf spark.dynamicAllocation.enabled=false --conf spark.task.cpus=1` do something like.
 `SPARK_CONF="spark.dynamicAllocation.enabled=false,spark.task.cpus=1" mvn ...`
- To run test ParquetWriterSuite in package com.nvidia.spark.rapids, issue `mvn test -DwildcardSuites="com.nvidia.spark.rapids.ParquetWriterSuite"`

## Integration Tests

Please refer to the integration-tests [README](../integration_tests/README.md)
