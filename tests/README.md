---
layout: page
title: Testing
nav_order: 1
parent: Developer Overview
---
# RAPIDS Accelerator for Apache Spark Testing

We have a stand-alone example that you can run in the [integration tests](../integration_tests).
The example is based off of the mortgage dataset you can download
[here](http://www.fanniemae.com/portal/funding-the-market/data/loan-performance-data.html)
and the code is in the `com.nvidia.spark.rapids.tests.mortgage` package.

## Unit Tests

Unit tests exist in the [tests]() directory. This is unconventional and is done so we can run the 
tests on the final shaded version of the plugin. It also helps with how we collect code coverage. 

The `tests` module depends on the `aggregator` module which shades dependencies. When running the
tests via `mvn test`, make sure to do an install via `mvn install` for the aggregator jar to the
local maven repository.
The steps to run the unit tests:
```bash
cd <root-path-of-spark-rapids>
mvn clean install
cd tests
mvn test
```

To run targeted Scala tests append `-DwildcardSuites=<comma separated list of wildcard suite
 names to execute>` to the above command. 
 
For more information about using scalatest with Maven please refer to the
[scalatest documentation](https://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin).
    
#### Running Unit Tests Against Specific Apache Spark Versions
You can run the unit tests against different versions of Spark using the different profiles. The
default version runs against Spark 3.1.1, to run against a specific version use one of the following
profiles:
   - `-Pspark311tests` (Spark 3.1.1)
   - `-Pspark312tests` (Spark 3.1.2)
   - `-Pspark313tests` (Spark 3.1.3)

Please refer to the [tests project POM](pom.xml) to see the list of test profiles supported.
Apache Spark specific configurations can be passed in by setting the `SPARK_CONF` environment
variable.

Examples: 
- To run tests against Apache Spark 3.1.1, 
 `mvn -P spark311tests test` 
- To pass Apache Spark configs `--conf spark.dynamicAllocation.enabled=false --conf spark.task.cpus=1` do something like.
 `SPARK_CONF="spark.dynamicAllocation.enabled=false,spark.task.cpus=1" mvn ...`
- To run test ParquetWriterSuite in package com.nvidia.spark.rapids, issue `mvn test -DwildcardSuites="com.nvidia.spark.rapids.ParquetWriterSuite"`

## Integration Tests

Please refer to the integration-tests [README](../integration_tests/README.md)
