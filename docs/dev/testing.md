### Testing your code
There are two types of tests in this project. Unit tests that are written in Scala, and integration tests that are written in Python. 
We encourage writing integration tests if you have to choose between the two as that helps us ensure the plugin is working across different platforms. 

#### Unit tests
Unit-tests are located [here](tests). In order to run the unit-tests follow these steps
1. Issue the Maven command to run the tests with `mvn test`. This will run all the tests in the tests module. 
2. To run individual tests append `-DwildcardSuites=<comma separated list of wildcard suite names to execute>` to the above command 

For more information about using scalatest with maven please refere [here](https://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin)
    
##### Running unit-tests against specific Apache Spark versions. 
There are Maven profiles that should be used to run Apache Spark version-specific tests e.g. To run tests against Apache Spark 3.1.0, 
`mvn -P spark310tests test -DwildcardSuites="com.nvidia.spark.rapids.ParquetWriterSuite"`. 
Please refer to the [pom.xml](tests/pom.xml) to see the list of profiles supported

Apache Spark specific configurations can be passed in by setting environment-variable SPARK_CONF 

Examples: 
- To pass Apache Spark configs `--conf spark.dynamicAllocation.enabled=false --conf spark.task.cpus=1` do something like.
`SPARK_CONF="spark.dynamicAllocation.enabled=false,spark.task.cpus=1" mvn ...`
- To run test ParquetWriterSuite in package com.nvidia.spark.rapids, issue `mvn test -DwildcardSuites="com.nvidia.spark.rapids.ParquetWriterSuite"`


#### Integration tests
Integration tests are located [here](integration_tests). The suggested way to run these tests is to use the shell-script file located in the module folder called
[run_pyspark_from_build.sh](integration_tests/run_pyspark_from_build.sh). This script takes care of some of the flags that are required to run the tests which
will have to be set for the plugin to work. It will be very useful to read the contents of the [run_pyspark_from_build.sh](integration_tests/run_pyspark_from_build.sh) to get
better insight into what is needed as we constantly keep working on to improve and expand the plugin-support.

The tests are written python and run with pytest and the script honors pytest parameters. Some handy flags are:
- `-k` <pytest-file-name>. This will run all the tests in that test file.
- `-k` <test-name>. This will also run an individual test.
- `-s` Doesn't capture the output and instead prints to the screen.
- `-v` increase verbosity of the tests
- `-rfExXs` show extra test summary info as specified by chars: (f)ailed, (E)rror, (x)failed, (X)passed, (s)kipped
- For other options and more details please visit [pytest-usage](https://docs.pytest.org/en/stable/usage.html) or type `pytest --help`

Example: 
- This command runs all the tests located in `cache_test.py` against Apache Spark 3.1.0 using the ParquetCachedBatchSerializer and other configs discussed above
and with the debugger listening on port 5005
`SPARK_SUBMIT_FLAGS="--driver-memory 4g --conf spark.sql.cache.serializer=com.nvidia.spark.rapids.shims.spark310.ParquetCachedBatchSerializer"
SPARK_HOME=~/spark-3.1.0-SNAPSHOT-bin-hadoop3.2/
COVERAGE_SUBMIT_FLAGS='-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005' ./run_pyspark_from_build.sh -k cache_test`
