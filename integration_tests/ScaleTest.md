# Scale Test

## Build

The Scale Test suite is bundled into `integration_tests` module, it can be built by maven directly.

```bash
mvn package
```

You can choose profiles to build against specific Spark and JDK version. e.g.

```bash
mvn package -Dbuildver=332 -Pjdk17
```

## Query Sets
The quries sets are defined in a text file with format:
```bash
<queryName>:<queryContent>
...
```

All queries can be found at [scale_test.txt](./src/test/resources/scale_test.txt)

## Submit

The Scale Test can be submit to Spark just as a normal Spark application.
The input arguments for the application is shown as below:

```bash
Usage: DataGenEntry [options] <scale factor> <complexity> <format> <input directory> <output directory> <path to query file> <path to save report file>

  <scale factor>               scale factor for data size
  <complexity>                 complexity level for processing
  <format>                     output format for the data
  <input directory>            input directory for table data
  <output directory>           directory for query output
  <path to query file>         text file contains template queries
  <path to save report file>   path to save the report file that contains test results
  -d, --seed <value>           seed used to generate random data columns. default is 41 if not specified
```

An example command to launch the Scale Test:

```bash
$SPARK_HOME/bin/spark-submit \
--master spark://<SPARK_MASTER>:7077 \
--conf spark.driver.memory=10G \
--conf spark.executor.memory=32G \
--conf spark.sql.parquet.int96RebaseModeInWrite=CORRECTED \
--conf spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED \
--jars $SPARK_HOME/examples/jars/scopt_2.12-3.7.1.jar \
--class com.nvidia.spark.rapids.tests.scaletest.ScaleTest \
./target/rapids-4-spark-integration-tests_2.12-23.10.0-SNAPSHOT-spark332.jar \
10 \
100 \
parquet \
/home/allxu/code/spark-rapids/datagen/testdata \
./output \
./query_file.txt \
./report.csv \
--overwrite

```

## Test Report

Test results are recorded in the report file in CSV format. Currently only execution time is recorded.