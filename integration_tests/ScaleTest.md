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

The queries are define in the source code. You can check the table below to see query content and the description:

| name | description | content |
|------|-------------|---------|
| q1   | Inner join with lots of ride along columns | SELECT a_facts.*, b_data_{1-10} FROM b_data JOIN a_facts WHERE primary_a = b_foreign_a |
| q2   | Full outer join with lots of ride along columns | SELECT a_facts.*, b_data_{1-10} FROM b_data FULL OUTER JOIN a_facts WHERE primary_a = b_foreign_a |
| q3   | Left outer join with lots of ride along columns | SELECT a_facts.*, b_data_{1-10} FROM b_data LEFT OUTER JOIN a_facts WHERE primary_a = b_foreign_a |

## Submit

The Scale Test can be submitted to Spark just as a normal Spark application.
The input arguments for the application are shown below:

```bash
Usage: ScaleTest [options] <scale factor> <complexity> <format> <input directory> <output directory> <path to save report file>

  <scale factor>           scale factor for data size
  <complexity>             complexity level for processing
  <format>                 output format for the data
  <input directory>        input directory for table data
  <output directory>       directory for query output
  <path to save report file>
                           path to save the report file that contains test results
  -d, --seed <value>       seed used to generate random data columns. default is 41 if not specified
  --overwrite              Flag argument. Whether to overwrite the existing data in the path.
  --iterations <value>     iterations to run for each query. default: 1
  --queries <value>        Specify queries to run specifically. the format must be query names with comma separated. e.g. --tables q1,q2,q3. If not specified, all queries will be run for `--iterations` rounds
  --timeout <value>        timeout for each query in milliseconds, default is 10 minutes(600000)
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
<path-to-data-generated-by-data-gen-tool> \
./output \
./report.json \
--overwrite \
--queries q1,q2
```

## Test Report

Test results are recorded in the report file in JSON format. Currently only execution time is recorded. The record looks like below:

```json
[ {
  "name" : "q1",
  "executionTime" : [ 5175 ]
}, {
  "name" : "q2",
  "executionTime" : [ 2830 ]
} ]

```