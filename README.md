# RAPIDS Plugin For Apache Spark

Plugin for [Apache Spark](https://spark.apache.org) that leverages GPUs to accelerate processing
via the [RAPIDS](https://rapids.ai) libraries.

As of Apache Spark release 3.0 users can schedule GPU resources and can replace the backend for 
many SQL and dataframe operations so that they are accelerated using GPUs. 

To enable this GPU acceleration you will need:
  * Apache Spark 3.0+
  * Running on cluster that has nodes that comply with the requirements for [CUDF](https://github.com/rapidsai/cudf).
  * Ship the `rapids-4-spark` jar with your job
  * Set the config `spark.sql.extensions` to `ai.rapids.spark.Plugin`.

If you need to control GPU acceleration for individual queries you can set `ai.rapids.gpu.enabled` to `false` to disable it or `true` to enable it again.

```
> spark-shell --jars 'rapids-4-spark-0.8-SNAPSHOT.jar,cudf-0.8-SNAPSHOT-cuda10.jar' --conf spark.sql.extensions=ai.rapids.spark.Plugin
```
Pooled GPU memory allocation can be enabled to improve performance, but this should not be used
if you want to use operators that also use GPU memory like XGBoost or Tensorflow, as the pool
it allocates cannot be used by other tools.
To enable pool GPU memory allocation set config `spark.executor.plugins` to `ai.rapids.spark.GpuResourceManager`

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
