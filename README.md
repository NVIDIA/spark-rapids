# RAPIDS Plugin For Apache Spark

Plugin for [Apache Spark](https://spark.apache.org) that leverages GPUs to accelerate processing
via the [RAPIDS](https://rapids.ai) libraries.

As of Apache Spark release 3.0 users can schedule GPU resources and can replace the backend for 
many SQL and dataframe operations so that they are accelerated using GPUs. 

To enable this GPU acceleration you will need Apache Spark 3.0+ and be running on cluster that has
nodes that comply with the requirements for [CUDF](https://github.com/rapidsai/cudf).  You can then
run your query by shipping the `rapids-4-spark` jar with your job, and setting the config
`spark.sql.extensions` to `ai.rapids.spark.Plugin`.  If you need to control GPU acceleration for
individual queries you can set `ai.rapids.gpu.enabled` to `false` to disable it or `true` to 
enable it again.

```
> spark-shell --jars 'rapids-4-spark-0.8-SNAPSHOT.jar,cudf-0.8-SNAPSHOT-cuda10.jar' --conf spark.sql.extensions=ai.rapids.spark.Plugin
```

## Notes on Building

The build requires Apache Spark 3.0+ built against the nohive ORC classifier.  Building with a
version of Spark built without the nohive support (e.g.: the Apache Spark hadoop-3.2 profile)
will cause errors during build and test due to the conflicting ORC jars.

## Tests

We have several tests that you can run in the tests subdirectory/jar.

One set is based off of the mortgage dataset you can download at https://rapidsai.github.io/demos/datasets/mortgage-data or http://www.fanniemae.com/portal/funding-the-market/data/loan-performance-data.html
and are in the ai.rapids.sparkexamples.mortgage package.

The other is based off of TPCH. You can use the TPCH `dbgen` tool to generate data for them.  They are
in the ai.rapids.sparkexamples.tpch package.

To setup for the queries you can run `TpchLikeSpark.setupAllCSV` for CSV formatted data or
`TpchLikeSpark.setupAllParquet` for parquet formatted data.  After that each query has its own
object.  They generally follow TPCH but are not guaranteed to be the same.  `Q1Like(spark)` will return
a dataframe that can be executed to run the corresponding query.
