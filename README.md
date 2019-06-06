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

