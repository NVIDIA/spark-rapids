---
layout: page
title: ML Integration
parent: Additional Functionality
nav_order: 1
---
# RAPIDS Accelerator for Apache Spark ML Library Integration

## Existing ML Libraries

The RAPIDS Accelerator for Apache Spark can be used to accelerate the ETL portions (e.g., loading
training data from parquet files) of applications using ML libraries with Spark DataFrame APIs.
Examples of such libraries include the original [Apache Spark
MLlib](https://spark.apache.org/mllib/), [XGBoost](https://xgboost.readthedocs.io/en/stable/),
[Spark RAPIDS ML](https://nvidia.github.io/spark-rapids-ml/), and the [DL inference UDF
function](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.functions.predict_batch_udf.html)
introduced in Spark 3.4.  The latter three also enable leveraging GPUs (in the case of the DL
inference UDF, indirectly via the underlying DL framework) to accelerate the core ML algorithms, and
thus, in conjunction with the RAPIDS Accelerator for Apache Spark for ETL, can further enhance the
cost-benefit of GPU accelerated Spark clusters.

For Spark API compatible ML libraries that implement their core ML computations inside pandas UDFs,
such as XGBoost’s pySpark API, Spark RAPIDS ML pySpark API, and the DL inference UDF it is
recommended to enable the RAPIDS Accelerator for Apache Spark’s [support for GPU accelerated pandas
UDFs](https://nvidia.github.io/spark-rapids/docs/additional-functionality/rapids-udfs.html#gpu-support-for-pandas-udf).

### RMM

One consideration when using the RAPIDS Accelerator for Apache Spark with a GPU accelerated ML
library is the sharing of GPU memory between the two, as the ML library would typically have a
distinct GPU memory manager from the RAPIDS Accelerator’s RMM instance.  Accordingly, you may need
to disable RMM pooling in the RAPIDS Accelerator via the config `spark.rapids.memory.gpu.pool` when
exporting data to an ML library since that library will likely not have access to any of the memory
that the RAPIDS Accelerator’s RMM instance is holding.  Similarly, aggressive GPU memory reservation
on the side of the ML library may also need to be disabled, as via these steps in the case of
[Tensorflow](https://www.tensorflow.org/guide/gpu#limiting_gpu_memory_growth).

## GPU accelerated ML Library development

### ColumnarRdd

When developing a GPU accelerated ML library for Spark, there are cases where you may want to get
access to the raw data on the GPU, preferably without copying it. One use case for this is exporting
the data to the ML library after doing feature extraction. To enable this for Scala development, the
RAPIDS Accelerator for Apache Spark provides a simple utility `com.nvidia.spark.rapids.ColumnarRdd`
that can be used to convert a `DataFrame` to an `RDD[ai.rapids.cudf.Table]`. Each `Table` will have
the same schema as the `DataFrame` passed in.

Note that `Table` is not a typical thing in an `RDD` so special care needs to be taken when working
with it.  By default, it is not serializable so repartitioning the `RDD` or any other operator that
involves a shuffle will not work. This is because it is relatively expensive to serialize and
deserialize GPU data using a conventional Spark shuffle. In addition, most of the memory associated
with the `Table` is on the GPU itself. So, each `Table` must be closed when it is no longer needed
to avoid running out of GPU memory. By convention, it is the responsibility of the one consuming the
data to close it when they no longer need it.

```scala
val df = spark.sql("""select my_column from my_table""")
val rdd: RDD[Table] = ColumnarRdd(df)
// Compute the max of the first column
val maxValue = rdd.map(table => {
  val max = table.getColumn(0).max().getLong
  // Close the table to avoid leaks
  table.close()
  max
}).max()
```

### Examples of Spark ML Implementations leveraging ColumnarRdd

Both the Scala Spark PCA
[implementation](https://github.com/NVIDIA/spark-rapids-ml/blob/ab575bc46e55f38ee52906b3c3b55b75f2418459/jvm/src/main/scala/org/apache/spark/ml/linalg/distributed/RapidsRowMatrix.scala)
in Spark RAPIDS ML and XGBoost’s [GPU accelerated Scala
SparkAPI](https://github.com/dmlc/xgboost/blob/f1e9bbcee52159d4bd5f7d25ef539777ceac147c/jvm-packages/xgboost4j-spark-gpu/src/main/scala/ml/dmlc/xgboost4j/scala/rapids/spark/GpuPreXGBoost.scala)
leverage ColumnarRdd (search for ColumnarRdd in these files) to accelerate data transfer between the
RAPIDS Accelerator for Apache Spark and the respective core ML algorithm computations.  XGBoost in
particular enables this when detecting that the RAPIDS Accelerator for Apache Spark is present and
enabled.
