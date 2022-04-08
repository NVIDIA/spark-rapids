---
layout: page
title: ML Integration
parent: Additional Functionality
nav_order: 1
---
# RAPIDS Accelerator for Apache Spark ML Library Integration

There are cases where you may want to get access to the raw data on the GPU, preferably without
copying it. One use case for this is exporting the data to an ML framework after doing feature
extraction. To do this we provide a simple Scala utility `com.nvidia.spark.rapids.ColumnarRdd` that can
be used to convert a `DataFrame` to an `RDD[ai.rapids.cudf.Table]`. Each `Table` will have the same
schema as the `DataFrame` passed in.

`Table` is not a typical thing in an `RDD` so special care needs to be taken when working with it.
By default, it is not serializable so repartitioning the `RDD` or any other operator that involves
a shuffle will not work. This is because it is relatively expensive to serialize and
deserialize GPU data using a conventional Spark shuffle. In addition, most of the memory associated
with the `Table` is on the GPU itself. So, each `Table` must be closed when it is no longer needed
to avoid running out of GPU memory. By convention, it is the responsibility of the one consuming
the data to close it when they no longer need it.

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

## RMM
You may need to disable RMM caching when exporting data to an ML library as that library
will likely want to use all of the GPU's memory and if it is not aware of RMM it will not have
access to any of the memory that RMM is holding.

## Spark ML Algorithms Supported by RAPIDS Accelerator

The [spark-rapids-examples repository](https://github.com/NVIDIA/spark-rapids-examples) provides a
[working example](https://github.com/NVIDIA/spark-rapids-examples/tree/branch-22.04/examples/Spark-cuML/pca)
of accelerating the `transform` API for
[Principal Component Analysis (PCA)](https://spark.apache.org/docs/latest/mllib-dimensionality-reduction#principal-component-analysis-pca).
The example leverages the [RAPIDS accelerated UDF interface](rapids-udfs.md) to provide a native
implementation of the algorithm. The details of the UDF implementation can be found in the
[spark-rapids-ml repository](https://github.com/NVIDIA/spark-rapids-ml).
