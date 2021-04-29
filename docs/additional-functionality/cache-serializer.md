---
layout: page
title: RAPIDS Cache Serializer
parent: Additional Functionality
nav_order: 2
---
# RAPIDS Cache Serializer  
  Apache Spark provides an important feature to cache intermediate data and provide
  significant performance improvement while running multiple queries on the same data. There
  are two ways to cache a Dataframe or a DataSet i.e. call `persist(storageLevel)` or
  `cache()`. Calling `cache()` is the same as calling `persist(MEMORY_AND_DISK)`. There are
  many articles online that can be read about caching and its benefits as well as when to
  cache but as a rule of thumb we should identify the Dataframe that is being reused in a
  Spark Application and cache it. Even if the system memory isn't big enough, Spark will
  utilize disk space to spill over. To read more about what storage levels are available look
  at `StorageLevel.scala` in Spark.

  Starting in Spark 3.1.1 users can add their own cache serializer, if they desire, by
  setting the `spark.sql.cache.serializer` configuration. This is a static configuration
  that is set once for the duration of a Spark application which means that you can only set the conf
  before starting a Spark application and cannot be changed for that application's Spark
  session.

  RAPIDS Accelerator for Apache Spark version 0.4+ has the `ParquetCachedBatchSerializer`
  that is optimized to run on the GPU and uses Parquet to compress data before caching it.
  ParquetCachedBatchSerializer can be used independent of what the value of
  `spark.rapids.sql.enabled` is. If it is set to true then the Parquet compression will run
  on the GPU if possible, and importantly
  `spark.sql.inMemoryColumnarStorage.enableVectorizedReader` will not be honored as the GPU
  data is always read in as columnar. If `spark.rapids.sql.enabled` is set to false
  the cached objects will still be compressed on the CPU as a part of the caching process.
  Also note that Parquet doesn't support CalendarIntervalType or NullType out of the box, but
  ParquetCachedBatchSerializer does by decomposing intervals to struct containing the
  months, days and microseconds and NullType to Int column containing nulls.

  Please make sure to use the right package corresponding to the spark version you are using. To use
  this serializer with Spark 3.1.1 please run Spark with the following conf.
  ```
  spark-shell --conf spark.sql.cache.serializer=com.nvidia.spark.rapids.shims.spark311.ParquetCachedBatchSerializer"
  ```
  Please see the below table for all the names of the serializers corresponding to the Spark
  versions

  | Spark version | Serializer name |
  | ------ | -----|
  | 3.1.1 | com.nvidia.spark.rapids.shims.spark311.ParquetCachedBatchSerializer |
  | 3.1.2 | com.nvidia.spark.rapids.shims.spark312.ParquetCachedBatchSerializer |
  | 3.2.0 | com.nvidia.spark.rapids.shims.spark320.ParquetCachedBatchSerializer |

  To use the default serializer don't set the `spark.sql.cache.serializer` conf
