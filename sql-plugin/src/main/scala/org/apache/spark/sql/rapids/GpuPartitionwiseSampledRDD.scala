package org.apache.spark.sql.rapids

import org.apache.spark.rdd.{PartitionwiseSampledRDD, RDD}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils
import org.apache.spark.util.random.RandomSampler

// PartitionwiseSampledRDD is private in [spark] package, so this is a forward to access it
class GpuPartitionwiseSampledRDD(prev: RDD[ColumnarBatch],
                                 sampler: RandomSampler[ColumnarBatch, ColumnarBatch],
                                 preservesPartitioning: Boolean,
                                 @transient private val seed: Long = Utils.random.nextLong)
  extends PartitionwiseSampledRDD[ColumnarBatch, ColumnarBatch](prev, sampler,
    preservesPartitioning, seed) {
}
