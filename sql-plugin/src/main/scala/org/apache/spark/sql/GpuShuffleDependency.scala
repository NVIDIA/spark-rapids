package org.apache.spark.sql

import org.apache.spark.{Aggregator, Partitioner, ShuffleDependency, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleWriteProcessor

import scala.reflect.ClassTag

class GpuShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    rdd: RDD[_ <: Product2[K, V]],
    partitioner: Partitioner,
    serializer: Serializer = SparkEnv.get.serializer,
    keyOrdering: Option[Ordering[K]] = None,
    aggregator: Option[Aggregator[K, V, C]] = None,
    mapSideCombine: Boolean = false,
    shuffleWriterProcessor: ShuffleWriteProcessor = new ShuffleWriteProcessor)
  extends ShuffleDependency[K, V, C](rdd, partitioner, serializer, keyOrdering,
    aggregator, mapSideCombine, shuffleWriterProcessor) {

  override def toString: String = "GPU Shuffle Dependency"
}

