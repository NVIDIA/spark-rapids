/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32

import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair

class GpuRangePartitioner extends Serializable {
  var rangeBounds: Array[InternalRow] = _
  /**
   * Sketches the input RDD via reservoir sampling on each partition.
   *
   * @param rdd                    the input RDD to sketch
   * @param sampleSizePerPartition max sample size per partition
   * @return (total number of items, an array of (partitionId, number of items, sample))
   */
  def sketch[K: ClassTag](
                           rdd: RDD[K],
                           sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    val shift = rdd.id
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      iter.map(unsafeRow => unsafeRow.asInstanceOf[InternalRow])
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }

  /**
   * Determines the bounds for range partitioning from candidates with weights indicating how many
   * items each represents. Usually this is 1 over the probability used to sample this candidate.
   *
   * @param candidates unordered candidates with weights
   * @param partitions number of partitions
   * @return selected bounds
   */
  def determineBounds[K: Ordering : ClassTag](candidates: ArrayBuffer[(K, Float)],
                                              partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }

  def createRangeBounds(partitions: Int, gpuOrdering: Seq[SortOrder], rdd: RDD[ColumnarBatch],
                        outputAttributes: Seq[Attribute],
                        samplePointsPerPartitionHint: Int): Unit = {
    val sparkShims = ShimLoader.getSparkShims
    val orderingAttributes = gpuOrdering.zipWithIndex.map { case (ord, i) =>
      sparkShims.copySortOrderWithNewChild(ord, BoundReference(i, ord.dataType, ord.nullable))
    }
    implicit val ordering: LazilyGeneratedOrdering = new LazilyGeneratedOrdering(orderingAttributes)
    val rowsRDD = rddForSampling(partitions, gpuOrdering, rdd, outputAttributes)
    // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
    require(partitions >= 0,
      s"Number of partitions cannot be negative but found $partitions.")
    require(samplePointsPerPartitionHint > 0,
      s"Sample points per partition must be greater than 0 but found $samplePointsPerPartitionHint")

    // An array of upper bounds for the first (partitions - 1) partitions
    val rangeBounds : Array[InternalRow] = {
      if (partitions < 1) {
        Array.empty[InternalRow]
      } else {
        // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
        // Cast to double to avoid overflowing ints or longs
        val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * partitions, 1e6)
        // Assume the input partitions are roughly balanced and over-sample a little bit.
        val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rowsRDD.partitions.length).toInt
        val (numItems, sketched) = sketch(rowsRDD.map(_._1), sampleSizePerPartition)
        if (numItems == 0L) {
          Array.empty
        } else {
          // If a partition contains much more than the average number of items,
          // we re-sample from it
          // to ensure that enough items are collected from that partition.
          val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
          val candidates = ArrayBuffer.empty[(InternalRow, Float)]
          var imbalancedPartitions = Set.empty[Int]
          sketched.foreach { case (idx, n, sample) =>
            if (fraction * n > sampleSizePerPartition) {
              imbalancedPartitions += idx
            } else {
              // The weight is 1 over the sampling probability.
              val weight = (n.toDouble / sample.length).toFloat
              for (key <- sample) {
                candidates += ((key, weight))
              }
            }
          }
          if (imbalancedPartitions.nonEmpty) {
            // Re-sample imbalanced partitions with the desired sampling probability.
            val imbalanced = new PartitionPruningRDD(rowsRDD.map(_._1),
              imbalancedPartitions.contains)
            val seed = byteswap32(-rowsRDD.id - 1)
            val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
            val weight = (1.0 / fraction).toFloat
            candidates ++= reSampled.map(x => (x, weight))
          }
          determineBounds(candidates, math.min(partitions, candidates.size))
        }
      }
    }
    this.rangeBounds = rangeBounds.asInstanceOf[Array[InternalRow]]
  }

  def rddForSampling(partitions: Int, gpuOrdering: Seq[SortOrder], rdd: RDD[ColumnarBatch],
                     outputAttributes: Seq[Attribute]) : RDD[MutablePair[InternalRow, Null]] = {

    val sortingExpressions = gpuOrdering
    lazy val toUnsafe = UnsafeProjection.create(
      sortingExpressions.map(_.child),
      outputAttributes)
    val rowsRDD = rdd.mapPartitions {
      batches => {
        new Iterator[InternalRow] {
          @transient var cb: ColumnarBatch = _
          var it: java.util.Iterator[InternalRow] = _

          private def closeCurrentBatch(): Unit = {
            if (cb != null) {
              cb.close()
              cb = null
            }
          }

          def loadNextBatch(): Unit = {
            closeCurrentBatch()
            if (batches.hasNext) {
              val devCb = batches.next()
              cb = try {
                new ColumnarBatch(GpuColumnVector.extractColumns(devCb).map(_.copyToHost()),
                    devCb.numRows())
              } finally {
                devCb.close()
              }
              it = cb.rowIterator()
            }
          }

          override def hasNext: Boolean = {
            val itHasNext = it != null && it.hasNext
            if (!itHasNext) {
              loadNextBatch()
              it != null && it.hasNext
            } else {
              itHasNext
            }
          }

          override def next(): InternalRow = {
            if (it == null || !it.hasNext) {
              loadNextBatch()
            }
            if (it == null) {
              throw new NoSuchElementException()
            }
            val a = it.next()
            a
          }
        }.map(toUnsafe)
      }
    }
    rowsRDD.mapPartitions(it => it.map(ir => MutablePair(ir, null)))
  }

  def getRangesBatch(schema: StructType, rangeBounds: Array[InternalRow]) : ColumnarBatch = {
    val rangeBoundsRowsIter = rangeBounds.toIterator
    if (!rangeBoundsRowsIter.hasNext) {
      throw new NoSuchElementException("Ranges columnar Batch is empty")
    }
    val builders = new GpuColumnarBatchBuilder(schema, rangeBounds.length, null)
    val converters = new GpuRowToColumnConverter(schema)
    try {
      var rowCount = 0
      while (rangeBoundsRowsIter.hasNext) {
        val row = rangeBoundsRowsIter.next()
        converters.convert(row.asInstanceOf[InternalRow], builders)
        rowCount += 1
      }
      // The returned batch will be closed by the consumer of it
      builders.build(rowCount)
    } finally {
      builders.close()
    }
  }
}
