/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.{GpuColumnVector, RmmRapidsRetryIterator}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.SparkShimImpl

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuBroadcastHelper {
  /**
   * Given a broadcast relation get a ColumnarBatch that can be used on the GPU.
   *
   * The broadcast relation may or may not contain any data, so we special case
   * the empty relation case (hash or identity depending on the type of join).
   *
   * If a broadcast result is unexpected we throw, but at this moment other
   * cases are not known, so this is a defensive measure.
   *
   * @param broadcastRelation - the broadcast as produced by a broadcast exchange
   * @param broadcastSchema - the broadcast schema
   * @return a `ColumnarBatch` or throw if the broadcast can't be handled
   */
  def getBroadcastBatch(broadcastRelation: Broadcast[Any],
                        broadcastSchema: StructType): ColumnarBatch = {
    broadcastRelation.value match {
      case broadcastBatch: SerializeConcatHostBuffersDeserializeBatch =>
        RmmRapidsRetryIterator.withRetryNoSplit {
          withResource(new NvtxRange("getBroadcastBatch", NvtxColor.YELLOW)) { _ =>
            broadcastBatch.batch.getColumnarBatch()
          }
        }
      case v if SparkShimImpl.isEmptyRelation(v) =>
        GpuColumnVector.emptyBatch(broadcastSchema)
      case t =>
        throw new IllegalStateException(s"Invalid broadcast batch received $t")
    }
  }

  /**
   * Given a broadcast relation get the number of rows that the received batch
   * contains
   *
   * The broadcast relation may or may not contain any data, so we special case
   * the empty relation case (hash or identity depending on the type of join).
   *
   * If a broadcast result is unexpected we throw, but at this moment other
   * cases are not known, so this is a defensive measure.
   *
   * @param broadcastRelation - the broadcast as produced by a broadcast exchange
   * @return number of rows for a batch received, or 0 if it's an empty relation
   */
  def getBroadcastBatchNumRows(broadcastRelation: Broadcast[Any]): Int = {
    broadcastRelation.value match {
      case broadcastBatch: SerializeConcatHostBuffersDeserializeBatch =>
        broadcastBatch.numRows
      case v if SparkShimImpl.isEmptyRelation(v) => 0
      case t =>
        throw new IllegalStateException(s"Invalid broadcast batch received $t")
    }
  }

  /**
   * Given a broadcast relation return an RDD of that relation.
   * @note This can only be called from driver code.
   */
  def asRDD(sc: SparkContext, broadcast: Broadcast[Any]): RDD[ColumnarBatch] = {
    broadcast.value match {
      case broadcastBatch: SerializeConcatHostBuffersDeserializeBatch =>
        val hostBatchRDD = sc.makeRDD(Seq(broadcastBatch), 1)
        hostBatchRDD.map { serializedBatch =>
          serializedBatch.batch.getColumnarBatch()
        }
      case v if SparkShimImpl.isEmptyRelation(v) => sc.emptyRDD
      case t => throw new IllegalStateException(s"Invalid broadcast batch received $t")
    }
  }
}

