/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.jni.kudo

import ai.rapids.cudf.Schema
import com.nvidia.spark.rapids.{CoalescedHostResult, GpuColumnVector, RmmRapidsRetryIterator, SpillableHostBuffer, SpillPriorities}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class KudoHostMergeResultWrapper(
    schema: Schema, columnInfoList: Array[ColumnViewInfo], spillableHostBuffer: SpillableHostBuffer)
  extends CoalescedHostResult {

  /** Convert itself to a GPU batch */
  override def toGpuBatch(dataTypes: Array[DataType]): ColumnarBatch = {
    RmmRapidsRetryIterator.withRetryNoSplit {
      val buf = spillableHostBuffer.getHostBuffer()
      try {
        withResource(
          KudoHostMergeResult.toTableStatic(buf, schema, columnInfoList)
        ) { cudfTable =>
          GpuColumnVector.from(cudfTable, dataTypes)
        }
      } finally {
        buf.close()
      }
    }
  }

  /** Get the data size */
  override def getDataSize: Long = spillableHostBuffer.length

  override def close(): Unit = spillableHostBuffer.close()
}

object KudoHostMergeResultWrapper {
  def apply(inner: KudoHostMergeResult): KudoHostMergeResultWrapper = {
    KudoHostMergeResultWrapper(inner.schema, inner.columnInfoList,
      SpillableHostBuffer(inner.hostBuf,
        inner.hostBuf.getLength, SpillPriorities.ACTIVE_BATCHING_PRIORITY
      )
    )
  }
}