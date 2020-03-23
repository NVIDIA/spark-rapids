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

package ai.rapids.spark

import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * Utility class with methods for calculating various metrics about GPU memory usage
 * prior to allocation.
 */
object GpuBatchUtils {

  /** Estimate the number of rows required to meet a batch size limit */
  def estimateRowCount(desiredBatchSizeBytes: Long, currentBatchSize: Long, currentBatchRowCount: Int): Int = {
    assert(currentBatchSize > 0, "batch must contain at least one byte")
    assert(currentBatchRowCount > 0, "batch must contain at least one row")
    if (currentBatchSize > desiredBatchSizeBytes) {
      return currentBatchRowCount
    }
    val targetRowCount: Long = ((desiredBatchSizeBytes / currentBatchSize.floatValue()) * currentBatchRowCount).toLong
    targetRowCount.min(Integer.MAX_VALUE).toInt
  }

  /** Estimate the amount of GPU memory a batch of rows will occupy once converted */
  def estimateGpuMemory(schema: StructType, rowCount: Long): Long = {
    val dataTypes = schema.fields.map(field => field.dataType)
    val validityBufferSize = calculateValidityBufferSize(rowCount) * schema.fields.count(_.nullable)
    val dataSizes : Array[Long] = dataTypes.map {
      case dt@DataTypes.BinaryType =>
        val offsetBufferSize = calculateOffsetBufferSize(rowCount)
        val dataSize = dt.defaultSize * rowCount
        dataSize + offsetBufferSize
      case dt@DataTypes.StringType =>
        val offsetBufferSize = calculateOffsetBufferSize(rowCount)
        val dataSize = dt.defaultSize * rowCount
        dataSize + offsetBufferSize
      case dt =>
        dt.defaultSize * rowCount
    }
    dataSizes.sum + validityBufferSize
  }

  def calculateValidityBufferSize(rows: Long): Long = {
    roundToBoundary((rows + 7)/8, 64)
  }

  def calculateOffsetBufferSize(rows: Long): Long = {
    (rows+1) * 4 // 32 bit offsets
  }

  private def roundToBoundary(bytes: Long, boundary: Int): Long = {
    val remainder = bytes % boundary
    if (remainder > 0) {
      bytes + boundary - remainder
    } else {
      bytes
    }
  }
}
