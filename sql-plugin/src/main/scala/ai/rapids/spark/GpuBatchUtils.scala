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

import org.apache.spark.sql.types.{DataType, DataTypes, ArrayType, MapType, StructType}

/**
 * Utility class with methods for calculating various metrics about GPU memory usage
 * prior to allocation.
 */
object GpuBatchUtils {

  /** Validity buffers are 64 byte aligned */
  val VALIDITY_BUFFER_BOUNDARY_BYTES = 64

  /** Validity buffers are 64 byte aligned and each byte represents 8 rows */
  val VALIDITY_BUFFER_BOUNDARY_ROWS = VALIDITY_BUFFER_BOUNDARY_BYTES * 8

  /** Number of bytes per offset (32 bit) */
  val OFFSET_BYTES = 4

  /** Estimate the number of rows required to meet a batch size limit */
  def estimateRowCount(desiredBatchSizeBytes: Long, currentBatchSize: Long, currentBatchRowCount: Long): Int = {
    assert(currentBatchRowCount > 0, "batch must contain at least one row")
    val targetRowCount: Long = if (currentBatchSize > desiredBatchSizeBytes) {
      currentBatchRowCount
    } else if (currentBatchSize == 0) {
      //  batch size can be 0 when doing a count() operation and the actual data isn't needed
      currentBatchRowCount
    } else {
      ((desiredBatchSizeBytes / currentBatchSize.floatValue()) * currentBatchRowCount).toLong
    }
    targetRowCount.min(Integer.MAX_VALUE).toInt
  }

  /** Estimate the amount of GPU memory a batch of rows will occupy once converted */
  def estimateGpuMemory(schema: StructType, rowCount: Long): Long = {
    schema.fields.indices.map(estimateGpuMemory(schema, _, rowCount)).sum
  }

  /** Estimate the amount of GPU memory a batch of rows will occupy once converted */
  def estimateGpuMemory(schema: StructType, columnIndex: Int, rowCount: Long): Long = {
    val field = schema.fields(columnIndex)
    val dataType = field.dataType
    val validityBufferSize = if (field.nullable) {
      calculateValidityBufferSize(rowCount)
    } else {
      0
    }
    val dataSize = dataType match {
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
    dataSize + validityBufferSize
  }

  def calculateValidityBufferSize(rows: Long): Long = {
    roundToBoundary((rows + 7)/8, 64)
  }

  def calculateOffsetBufferSize(rows: Long): Long = {
    (rows+1) * 4 // 32 bit offsets
  }

  def isVariableWidth(dt: DataType): Boolean = !isFixedWidth(dt)

  def isFixedWidth(dt: DataType): Boolean = dt match {
    case DataTypes.StringType | DataTypes.BinaryType => false
    case _: ArrayType  => false
    case _: StructType  => false
    case _: MapType  => false
    case _ => true
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
