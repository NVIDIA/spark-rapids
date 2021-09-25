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

import ai.rapids.cudf.{ColumnVector, Scalar}

import org.apache.spark.TaskContext
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * An expression that returns monotonically increasing 64-bit integers just like
 * `org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID`
 *
 * The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
 * This implementations should match what spark does which is to put the partition ID in the upper
 * 31 bits, and the lower 33 bits represent the record number within each partition.
 */
case class GpuMonotonicallyIncreasingID() extends GpuLeafExpression {
  /**
   * We need to recompute this if something fails.
   */
  override lazy val deterministic: Boolean = false
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = LongType
  override def prettyName: String = "monotonically_increasing_id"
  override def sql: String = s"$prettyName()"

  @transient private[this] var count: Long = _
  @transient private[this] var partitionMask: Long = _
  @transient private[this] var wasInitialized: Boolean = _

  override def columnarEval(batch: ColumnarBatch): Any = {
    if (!wasInitialized) {
      count = 0
      partitionMask = TaskContext.getPartitionId().toLong << 33
      wasInitialized = true
    }
    var start: Scalar = null
    var mask: Scalar = null
    var sequence: ColumnVector = null
    try {
      val numRows = batch.numRows()
      start = Scalar.fromLong(count)
      mask = Scalar.fromLong(partitionMask)
      sequence = ColumnVector.sequence(start, numRows)
      count += numRows
      GpuColumnVector.from(sequence.add(mask), dataType)
    } finally {
      if (start != null) {
        start.close()
      }
      if (mask != null) {
        mask.close()
      }
      if (sequence != null) {
        sequence.close()
      }
    }
  }
}
