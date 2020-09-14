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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.{ColumnVector, Scalar}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuLeafExpression}

import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.sql.types.{DataType, LongType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Returns the name of the file being read, or empty string if not available.
 * This is extra difficult because we cannot coalesce batches in between when this
 * is used and the input file or else we could run into problems with returning the wrong thing.
 */
case class GpuInputFileName() extends GpuLeafExpression {
  /**
   * We need to recompute this if something fails.
   */
  override lazy val deterministic: Boolean = false
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = StringType
  override def prettyName: String = "input_file_name"

  /**
   * We need our input to be a single block per file.
   */
  override def disableCoalesceUntilInput(): Boolean = true

  override def columnarEval(batch: ColumnarBatch): Any = {
    val scalar = Scalar.fromString(InputFileBlockHolder.getInputFilePath.toString)
    try {
      GpuColumnVector.from(ColumnVector.fromScalar(scalar, batch.numRows()))
    } finally {
      scalar.close()
    }
  }
}

object InputFileUtils {
  def setInputFileBlock(filePath: String, start: Long, length: Long): Unit = {
    InputFileBlockHolder.set(filePath, start, length)
  }
}

/**
 * Returns the start offset of the block being read, or -1 if not available.
 * This is extra difficult because we cannot coalesce batches in between when this
 * is used and the input file or else we could run into problems with returning the wrong thing.
 */
case class GpuInputFileBlockStart() extends GpuLeafExpression {
  /**
   * We need to recompute this if something fails.
   */
  override lazy val deterministic: Boolean = false
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = LongType
  override def prettyName: String = "input_file_block_start"

  /**
   * We need our input to be a single block per file.
   */
  override def disableCoalesceUntilInput(): Boolean = true

  override def columnarEval(batch: ColumnarBatch): Any = {
    val scalar = Scalar.fromLong(InputFileBlockHolder.getStartOffset)
    try {
      GpuColumnVector.from(ColumnVector.fromScalar(scalar, batch.numRows()))
    } finally {
      scalar.close()
    }
  }
}

/**
 * Returns the length of the block being read, or -1 if not available.
 * This is extra difficult because we cannot coalesce batches in between when this
 * is used and the input file or else we could run into problems with returning the wrong thing.
 */
case class GpuInputFileBlockLength() extends GpuLeafExpression {
  /**
   * We need to recompute this if something fails.
   */
  override lazy val deterministic: Boolean = false
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = LongType
  override def prettyName: String = "input_file_block_length"

  /**
   * We need our input to be a single block per file.
   */
  override def disableCoalesceUntilInput(): Boolean = true

  override def columnarEval(batch: ColumnarBatch): Any = {
    val scalar = Scalar.fromLong(InputFileBlockHolder.getLength)
    try {
      GpuColumnVector.from(ColumnVector.fromScalar(scalar, batch.numRows()))
    } finally {
      scalar.close()
    }
  }
}
