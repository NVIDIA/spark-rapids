/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf
import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.GpuRowToColumnConverter.TypeConverter

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnVector

object GpuTypeShims {

  /**
   * If Shim supports the data type for row to column converter
   * @param otherType the data type that should be checked in the Shim
   * @return true if Shim support the otherType, false otherwise.
   */
  def hasConverterForType(otherType: DataType) : Boolean = false

  /**
   * Get the TypeConverter of the data type for this Shim
   * Note should first calling hasConverterForType
   * @param t the data type
   * @param nullable is nullable
   * @return the row to column convert for the data type
   */
  def getConverterForType(t: DataType, nullable: Boolean): TypeConverter = {
    throw new RuntimeException(s"No converter is found for type $t.")
  }

  /**
   * Get the cuDF type for the Spark data type
   * @param t the Spark data type
   * @return the cuDF type if the Shim supports
   */
  def toRapidsOrNull(t: DataType): DType = null

  /** Whether the Shim supports columnar copy for the given type */
  def isColumnarCopySupportedForType(colType: DataType): Boolean = false

  /**
   * Copy a column for computing on GPU.
   * Better to check if the type is supported first by calling 'isColumnarCopySupportedForType'
   */
  def columnarCopy(cv: ColumnVector,
      b: ai.rapids.cudf.HostColumnVector.ColumnBuilder, rows: Int): Unit = {
    val t = cv.dataType()
    throw new UnsupportedOperationException(s"Converting to GPU for $t is not supported yet")
  }

  def isParquetColumnarWriterSupportedForType(colType: DataType): Boolean = false

  /**
   * Whether the Shim supports converting the given type to GPU Scalar
   */
  def supportToScalarForType(t: DataType): Boolean = false

  /**
   * Convert the given value to Scalar
   */
  def toScalarForType(t: DataType, v: Any) = {
    throw new RuntimeException(s"Can not convert $v to scalar for type $t.")
  }

  def supportCsvRead(dt: DataType) : Boolean = false

  def csvRead(cv: cudf.ColumnVector, dt: DataType): cudf.ColumnVector =
    throw new RuntimeException(s"Not support type $dt.")

  /**
   * Spark supports interval type from 320, but GPU from 330, so just return false
   */
  def isDayTimeInterval(dt: DataType) : Boolean = false
}
