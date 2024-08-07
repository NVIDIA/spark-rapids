/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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
/*** spark-rapids-shim-json-lines
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf
import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.GpuRowToColumnConverter.TypeConverter
import com.nvidia.spark.rapids.RapidsHostColumnBuilder
import com.nvidia.spark.rapids.TypeSig

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
   *
   * Data type is passed explicitly to allow overriding the reported type from the column vector.
   * There are cases where the type reported by the column vector does not match the data.
   * See https://github.com/apache/iceberg/issues/6116.
   */
  def columnarCopy(
      cv: ColumnVector,
      b: RapidsHostColumnBuilder,
      dataType: DataType,
      rows: Int): Unit = {
    throw new UnsupportedOperationException(s"Converting to GPU for $dataType is not supported yet")
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
   * Whether the Shim supports day-time interval type for specific operator
   * Alias, Add, Subtract, Positive... operators do not support day-time interval type on this Shim
   * Note: Spark 3.2.x does support `DayTimeIntervalType`, this is for the GPU operators
   */
  def isSupportedDayTimeType(dt: DataType): Boolean = false

  /**
   * Whether the Shim supports year-month interval type
   * Alias, Add, Subtract, Positive... operators do not support year-month interval type
   */
  def isSupportedYearMonthType(dt: DataType): Boolean = false

  /**
   * Get additional arithmetic supported types for this Shim
   */
  def additionalArithmeticSupportedTypes: TypeSig = TypeSig.none

  /**
   * Get additional predicate supported types for this Shim
   */
  def additionalPredicateSupportedTypes: TypeSig = TypeSig.none

  /**
   * Get additional Csv supported types for this Shim
   */
  def additionalCsvSupportedTypes: TypeSig = TypeSig.none

  /**
   * Get additional Parquet supported types for this Shim
   */
  def additionalParquetSupportedTypes: TypeSig = TypeSig.none

  /**
   * Get additional common operators supported types for this Shim
   * (filter, sample, project, alias, table scan ...... which GPU supports from 330)
   */
  def additionalCommonOperatorSupportedTypes: TypeSig = TypeSig.none

  def hasSideEffectsIfCastIntToYearMonth(ym: DataType): Boolean = false

  def hasSideEffectsIfCastIntToDayTime(dt: DataType): Boolean = false

  def hasSideEffectsIfCastFloatToTimestamp: Boolean = false
}
