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

  /** Whether the Shim supports columnar copy for the given type */
  def isColumnarCopySupportedForType(colType: DataType): Boolean = false

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

  def typesDayTimeCanCastTo: TypeSig = TypeSig.none

  def typesYearMonthCanCastTo: TypeSig = TypeSig.none

  def typesDayTimeCanCastToOnSpark: TypeSig = TypeSig.DAYTIME + TypeSig.STRING

  def typesYearMonthCanCastToOnSpark: TypeSig = TypeSig.YEARMONTH + TypeSig.STRING

  def additionalTypesIntegralCanCastTo: TypeSig = TypeSig.none

  def additionalTypesStringCanCastTo: TypeSig = TypeSig.none

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
