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

import com.nvidia.spark.rapids.{TypeEnum, TypeSig}

import org.apache.spark.sql.types.DataType

/** TypeSig Support for [3.0.1, 3.2.0) */
object TypeSigUtil extends com.nvidia.spark.rapids.TypeSigUtilBase {

  /**
   * Check if this type of Spark-specific is supported by the plugin or not.
   *
   * @param check        the Supported Types
   * @param dataType     the data type to be checked
   * @return true if it is allowed else false.
   */
  override def isSupported(
    check: TypeEnum.ValueSet,
    dataType: DataType): Boolean = false

  /**
   * Get all supported types for the spark-specific
   *
   * @return the all supported typ
   */
  override def getAllSupportedTypes(): TypeEnum.ValueSet =
    TypeEnum.values - TypeEnum.DAYTIME - TypeEnum.YEARMONTH

  /**
   * Return the reason why this type is not supported.\
   *
   * @param check              the Supported Types
   * @param dataType           the data type to be checked
   * @param notSupportedReason the reason for not supporting
   * @return the reason
   */
  override def reasonNotSupported(
    check: TypeEnum.ValueSet,
    dataType: DataType,
    notSupportedReason: Seq[String]): Seq[String] = notSupportedReason

  /**
   * Map DataType to TypeEnum
   *
   * @param dataType the data type to be mapped
   * @return the TypeEnum
   */
  override def mapDataTypeToTypeEnum(dataType: DataType): TypeEnum.Value = TypeEnum.UDT

  /** Get numeric and interval TypeSig */
  override def getNumericAndInterval(): TypeSig =
    TypeSig.cpuNumeric + TypeSig.CALENDAR
}
