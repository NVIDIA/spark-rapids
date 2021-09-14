/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.v2

import com.nvidia.spark.rapids.{TypeEnum, TypeSig, TypeSigUtil}

import org.apache.spark.sql.types.DataType

/**
 * This TypeSigUtil is for [spark 3.0.1, spark 3.2.0)
 */
object TypeSigUtilUntil320 extends TypeSigUtil {
  /**
   * Check if this type of Spark-specific is supported by the plugin or not.
   *
   * @param check        the Supported Types
   * @param dataType     the data type to be checked
   * @param allowDecimal whether decimal support is enabled or not
   * @return true if it is allowed else false.
   */
  override def isSupported(
    check: TypeEnum.ValueSet,
    dataType: DataType,
    allowDecimal: Boolean): Boolean = false

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
   * @param allowDecimal       whether decimal support is enabled or not
   * @param notSupportedReason the reason for not supporting
   * @return the reason
   */
  override def reasonNotSupported(
      check: TypeEnum.ValueSet,
      dataType: DataType,
      allowDecimal: Boolean, notSupportedReason: Seq[String]): Seq[String] = notSupportedReason

  /**
   * Get checks from TypeEnum
   *
   * @param from the TypeEnum to be matched
   * @return the TypeSigs
   */
  override def getCastChecksAndSigs(from: TypeEnum.Value): (TypeSig, TypeSig) =
    throw new RuntimeException("Unsupported " + from)

  /**
   * Get TypeSigs from DataType
   *
   * @param from         the data type to be matched
   * @param default      the default TypeSig
   * @param sparkDefault the default Spark TypeSig
   * @return the TypeSigs
   */
  override def getCastChecksAndSigs(
      from: DataType,
      default: TypeSig,
      sparkDefault: TypeSig): (TypeSig, TypeSig) = (default, sparkDefault)
}
