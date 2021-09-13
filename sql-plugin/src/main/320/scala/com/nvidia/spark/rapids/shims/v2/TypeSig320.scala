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

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.{TypeEnum, TypeSig}

import org.apache.spark.sql.types.{DataType, DayTimeIntervalType, YearMonthIntervalType}

/** TypeSig for Spark 3.2.0+ which adds DayTimeIntervalType and YearMonthIntervalType support */
final class TypeSig320(
    override val initialTypes: TypeEnum.ValueSet,
    override val maxAllowedDecimalPrecision: Int = DType.DECIMAL64_MAX_PRECISION,
    override val childTypes: TypeEnum.ValueSet = TypeEnum.ValueSet(),
    override val litOnlyTypes: TypeEnum.ValueSet = TypeEnum.ValueSet(),
    override val notes: Map[TypeEnum.Value, String] = Map.empty)
  extends TypeSig(initialTypes, maxAllowedDecimalPrecision, childTypes, litOnlyTypes, notes) {

  override protected[this] def isLitOnly(dataType: DataType): Boolean = {
    dataType match {
      case _: DayTimeIntervalType => litOnlyTypes.contains(TypeEnum.DAYTIME)
      case _: YearMonthIntervalType => litOnlyTypes.contains(TypeEnum.YEARMONTH)
      case _ => super.isLitOnly(dataType)
    }
  }

  override protected[this] def isSupported(
      check: TypeEnum.ValueSet,
      dataType: DataType,
      allowDecimal: Boolean): Boolean = {
    dataType match {
      case _: DayTimeIntervalType => check.contains(TypeEnum.DAYTIME)
      case _: YearMonthIntervalType => check.contains(TypeEnum.YEARMONTH)
      case _ => super.isSupported(check, dataType, allowDecimal)
    }
  }

  override protected[this] def reasonNotSupported(
      check: TypeEnum.ValueSet,
      dataType: DataType,
      isChild: Boolean,
      allowDecimal: Boolean): Seq[String] = {
    dataType match {
      case _: DayTimeIntervalType =>
        basicNotSupportedMessage(dataType, TypeEnum.DAYTIME, check, isChild)
      case _: YearMonthIntervalType =>
        basicNotSupportedMessage(dataType, TypeEnum.YEARMONTH, check, isChild)
      case _ => super.reasonNotSupported(check, dataType, isChild, allowDecimal)
    }
  }
}

object TypeSig320 {

  /** Convert TypeSig to TypeSig320 */
  def apply(typeSig: TypeSig): TypeSig320 = {
    new TypeSig320(typeSig.initialTypes, typeSig.maxAllowedDecimalPrecision, typeSig.childTypes,
      typeSig.litOnlyTypes, typeSig.notes)
  }

}
