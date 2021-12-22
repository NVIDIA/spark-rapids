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

import com.nvidia.spark.rapids.{TypeEnum, TypeSig, TypeSigUtilBase}

import org.apache.spark.sql.types.{DataType, DayTimeIntervalType, YearMonthIntervalType}

/**
 * Add DayTimeIntervalType and YearMonthIntervalType support
 */
object TypeSigUtil extends TypeSigUtilBase {

  override def isSupported(
    check: TypeEnum.ValueSet,
    dataType: DataType): Boolean = {
    dataType match {
      case _: DayTimeIntervalType => check.contains(TypeEnum.DAYTIME)
      case _: YearMonthIntervalType => check.contains(TypeEnum.YEARMONTH)
      case _ => false
    }
  }

  override def getAllSupportedTypes(): TypeEnum.ValueSet = TypeEnum.values

  override def reasonNotSupported(
    check: TypeEnum.ValueSet,
    dataType: DataType,
    notSupportedReason: Seq[String]): Seq[String] = {
    dataType match {
      case _: DayTimeIntervalType =>
        if (check.contains(TypeEnum.DAYTIME)) Seq.empty else notSupportedReason
      case _: YearMonthIntervalType =>
        if (check.contains(TypeEnum.YEARMONTH)) Seq.empty else notSupportedReason
      case _ => notSupportedReason
    }
  }

  override def mapDataTypeToTypeEnum(dataType: DataType): TypeEnum.Value = {
    dataType match {
      case _: DayTimeIntervalType => TypeEnum.DAYTIME
      case _: YearMonthIntervalType => TypeEnum.YEARMONTH
      case _ => TypeEnum.UDT // default to UDT
    }
  }

  /** Get numeric and interval TypeSig */
  override def getNumericAndInterval(): TypeSig =
    TypeSig.numeric + TypeSig.CALENDAR + TypeSig.DAYTIME + TypeSig.YEARMONTH
}
