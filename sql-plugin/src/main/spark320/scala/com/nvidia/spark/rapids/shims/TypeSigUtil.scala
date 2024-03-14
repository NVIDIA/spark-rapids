/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "350"}
{"spark": "351"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

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
    TypeSig.cpuNumeric + TypeSig.CALENDAR + TypeSig.DAYTIME + TypeSig.YEARMONTH

  /** Get Ansi year-month and day-time TypeSig */
  override def getAnsiInterval: TypeSig = TypeSig.DAYTIME + TypeSig.YEARMONTH
}
