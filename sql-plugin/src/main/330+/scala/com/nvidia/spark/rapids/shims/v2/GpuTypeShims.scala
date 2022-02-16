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
package com.nvidia.spark.rapids.shims.v2

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.GpuRowToColumnConverter.{IntConverter, LongConverter, NotNullIntConverter, NotNullLongConverter, TypeConverter}

import org.apache.spark.sql.types.{DataType, DayTimeIntervalType, YearMonthIntervalType}

object GpuTypeShims {

  private lazy val getPartialConverter: PartialFunction[(DataType, Boolean), TypeConverter] = {
    case (YearMonthIntervalType(_, _), true) => IntConverter
    case (YearMonthIntervalType(_, _), false) => NotNullIntConverter
    case (DayTimeIntervalType(_, _), true) => LongConverter
    case (DayTimeIntervalType(_, _), false) => NotNullLongConverter
  }

  // Get shim matches
  // Support ANSI interval types
  lazy val getConverterForType = Option(getPartialConverter)

  // Get type that shim supporting
  // Support ANSI interval types
  def toRapidsOrNull(t: DataType): DType = {
    t match {
      case _: YearMonthIntervalType =>
        DType.INT32
      case _: DayTimeIntervalType =>
        DType.INT64
      case _ =>
        null
    }
  }
}
