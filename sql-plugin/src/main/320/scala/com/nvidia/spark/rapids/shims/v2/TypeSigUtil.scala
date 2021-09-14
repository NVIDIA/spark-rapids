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

import com.nvidia.spark.rapids.{TypeEnum, TypeSig}

import org.apache.spark.sql.types.{DataType, DayTimeIntervalType, YearMonthIntervalType}

/**
 * Add DayTimeIntervalType and YearMonthIntervalType support
 */
object TypeSigUtil extends com.nvidia.spark.rapids.TypeSigUtil {

  override def isSupported(
      check: TypeEnum.ValueSet,
      dataType: DataType,
      allowDecimal: Boolean): Boolean = {
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
      allowDecimal: Boolean,
      notSupportedReason: Seq[String]): Seq[String] = {
    dataType match {
      case _: DayTimeIntervalType =>
        if (check.contains(TypeEnum.DAYTIME)) Seq.empty else notSupportedReason
      case _: YearMonthIntervalType =>
        if (check.contains(TypeEnum.YEARMONTH)) Seq.empty else notSupportedReason
      case _ => notSupportedReason
    }
  }

  override def getCastChecksAndSigs(
      from: DataType,
      default: TypeSig,
      sparkDefault: TypeSig): (TypeSig, TypeSig) = {
    from match {
      case _: DayTimeIntervalType => (daytimeChecks, sparkDaytimeSig)
      case _: YearMonthIntervalType =>(yearmonthChecks, sparkYearmonthSig)
      case _ => (default, sparkDefault)
    }
  }

  override def getCastChecksAndSigs(from: TypeEnum.Value): (TypeSig, TypeSig) = {
    from match {
      case TypeEnum.DAYTIME => (daytimeChecks, sparkDaytimeSig)
      case TypeEnum.YEARMONTH => (yearmonthChecks, sparkYearmonthSig)
    }
  }

  def daytimeChecks: TypeSig = TypeSig.none
  def sparkDaytimeSig: TypeSig = TypeSig.DAYTIME + TypeSig.STRING

  def yearmonthChecks: TypeSig = TypeSig.none
  def sparkYearmonthSig: TypeSig = TypeSig.YEARMONTH + TypeSig.STRING
}
