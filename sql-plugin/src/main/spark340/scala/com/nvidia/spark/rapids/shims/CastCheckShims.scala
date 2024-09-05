/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "350db"}
{"spark": "351"}
{"spark": "352"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.TypeSig

import org.apache.spark.sql.internal.SQLConf

object CastCheckShims {

  def typesDayTimeCanCastTo: TypeSig = TypeSig.DAYTIME + TypeSig.STRING + TypeSig.integral

  def typesYearMonthCanCastTo: TypeSig = TypeSig.integral

  def typesDayTimeCanCastToOnSpark: TypeSig = TypeSig.DAYTIME + TypeSig.STRING + TypeSig.integral

  def additionalTypesIntegralCanCastTo: TypeSig = TypeSig.YEARMONTH + TypeSig.DAYTIME

  def additionalTypesStringCanCastTo: TypeSig = TypeSig.DAYTIME


  def typesYearMonthCanCastToOnSpark: TypeSig = TypeSig.YEARMONTH + TypeSig.STRING +
    TypeSig.integral

  def additionalTypesBooleanCanCastTo: TypeSig = if (SQLConf.get.ansiEnabled) {
    TypeSig.none
  } else {
    TypeSig.TIMESTAMP
  }

  def additionalTypesDateCanCastTo: TypeSig = if (SQLConf.get.ansiEnabled) {
    TypeSig.none
  } else {
    TypeSig.BOOLEAN + TypeSig.integral + TypeSig.fp
  }

  def additionalTypesTimestampCanCastTo: TypeSig = if (SQLConf.get.ansiEnabled) {
    TypeSig.none
  } else {
    TypeSig.BOOLEAN
  }
}