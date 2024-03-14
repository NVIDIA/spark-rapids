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
{"spark": "311"}
{"spark": "312"}
{"spark": "313"}
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.{ColumnVector, ColumnView}

import org.apache.spark.sql.types.DataType

/**
 * Should not support in this Shim
 */
object GpuIntervalUtils {

  def castStringToDayTimeIntervalWithThrow(cv: ColumnView, t: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def toDayTimeIntervalString(micros: ColumnView, dayTimeType: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def dayTimeIntervalToLong(dtCv: ColumnView, dt: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def dayTimeIntervalToInt(dtCv: ColumnView, dt: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def dayTimeIntervalToShort(dtCv: ColumnView, dt: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def dayTimeIntervalToByte(dtCv: ColumnView, dt: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def yearMonthIntervalToLong(ymCv: ColumnView, ym: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def yearMonthIntervalToInt(ymCv: ColumnView, ym: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def yearMonthIntervalToShort(ymCv: ColumnView, ym: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def yearMonthIntervalToByte(ymCv: ColumnView, ym: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def longToDayTimeInterval(longCv: ColumnView, dt: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def intToDayTimeInterval(intCv: ColumnView, dt: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def longToYearMonthInterval(longCv: ColumnView, ym: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def intToYearMonthInterval(intCv: ColumnView, ym: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }
}
