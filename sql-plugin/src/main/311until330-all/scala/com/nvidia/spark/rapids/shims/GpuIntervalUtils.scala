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

import ai.rapids.cudf.ColumnVector

import org.apache.spark.sql.types.DataType

/**
 * Should not support in this Shim
 */
object GpuIntervalUtils {

  def castStringToDayTimeIntervalWithThrow(cv: ColumnVector, t: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def toDayTimeIntervalString(micros: ColumnVector, dayTimeType: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def dayTimeIntervalToLong(dtCv: ColumnVector, dt: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def dayTimeIntervalToInt(dtCv: ColumnVector, dt: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def dayTimeIntervalToShort(dtCv: ColumnVector, dt: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def dayTimeIntervalToByte(dtCv: ColumnVector, dt: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def yearMonthIntervalToLong(ymCv: ColumnVector, ym: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def yearMonthIntervalToInt(ymCv: ColumnVector, ym: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def yearMonthIntervalToShort(ymCv: ColumnVector, ym: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def yearMonthIntervalToByte(ymCv: ColumnVector, ym: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def longToDayTimeInterval(longCv: ColumnVector, dt: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def intToDayTimeInterval(intCv: ColumnVector, dt: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def longToYearMonthInterval(longCv: ColumnVector, ym: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }

  def intToYearMonthInterval(intCv: ColumnVector, ym: DataType): ColumnVector = {
    throw new IllegalStateException("Not supported in this Shim")
  }
}
