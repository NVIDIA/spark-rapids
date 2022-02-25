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

import ai.rapids.cudf.{ColumnVector, DType}
import com.nvidia.spark.rapids.GpuRowToColumnConverter.{LongConverter, NotNullLongConverter, TypeConverter}

import org.apache.spark.sql.types.{DataType, DayTimeIntervalType}

/**
 * Spark stores ANSI YearMonthIntervalType as int32 and ANSI DayTimeIntervalType as int64
 * internally when computing.
 * See the comments of YearMonthIntervalType, below is copied from Spark
 *   Internally, values of year-month intervals are stored in `Int` values as amount of months
 *     that are calculated by the formula:
 *     -/+ (12 * YEAR + MONTH)
 * See the comments of DayTimeIntervalType, below is copied from Spark
 *   Internally, values of day-time intervals are stored in `Long` values as amount of time in terms
 *   of microseconds that are calculated by the formula:
 *     -/+ (24*60*60 * DAY + 60*60 * HOUR + 60 * MINUTE + SECOND) * 1000000
 *
 * Spark also stores ANSI intervals as int32 and int64 in Parquet file:
 *   - year-month intervals as `INT32`
 *   - day-time intervals as `INT64`
 * To load the values as intervals back, Spark puts the info about interval types
 * to the extra key `org.apache.spark.sql.parquet.row.metadata`:
 * $ java -jar parquet-tools-1.12.0.jar meta ./part-...-c000.snappy.parquet
 *     creator: parquet-mr version 1.12.1 (build 2a5c06c58fa987f85aa22170be14d927d5ff6e7d)
 *     extra:   org.apache.spark.version = 3.3.0
 *     extra:   org.apache.spark.sql.parquet.row.metadata =
 *     {"type":"struct","fields":[...,
 *       {"name":"i","type":"interval year to month","nullable":false,"metadata":{}}]}
 *     file schema: spark_schema
 *     --------------------------------------------------------------------------------
 *     ...
 *     i:  REQUIRED INT32 R:0 D:0
 *
 * For details See https://issues.apache.org/jira/browse/SPARK-36825
 */
object GpuTypeShims {

  /**
   * If Shim supports the data type for row to column converter
   * @param otherType the data type that should be checked in the Shim
   * @return true if Shim support the otherType, false otherwise.
   */
  def hasConverterForType(otherType: DataType) : Boolean = {
    otherType match {
      case DayTimeIntervalType(_, _) => true
      case _ => false
    }
  }

  /**
   * Get the TypeConverter of the data type for this Shim
   * Note should first calling hasConverterForType
   * @param t the data type
   * @param nullable is nullable
   * @return the row to column convert for the data type
   */
  def getConverterForType(t: DataType, nullable: Boolean): TypeConverter = {
    (t, nullable) match {
      case (DayTimeIntervalType(_, _), true) => LongConverter
      case (DayTimeIntervalType(_, _), false) => NotNullLongConverter
      case _ => throw new RuntimeException(s"No converter is found for type $t.")
    }
  }

  /**
   * Get the cuDF type for the Spark data type
   * @param t the Spark data type
   * @return the cuDF type if the Shim supports
   */
  def toRapidsOrNull(t: DataType): DType = {
    t match {
      case _: DayTimeIntervalType =>
        // use int64 as Spark does
        DType.INT64
      case _ =>
        null
    }
  }

  def supportCsvRead(dt: DataType) : Boolean = {
    dt match {
      case DayTimeIntervalType(_, _) => true
      case _ => false
    }
  }

  def csvRead(cv: ColumnVector, dt: DataType): ColumnVector  = {
    dt match {
      case d: DayTimeIntervalType => GpuIntervalUtils.castStringToDTInterval(cv, d)
      case _ => throw new RuntimeException(s"Not support type $dt.")
    }
  }

}
