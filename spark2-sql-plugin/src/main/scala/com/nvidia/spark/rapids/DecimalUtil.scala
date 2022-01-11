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

package com.nvidia.spark.rapids

import org.apache.spark.sql.types._

object DecimalUtil {

  def getNonNestedRapidsType(dtype: DataType): String = {
    val res = toRapidsStringOrNull(dtype)
    res.getOrElse(throw new
      IllegalArgumentException(dtype + " is not supported for GPU processing yet."))
  }

 def createCudfDecimal(precision: Int, scale: Int): Option[String] = {
    if (precision <= GpuOverrides.DECIMAL32_MAX_PRECISION) {
      Some("DECIMAL32")
    } else if (precision <= GpuOverrides.DECIMAL64_MAX_PRECISION) {
      Some("DECIMAL64")
    } else if (precision <= GpuOverrides.DECIMAL128_MAX_PRECISION) {
      Some("DECIMAL128")
    } else {
      throw new IllegalArgumentException(s"precision overflow: $precision")
      None
    }
  }

  // don't want to pull in cudf for explain only so use strings
  // instead of DType
  def toRapidsStringOrNull(dtype: DataType): Option[String] = {
    dtype match {
      case _: LongType => Some("INT64")
      case _: DoubleType => Some("FLOAT64")
      case _: ByteType => Some("INT8")
      case _: BooleanType => Some("BOOL8")
      case _: ShortType => Some("INT16")
      case _: IntegerType => Some("INT32")
      case _: FloatType => Some("FLOAT32")
      case _: DateType => Some("TIMESTAMP_DAYS")
      case _: TimestampType => Some("TIMESTAMP_MICROSECONDS")
      case _: StringType => Some("STRING")
      case _: BinaryType => Some("LIST")
      case _: NullType => Some("INT8")
      case _: DecimalType => 
        // Decimal supportable check has been conducted in the GPU plan overriding stage.
        // So, we don't have to handle decimal-supportable problem at here.
        val dt = dtype.asInstanceOf[DecimalType]
        createCudfDecimal(dt.precision, dt.scale)
      case _ => None
    }
  }

  /**
   * Get the number of decimal places needed to hold the integral type held by this column
   */
  def getPrecisionForIntegralType(input: String): Int = input match {
    case "INT8" =>  3 // -128 to 127
    case "INT16" => 5 // -32768 to 32767
    case "INT32" => 10 // -2147483648 to 2147483647
    case "INT64" => 19 // -9223372036854775808 to 9223372036854775807
    case t => throw new IllegalArgumentException(s"Unsupported type $t")
  }
  // The following types were copied from Spark's DecimalType class
  private val BooleanDecimal = DecimalType(1, 0)

  def optionallyAsDecimalType(t: DataType): Option[DecimalType] = t match {
    case dt: DecimalType => Some(dt)
    case ByteType | ShortType | IntegerType | LongType =>
      val prec = DecimalUtil.getPrecisionForIntegralType(getNonNestedRapidsType(t))
      Some(DecimalType(prec, 0))
    case BooleanType => Some(BooleanDecimal)
    case _ => None
  }

  def asDecimalType(t: DataType): DecimalType = optionallyAsDecimalType(t) match {
    case Some(dt) => dt
    case _ =>
      throw new IllegalArgumentException(
        s"Internal Error: type $t cannot automatically be cast to a supported DecimalType")
  }
}
