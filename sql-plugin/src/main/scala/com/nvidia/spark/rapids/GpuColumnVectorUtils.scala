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

package com.nvidia.spark.rapids

import java.lang.reflect.Method

import ai.rapids.cudf.{ColumnVector => CudfCV, Table}

import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.unsafe.types.UTF8String

object GpuColumnVectorUtils {
  lazy val extractHostColumnsMethod: Method = ShimLoader.loadGpuColumnVector()
    .getDeclaredMethod("extractHostColumns", classOf[Table], classOf[Array[DataType]])

  /**
   * Extract the columns from a table and convert them to RapidsHostColumnVector.
   * @param table to be extracted
   * @param colType the column types
   * @return an array of ColumnVector
   */
  def extractHostColumns(table: Table, colType: Array[DataType]): Array[ColumnVector] = {
    val columnVectors = extractHostColumnsMethod.invoke(null, table, colType)
    columnVectors.asInstanceOf[Array[ColumnVector]]
  }

  def isCaseWhenFusionSupportedType(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType => true
      case ByteType => true
      case ShortType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case StringType => true
      case DecimalType() => true
      case _ => false
    }
  }

  def createFromScalarList(scalars: Seq[GpuScalar]): CudfCV = {
    scalars.head.dataType match {
      case BooleanType =>
        val booleans = scalars.map(s => s.getValue.asInstanceOf[java.lang.Boolean])
        CudfCV.fromBoxedBooleans(booleans: _*)
      case ByteType =>
        val bytes = scalars.map(s => s.getValue.asInstanceOf[java.lang.Byte])
        CudfCV.fromBoxedBytes(bytes: _*)
      case ShortType =>
        val shorts = scalars.map(s => s.getValue.asInstanceOf[java.lang.Short])
        CudfCV.fromBoxedShorts(shorts: _*)
      case IntegerType =>
        val ints = scalars.map(s => s.getValue.asInstanceOf[java.lang.Integer])
        CudfCV.fromBoxedInts(ints: _*)
      case LongType =>
        val longs = scalars.map(s => s.getValue.asInstanceOf[java.lang.Long])
        CudfCV.fromBoxedLongs(longs: _*)
      case FloatType =>
        val floats = scalars.map(s => s.getValue.asInstanceOf[java.lang.Float])
        CudfCV.fromBoxedFloats(floats: _*)
      case DoubleType =>
        val doubles = scalars.map(s => s.getValue.asInstanceOf[java.lang.Double])
        CudfCV.fromBoxedDoubles(doubles: _*)
      case StringType =>
        val utf8Bytes = scalars.map(s => s.getValue.asInstanceOf[UTF8String].getBytes)
        CudfCV.fromUTF8Strings(utf8Bytes: _*)
      case DecimalType() =>
        val utf8Bytes = scalars.map(s => s.getValue.asInstanceOf[Decimal].toBigDecimal
            .asInstanceOf[java.math.BigDecimal])
        CudfCV.fromDecimals(utf8Bytes: _*)
      case _ =>
        throw new UnsupportedOperationException(s"Creating column vector from a GpuScalar list" +
            s" is not supported for type ${scalars.head.dataType}.")
    }
  }
}
