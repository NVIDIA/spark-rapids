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

import ai.rapids.cudf.{ColumnVector => CudfCV, DType, HostColumnVector, Table}
import com.nvidia.spark.rapids.Arm.withResource
import java.lang.reflect.Method
import java.util.function.Consumer

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

  /**
   * Create column vector from scalars
   * @param scalars literals
   * @param dataType only used by creating decimal column vector,
   *                 need to distinct decimal32, decimal64, decimal128,
   *                 should not infer from scalars
   * @return column vector for the specified scalars
   */
  def createFromScalarList(scalars: Seq[GpuScalar], dataType: DataType): CudfCV = {
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
        val decimals = scalars.map(s => s.getValue.asInstanceOf[Decimal].toJavaBigDecimal)
        fromDecimals(dataType.asInstanceOf[DecimalType], decimals: _*)
      case _ =>
        throw new UnsupportedOperationException(s"Creating column vector from a GpuScalar list" +
            s" is not supported for type ${scalars.head.dataType}.")
    }
  }

  /**
   * Create decimal column vector according to DecimalType.
   * Note: it will create 3 types of column vector according to DecimalType precision
   *  - Decimal 32 bits
   *  - Decimal 64 bits
   *  - Decimal 128 bits
   * E.g.: If the max of values are decimal 32 bits, but DecimalType is 128 bits,
   * then return a Decimal 128 bits column vector
   */
  def fromDecimals(dt: DecimalType, values: java.math.BigDecimal*): CudfCV = {
    val hcv = HostColumnVector.build(
      fromJavaBigDecimal(dt),
      values.length,
      new Consumer[HostColumnVector.Builder]() {
        override def accept(b: HostColumnVector.Builder): Unit = {
          b.appendBoxed(values: _*)
        }
      }
    )
    withResource(hcv) { _ =>
      hcv.copyToDevice()
    }
  }

  private def fromJavaBigDecimal(dt: DecimalType): DType = {
    if (dt.precision <= 9) {
      DType.create(DType.DTypeEnum.DECIMAL32, -dt.scale)
    } else if (dt.precision <= 18) {
      DType.create(DType.DTypeEnum.DECIMAL64, -dt.scale)
    }
    else if (dt.precision <= 38) {
      DType.create(DType.DTypeEnum.DECIMAL128, -dt.scale)
    }
    else {
      throw new IllegalArgumentException(
        "Precision " + dt.precision + " exceeds max precision cuDF can support " + 38)
    }
  }
}
