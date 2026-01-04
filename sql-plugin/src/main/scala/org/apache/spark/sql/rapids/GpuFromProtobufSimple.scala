/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import ai.rapids.cudf
import ai.rapids.cudf.BinaryOp
import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.{GpuColumnVector, GpuUnaryExpression}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.ProtobufSimple
import com.nvidia.spark.rapids.shims.NullIntolerantShim

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.types._

/**
 * GPU implementation for Spark's `from_protobuf` decode path (simple types only).
 *
 * This is designed to replace `org.apache.spark.sql.protobuf.ProtobufDataToCatalyst` when
 * supported.
 */
case class GpuFromProtobufSimple(
    outputSchema: StructType,
    fieldNumbers: Array[Int],
    cudfTypeIds: Array[Int],
    cudfTypeScales: Array[Int],
    failOnErrors: Boolean,
    child: Expression)
  extends GpuUnaryExpression with ExpectsInputTypes with NullIntolerantShim {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = outputSchema.asNullable

  override def nullable: Boolean = true

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    // Spark BinaryType is represented in cuDF as a LIST<UINT8/INT8>.
    // ProtobufSimple returns a non-null STRUCT with nullable children. Spark's
    // ProtobufDataToCatalyst is NullIntolerant, so if the input binary row is null the output
    // struct row must be null as well.
    val decoded = ProtobufSimple.decodeToStruct(
      input.getBase,
      fieldNumbers,
      cudfTypeIds,
      cudfTypeScales,
      failOnErrors)
    if (input.getBase.hasNulls) {
      withResource(decoded) { _ =>
        decoded.mergeAndSetValidity(BinaryOp.BITWISE_AND, input.getBase)
      }
    } else {
      decoded
    }
  }
}

object GpuFromProtobufSimple {
  // Encodings from com.nvidia.spark.rapids.jni.ProtobufSimple
  val ENC_DEFAULT = 0
  val ENC_FIXED   = 1
  val ENC_ZIGZAG  = 2

  /**
   * Maps a Spark DataType to the corresponding cuDF native type ID.
   * Note: The encoding (varint/zigzag/fixed) is determined by the protobuf field type,
   * not the Spark data type, so it must be set separately based on the protobuf schema.
   */
  def sparkTypeToCudfId(dt: DataType): Int = dt match {
    case BooleanType => DType.BOOL8.getTypeId.getNativeId
    case IntegerType => DType.INT32.getTypeId.getNativeId
    case LongType => DType.INT64.getTypeId.getNativeId
    case FloatType => DType.FLOAT32.getTypeId.getNativeId
    case DoubleType => DType.FLOAT64.getTypeId.getNativeId
    case StringType => DType.STRING.getTypeId.getNativeId
    case BinaryType => DType.LIST.getTypeId.getNativeId
    case other =>
      throw new IllegalArgumentException(s"Unsupported Spark type for protobuf(simple): $other")
  }
}