/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
      cudfTypeScales)
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
  def sparkTypeToCudfId(dt: DataType): (Int, Int) = dt match {
    case BooleanType => (DType.BOOL8.getTypeId.getNativeId, 0)
    case IntegerType => (DType.INT32.getTypeId.getNativeId, 0)
    case LongType => (DType.INT64.getTypeId.getNativeId, 0)
    case FloatType => (DType.FLOAT32.getTypeId.getNativeId, 0)
    case DoubleType => (DType.FLOAT64.getTypeId.getNativeId, 0)
    case StringType => (DType.STRING.getTypeId.getNativeId, 0)
    case other =>
      throw new IllegalArgumentException(s"Unsupported Spark type for protobuf(simple): $other")
  }
}



