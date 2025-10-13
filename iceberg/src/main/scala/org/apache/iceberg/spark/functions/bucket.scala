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

package org.apache.iceberg.spark.functions

import scala.util.{Success, Try}

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, DType, Scalar}
import com.nvidia.spark.rapids.{ExprMeta, GpuBinaryExpression, GpuColumnVector, GpuScalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.Hash
import org.apache.iceberg.spark.functions.GpuBucketExpression.cast

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.{ByteType, DataType, DataTypes, DateType, IntegerType, LongType, ShortType, TimestampNTZType, TimestampType}

case class GpuBucketExpression(numBuckets: Expression, value: Expression)
  extends GpuBinaryExpression {

  private lazy val sanityCheckResult: Unit = sanityCheck()

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): CudfColumnVector = {
    throw new IllegalStateException("GpuBucketExpression requires first argument to be scalar, " +
      "second to be column, but both are columns")
  }

  override def doColumnar(numBuckets: GpuScalar, rhs: GpuColumnVector): CudfColumnVector = {
    sanityCheckResult

    val hash = withResource(cast(rhs.getBase)) { castedValue =>
      Hash.murmurHash32(0, Array(castedValue))
    }

    val nonNegativeHash = withResource(hash) { _ =>
      withResource(Scalar.fromInt(Integer.MAX_VALUE)) { intMax =>
        hash.bitAnd(intMax)
      }
    }

    withResource(nonNegativeHash) { _ =>
      nonNegativeHash.mod(numBuckets.getBase, DType.INT32)
    }
  }

  private def sanityCheck(): Unit = {
    require(numBuckets.dataType == DataTypes.IntegerType,
      s"buckets number must be an integer, got ${numBuckets.dataType}")

    require(!value.nullable,
      s"Bucket function does not support nullable values for type ${value.dataType}")

    require(GpuBucketExpression.isSupportedValueType(value.dataType),
      s"Bucket function does not support type ${value.dataType} as values")
  }


  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): CudfColumnVector = {
    throw new IllegalStateException("GpuBucketExpression requires first argument to be scalar, " +
      "second to be column, but first is column, second is scalar")
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): CudfColumnVector = {
    throw new IllegalStateException("GpuBucketExpression requires first argument to be scalar, " +
      "second to be column, but both are scalars")
  }

  override def left: Expression = numBuckets

  override def right: Expression = value

  override def dataType: DataType = DataTypes.IntegerType
}

object GpuBucketExpression {
  private[functions] def cast(cv: CudfColumnVector): CudfColumnVector = {
    cv.getType match {
      case d if d.isBackedByInt => cv.castTo(DType.INT64)
      case d if d.isBackedByLong => cv.incRefCount()
      case u => throw new IllegalStateException(s"Unsupported type for bucketing: $u")
    }
  }

  private lazy val supportedFunctionClasses: Try[Set[Class[_]]] = Try {
    Set(
      classOf[BucketFunction.BucketInt],
      classOf[BucketFunction.BucketLong]
    )
  }

  def isSupportedValueType(dataType: DataType): Boolean = {
    dataType match {
      case ByteType |  ShortType | IntegerType | DateType |
           LongType | TimestampType | TimestampNTZType => true
      case _ => false
    }
  }

  def tagExprForGpu(meta: ExprMeta[StaticInvoke]): Unit = {
    require(meta.childExprs.length == 2,
      s"BucketFunction should have exactly two arguments, got ${meta.childExprs.length}")
    val exprCls = meta.wrapped.staticObject

    supportedFunctionClasses match {
      case Success(supported) =>
        if (!supported.contains(exprCls)) {
          meta.willNotWorkOnGpu(s"Supported iceberg partition function classes are: " +
            s"${supported.mkString("[", ",", "]")},  actual: $exprCls")
        }
      case scala.util.Failure(e) =>
        meta.willNotWorkOnGpu(s"Unable to load supported iceberg partition function classes: " +
          s"${e.getMessage}")
    }

    val bucketExpr = meta.wrapped.arguments.head
    if (bucketExpr.dataType != DataTypes.IntegerType) {
      throw new IllegalStateException(
        s"BucketFunction number of buckets must be an integer, got ${bucketExpr.dataType}")
    }

    val valueExpr = meta.wrapped.arguments(1)
    if (valueExpr.nullable) {
      meta.willNotWorkOnGpu(s"Gpu bucket function does not support nullable values for type " +
        s"${valueExpr.dataType}")
    }

    if (!isSupportedValueType(valueExpr.dataType)) {
      meta.willNotWorkOnGpu(s"Gpu bucket function does not support type ${valueExpr.dataType} " +
        s"as values")
    }
  }
}
