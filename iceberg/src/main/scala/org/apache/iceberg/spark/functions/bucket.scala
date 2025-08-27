package org.apache.iceberg.spark.functions

import ai.rapids.cudf.{ColumnVector, DType}
import com.nvidia.spark.rapids.{GpuBinaryExpression, GpuColumnVector, GpuScalar}
import com.nvidia.spark.rapids.jni.Hash

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, DataTypes}


case class GpuBucketExpression(numBuckets: Expression, value: Expression)
  extends GpuBinaryExpression {
  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    throw new IllegalStateException("GpuBucketExpression requires first argument to be scalar, " +
      "second to be column, but both are columns")
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    Hash.murmurHash32(0, Array(rhs.getBase)).mod(lhs.getBase, DType.INT32)
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    throw new IllegalStateException("GpuBucketExpression requires first argument to be scalar, " +
      "second to be column, but first is column, second is scalar")
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    throw new IllegalStateException("GpuBucketExpression requires first argument to be scalar, " +
      "second to be column, but both are scalars")
  }

  override def left: Expression = numBuckets

  override def right: Expression = value

  override def dataType: DataType = DataTypes.IntegerType
}

