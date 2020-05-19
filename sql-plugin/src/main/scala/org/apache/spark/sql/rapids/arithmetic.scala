/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf._
import ai.rapids.spark._

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, NullIntolerant}
import org.apache.spark.sql.types._

case class GpuUnaryMinus(child: GpuExpression) extends GpuUnaryExpression
    with ExpectsInputTypes with NullIntolerant {
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def toString: String = s"-$child"

  override def sql: String = s"(- ${child.sql})"

  override def doColumnar(input: GpuColumnVector) : GpuColumnVector = {
    val scalar = Scalar.fromByte(0.toByte)
    try {
      GpuColumnVector.from(scalar.sub(input.getBase))
    } finally {
      scalar.close()
    }
  }
}

case class GpuUnaryPositive(child: GpuExpression) extends GpuUnaryExpression
    with ExpectsInputTypes with NullIntolerant {
  override def prettyName: String = "positive"

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def sql: String = s"(+ ${child.sql})"

  override def doColumnar(input: GpuColumnVector) : GpuColumnVector = input
}

case class GpuAbs(child: GpuExpression) extends CudfUnaryExpression
    with ExpectsInputTypes with NullIntolerant {
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def dataType: DataType = child.dataType

  override def unaryOp: UnaryOp = UnaryOp.ABS
}

abstract class CudfBinaryArithmetic extends CudfBinaryOperator with NullIntolerant {
  override def dataType: DataType = left.dataType

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess
}

case class GpuAdd(left: GpuExpression, right: GpuExpression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  override def binaryOp: BinaryOp = BinaryOp.ADD
}

case class GpuSubtract(left: GpuExpression, right: GpuExpression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"

  override def binaryOp: BinaryOp = BinaryOp.SUB
}

case class GpuMultiply(left: GpuExpression, right: GpuExpression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "*"

  override def binaryOp: BinaryOp = BinaryOp.MUL
}

object GpuDivModLike {
  def replaceZeroWithNull(v: GpuColumnVector): GpuColumnVector = {
    var zeroScalar: Scalar = null
    var nullScalar: Scalar = null
    var zeroVec: ColumnVector = null
    var nullVec: ColumnVector = null
    try {
      val dtype = v.getBase.getType
      zeroScalar = makeZeroScalar(dtype)
      nullScalar = Scalar.fromNull(dtype)
      zeroVec = ColumnVector.fromScalar(zeroScalar, 1)
      nullVec = ColumnVector.fromScalar(nullScalar, 1)
      GpuColumnVector.from(v.getBase.findAndReplaceAll(zeroVec, nullVec))
    } finally {
      if (zeroScalar != null) {
        zeroScalar.close()
      }
      if (nullScalar != null) {
        nullScalar.close()
      }
      if (zeroVec != null) {
        zeroVec.close()
      }
      if (nullVec != null) {
        nullVec.close()
      }
    }
  }

  def isScalarZero(s: Scalar): Boolean = {
    s.getType match {
      case DType.INT8 => s.getByte == 0
      case DType.INT16 => s.getShort == 0
      case DType.INT32 => s.getInt == 0
      case DType.INT64 => s.getLong == 0
      case DType.FLOAT32 => s.getFloat == 0f
      case DType.FLOAT64 => s.getDouble == 0
      case t => throw new IllegalArgumentException(s"Unexpected type: $t")
    }
  }

  def makeZeroScalar(dtype: DType): Scalar = {
    dtype match {
      case DType.INT8 => Scalar.fromByte(0.toByte)
      case DType.INT16 => Scalar.fromShort(0.toShort)
      case DType.INT32 => Scalar.fromInt(0)
      case DType.INT64 => Scalar.fromLong(0L)
      case DType.FLOAT32 => Scalar.fromFloat(0f)
      case DType.FLOAT64 => Scalar.fromDouble(0)
      case t => throw new IllegalArgumentException(s"Unexpected type: $t")
    }
  }
}

trait GpuDivModLike extends CudfBinaryArithmetic {
  override def nullable: Boolean = true

  import GpuDivModLike._

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector = {
    val replaced = replaceZeroWithNull(rhs)
    try {
      super.doColumnar(lhs, replaced)
    } finally {
      replaced.close()
    }
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector = {
    val replaced = replaceZeroWithNull(rhs)
    try {
      super.doColumnar(lhs, replaced)
    } finally {
      replaced.close()
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    if (isScalarZero(rhs)) {
      val nullScalar = Scalar.fromNull(lhs.getBase.getType)
      try {
        GpuColumnVector.from(ColumnVector.fromScalar(nullScalar, lhs.getRowCount.toInt))
      } finally {
        nullScalar.close()
      }
    } else {
      super.doColumnar(lhs, rhs)
    }
  }
}

// This is for doubles and floats...
case class GpuDivide(left: GpuExpression, right: GpuExpression) extends GpuDivModLike {
  override def inputType: AbstractDataType = TypeCollection(DoubleType, DecimalType)

  override def symbol: String = "/"

  override def binaryOp: BinaryOp = BinaryOp.TRUE_DIV
}

case class GpuIntegralDivide(left: GpuExpression, right: GpuExpression) extends GpuDivModLike {
  override def inputType: AbstractDataType = TypeCollection(IntegralType, DecimalType)

  override def dataType: DataType = LongType
  override def outputTypeOverride: DType = DType.INT64

  override def symbol: String = "/"

  override def binaryOp: BinaryOp = BinaryOp.DIV

  override def sqlOperator: String = "div"
}

case class GpuRemainder(left: GpuExpression, right: GpuExpression) extends GpuDivModLike {
  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "%"

  override def binaryOp: BinaryOp = BinaryOp.MOD
}


case class GpuPmod(left: GpuExpression, right: GpuExpression) extends GpuDivModLike {
  override def inputType: AbstractDataType = NumericType

  override def binaryOp: BinaryOp = BinaryOp.PMOD

  override def symbol: String = "pmod"

  override def dataType: DataType = left.dataType
}