/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, BinaryOp, DType, Scalar}
import ai.rapids.spark.{GpuBinaryExpression, GpuColumnVector, GpuExpression, GpuLiteral, GpuScalar, GpuTernaryExpression, GpuUnaryExpression}

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ImplicitCastInputTypes, NullIntolerant, Predicate}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, IntegerType, StringType, TypeCollection}

abstract class GpuUnaryString2StringExpression extends GpuUnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def dataType: DataType = StringType
}

case class GpuUpper(child: Expression) extends GpuUnaryString2StringExpression {

  override def toString: String = s"upper($child)"

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.upper())
}

case class GpuLower(child: Expression) extends GpuUnaryString2StringExpression {

  override def toString: String = s"lower($child)"

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.lower())
}

case class GpuStringLocate(substr: Expression, col: Expression, start: Expression)
  extends GpuTernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = Seq(substr, col, start)

  def this(substr: Expression, col: Expression) = {
    this(substr, col, GpuLiteral(1, IntegerType))
  }

  override def doColumnar(val0: GpuColumnVector, val1: GpuColumnVector, val2: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(val0: Scalar, val1: GpuColumnVector, val2: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(val0: Scalar, val1: Scalar, val2: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(val0: Scalar, val1: GpuColumnVector, val2: Scalar): GpuColumnVector = {
    if (!val2.isValid()) {
      val zeroScalar = GpuScalar.from(0, IntegerType)
      try {
        GpuColumnVector.from(zeroScalar, val1.getRowCount().toInt)
      } finally {
        zeroScalar.close()
      }
    } else if (!val0.isValid()) { //if null substring // or null column? <-- needs to be looked for/tested
      val nullScalar = GpuScalar.from(null, IntegerType)
      try {
        GpuColumnVector.from(nullScalar, val1.getRowCount().toInt)
      } finally {
        nullScalar.close()
      }
    } else if (val2.getInt() < 1 || val0.getJavaString().isEmpty()) {
      val isNotNullColumn = val1.getBase.isNotNull()
      try {
        val nullScalar = GpuScalar.from(null, IntegerType)
        try {
          if (val2.getInt() >= 1) {
            val sv1 = GpuScalar.from(1, IntegerType)
            try {
              GpuColumnVector.from(isNotNullColumn.ifElse(sv1, nullScalar))
            } finally {
              sv1.close()
            }
          } else {
            val sv0 = GpuScalar.from(0, IntegerType)
            try {
              GpuColumnVector.from(isNotNullColumn.ifElse(sv0, nullScalar))
            } finally {
              sv0.close()
            }
          }
        } finally {
          nullScalar.close()
        }
      } finally {
        isNotNullColumn.close()
      }
    } else {
      val skewedResult = val1.getBase.stringLocate(val0, val2.getInt() - 1, -1)
      try {
        val sv1 = GpuScalar.from(1, IntegerType)
        try {
          GpuColumnVector.from(skewedResult.add(sv1))
        } finally {
          sv1.close()
        }
      } finally {
        skewedResult.close()
      }
    }
  }
  override def doColumnar(val0: GpuColumnVector, val1: Scalar, val2: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(val0: GpuColumnVector, val1: Scalar, val2: Scalar): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(val0: GpuColumnVector, val1: GpuColumnVector, val2: Scalar): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}

case class GpuStartsWith(left: GpuExpression, right: GpuExpression)
  extends GpuBinaryExpression with Predicate with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql.toString
    s"($inputSQL STARTSWITH ($listSQL))"
  }

  override def toString: String = s"gpustartswith($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    if (rhs.getJavaString.isEmpty) {
      val boolScalar = Scalar.fromBool(true)
      try {
        GpuColumnVector.from(ColumnVector.fromScalar(boolScalar, lhs.getRowCount.toInt))
      } finally {
        boolScalar.close()
      }
    } else {
      GpuColumnVector.from(lhs.getBase.startsWith(rhs))
    }
  }

  override def doColumnar(lhs: GpuColumnVector,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here, " +
    "Cannot have two column vectors as input in StartsWith")

  override def doColumnar(lhs: Scalar,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here," +
    "Cannot have a scalar as left side operand in StartsWith")
}

case class GpuEndsWith(left: GpuExpression, right: GpuExpression)
  extends GpuBinaryExpression with Predicate with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql.toString
    s"($inputSQL ENDSWITH ($listSQL))"
  }

  override def toString: String = s"gpuendswith($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    if (rhs.getJavaString.isEmpty) {
      val boolScalar = Scalar.fromBool(true)
      try {
        GpuColumnVector.from(ColumnVector.fromScalar(boolScalar, lhs.getRowCount.toInt))
      } finally {
        boolScalar.close()
      }
    } else {
      GpuColumnVector.from(lhs.getBase.endsWith(rhs))
    }
  }

  override def doColumnar(lhs: GpuColumnVector,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here, " +
    "Cannot have two column vectors as input in EndsWith")

  override def doColumnar(lhs: Scalar,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here, " +
    "Cannot have a scalar as left side operand in EndsWith")
}

case class GpuContains(left: GpuExpression, right: GpuExpression) extends GpuBinaryExpression
  with Predicate with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql.toString
    s"($inputSQL CONTAINS ($listSQL))"
  }

  override def toString: String = s"gpucontains($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    if (rhs.getJavaString.isEmpty) {
      val boolScalar = Scalar.fromBool(true)
      try {
        GpuColumnVector.from(ColumnVector.fromScalar(boolScalar, lhs.getRowCount.toInt))
      } finally {
        boolScalar.close()
      }
    } else {
      GpuColumnVector.from(lhs.getBase.stringContains(rhs))
    }
  }

  override def doColumnar(lhs: GpuColumnVector,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here, " +
    "Cannot have two column vectors as input in Contains")

  override def doColumnar(lhs: Scalar,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here," +
    "Cannot have a scalar as left side operand in Contains")
}

case class GpuSubString(str: Expression, pos: Expression, len: Expression)
  extends GpuTernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = str.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, BinaryType), IntegerType, IntegerType)

  override def children: Seq[Expression] = Seq(str, pos, len)

  def this(str: Expression, pos: Expression) = {
    this(str, pos, GpuLiteral(Integer.MAX_VALUE, IntegerType))
  }

  override def doColumnar(val0: GpuColumnVector, val1: GpuColumnVector, val2: GpuColumnVector)
  : GpuColumnVector = throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(val0: Scalar, val1: GpuColumnVector, val2: GpuColumnVector)
  : GpuColumnVector = throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(val0: Scalar, val1: Scalar, val2: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(val0: Scalar, val1: GpuColumnVector, val2: Scalar): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(val0: GpuColumnVector, val1: Scalar, val2: GpuColumnVector)
  : GpuColumnVector = throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(column: GpuColumnVector, position: Scalar, length: Scalar): GpuColumnVector = {
    val substringPos = position.getInt
    val substringLen = length.getInt
    if (substringLen < 0) { // Spark returns empty string if length is negative
      GpuColumnVector.from(column.getBase.substring(0, 0))
    } else if (substringPos >= 0) {
      if (substringPos == 0) {
        GpuColumnVector.from(column.getBase.substring(substringPos, substringLen))
      } else {
        GpuColumnVector.from(column.getBase.substring(substringPos - 1, substringPos + substringLen - 1))
      }
    } else {
      GpuColumnVector.from(column.getBase.substring(substringPos, Integer.MAX_VALUE))
    }
  }

  override def doColumnar(val0: GpuColumnVector, val1: GpuColumnVector, val2: Scalar)
  : GpuColumnVector = throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}
