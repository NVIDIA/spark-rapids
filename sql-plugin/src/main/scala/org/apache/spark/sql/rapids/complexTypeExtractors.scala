/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, Scalar}
import com.nvidia.spark.rapids.{BinaryExprMeta, DataFromReplacementRule, GpuBinaryExpression, GpuColumnVector, GpuExpression, GpuOverrides, GpuScalar, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExtractValue, GetArrayItem, GetMapValue, ImplicitCastInputTypes, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, TypeUtils}
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, ArrayType, BooleanType, DataType, IntegralType, MapType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuGetStructField(child: Expression, ordinal: Int, name: Option[String] = None)
    extends UnaryExpression with GpuExpression with ExtractValue with NullIntolerant {

  lazy val childSchema: StructType = child.dataType.asInstanceOf[StructType]

  override def dataType: DataType = childSchema(ordinal).dataType
  override def nullable: Boolean = child.nullable || childSchema(ordinal).nullable

  override def toString: String = {
    val fieldName = if (resolved) childSchema(ordinal).name else s"_$ordinal"
    s"$child.${name.getOrElse(fieldName)}"
  }

  override def sql: String =
    child.sql + s".${quoteIdentifier(name.getOrElse(childSchema(ordinal).name))}"

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResourceIfAllowed(child.columnarEval(batch)) { input =>
      val dt = dataType
      input match {
        case cv: GpuColumnVector =>
          withResource(cv.getBase.getChildColumnView(ordinal)) { view =>
            GpuColumnVector.from(view.copyToColumnVector(), dt)
          }
        case null =>
          GpuColumnVector.fromNull(batch.numRows(), dt)
        case ir: InternalRow =>
          // Literal struct values are not currently supported, but just in case...
          val tmp = ir.get(ordinal, dt)
          withResource(GpuScalar.from(tmp, dt)) { scalar =>
            GpuColumnVector.from(scalar, batch.numRows(), dt)
          }
      }
    }
  }
}

class GpuGetArrayItemMeta(
    expr: GetArrayItem,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends BinaryExprMeta[GetArrayItem](expr, conf, parent, rule) {
  import GpuOverrides._

  override def tagExprForGpu(): Unit = {
    extractLit(expr.ordinal).foreach { litOrd =>
      // Once literal array/struct types are supported this can go away
      val ord = litOrd.value
      if (ord == null || ord.asInstanceOf[Int] < 0) {
        expr.dataType match {
          case ArrayType(_, _) | MapType(_, _, _) | StructType(_) =>
            willNotWorkOnGpu("negative and null indexes are not supported for nested types")
          case _ =>
        }
      }
    }
  }
  override def convertToGpu(
      arr: Expression,
      ordinal: Expression): GpuExpression =
    GpuGetArrayItem(arr, ordinal)
}

/**
 * Returns the field at `ordinal` in the Array `child`.
 *
 * We need to do type checking here as `ordinal` expression maybe unresolved.
 */
case class GpuGetArrayItem(child: Expression, ordinal: Expression)
    extends GpuBinaryExpression with ExpectsInputTypes with ExtractValue {

  // We have done type checking for child in `ExtractValue`, so only need to check the `ordinal`.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegralType)

  override def toString: String = s"$child[$ordinal]"
  override def sql: String = s"${child.sql}[${ordinal.sql}]"

  override def left: Expression = child
  override def right: Expression = ordinal
  // Eventually we need something more full featured like
  // GetArrayItemUtil.computeNullabilityFromArray
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType.asInstanceOf[ArrayType].elementType

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuColumnVector, ordinalS: GpuScalar): ColumnVector = {
    // Need to handle negative indexes...
    val ordinal = ordinalS.getValue.asInstanceOf[Int]
    if (ordinalS.isValid && ordinal >= 0) {
      lhs.getBase.extractListElement(ordinal)
    } else {
      GpuColumnVector.columnVectorFromNull(lhs.getRowCount.toInt, dataType)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

class GpuGetMapValueMeta(
  expr: GetMapValue,
  conf: RapidsConf,
  parent: Option[RapidsMeta[_, _, _]],
  rule: DataFromReplacementRule)
  extends BinaryExprMeta[GetMapValue](expr, conf, parent, rule) {

  override def convertToGpu(child: Expression, key: Expression): GpuExpression =
    GpuGetMapValue(child, key)
}

case class GpuGetMapValue(child: Expression, key: Expression)
  extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  private def keyType = child.dataType.asInstanceOf[MapType].keyType

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes() match {
      case f: TypeCheckResult.TypeCheckFailure => f
      case TypeCheckResult.TypeCheckSuccess =>
        TypeUtils.checkForOrderingExpr(keyType, s"function $prettyName")
    }
  }

  override def dataType: DataType = child.dataType.asInstanceOf[MapType].valueType

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, keyType)

  override def prettyName: String = "getMapValue"

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector =
    lhs.getBase.getMapValue(rhs.getBase)

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def left: Expression = child

  override def right: Expression = key
}

/** Checks if the array (left) has the element (right)
*/
case class GpuArrayContains(left: Expression, right: Expression)
  extends GpuBinaryExpression with NullIntolerant {

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = {
    left.nullable || right.nullable || left.dataType.asInstanceOf[ArrayType].containsNull
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector =
    lhs.getBase.listContains(rhs.getBase)

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector =
    lhs.getBase.listContainsColumn(rhs.getBase)

  override def prettyName: String = "array_contains"
}
