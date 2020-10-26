/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.{BinaryExprMeta, ConfKeysAndIncompat, GpuBinaryExpression, GpuColumnVector, GpuExpression, GpuOverrides, RapidsConf, RapidsMeta}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExtractValue, GetArrayItem, GetMapValue, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, ArrayType, DataType, IntegralType, MapType, StringType}

class GpuGetArrayItemMeta(
    expr: GetArrayItem,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
    extends BinaryExprMeta[GetArrayItem](expr, conf, parent, rule) {
  import GpuOverrides._

  override def tagExprForGpu(): Unit = {
    if (!isLit(expr.ordinal)) {
      willNotWorkOnGpu("only literal ordinals are supported")
    }
  }
  override def convertToGpu(
      arr: Expression,
      ordinal: Expression): GpuExpression =
    GpuGetArrayItem(arr, ordinal)

  def isSupported(t: DataType) = t match {
    // For now we will only do one level of array type support
    case a : ArrayType => isSupportedType(a.elementType)
    case _ => isSupportedType(t)
  }

  override def areAllSupportedTypes(types: DataType*): Boolean = types.forall(isSupported)
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

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuColumnVector, ordinal: Scalar): GpuColumnVector = {
    // Need to handle negative indexes...
    if (ordinal.isValid && ordinal.getInt >= 0) {
      GpuColumnVector.from(lhs.getBase.extractListElement(ordinal.getInt), lhs.dataType())
    } else {
      withResource(Scalar.fromNull(GpuColumnVector.getRapidsType(dataType))) { nullScalar =>
        GpuColumnVector.from(
          ColumnVector.fromScalar(nullScalar, lhs.getRowCount.toInt), lhs.dataType())
      }
    }
  }
}

class GpuGetMapValueMeta(
  expr: GetMapValue,
  conf: RapidsConf,
  parent: Option[RapidsMeta[_, _, _]],
  rule: ConfKeysAndIncompat)
  extends BinaryExprMeta[GetMapValue](expr, conf, parent, rule) {
  import GpuOverrides._

  override def tagExprForGpu(): Unit = {
    if (!isStringLit(expr.key)) {
      willNotWorkOnGpu("Only String literal keys are supported")
    }
  }

  override def convertToGpu(
    child: Expression,
    key: Expression): GpuExpression =
    GpuGetMapValue(child, key)

  def isSupported(t: DataType) = t match {
    case MapType(StringType, StringType, _) => true
    case StringType => true
  }

  override def areAllSupportedTypes(types: DataType*): Boolean = types.forall(isSupported)
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

  override def doColumnar(lhs: GpuColumnVector,
    rhs: Scalar): GpuColumnVector = GpuColumnVector.from(
    lhs.getBase.getMapValue(rhs), lhs.dataType())


  override def doColumnar(lhs: Scalar,
    rhs: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuColumnVector,
    rhs: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def left: Expression = child

  override def right: Expression = key
}