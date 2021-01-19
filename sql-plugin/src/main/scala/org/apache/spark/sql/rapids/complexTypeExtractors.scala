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
import com.nvidia.spark.rapids.{BinaryExprMeta, DataFromReplacementRule, GpuBinaryExpression, GpuColumnVector, GpuExpression, GpuOverrides, GpuScalar, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{DecimalPrecision, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.{findTightestCommonType, findWiderTypeForTwo}
import org.apache.spark.sql.catalyst.expressions.{Cast, ExpectsInputTypes, Expression, ExtractValue, GetArrayItem, GetMapValue, ImplicitCastInputTypes, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, ArrayData, TypeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, ArrayType, BooleanType, DataType, DecimalType, DoubleType, FractionalType, IntegralType, MapType, NullType, StructType}
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
        case null => null
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

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuColumnVector, ordinal: Scalar): ColumnVector = {
    // Need to handle negative indexes...
    if (ordinal.isValid && ordinal.getInt >= 0) {
      lhs.getBase.extractListElement(ordinal.getInt)
    } else {
      withResource(Scalar.fromNull(
        GpuColumnVector.getNonNestedRapidsType(dataType))) { nullScalar =>
        ColumnVector.fromScalar(nullScalar, lhs.getRowCount.toInt)
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: Scalar, rhs: Scalar): ColumnVector = {
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

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): ColumnVector =
    lhs.getBase.getMapValue(rhs)

  override def doColumnar(numRows: Int, lhs: Scalar, rhs: Scalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def left: Expression = child

  override def right: Expression = key
}

case class GpuArrayContains(left: Expression, right: Expression)
  extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = BooleanType

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(right.dataType)

  override def inputTypes: Seq[AbstractDataType] = {
    (left.dataType, right.dataType) match {
      case (_, NullType) => Seq.empty
      case (ArrayType(e1, hasNull), e2) =>
        findWiderTypeWithoutStringPromotionForTwo(e1, e2) match {
          case Some(dt) => Seq(ArrayType(dt, hasNull), dt)
          case _ => Seq.empty
        }
      case _ => Seq.empty
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (_, NullType) =>
        TypeCheckResult.TypeCheckFailure("Null typed values cannot be used as arguments")
      case (ArrayType(e1, _), e2) if e1.sameType(e2) =>
        TypeUtils.checkForOrderingExpr(e2, s"function $prettyName")
      case _ => TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
        s"been ${ArrayType.simpleString} followed by a value with same element type, but it's " +
        s"[${left.dataType.catalogString}, ${right.dataType.catalogString}].")
    }
  }

  override def nullable: Boolean = {
    left.nullable || right.nullable || left.dataType.asInstanceOf[ArrayType].containsNull
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): ColumnVector =
    lhs.getBase.listContains(rhs)

  override def doColumnar(numRows: Int, lhs: Scalar, rhs: Scalar): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector =
    lhs.getBase.listContainsColumn(rhs.getBase)

  override def nullSafeEval(arr: Any, value: Any): Any = {
    var hasNull = false
    arr.asInstanceOf[ArrayData].foreach(right.dataType, (i, v) =>
      if (v == null) {
        hasNull = true
      } else if (ordering.equiv(v, value)) {
        return true
      }
    )
    if (hasNull) {
      null
    } else {
      false
    }
  }

  override def prettyName: String = "array_contains"

  /**
    * Similar to [[findWiderTypeForTwo]] that can handle decimal types, but can't promote to
    * string. If the wider decimal type exceeds system limitation, this rule will truncate
    * the decimal type before return it.
    */
  private def findWiderTypeWithoutStringPromotionForTwo(
    t1: DataType,
    t2: DataType): Option[DataType] = {
    findTightestCommonType(t1, t2)
      .orElse(findWiderTypeForDecimal(t1, t2))
      .orElse(findTypeForComplex(t1, t2, findWiderTypeWithoutStringPromotionForTwo))
  }

  /**
    * Finds a wider type when one or both types are decimals. If the wider decimal type exceeds
    * system limitation, this rule will truncate the decimal type. If a decimal and other fractional
    * types are compared, returns a double type.
    */
  private def findWiderTypeForDecimal(dt1: DataType, dt2: DataType): Option[DataType] = {
    (dt1, dt2) match {
      case (t1: DecimalType, t2: DecimalType) =>
        Some(DecimalPrecision.widerDecimalType(t1, t2))
      case (t: IntegralType, d: DecimalType) =>
        Some(DecimalPrecision.widerDecimalType(DecimalType.forType(t), d))
      case (d: DecimalType, t: IntegralType) =>
        Some(DecimalPrecision.widerDecimalType(DecimalType.forType(t), d))
      case (_: FractionalType, _: DecimalType) | (_: DecimalType, _: FractionalType) =>
        Some(DoubleType)
      case _ => None
    }
  }

  private def findTypeForComplex(
    t1: DataType,
    t2: DataType,
    findTypeFunc: (DataType, DataType) => Option[DataType]): Option[DataType] = (t1, t2) match {
    case (ArrayType(et1, containsNull1), ArrayType(et2, containsNull2)) =>
      findTypeFunc(et1, et2).map { et =>
        ArrayType(et, containsNull1 || containsNull2 ||
          Cast.forceNullable(et1, et) || Cast.forceNullable(et2, et))
      }
    case (MapType(kt1, vt1, valueContainsNull1), MapType(kt2, vt2, valueContainsNull2)) =>
      findTypeFunc(kt1, kt2)
        .filter { kt => !Cast.forceNullable(kt1, kt) && !Cast.forceNullable(kt2, kt) }
        .flatMap { kt =>
          findTypeFunc(vt1, vt2).map { vt =>
            MapType(kt, vt, valueContainsNull1 || valueContainsNull2 ||
              Cast.forceNullable(vt1, vt) || Cast.forceNullable(vt2, vt))
          }
        }
    case (StructType(fields1), StructType(fields2)) if fields1.length == fields2.length =>
      val resolver = SQLConf.get.resolver
      fields1.zip(fields2).foldLeft(Option(new StructType())) {
        case (Some(struct), (field1, field2)) if resolver(field1.name, field2.name) =>
          findTypeFunc(field1.dataType, field2.dataType).map { dt =>
            struct.add(field1.name, dt, field1.nullable || field2.nullable ||
              Cast.forceNullable(field1.dataType, dt) || Cast.forceNullable(field2.dataType, dt))
          }
        case _ => None
      }
    case _ => None
  }
}