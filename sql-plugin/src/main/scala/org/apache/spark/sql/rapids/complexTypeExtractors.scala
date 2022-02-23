/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, DType, Scalar}
import com.nvidia.spark.rapids.{BinaryExprMeta, DataFromReplacementRule, GpuBinaryExpression, GpuColumnVector, GpuExpression, GpuScalar, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.BoolUtils.isAnyValidTrue
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.v2.{RapidsErrorUtils, ShimUnaryExpression}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExtractValue, GetArrayItem, GetMapValue, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, TypeUtils}
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, ArrayType, BooleanType, DataType, IntegralType, MapType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

case class GpuGetStructField(child: Expression, ordinal: Int, name: Option[String] = None)
    extends ShimUnaryExpression with GpuExpression with ExtractValue with NullIntolerant {

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

  override def convertToGpu(
      arr: Expression,
      ordinal: Expression): GpuExpression =
    // this will be called under 3.0.x version, so set failOnError to false to match CPU behavior
    GpuGetArrayItem(arr, ordinal, failOnError = false)
}

/**
 * Returns the field at `ordinal` in the Array `child`.
 *
 * We need to do type checking here as `ordinal` expression maybe unresolved.
 */
case class GpuGetArrayItem(child: Expression, ordinal: Expression, failOnError: Boolean)
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

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val (array, indices) = (lhs.getBase, rhs.getBase)
    val indicesCol = withResource(Scalar.fromInt(0)) { zeroS =>
      withResource(indices.lessThan(zeroS)) { hasNegativeIndicesCV =>
        val hasNegativeIndices = isAnyValidTrue(hasNegativeIndicesCV)
        if (failOnError) {
          // Check if any index is out of bound only when ansi mode is enabled.
          // No exception should be raised if no valid entry (An entry is valid when both
          // the array row and its index are not null), the same with what Spark does.
          if(hasNegativeIndices && array.getNullCount != array.getRowCount) {
            // fast fail
            throw new ArrayIndexOutOfBoundsException("Invalid index: some indices are < 0")
          }
          val hasLargerIndicesCV = withResource(array.countElements()) { numElements =>
            indices.greaterOrEqualTo(numElements)
          }
          withResource(hasLargerIndicesCV) { _ =>
            if(isAnyValidTrue(hasLargerIndicesCV)) {
              // No need to check the validity of array column here, since the validity info
              // is included in this `hasLargerIndicesCV`.
              throw new ArrayIndexOutOfBoundsException(
                "Invalid index: some indices are larger than the element number")
            }
          }
        } // end of "if (failOnError)"

        if (hasNegativeIndices) {
          // Replace negative indices with nulls, then cuDF will return nulls, the same
          // with what Spark returns for negative indices.
          withResource(Scalar.fromNull(DType.INT32)) { nullS =>
            hasNegativeIndicesCV.ifElse(nullS, indices)
          }
        } else {
          indices.incRefCount()
        }
      }
    }

    withResource(indicesCol) { _ =>
      array.extractListElement(indicesCol)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector =
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt, lhs.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }

  override def doColumnar(lhs: GpuColumnVector, ordinalS: GpuScalar): ColumnVector = {
    if (!ordinalS.isValid || lhs.getRowCount == lhs.numNulls()) {
      // Return nulls when index is null or all the array rows are null,
      // the same with what Spark does.
      GpuColumnVector.columnVectorFromNull(lhs.getRowCount.toInt, dataType)
    } else {
      // The index is valid, and array column contains at least one non-null row.
      val ordinal = ordinalS.getValue.asInstanceOf[Int]
      if (failOnError) {
        // Check if index is out of bound only when ansi mode is enabled, since cuDF
        // returns nulls if index is out of bound, the same with what Spark does when
        // ansi mode is disabled.
        withResource(lhs.getBase.countElements()) { numElementsCV =>
          withResource(numElementsCV.min) { minScalar =>
            val minNumElements = minScalar.getInt
            if (ordinal < 0 || ordinal >= minNumElements) {
              RapidsErrorUtils.throwArrayIndexOutOfBoundsException(ordinal, minNumElements)
            }
          }
        }
      } else if (ordinal < 0) {
        // Only take care of this case since cuDF treats negative index as offset to the
        // end of array, instead of invalid index.
        return GpuColumnVector.columnVectorFromNull(lhs.getRowCount.toInt, dataType)
      }
      lhs.getBase.extractListElement(ordinal)
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

  override def convertToGpu(child: Expression, key: Expression): GpuExpression = {
    // this will be called under 3.0.x version, so set failOnError to false to match CPU behavior
    GpuGetMapValue(child, key, failOnError = false)
  }
}

case class GpuGetMapValue(child: Expression, key: Expression, failOnError: Boolean)
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

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    if (failOnError){
      withResource(lhs.getBase.getMapKeyExistence(rhs.getBase)) { keyExistenceColumn =>
        withResource(keyExistenceColumn.all) { exist =>
          if (exist.isValid && !exist.getBoolean) {
            RapidsErrorUtils.throwInvalidElementAtIndexError(
              rhs.getValue.asInstanceOf[UTF8String].toString)
          }
        }
      }
    }
    lhs.getBase.getMapValue(rhs.getBase)
  }

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

  /**
   * Helper function to account for `libcudf`'s `listContains()` semantics.
   * 
   * If a list row contains at least one null element, and is found not to contain 
   * the search key, `libcudf` returns false instead of null.  SparkSQL expects to 
   * return null in those cases.
   * 
   * This method determines the result's validity mask by ORing the output of 
   * `listContains()` with the NOT of `listContainsNulls()`.
   * A result row is thus valid if either the search key is found in the list, 
   * or if the list does not contain any null elements.
   */
  private def orNotContainsNull(containsResult: ColumnVector, 
                                inputListsColumn:ColumnVector): ColumnVector = {
    val notContainsNull = withResource(inputListsColumn.listContainsNulls) {
      _.not
    }
    val containsKeyOrNotContainsNull = withResource(notContainsNull) {
      containsResult.or(_)
    }
    withResource(containsKeyOrNotContainsNull) {
      containsResult.copyWithBooleanColumnAsValidity(_)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val inputListsColumn = lhs.getBase
    withResource(inputListsColumn.listContains(rhs.getBase)) {
      orNotContainsNull(_, inputListsColumn)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val inputListsColumn = lhs.getBase
    withResource(inputListsColumn.listContainsColumn(rhs.getBase)) { 
      orNotContainsNull(_, inputListsColumn)
    }
  }

  override def prettyName: String = "array_contains"
}
