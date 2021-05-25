/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, ColumnView, Scalar}
import com.nvidia.spark.rapids.{GpuBinaryExpression, GpuColumnVector, GpuComplexTypeMergingExpression, GpuExpressionsUtils, GpuScalar, GpuUnaryExpression}
import com.nvidia.spark.rapids.GpuExpressionsUtils.columnarEvalToColumn
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

case class GpuConcat(children: Seq[Expression]) extends GpuComplexTypeMergingExpression {

  @transient override lazy val dataType: DataType = {
    if (children.isEmpty) {
      StringType
    } else {
      super.dataType
    }
  }

  override def nullable: Boolean = children.exists(_.nullable)

  override def columnarEval(batch: ColumnarBatch): Any = dataType match {
    // Explicitly return null for empty concat as Spark, since cuDF doesn't support empty concat.
    case dt if children.isEmpty => GpuScalar.from(null, dt)
    // For single column concat, we pass the result of child node to avoid extra cuDF call.
    case _ if children.length == 1 => children.head.columnarEval(batch)
    case StringType => stringConcat(batch)
    case ArrayType(_, _) => listConcat(batch)
    case _ => throw new IllegalArgumentException(s"unsupported dataType $dataType")
  }

  private def stringConcat(batch: ColumnarBatch): GpuColumnVector = {
    withResource(ArrayBuffer.empty[ColumnVector]) { buffer =>
      // build input buffer
      children.foreach {
        buffer += columnarEvalToColumn(_, batch).getBase
      }
      // run string concatenate
      GpuColumnVector.from(
        ColumnVector.stringConcatenate(buffer.toArray[ColumnView]), StringType)
    }
  }

  private def listConcat(batch: ColumnarBatch): GpuColumnVector = {
    withResource(ArrayBuffer[ColumnVector]()) { buffer =>
      // build input buffer
      children.foreach {
        buffer += columnarEvalToColumn(_, batch).getBase
      }
      // run list concatenate
      GpuColumnVector.from(ColumnVector.listConcatenateByRow(buffer: _*), dataType)
    }
  }
}

case class GpuElementAt(left: Expression, right: Expression, failOnError: Boolean)
  extends GpuBinaryExpression with ExpectsInputTypes {

  override lazy val dataType: DataType = left.dataType match {
    case ArrayType(elementType, _) => elementType
    case MapType(_, valueType, _) => valueType
  }

  override def inputTypes: Seq[AbstractDataType] = {
    (left.dataType, right.dataType) match {
      case (arr: ArrayType, e2: IntegralType) if (e2 != LongType) =>
        Seq(arr, IntegerType)
      case (MapType(keyType, valueType, hasNull), e2) =>
        TypeCoercion.findTightestCommonType(keyType, e2) match {
          case Some(dt) => Seq(MapType(dt, valueType, hasNull), dt)
          case _ => Seq.empty
        }
      case (l, r) => Seq.empty
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (_: ArrayType, e2) if e2 != IntegerType =>
        TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
          s"been ${ArrayType.simpleString} followed by a ${IntegerType.simpleString}, but it's " +
          s"[${left.dataType.catalogString}, ${right.dataType.catalogString}].")
      case (MapType(e1, _, _), e2) if (!e2.sameType(e1)) =>
        TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
          s"been ${MapType.simpleString} followed by a value of same key type, but it's " +
          s"[${left.dataType.catalogString}, ${right.dataType.catalogString}].")
      case (e1, _) if (!e1.isInstanceOf[MapType] && !e1.isInstanceOf[ArrayType]) =>
        TypeCheckResult.TypeCheckFailure(s"The first argument to function $prettyName should " +
          s"have been ${ArrayType.simpleString} or ${MapType.simpleString} type, but its " +
          s"${left.dataType.catalogString} type.")
      case _ => TypeCheckResult.TypeCheckSuccess
    }
  }

  // Eventually we need something more full featured like
  // GetArrayItemUtil.computeNullabilityFromArray
  override def nullable: Boolean = true

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    lhs.dataType match {
      case _: ArrayType => {
        if (rhs.isValid) {
          if (rhs.getValue.asInstanceOf[Int] == 0) {
            throw new ArrayIndexOutOfBoundsException("SQL array indices start at 1")
          } else  {
            // Positive or negative index
            val ordinalValue = rhs.getValue.asInstanceOf[Int]
            withResource(lhs.getBase.countElements) { numElementsCV =>
              withResource(numElementsCV.min) { minScalar =>
                val minNumElements = minScalar.getInt
                // index out of bound
                // Note: when the column is containing all null arrays, CPU will not throw, so make
                // GPU to behave the same.
                if (failOnError &&
                  minNumElements < math.abs(ordinalValue) &&
                  lhs.getBase.getNullCount != lhs.getBase.getRowCount) {
                  throw new ArrayIndexOutOfBoundsException(
                    s"Invalid index: $ordinalValue, minimum numElements in this ColumnVector: " +
                      s"$minNumElements")
                } else {
                  if (ordinalValue > 0) {
                    // Positive index
                    lhs.getBase.extractListElement(ordinalValue - 1)
                  } else {
                    // Negative index
                    lhs.getBase.extractListElement(ordinalValue)
                  }
                }
              }
            }
          }
        } else {
          GpuColumnVector.columnVectorFromNull(lhs.getRowCount.toInt, dataType)
        }
      }
      case _: MapType => {
        if (failOnError) {
          withResource(lhs.getBase.getMapKeyExistence(rhs.getBase)){ keyExistenceColumn =>
            withResource(keyExistenceColumn.all()) { exist =>
              if (exist.getBoolean) {
                lhs.getBase.getMapValue(rhs.getBase)
              } else {
                throw new NoSuchElementException(
                  s"Key: ${rhs.getValue.asInstanceOf[UTF8String].toString} " +
                    s"does not exist in one of the rows in the map column")
              }
            }
          }
        } else {
          lhs.getBase.getMapValue(rhs.getBase)
        }
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector =
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }

  override def prettyName: String = "element_at"
}

case class GpuSize(child: Expression, legacySizeOfNull: Boolean)
    extends GpuUnaryExpression {

  require(child.dataType.isInstanceOf[ArrayType] || child.dataType.isInstanceOf[MapType],
    s"The size function doesn't support the operand type ${child.dataType}")

  override def dataType: DataType = IntegerType
  override def nullable: Boolean = if (legacySizeOfNull) false else super.nullable

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {

    // Compute sizes of cuDF.ListType to get sizes of each ArrayData or MapData, considering
    // MapData is represented as List of Struct in terms of cuDF.
    withResource(input.getBase.countElements()) { collectionSize =>
      if (legacySizeOfNull) {
        withResource(Scalar.fromInt(-1)) { nullScalar =>
          withResource(input.getBase.isNull) { inputIsNull =>
            inputIsNull.ifElse(nullScalar, collectionSize)
          }
        }
      } else {
        collectionSize.incRefCount()
      }
    }
  }
}
