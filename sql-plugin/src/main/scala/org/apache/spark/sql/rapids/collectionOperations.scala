/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf
import ai.rapids.cudf.{BinaryOperable, ColumnVector, ColumnView, GroupByAggregation, GroupByOptions, Scalar}
import com.nvidia.spark.rapids.{DataFromReplacementRule, ExprMeta, GpuBinaryExpression, GpuColumnVector, GpuComplexTypeMergingExpression, GpuExpression, GpuLiteral, GpuMapUtils, GpuScalar, GpuTernaryExpression, GpuUnaryExpression, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.GpuExpressionsUtils.columnarEvalToColumn
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ImplicitCastInputTypes, RowOrdering, Sequence, TimeZoneAwareExpression}
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
    withResource(ArrayBuffer.empty[cudf.ColumnVector]) { buffer =>
      // build input buffer
      children.foreach {
        buffer += columnarEvalToColumn(_, batch).getBase
      }
      // run string concatenate
      GpuColumnVector.from(
        cudf.ColumnVector.stringConcatenate(buffer.toArray[ColumnView]), StringType)
    }
  }

  private def listConcat(batch: ColumnarBatch): GpuColumnVector = {
    withResource(ArrayBuffer[cudf.ColumnVector]()) { buffer =>
      // build input buffer
      children.foreach {
        buffer += columnarEvalToColumn(_, batch).getBase
      }
      // run list concatenate
      GpuColumnVector.from(cudf.ColumnVector.listConcatenateByRow(buffer: _*), dataType)
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
      case (arr: ArrayType, e2: IntegralType) if e2 != LongType =>
        Seq(arr, IntegerType)
      case (MapType(keyType, valueType, hasNull), e2) =>
        TypeCoercion.findTightestCommonType(keyType, e2) match {
          case Some(dt) => Seq(MapType(dt, valueType, hasNull), dt)
          case _ => Seq.empty
        }
      case _ => Seq.empty
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (_: ArrayType, e2) if e2 != IntegerType =>
        TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
          s"been ${ArrayType.simpleString} followed by a ${IntegerType.simpleString}, but it's " +
          s"[${left.dataType.catalogString}, ${right.dataType.catalogString}].")
      case (MapType(e1, _, _), e2) if !e2.sameType(e1) =>
        TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
          s"been ${MapType.simpleString} followed by a value of same key type, but it's " +
          s"[${left.dataType.catalogString}, ${right.dataType.catalogString}].")
      case (e1, _) if !e1.isInstanceOf[MapType] && !e1.isInstanceOf[ArrayType] =>
        TypeCheckResult.TypeCheckFailure(s"The first argument to function $prettyName should " +
          s"have been ${ArrayType.simpleString} or ${MapType.simpleString} type, but its " +
          s"${left.dataType.catalogString} type.")
      case _ => TypeCheckResult.TypeCheckSuccess
    }
  }

  // Eventually we need something more full featured like
  // GetArrayItemUtil.computeNullabilityFromArray
  override def nullable: Boolean = true

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): cudf.ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): cudf.ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): cudf.ColumnVector = {
    lhs.dataType match {
      case _: ArrayType =>
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
      case _: MapType =>
        if (failOnError) {
          withResource(lhs.getBase.getMapKeyExistence(rhs.getBase)){ keyExistenceColumn =>
            withResource(keyExistenceColumn.all()) { exist =>
              if (!exist.isValid || exist.getBoolean) {
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

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): cudf.ColumnVector =
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

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {

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

case class GpuMapKeys(child: Expression)
    extends GpuUnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = ArrayType(child.dataType.asInstanceOf[MapType].keyType)

  override def prettyName: String = "map_keys"

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    withResource(GpuMapUtils.getKeysAsListView(input.getBase)) { retView =>
      retView.copyToColumnVector()
    }
  }
}

case class GpuMapValues(child: Expression)
    extends GpuUnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = {
    val mt = child.dataType.asInstanceOf[MapType]
    ArrayType(mt.valueType, containsNull = mt.valueContainsNull)
  }

  override def prettyName: String = "map_values"

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    withResource(GpuMapUtils.getValuesAsListView(input.getBase)) { retView =>
      retView.copyToColumnVector()
    }
  }
}

case class GpuMapEntries(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  @transient private lazy val childDataType: MapType = child.dataType.asInstanceOf[MapType]

  override def dataType: DataType = {
    ArrayType(
      StructType(
        StructField("key", childDataType.keyType, false) ::
            StructField("value", childDataType.valueType, childDataType.valueContainsNull) ::
            Nil),
      false)
  }

  override def prettyName: String = "map_entries"

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    // Internally the format for a list of key/value structs is the same, so just
    // return the same thing, and let Spark think it is a different type.
    input.getBase.incRefCount()
  }
}

case class GpuSortArray(base: Expression, ascendingOrder: Expression)
    extends GpuBinaryExpression with ExpectsInputTypes {

  override def left: Expression = base

  override def right: Expression = ascendingOrder

  override def dataType: DataType = base.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = base.dataType match {
    case ArrayType(dt, _) if RowOrdering.isOrderable(dt) =>
      ascendingOrder match {
        // replace Literal with GpuLiteral here
        case GpuLiteral(_: Boolean, BooleanType) =>
          TypeCheckResult.TypeCheckSuccess
        case order =>
          TypeCheckResult.TypeCheckFailure(
            s"Sort order in second argument requires a boolean literal, but found $order")
      }
    case ArrayType(dt, _) =>
      val dtSimple = dt.catalogString
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName does not support sorting array of type $dtSimple which is not orderable")
    case dt =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input, but found $dt")
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): cudf.ColumnVector =
    throw new IllegalArgumentException("lhs has to be a vector and rhs has to be a scalar for " +
        "the sort_array operator to work")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): cudf.ColumnVector =
    throw new IllegalArgumentException("lhs has to be a vector and rhs has to be a scalar for " +
        "the sort_array operator to work")

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): cudf.ColumnVector = {
    val isDescending = isDescendingOrder(rhs)
    lhs.getBase.listSortRows(isDescending, true)
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): cudf.ColumnVector = {
    val isDescending = isDescendingOrder(rhs)
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { cv =>
      cv.getBase.listSortRows(isDescending, true)
    }
  }

  private def isDescendingOrder(scalar: GpuScalar): Boolean = scalar.getValue match {
    case ascending: Boolean => !ascending
    case invalidValue => throw new IllegalArgumentException(s"invalid value $invalidValue")
  }
}

trait GpuBaseArrayAgg extends GpuUnaryExpression {

  protected def agg: GroupByAggregation

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    val baseInput = input.getBase
    // TODO switch over to array aggregations once
    //  https://github.com/rapidsai/cudf/issues/9135 is done
    val inputTab = withResource(Scalar.fromInt(0)) { zero =>
      withResource(cudf.ColumnVector.sequence(zero, input.getRowCount.toInt)) { rowNums =>
        new cudf.Table(rowNums, baseInput)
      }
    }

    val explodedTab = withResource(inputTab) { inputTab =>
      inputTab.explodeOuter(1)
    }

    val retTab = withResource(explodedTab) { explodedTab =>
      explodedTab.groupBy(GroupByOptions.builder()
          .withKeysSorted(true)
          .withIgnoreNullKeys(true)
          .build(), 0)
          .aggregate(agg.onColumn(1))
    }

    withResource(retTab) { retTab =>
      assert(retTab.getRowCount == baseInput.getRowCount)
      retTab.getColumn(1).incRefCount()
    }
  }
}

case class GpuArrayMin(child: Expression) extends GpuBaseArrayAgg with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  @transient override lazy val dataType: DataType = child.dataType match {
    case ArrayType(dt, _) => dt
    case _ => throw new IllegalStateException(s"$prettyName accepts only arrays.")
  }

  override def prettyName: String = "array_min"

  override protected def agg: GroupByAggregation = GroupByAggregation.min()
}

case class GpuArrayMax(child: Expression) extends GpuBaseArrayAgg with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  @transient override lazy val dataType: DataType = child.dataType match {
    case ArrayType(dt, _) => dt
    case _ => throw new IllegalStateException(s"$prettyName accepts only arrays.")
  }

  override def prettyName: String = "array_max"

  override protected def agg: GroupByAggregation = GroupByAggregation.max()
}

class GpuSequenceMeta(
    expr: Sequence,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[Sequence](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    //  We have to fall back to the CPU if the timeZoneId is not UTC when
    //  we are processing date/timestamp.
    //  Date/Timestamp are not enabled right now so this is probably fine.
  }

  override def convertToGpu(): GpuExpression = {
    if (expr.stepOpt.isDefined) {
      val Seq(start, stop, step) = childExprs.map(_.convertToGpu())
      GpuSequenceWithStep(start, stop, step, expr.timeZoneId)
    } else {
      val Seq(start, stop) = childExprs.map(_.convertToGpu())
      GpuSequence(start, stop, expr.timeZoneId)
    }
  }
}

object GpuSequenceUtil {

  def numberScalar(dt: DataType, value: Int): Scalar = dt match {
    case ByteType => Scalar.fromByte(value.toByte)
    case ShortType => Scalar.fromShort(value.toShort)
    case IntegerType => Scalar.fromInt(value)
    case LongType => Scalar.fromLong(value.toLong)
    case _ =>
      throw new IllegalArgumentException("wrong data type: " + dt)
  }
}

/** GpuSequence without step */
case class GpuSequence(start: Expression, stop: Expression, timeZoneId: Option[String] = None)
    extends GpuBinaryExpression with TimeZoneAwareExpression {

  override def left: Expression = start

  override def right: Expression = stop

  override def dataType: DataType = ArrayType(start.dataType, containsNull = false)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Some(timeZoneId))

  /**
   * Calculate the size and step (1 or -1) between start and stop both inclusive
   * size = |stop - start| + 1
   * step = 1 if stop >= start else -1
   * @param start first values in the result sequences
   * @param stop end values in the result sequences
   * @return (size, step)
   */
  private def calculateSizeAndStep(start: BinaryOperable, stop: BinaryOperable, dt: DataType):
      Seq[ColumnVector] = {
    withResource(stop.sub(start)) { difference =>
      withResource(GpuSequenceUtil.numberScalar(dt, 1)) { one =>
        val step = withResource(GpuSequenceUtil.numberScalar(dt, -1)) { negativeOne =>
          withResource(GpuSequenceUtil.numberScalar(dt, 0)) { zero =>
            withResource(difference.greaterOrEqualTo(zero)) { pred =>
              pred.ifElse(one, negativeOne)
            }
          }
        }
        val size = closeOnExcept(step) { _ =>
          withResource(difference.abs()) { absDifference =>
            absDifference.add(one)
          }
        }
        Seq(size, step)
      }
    }
  }

  override def doColumnar(start: GpuColumnVector, stop: GpuColumnVector): ColumnVector = {
    withResource(calculateSizeAndStep(start.getBase, stop.getBase, start.dataType())) { ret =>
      ColumnVector.sequence(start.getBase, ret(0), ret(1))
    }
  }

  override def doColumnar(start: GpuScalar, stop: GpuColumnVector): ColumnVector = {
    withResource(calculateSizeAndStep(start.getBase, stop.getBase, stop.dataType())) { ret =>
      withResource(ColumnVector.fromScalar(start.getBase, stop.getRowCount.toInt)) { startV =>
        ColumnVector.sequence(startV, ret(0), ret(1))
      }
    }
  }

  override def doColumnar(start: GpuColumnVector, stop: GpuScalar): ColumnVector = {
    withResource(calculateSizeAndStep(start.getBase, stop.getBase, start.dataType())) { ret =>
      ColumnVector.sequence(start.getBase, ret(0), ret(1))
    }
  }

  override def doColumnar(numRows: Int, start: GpuScalar, stop: GpuScalar): ColumnVector = {
    val startV = GpuColumnVector.from(ColumnVector.fromScalar(start.getBase, numRows),
      start.dataType)
    doColumnar(startV, stop)
  }
}

/** GpuSequence with step */
case class GpuSequenceWithStep(start: Expression, stop: Expression, step: Expression,
    timeZoneId: Option[String] = None) extends GpuTernaryExpression with TimeZoneAwareExpression {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Some(timeZoneId))

  override def first: Expression = start

  override def second: Expression = stop

  override def third: Expression = step

  override def dataType: DataType = ArrayType(start.dataType, containsNull = false)

  private def calculateSize(
      start: BinaryOperable,
      stop: BinaryOperable,
      step: BinaryOperable,
      rows: Int,
      dt: DataType): ColumnVector = {
    // First, calculate sizeWithNegative=floor((stop-start)/step)+1.
    //     if step = 0, the div operation in cudf will get MIN_VALUE, which is ok for this case,
    //     since when size < 0, cudf will not generate sequence
    // Second, calculate size = if(sizeWithNegative < 0) 0 else sizeWithNegative
    // Third, if (start == stop && step == 0), let size = 1.
    withResource(GpuSequenceUtil.numberScalar(dt, 1)) { one =>
      withResource(GpuSequenceUtil.numberScalar(dt, 0)) { zero =>

        val (sizeWithNegative, diffHasZero) = withResource(stop.sub(start)) { difference =>
          // sizeWithNegative=floor((stop-start)/step)+1
          val sizeWithNegative = withResource(difference.floorDiv(step)) { quotient =>
            quotient.add(one)
          }
          val tmpDiffHasZero = closeOnExcept(sizeWithNegative) { _ =>
            difference.equalTo(zero)
          }
          (sizeWithNegative, tmpDiffHasZero)
        }

        val tmpSize = closeOnExcept(diffHasZero) { _ =>
          // tmpSize = if(sizeWithNegative < 0) 0 else sizeWithNegative
          withResource(sizeWithNegative) { _ =>
            withResource(sizeWithNegative.greaterOrEqualTo(zero)) { pred =>
              pred.ifElse(sizeWithNegative, zero)
            }
          }
        }

        // when start==stop && step==0, size will be 0.
        // but we should change size to 1
        withResource(tmpSize) { tmpSize =>
          withResource(diffHasZero) { diffHasZero =>
            step match {
              case stepScalar: Scalar =>
                withResource(ColumnVector.fromScalar(stepScalar, rows)) { stepV =>
                  withResource(stepV.equalTo(zero)) { stepHasZero =>
                    withResource(diffHasZero.and(stepHasZero)) { predWithZero =>
                      predWithZero.ifElse(one, tmpSize)
                    }
                  }
                }
              case _ =>
                withResource(step.equalTo(zero)) { stepHasZero =>
                  withResource(diffHasZero.and(stepHasZero)) { predWithZero =>
                    predWithZero.ifElse(one, tmpSize)
                  }
                }
            }
          }
        }
      }
    }
  }

  override def doColumnar(
      start: GpuColumnVector,
      stop: GpuColumnVector,
      step: GpuColumnVector): ColumnVector = {
    withResource(calculateSize(start.getBase, stop.getBase, step.getBase, start.getRowCount.toInt,
        start.dataType())) { size =>
      ColumnVector.sequence(start.getBase, size, step.getBase)
    }
  }

  override def doColumnar(
      start: GpuScalar,
      stop: GpuColumnVector,
      step: GpuColumnVector): ColumnVector = {
    withResource(calculateSize(start.getBase, stop.getBase, step.getBase, stop.getRowCount.toInt,
        start.dataType)) { size =>
      withResource(ColumnVector.fromScalar(start.getBase, stop.getRowCount.toInt)) { startV =>
        ColumnVector.sequence(startV, size, step.getBase)
      }
    }
  }

  override def doColumnar(
      start: GpuScalar,
      stop: GpuScalar,
      step: GpuColumnVector): ColumnVector = {
    withResource(ColumnVector.fromScalar(start.getBase, step.getRowCount.toInt)) { startV =>
      withResource(calculateSize(startV, stop.getBase, step.getBase, step.getRowCount.toInt,
          start.dataType)) { size =>
        ColumnVector.sequence(startV, size, step.getBase)
      }
    }
  }

  override def doColumnar(
      start: GpuScalar,
      stop: GpuColumnVector,
      step: GpuScalar): ColumnVector = {
    withResource(calculateSize(start.getBase, stop.getBase, step.getBase, stop.getRowCount.toInt,
        start.dataType)) { size =>
      withResource(ColumnVector.fromScalar(start.getBase, stop.getRowCount.toInt)) { startV =>
        withResource(ColumnVector.fromScalar(step.getBase, stop.getRowCount.toInt)) { stepV =>
          ColumnVector.sequence(startV, size, stepV)
        }
      }
    }
  }

  override def doColumnar(
      start: GpuColumnVector,
      stop: GpuScalar,
      step: GpuColumnVector): ColumnVector = {
    withResource(calculateSize(start.getBase, stop.getBase, step.getBase, start.getRowCount.toInt,
        start.dataType())) { size =>
      ColumnVector.sequence(start.getBase, size, step.getBase)
    }
  }

  override def doColumnar(
      start: GpuColumnVector,
      stop: GpuScalar,
      step: GpuScalar): ColumnVector = {
    withResource(calculateSize(start.getBase, stop.getBase, step.getBase, start.getRowCount.toInt,
        start.dataType())) { size =>
      withResource(ColumnVector.fromScalar(step.getBase, start.getRowCount.toInt)) { stepV =>
        ColumnVector.sequence(start.getBase, size, stepV)
      }
    }
  }

  override def doColumnar(
      start: GpuColumnVector,
      stop: GpuColumnVector,
      step: GpuScalar): ColumnVector =
    withResource(calculateSize(start.getBase, stop.getBase, step.getBase, start.getRowCount.toInt,
        start.dataType())) { size =>
      withResource(ColumnVector.fromScalar(step.getBase, start.getRowCount.toInt)) { stepV =>
        ColumnVector.sequence(start.getBase, size, stepV)
      }
    }

  override def doColumnar(
      numRows: Int,
      start: GpuScalar,
      stop: GpuScalar,
      step: GpuScalar): ColumnVector = {
    val startV = GpuColumnVector.from(ColumnVector.fromScalar(start.getBase, numRows),
      start.dataType)
    doColumnar(startV, stop, step)
  }
}
