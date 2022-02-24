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

import java.util.Optional

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf
import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType, GroupByAggregation, GroupByOptions, Scalar, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.BoolUtils.isAllValidTrue
import com.nvidia.spark.rapids.GpuExpressionsUtils.columnarEvalToColumn
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.{RapidsErrorUtils, ShimExpression}

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ImplicitCastInputTypes, RowOrdering, Sequence, TimeZoneAwareExpression}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH
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
                  RapidsErrorUtils.throwArrayIndexOutOfBoundsException(ordinalValue, minNumElements)
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
                RapidsErrorUtils.throwInvalidElementAtIndexError(
                  rhs.getValue.asInstanceOf[UTF8String].toString, true)
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
    val (startExpr, stopExpr, stepOpt) = if (expr.stepOpt.isDefined) {
        val Seq(start, stop, step) = childExprs.map(_.convertToGpu())
        (start, stop, Some(step))
      } else {
        val Seq(start, stop) = childExprs.map(_.convertToGpu())
        (start, stop, None)
      }
    GpuSequence(startExpr, stopExpr, stepOpt, expr.timeZoneId)
  }
}

object GpuSequenceUtil extends Arm {

  private def checkSequenceInputs(
      start: ColumnVector,
      stop: ColumnVector,
      step: ColumnVector): Unit = {
    // Keep the same requirement with Spark:
    // (step > 0 && start <= stop) || (step < 0 && start >= stop) || (step == 0 && start == stop)
    withResource(Scalar.fromByte(0.toByte)) { zero =>
      // The check should ignore each row (Row(start, stop, step)) that contains at least
      // one null element according to Spark's code. Thanks to the cudf binary ops, who ignore
      // nulls already, skipping nulls can be done without any additional process.
      //
      // Because the filtered table (e.g. upTbl) in each rule check excludes the rows that the
      // step is null. Next a null row will be produced when comparing start or stop when any
      // of them is null, and the nulls are skipped in the final assertion 'isAllValidTrue'.
      withResource(new Table(start, stop)) { startStopTable =>
        // (step > 0 && start <= stop)
        val upTbl = withResource(step.greaterThan(zero)) { positiveStep =>
          startStopTable.filter(positiveStep)
        }
        val allUp = withResource(upTbl) { _ =>
          upTbl.getColumn(0).lessOrEqualTo(upTbl.getColumn(1))
        }
        withResource(allUp) { _ =>
          require(isAllValidTrue(allUp), "Illegal sequence boundaries: step > 0 but start > stop")
        }

        // (step < 0 && start >= stop)
        val downTbl = withResource(step.lessThan(zero)) { negativeStep =>
          startStopTable.filter(negativeStep)
        }
        val allDown = withResource(downTbl) { _ =>
          downTbl.getColumn(0).greaterOrEqualTo(downTbl.getColumn(1))
        }
        withResource(allDown) { _ =>
          require(isAllValidTrue(allDown),
            "Illegal sequence boundaries: step < 0 but start < stop")
        }

        // (step == 0 && start == stop)
        val equalTbl = withResource(step.equalTo(zero)) { zeroStep =>
          startStopTable.filter(zeroStep)
        }
        val allEq = withResource(equalTbl) { _ =>
          equalTbl.getColumn(0).equalTo(equalTbl.getColumn(1))
        }
        withResource(allEq) { _ =>
          require(isAllValidTrue(allEq),
            "Illegal sequence boundaries: step == 0 but start != stop")
        }
      }
    } // end of zero
  }

  /**
   * Compute the size of each sequence according to 'start', 'stop' and 'step'.
   * A row (Row[start, stop, step]) contains at least one null element will produce
   * a null in the output.
   *
   * The returned column should be closed.
   */
  def computeSequenceSizes(
      start: ColumnVector,
      stop: ColumnVector,
      step: ColumnVector): ColumnVector = {
    checkSequenceInputs(start, stop, step)

    // Spark's algorithm to get the length (aka size)
    // ``` Scala
    //   size = if (start == stop) 1L else 1L + (stop.toLong - start.toLong) / estimatedStep.toLong
    //   require(size <= MAX_ROUNDED_ARRAY_LENGTH,
    //       s"Too long sequence: $len. Should be <= $MAX_ROUNDED_ARRAY_LENGTH")
    //   size.toInt
    // ```
    val sizeAsLong = withResource(Scalar.fromLong(1L)) { one =>
      val diff = withResource(stop.castTo(DType.INT64)) { stopAsLong =>
        withResource(start.castTo(DType.INT64)) { startAsLong =>
          stopAsLong.sub(startAsLong)
        }
      }
      val quotient = withResource(diff) { _ =>
        withResource(step.castTo(DType.INT64)) { stepAsLong =>
          diff.div(stepAsLong)
        }
      }
      // actualSize = 1L + (stop.toLong - start.toLong) / estimatedStep.toLong
      val actualSize = withResource(quotient) { quotient =>
        quotient.add(one, DType.INT64)
      }
      withResource(actualSize) { _ =>
        val mergedEquals = withResource(start.equalTo(stop)) { equals =>
          if (step.hasNulls) {
            // Also set the row to null where step is null.
            equals.mergeAndSetValidity(BinaryOp.BITWISE_AND, equals, step)
          } else {
            equals.incRefCount()
          }
        }
        withResource(mergedEquals) { _ =>
          mergedEquals.ifElse(one, actualSize)
        }
      }
    }

    withResource(sizeAsLong) { _ =>
      // check max size
      withResource(Scalar.fromInt(MAX_ROUNDED_ARRAY_LENGTH)) { maxLen =>
        withResource(sizeAsLong.lessOrEqualTo(maxLen)) { allValid =>
          require(isAllValidTrue(allValid),
              s"Too long sequence found. Should be <= $MAX_ROUNDED_ARRAY_LENGTH")
        }
      }
      // cast to int and return
      sizeAsLong.castTo(DType.INT32)
    }
  }

}

case class GpuSequence(start: Expression, stop: Expression, stepOpt: Option[Expression],
    timeZoneId: Option[String] = None) extends TimeZoneAwareExpression with GpuExpression
    with ShimExpression {

  import GpuSequenceUtil._

  override def dataType: ArrayType = ArrayType(start.dataType, containsNull = false)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Some(timeZoneId))

  override def children: Seq[Expression] = Seq(start, stop) ++ stepOpt

  override def nullable: Boolean = children.exists(_.nullable)

  override def foldable: Boolean = children.forall(_.foldable)

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResource(columnarEvalToColumn(start, batch)) { startGpuCol =>
      withResource(stepOpt.map(columnarEvalToColumn(_, batch))) { stepGpuColOpt =>
        val startCol = startGpuCol.getBase

        // 1 Compute the sequence size for each row.
        val (sizeCol, stepCol) = withResource(columnarEvalToColumn(stop, batch)) { stopGpuCol =>
          val stopCol = stopGpuCol.getBase
          val steps = stepGpuColOpt.map(_.getBase.incRefCount())
              .getOrElse(defaultStepsFunc(startCol, stopCol))
          closeOnExcept(steps) { _ =>
            (computeSequenceSizes(startCol, stopCol, steps), steps)
          }
        }

        // 2 Generate the sequence
        //
        // cudf 'sequence' requires 'step' has the same type with 'start'.
        // And the step type may differ due to the default steps.
        val castedStepCol = withResource(stepCol) { _ =>
          closeOnExcept(sizeCol) { _ =>
            stepCol.castTo(startCol.getType)
          }
        }
        withResource(Seq(sizeCol, castedStepCol)) { _ =>
          GpuColumnVector.from(genSequence(startCol, sizeCol, castedStepCol), dataType)
        }
      }
    }
  }

  @transient
  private lazy val defaultStepsFunc: (ColumnView, ColumnView) => ColumnVector =
      dataType.elementType match {
    case _: IntegralType =>
      // Default step:
      //   start > stop, step == -1
      //   start <= stop, step == 1
      (starts, stops) => {
        // It is ok to always use byte, since it will be casted to the same type before
        // going into cudf sequence. Besides byte saves memory, and does not cause any
        // type promotion during computation.
        withResource(Scalar.fromByte((-1).toByte)) { minusOne =>
          withResource(Scalar.fromByte(1.toByte)) { one =>
            withResource(starts.greaterThan(stops)) { decrease =>
              decrease.ifElse(minusOne, one)
            }
          }
        }
      }
    // Timestamp and Date will come soon
    // case TimestampType =>
    // case DateType =>
  }

  private def genSequence(
      start: ColumnView,
      size: ColumnView,
      step: ColumnView): ColumnVector = {
    // size is calculated from start, stop and step, so its validity mask is equal to
    // the merged validity of the three columns, and can be used as the final output
    // validity mask directly.
    // Then checking nulls only in size column is enough.
    if(size.getNullCount > 0) {
      // Nulls are not acceptable in cudf 'list::sequences'. (Pls refer to
      //     https://github.com/rapidsai/cudf/issues/10012),
      //
      // So replace the nulls with 0 for size, and create temp views for start and
      // stop with forcing null count to be 0.
      val sizeNoNull = withResource(Scalar.fromInt(0)) { zero =>
        size.replaceNulls(zero)
      }
      val ret = withResource(sizeNoNull) { _ =>
        val startNoNull = new ColumnView(start.getType, start.getRowCount, Optional.of(0L),
          start.getData, null)
        withResource(startNoNull) { _ =>
          val stepNoNull = new ColumnView(step.getType, step.getRowCount, Optional.of(0L),
            step.getData, null)
          withResource(stepNoNull) { _ =>
            ColumnVector.sequence(startNoNull, sizeNoNull, stepNoNull)
          }
        }
      }
      withResource(ret) { _ =>
        // Restore the null rows by setting the validity mask.
        ret.mergeAndSetValidity(BinaryOp.BITWISE_AND, size)
      }
    } else {
      ColumnVector.sequence(start, size, step)
    }
  }
}
