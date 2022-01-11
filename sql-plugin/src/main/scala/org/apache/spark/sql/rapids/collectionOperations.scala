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
import ai.rapids.cudf.{ColumnVector, ColumnView, DType, GroupByAggregation, GroupByOptions, NullPolicy, Scalar, ScanAggregation, ScanType, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuExpressionsUtils.columnarEvalToColumn
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.v2.{RapidsErrorUtils, ShimExpression}

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

  def isAllTrue(col: ColumnVector): Boolean = {
    assert(DType.BOOL8 == col.getType)
    if (col.getRowCount == 0) {
      return true
    }
    if (col.hasNulls) {
      return false
    }
    withResource(col.all()) { allTrue =>
      // Guaranteed there is at least one row and no nulls so result must be valid
      allTrue.getBoolean
    }
  }

  /** (Copied most of the part from GpuConditionalExpression) */
  def gather(predicate: ColumnVector, toBeGathered: ColumnVector): ColumnVector = {
    // convert the predicate boolean column to numeric where 1 = true
    // and 0 (or null) = false and then use `scan` with `sum` to convert to indices.
    //
    // For example, if the predicate evaluates to [F, null, T, F, T] then this
    // gets translated first to [0, 0, 1, 0, 1] and then the scan operation
    // will perform an exclusive sum on these values and produce [0, 0, 0, 1, 1].
    // Combining this with the original predicate boolean array results in the two
    // T values mapping to indices 0 and 1, respectively.
    val boolAsInt = withResource(Scalar.fromInt(1)) { one =>
      withResource(Scalar.fromInt(0)) { zero =>
        predicate.ifElse(one, zero)
      }
    }
    val prefixSumExclusive = withResource(boolAsInt) { boolsAsInts =>
      boolsAsInts.scan(ScanAggregation.sum(), ScanType.EXCLUSIVE, NullPolicy.INCLUDE)
    }
    val gatherMap = withResource(prefixSumExclusive) { prefixSumExclusive =>
      // for the entries in the gather map that do not represent valid
      // values to be gathered, we change the value to -MAX_INT which
      // will be treated as null values in the gather algorithm
      withResource(Scalar.fromInt(Int.MinValue)) { outOfBoundsFlag =>
        predicate.ifElse(prefixSumExclusive, outOfBoundsFlag)
      }
    }
    withResource(gatherMap) { _ =>
      withResource(new Table(toBeGathered)) { tbl =>
        withResource(tbl.gather(gatherMap)) { gatherTbl =>
          gatherTbl.getColumn(0).incRefCount()
        }
      }
    }
  }

  /**
   * Compute the size of each array from 'start', 'stop' and 'step'.
   * The returned column should be closed.
   */
  def computeArraySizes(
      start: ColumnVector,
      stop: ColumnVector,
      step: ColumnVector): ColumnVector = {
    // Keep the same requirement with Spark:
    //  (step > 0 && start <= stop) || (step < 0 && start >= stop) || (step == 0 && start == stop)
    withResource(Scalar.fromByte(0.toByte)) { zero =>
      withResource(new Table(start, stop)) { startStopTable =>
        // (step > 0 && start <= stop)
        withResource(step.greaterThan(zero)) { positiveStep =>
          withResource(startStopTable.filter(positiveStep)) { upTbl =>
            withResource(upTbl.getColumn(0).lessOrEqualTo(upTbl.getColumn(1))) { allUp =>
              require(isAllTrue(allUp), "Illegal sequence boundaries: step > 0 but start > stop")
            }
          }
        }
        // (step < 0 && start >= stop)
        withResource(step.lessThan(zero)) { negativeStep =>
          withResource(startStopTable.filter(negativeStep)) { downTbl =>
            withResource(downTbl.getColumn(0).greaterOrEqualTo(downTbl.getColumn(1))) { allDown =>
              require(isAllTrue(allDown),
                "Illegal sequence boundaries: step < 0 but start < stop")
            }
          }
        }
        // (step == 0 && start == stop)
        withResource(step.equalTo(zero)) { zeroStep =>
          withResource(startStopTable.filter(zeroStep)) { equalTbl =>
            withResource(equalTbl.getColumn(0).equalTo(equalTbl.getColumn(1))) { allEqual =>
              require(isAllTrue(allEqual),
                "Illegal sequence boundaries:  step == 0 but start != stop")
            }
          }
        }
      }
    }
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
      // actualSize = 1L + (stop.toLong - start.toLong) / estimatedStep.toLong
      val actualSize = withResource(diff) { _ =>
        withResource(step.castTo(DType.INT64)) { stepAsLong =>
          withResource(diff.div(stepAsLong)) { quotient =>
            quotient.add(one, DType.INT64)
          }
        }
      }
      withResource(actualSize) { _ =>
        withResource(start.equalTo(stop)) { equals =>
          equals.ifElse(one, actualSize)
        }
      }
    }
    // check size
    closeOnExcept(sizeAsLong) { _ =>
      withResource(Scalar.fromInt(MAX_ROUNDED_ARRAY_LENGTH)) { maxLen =>
        withResource(sizeAsLong.lessOrEqualTo(maxLen)) { allValid =>
          require(isAllTrue(allValid),
            s"Too long sequence found. Should be <= $MAX_ROUNDED_ARRAY_LENGTH")
        }
      }
    }
    withResource(sizeAsLong) { _ =>
      sizeAsLong.castTo(DType.INT32)
    }
  }

  /**
   * Filter out the nulls for 'start', 'stop', and 'step'.
   * They should have the same number of rows.
   *
   * @return 3 corresponding columns without nulls, and a bool column that
   *         row[i] is true if all the values of the 3 input columns at 'i' are valid,
   *         Otherwise, row[i] is false. All the columns need to be closed by users.
   */
  def filterOutNullsIfNeeded(
      start: ColumnVector,
      stop: ColumnVector,
      stepOpt: Option[ColumnVector]):
      (ColumnVector, ColumnVector, Option[ColumnVector], Option[ColumnVector]) = {
    // If has nulls, filter out the nulls
    val hasNull = start.hasNulls || stop.hasNulls || stepOpt.exists(_.hasNulls)
    if (hasNull) {
      // Translate to booleans (false for null, true for others) for start, stop and step,
      // and merge them by 'and'. For example
      //  <start>  <stop>  <step>                                  <nonNullMask>
      //    1       null    7             true   false  true        false
      //    2       4       null     =>   true   true   false  =>   false
      //    null    5       8             false  true   true        false
      //    3       6       9             true   true   true        true
      val startStopMask = withResource(start.isNotNull) { startMask =>
        withResource(stop.isNotNull) { stopMask =>
          startMask.and(stopMask)
        }
      }
      val nonNullMask = withResource(startStopMask) { _ =>
        withResource(stepOpt.map(_.isNotNull)) { stepMaskOpt =>
          stepMaskOpt.map(_.and(startStopMask)).getOrElse(startStopMask.incRefCount())
        }
      }
      // Now it is ready to filter out the nulls
      closeOnExcept(nonNullMask) { _ =>
        val inputTable = stepOpt.map(new Table(start, stop, _)).getOrElse(new Table(start, stop))
        withResource(inputTable) { _ =>
          withResource(inputTable.filter(nonNullMask)) { tbl =>
            ( tbl.getColumn(0).incRefCount(),
              tbl.getColumn(1).incRefCount(),
              stepOpt.map(_ => tbl.getColumn(2).incRefCount()),
              Some(nonNullMask)
            )
          }
        }
      }
    } else {
      (start.incRefCount(), stop.incRefCount(), stepOpt.map(_.incRefCount()), None)
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
    // 1 Compute the array size for all the rows
    val (startCol, sizeCol, stepCol, nonNullPredOpt) =
      withResource(columnarEvalToColumn(start, batch)) { startGpuCol =>
        withResource(columnarEvalToColumn(stop, batch)) { stopGpuCol =>
          withResource(stepOpt.map(columnarEvalToColumn(_, batch))) { stepColOpt =>
            // Filter out the nulls before any computation, since cudf 'sequences' does
            // not allow nulls.(Pls refer to https://github.com/rapidsai/cudf/issues/10012)
            // Then no need to do computation for the null rows.
            val (starts, stops, stepsOption, nonNullOption) = filterOutNullsIfNeeded(
                startGpuCol.getBase, stopGpuCol.getBase, stepColOpt.map(_.getBase))

            // Get the array size of each valid row, along with the steps
            withResource(stops) { _ =>
              closeOnExcept(starts) { _ =>
                closeOnExcept(nonNullOption) { _ =>
                  closeOnExcept(stepsOption.getOrElse(defaultSteps(starts, stops))) { steps =>
                    val sizes = computeArraySizes(starts, stops, steps)
                    (starts, sizes, steps, nonNullOption)
                  }
                }
              }
            }
          }
        }
      }

    // 2 Generate the sequence
    val castedStepCol = withResource(stepCol) { _ =>
      // cudf 'sequence' requires 'step' has the same type with 'start'.
      // And the step type may differ due to the default steps.
      stepCol.castTo(startCol.getType)
    }
    withResource(Seq(startCol, sizeCol, castedStepCol)) { _ =>
      withResource(ColumnVector.sequence(startCol, sizeCol, castedStepCol)) { ret =>
        val finalRet = withResource(nonNullPredOpt) { _ =>
          // if has nulls, need to restore the valid values to the original positions.
          nonNullPredOpt.map(gather(_, ret)).getOrElse(ret.incRefCount)
        }
        GpuColumnVector.from(finalRet, dataType)
      }
    }
  }

  @transient
  private lazy val defaultSteps: (ColumnView, ColumnView) => ColumnVector =
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

}
