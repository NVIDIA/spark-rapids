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

package com.nvidia.spark.rapids

import scala.language.existentials

import ai.rapids.cudf.{Aggregation, AggregationOnColumn, ColumnVector, DType, RollingAggregation, Scalar, WindowOptions}
import ai.rapids.cudf.Aggregation.{LagAggregation, LeadAggregation, RowNumberAggregation}
import com.nvidia.spark.rapids.GpuOverrides.wrapExpr

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.rapids.GpuAggregateExpression
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.CalendarInterval

class GpuWindowExpressionMeta(
    windowExpression: WindowExpression,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: DataFromReplacementRule)
  extends ExprMeta[WindowExpression](windowExpression, conf, parent, rule) {

  private def getBoundaryValue(boundary : Expression) : Int = boundary match {
    case literal: Literal =>
      literal.dataType match {
        case IntegerType =>
          literal.value.asInstanceOf[Int]
        case CalendarIntervalType =>
          val ci = literal.value.asInstanceOf[CalendarInterval]
          if (ci.months != 0 || ci.microseconds != 0) {
            willNotWorkOnGpu("only days are supported for window range intervals")
          }
          ci.days
        case t =>
          willNotWorkOnGpu(s"unsupported window boundary type $t")
          -1
      }
    case UnboundedPreceding => Int.MinValue
    case UnboundedFollowing => Int.MaxValue
    case CurrentRow => 0
    case _ =>
      willNotWorkOnGpu("unsupported window boundary type")
      -1
  }

  /**
   * Check if the boundary is unbounded
   * @param boundary boundary expression
   * @return Boolean
   */
  private def rangeBoundaryIsUnBounded(boundary: Expression): Boolean = boundary match {
    case literal: Literal =>
      literal.dataType match {
        case IntegerType =>
          val x = literal.value.asInstanceOf[Int]
          x == Int.MaxValue || x == Int.MinValue
        case ByteType =>
          val x = literal.value.asInstanceOf[Byte]
          x == Byte.MaxValue || x == Byte.MinValue
        case ShortType =>
          val x = literal.value.asInstanceOf[Short]
          x == Short.MaxValue || x == Short.MinValue
        case LongType =>
          val x = literal.value.asInstanceOf[Long]
          x == Long.MaxValue || x == Long.MinValue
        case CalendarIntervalType =>
          val ci = literal.value.asInstanceOf[CalendarInterval]
          if (ci.months != 0 || ci.microseconds != 0) {
            willNotWorkOnGpu("only days are supported for window range intervals")
          }
          ci.days == Int.MaxValue || ci.days == Int.MinValue
        case t =>
          willNotWorkOnGpu(s"unsupported window boundary type $t")
          false
      }
    case UnboundedPreceding => true
    case UnboundedFollowing => true
    case CurrentRow => false
    case _ =>
      willNotWorkOnGpu(s"unsupported window boundary type ${boundary}")
      false
  }

  override def tagExprForGpu(): Unit = {

    // Must have two children:
    //  1. An AggregateExpression as the window function: SUM, MIN, MAX, COUNT
    //  2. A WindowSpecDefinition, defining the window-bounds, partitioning, and ordering.
    val windowFunction = wrapped.windowFunction

    wrapped.windowSpec.frameSpecification match {
      case spec: SpecifiedWindowFrame =>
        spec.frameType match {
          case RowFrame =>
            // Will also verify that the types are what we expect.
            val lower = getBoundaryValue(spec.lower)
            val upper = getBoundaryValue(spec.upper)
            windowFunction match {
              case Lead(_, _, _) | Lag(_, _, _) => // ignored we are good
              case _ =>
                // need to be sure that the lower/upper are acceptable
                if (lower > 0) {
                  willNotWorkOnGpu(s"lower-bounds ahead of current row is not supported. " +
                      s"Found $lower")
                }
                if (upper < 0) {
                  willNotWorkOnGpu(s"upper-bounds behind the current row is not supported. " +
                      s"Found $upper")
                }
            }
          case RangeFrame =>
            // Spark by default does a RangeFrame if no RowFrame is given
            // even for columns that are not time type columns. We can switch this back to row
            // based iff the ranges we are looking at both unbounded. We do this for all range
            // queries because https://github.com/NVIDIA/spark-rapids/issues/1039 makes it so
            // we cannot support nulls in range queries
            // Will also verify that the types are what we expect.
            val lowerIsUnBounded = rangeBoundaryIsUnBounded(spec.lower)
            val upperIsUnBounded = rangeBoundaryIsUnBounded(spec.upper)
            if (lowerIsUnBounded && upperIsUnBounded) {
              // this is okay because we will translate it to be a row query
            } else {
              val orderSpec = wrapped.windowSpec.orderSpec
              if (orderSpec.length > 1) {
                // We only support a single time column
                willNotWorkOnGpu("only a single date/time or integral (Boolean exclusive)" +
                  "based column in window range functions is supported")
              }
              val supported = orderSpec.forall { so =>
                so.dataType match {
                  case ByteType | ShortType | IntegerType | LongType => true
                  case DateType | TimestampType => true
                  case _ => false
                }
              }
              if (!supported) {
                willNotWorkOnGpu(s"the type of orderBy column is not supported in a window" +
                  s" range function, found ${orderSpec(0).dataType}")
              }
            }
        }
      case other =>
        willNotWorkOnGpu(s"only SpecifiedWindowFrame is a supported window-frame specification. " +
            s"Found ${other.prettyName}")
    }
  }

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  override def convertToGpu(): GpuExpression = {
    val Seq(left, right) = childExprs.map(_.convertToGpu())
    GpuWindowExpression(left, right.asInstanceOf[GpuWindowSpecDefinition])
  }
}

case class GpuWindowExpression(windowFunction: Expression, windowSpec: GpuWindowSpecDefinition)
  extends GpuExpression {

  override def children: Seq[Expression] = windowFunction :: windowSpec :: Nil

  override def dataType: DataType = windowFunction.dataType

  override def foldable: Boolean = windowFunction.foldable

  override def nullable: Boolean = windowFunction.nullable

  override def toString: String = s"$windowFunction $windowSpec"

  override def sql: String = windowFunction.sql + " OVER " + windowSpec.sql

  private val windowFrameSpec = windowSpec.frameSpecification.asInstanceOf[GpuSpecifiedWindowFrame]
  private val frameType : FrameType = windowFrameSpec.frameType
  private val windowFunc = windowFunction match {
    case func: GpuAggregateWindowFunction[_] => func
    case agg: GpuAggregateExpression => agg.aggregateFunction match {
      case func: GpuAggregateWindowFunction[_] => func
      case other =>
        throw new IllegalStateException(s"${other.getClass} is not a supported window aggregation")
    }
    case other =>
      throw new IllegalStateException(s"${other.getClass} is not a supported window function")
  }
  private lazy val boundRowProjectList = windowSpec.partitionSpec ++
      windowFunc.windowInputProjection
  private lazy val boundRangeProjectList = windowSpec.partitionSpec ++
      windowSpec.orderSpec.map(_.child.asInstanceOf[GpuExpression]) ++
      windowFunc.windowInputProjection

  override def columnarEval(cb: ColumnarBatch) : Any = {
    frameType match {
      case RowFrame   => evaluateRowBasedWindowExpression(cb)
      case RangeFrame =>
        val orderByType = cb.column(windowSpec.partitionSpec.length).dataType()
        val (isUnBoundedPreceding, _) = GpuWindowExpression.getRangeBasedLower(
          GpuColumnVector.getNonNestedRapidsType(orderByType), windowFrameSpec, false)
        val (isUnBoundedFollowing, _) = GpuWindowExpression.getRangeBasedUpper(
          GpuColumnVector.getNonNestedRapidsType(orderByType), windowFrameSpec, false)
        if (isUnBoundedPreceding && isUnBoundedFollowing) {
          // We already verified that this will be okay...
          evaluateRowBasedWindowExpression(cb)
        } else {
          evaluateRangeBasedWindowExpression(cb)
        }

      case allElse    =>
        throw new UnsupportedOperationException(
          s"Unsupported window expression frame type: $allElse")
    }
  }

  private def evaluateRowBasedWindowExpression(cb : ColumnarBatch) : GpuColumnVector = {
    val numGroupingColumns = windowSpec.partitionSpec.length
    val totalExtraColumns = numGroupingColumns

    val aggColumn = withResource(GpuProjectExec.project(cb, boundRowProjectList)) { projected =>

      // in case boundRowProjectList is empty
      val finalCb = if (boundRowProjectList.nonEmpty) projected else cb

      withResource(GpuColumnVector.from(finalCb)) { table =>
        val bases = GpuColumnVector.extractBases(finalCb).zipWithIndex
            .slice(totalExtraColumns, boundRowProjectList.length)

        val agg = windowFunc.windowAggregation(bases)
            .overWindow(GpuWindowExpression.getRowBasedWindowOptions(windowFrameSpec))

        withResource(table
            .groupBy(0 until numGroupingColumns: _*)
            .aggregateWindows(agg)) { aggResultTable =>
          aggResultTable.getColumn(0).incRefCount()
        }
      }
    }
    // For nested type, do not cast
    aggColumn.getType match {
      case dType if dType.isNestedType =>
        GpuColumnVector.from(aggColumn, windowFunc.dataType)
      case _ =>
        val expectedType = GpuColumnVector.getNonNestedRapidsType(windowFunc.dataType)
        // The API 'castTo' will take care of the 'from' type and 'to' type, and
        // just increase the reference count by one when they are the same.
        // so it is OK to always call it here.
        withResource(aggColumn) { aggColumn =>
          GpuColumnVector.from(aggColumn.castTo(expectedType), windowFunc.dataType)
        }
    }
  }

  private def evaluateRangeBasedWindowExpression(cb : ColumnarBatch) : GpuColumnVector = {
    val numGroupingColumns = windowSpec.partitionSpec.length
    val numSortColumns = windowSpec.orderSpec.length
    assert(numSortColumns == 1)
    val totalExtraColumns = numGroupingColumns + numSortColumns

    val aggColumn = withResource(GpuProjectExec.project(cb, boundRangeProjectList)) { projected =>
      withResource(GpuColumnVector.from(projected)) { table =>
        val bases = GpuColumnVector.extractBases(projected).zipWithIndex
          .slice(totalExtraColumns, boundRangeProjectList.length)

        // get the preceding/following scalar to construct WindowOptions
        val orderByType = table.getColumn(numGroupingColumns).getType
        val (isUnboundedPreceding, preceding) = GpuWindowExpression.getRangeBasedLower(orderByType,
          windowFrameSpec)
        val (isUnBoundedFollowing, following) = GpuWindowExpression.getRangeBasedUpper(orderByType,
          windowFrameSpec)

        withResource(preceding) { preceding =>
          withResource(following) { following =>
            val agg = windowFunc.windowAggregation(bases)
              .overWindow(GpuWindowExpression.getRangeBasedWindowOptions(windowSpec.orderSpec,
                numGroupingColumns,
                isUnboundedPreceding,
                preceding,
                isUnBoundedFollowing,
                following))
            withResource(table
              .groupBy(0 until numGroupingColumns: _*)
              .aggregateWindowsOverRanges(agg)) { aggResultTable =>
              aggResultTable.getColumn(0).incRefCount()
            }
          }
        }
      }
    }
    // For nested type, do not cast
    aggColumn.getType match {
      case dType if dType.isNestedType =>
        GpuColumnVector.from(aggColumn, windowFunc.dataType)
      case _ =>
        val expectedType = GpuColumnVector.getNonNestedRapidsType(windowFunc.dataType)
        // The API 'castTo' will take care of the 'from' type and 'to' type, and
        // just increase the reference count by one when they are the same.
        // so it is OK to always call it here.
        withResource(aggColumn) { aggColumn =>
          GpuColumnVector.from(aggColumn.castTo(expectedType), windowFunc.dataType)
        }
    }
  }
}

object GpuWindowExpression {

  def getRowBasedLower(windowFrameSpec : GpuSpecifiedWindowFrame): Int = {
    val lower = getBoundaryValue(windowFrameSpec.lower)

    // Translate the lower bound value to CUDF semantics:
    // In spark 0 is the current row and lower bound is negative relative to that
    // In CUDF the preceding window starts at the current row with 1 and up from there the
    // further from the current row.
    if (lower >= Int.MaxValue) {
      Int.MinValue
    } else if (lower <= Int.MinValue) {
      Int.MaxValue
    } else {
      -(lower-1)
    }
  }

  def getRowBasedUpper(windowFrameSpec : GpuSpecifiedWindowFrame): Int =
    getBoundaryValue(windowFrameSpec.upper)

  def getRowBasedWindowOptions(windowFrameSpec : GpuSpecifiedWindowFrame): WindowOptions = {
    val lower = getRowBasedLower(windowFrameSpec)
    val upper = getRowBasedUpper(windowFrameSpec)

    WindowOptions.builder().minPeriods(1)
        .window(lower, upper).build()
  }

  def getRangeBasedLower(orderByType: DType, windowFrameSpec: GpuSpecifiedWindowFrame,
      constructScalar: Boolean = true): (Boolean, Scalar) = {
    // FIXME: Currently, only negative or 0 values are supported.
    getRangeBoundaryValue(orderByType, windowFrameSpec.lower, constructScalar)
  }

  def getRangeBasedUpper(orderByType: DType, windowFrameSpec: GpuSpecifiedWindowFrame,
      constructScalar: Boolean = true): (Boolean, Scalar) = {
    getRangeBoundaryValue(orderByType, windowFrameSpec.upper, constructScalar)
  }

  def getRangeBasedWindowOptions(
      orderSpec: Seq[SortOrder],
      orderByColumnIndex : Int,
      isUnboundedPreceding: Boolean,
      preceding: Scalar,
      isUnBoundedFollowing: Boolean,
      following: Scalar): WindowOptions = {
    val windowOptionBuilder = WindowOptions.builder()
                                .minPeriods(1)
                                .orderByColumnIndex(orderByColumnIndex)

    if (isUnboundedPreceding) {
      windowOptionBuilder.unboundedPreceding()
    } else {
      windowOptionBuilder.preceding(preceding)
    }

    if (isUnBoundedFollowing) {
      windowOptionBuilder.unboundedFollowing()
    } else {
      windowOptionBuilder.following(following)
    }

    // We only support a single time based column to order by right now, so just verify
    // that it is correct.
    assert(orderSpec.length == 1)
    if (orderSpec.head.isAscending) {
      windowOptionBuilder.orderByAscending()
    } else {
      windowOptionBuilder.orderByDescending()
    }

    windowOptionBuilder.build()
  }

  def getBoundaryValue(boundary : Expression) : Int = boundary match {
    case literal: GpuLiteral if literal.dataType.equals(IntegerType) =>
      literal.value.asInstanceOf[Int]
    case literal: GpuLiteral if literal.dataType.equals(CalendarIntervalType) =>
      literal.value.asInstanceOf[CalendarInterval].days
    case special: GpuSpecialFrameBoundary =>
      special.value
    case anythingElse =>
      throw new UnsupportedOperationException(s"Unsupported window frame expression $anythingElse")
  }

  /**
   * Get the range boundary tuple
   * @param orderByType the type of order by column
   * @param boundary boundary expression
   * @param constructScalar specifies if need to create Scalar for boundary
   * @return ret: (Boolean, Scalar). the first element of tuple specifies if the boundary is
   *         unBounded, the second element of tuple specifies the Scalar created from boundary
   */
  def getRangeBoundaryValue(orderByType: DType, boundary: Expression, constructScalar: Boolean):
      (Boolean, Scalar) = boundary match {
    case literal: GpuLiteral if literal.dataType.equals(ByteType) =>
      val x = literal.value.asInstanceOf[Byte]
      val isUnbounded = if (x == Byte.MinValue || x == Byte.MaxValue) true else false
      val scalar = if (constructScalar) {
        GpuColumnVector.createRangeWindowBoundary(orderByType, Math.abs(x).toByte)
      } else null
      (isUnbounded, scalar)
    case literal: GpuLiteral if literal.dataType.equals(ShortType) =>
      val x = literal.value.asInstanceOf[Short]
      val isUnbounded = if (x == Short.MinValue || x == Short.MaxValue) true else false
      val scalar = if (constructScalar) {
        GpuColumnVector.createRangeWindowBoundary(orderByType, Math.abs(x).toShort)
      } else null
      (isUnbounded, scalar)
    case literal: GpuLiteral if literal.dataType.equals(IntegerType) =>
      val x = literal.value.asInstanceOf[Int]
      val isUnbounded = if (x == Int.MinValue || x == Int.MaxValue) true else false
      val scalar = if (constructScalar) {
        GpuColumnVector.createRangeWindowBoundary(orderByType, Math.abs(x))
      } else null
      (isUnbounded, scalar)
    case literal: GpuLiteral if literal.dataType.equals(LongType) =>
      val x = literal.value.asInstanceOf[Long]
      val isUnbounded = if (x == Long.MinValue || x == Long.MaxValue) true else false
      val scalar = if (constructScalar) {
        GpuColumnVector.createRangeWindowBoundary(orderByType, Math.abs(x))
      } else null
      (isUnbounded, scalar)
    case literal: GpuLiteral if literal.dataType.equals(CalendarIntervalType) =>
      // TimeStampDays -> DurationDays
      val x = literal.value.asInstanceOf[CalendarInterval].days
      val isUnbounded = if (x == Int.MinValue || x == Int.MaxValue) true else false
      val scalar = if (constructScalar) {
        GpuColumnVector.createRangeWindowBoundary(orderByType, Math.abs(x))
      } else null
      (isUnbounded, scalar)
    case special: GpuSpecialFrameBoundary =>
      val x = special.value
      val isUnbounded = if (x == Int.MinValue || x == Int.MaxValue) true else false
      val scalar = if (constructScalar) {
        GpuColumnVector.createRangeWindowBoundary(orderByType, Math.abs(x))
      } else null
      (isUnbounded, scalar)
    case anythingElse =>
      throw new UnsupportedOperationException(s"Unsupported window frame expression $anythingElse")
  }
}

class GpuWindowSpecDefinitionMeta(
    windowSpec: WindowSpecDefinition,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: DataFromReplacementRule)
  extends ExprMeta[WindowSpecDefinition](windowSpec, conf, parent, rule) {

  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    windowSpec.partitionSpec.map(wrapExpr(_, conf, Some(this)))
  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    windowSpec.orderSpec.map(wrapExpr(_, conf, Some(this)))
  val windowFrame: BaseExprMeta[WindowFrame] =
    wrapExpr(windowSpec.frameSpecification, conf, Some(this))

  override val ignoreUnsetDataTypes: Boolean = true

  override def tagExprForGpu(): Unit = {
    if (!windowSpec.frameSpecification.isInstanceOf[SpecifiedWindowFrame]) {
      willNotWorkOnGpu(s"WindowFunctions without a SpecifiedWindowFrame are unsupported.")
    }
  }

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  override def convertToGpu(): GpuExpression = {
    GpuWindowSpecDefinition(
      partitionSpec.map(_.convertToGpu()),
      orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
      windowFrame.convertToGpu().asInstanceOf[GpuWindowFrame])
  }
}

case class GpuWindowSpecDefinition(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    frameSpecification: GpuWindowFrame)
  extends GpuExpression with GpuUnevaluable {

  override def children: Seq[Expression] = partitionSpec ++ orderSpec :+ frameSpecification

  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess &&
      frameSpecification.isInstanceOf[GpuSpecifiedWindowFrame]

  override def nullable: Boolean = true

  override def foldable: Boolean = false

  override def dataType: DataType = {
    // Note: WindowSpecDefinition has no dataType. Should throw UnsupportedOperationException.
    // Setting this to a concrete type to work around bug in SQL logging in certain
    // Spark versions, which mistakenly call `dataType()` on Unevaluable expressions.
    IntegerType
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    frameSpecification match {
      case GpuUnspecifiedFrame =>
        TypeCheckFailure(
          "Cannot use an UnspecifiedFrame. This should have been converted during analysis. " +
            "Please file a bug report.")
      case f: GpuSpecifiedWindowFrame if f.frameType == RangeFrame && !f.isUnbounded &&
        orderSpec.isEmpty =>
        TypeCheckFailure(
          "A range window frame cannot be used in an unordered window specification.")
      case f: GpuSpecifiedWindowFrame if f.frameType == RangeFrame && f.isValueBound &&
        orderSpec.size > 1 =>
        TypeCheckFailure(
          s"A range window frame with value boundaries cannot be used in a window specification " +
            s"with multiple order by expressions: ${orderSpec.mkString(",")}")
      case f: GpuSpecifiedWindowFrame if f.frameType == RangeFrame && f.isValueBound &&
        !isValidFrameType(f.valueBoundary.head.dataType) =>
        TypeCheckFailure(
          s"The data type '${orderSpec.head.dataType.catalogString}' used in the order " +
            "specification does not match the data type " +
            s"'${f.valueBoundary.head.dataType.catalogString}' which is used in the range frame.")
      case _ => TypeCheckSuccess
    }
  }

  override def sql: String = {
    def toSql(exprs: Seq[Expression], prefix: String): Seq[String] = {
      Seq(exprs).filter(_.nonEmpty).map(_.map(_.sql).mkString(prefix, ", ", ""))
    }

    val elements =
      toSql(partitionSpec, "PARTITION BY ") ++
        toSql(orderSpec, "ORDER BY ") ++
        Seq(frameSpecification.sql)
    elements.mkString("(", " ", ")")
  }

  private def isValidFrameType(ft: DataType): Boolean = (orderSpec.head.dataType, ft) match {
    case (DateType, IntegerType) => true
    case (TimestampType, CalendarIntervalType) => true
    case (a, b) => a == b
  }
}

class GpuSpecifiedWindowFrameMeta(
    windowFrame: SpecifiedWindowFrame,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: DataFromReplacementRule)
  extends ExprMeta[SpecifiedWindowFrame](windowFrame, conf, parent, rule) {

  // SpecifiedWindowFrame has no associated dataType.
  override val ignoreUnsetDataTypes: Boolean = true

  override def tagExprForGpu(): Unit = {
    if (windowFrame.frameType.equals(RangeFrame)) {
      // Expect either SpecialFrame (UNBOUNDED PRECEDING/FOLLOWING, or CURRENT ROW),
      // or CalendarIntervalType in days.

      // Check that:
      //  1. if `bounds` is specified as a Literal, it is specified in DAYS.
      //  2. if `bounds` is a  lower-bound, it can't be ahead of the current row.
      //  3. if `bounds` is an upper-bound, it can't be behind the current row.
      def checkIfInvalid(bounds : Expression, isLower : Boolean) : Option[String] = {

        if (!bounds.isInstanceOf[Literal]) {
          // Bounds are likely SpecialFrameBoundaries (CURRENT_ROW, UNBOUNDED PRECEDING/FOLLOWING).
          return None
        }

        bounds.dataType match {
          case ByteType | ShortType | IntegerType | LongType => None
          case CalendarIntervalType =>
            val interval = bounds.asInstanceOf[Literal].value.asInstanceOf[CalendarInterval]
            if (interval.microseconds != 0 || interval.months != 0) { // DAYS == 0 is permitted.
              Some(s"Bounds for Range-based window frames must be specified only in DAYS. " +
                s"Found $interval")
            } else if (isLower && interval.days > 0) {
              Some(s"Lower-bounds ahead of current row is not supported. Found: ${interval.days}")
            } else if (!isLower && interval.days < 0) {
              Some(s"Upper-bounds behind current row is not supported. Found: ${interval.days}")
            } else {
              None
            }
          case _ => Some(s"Bounds for Range-based window frames must be specified in Integral" +
            s" type (Boolean exclusive) or DAYS. Found ${bounds.dataType}")
        }
      }

      val invalidUpper = checkIfInvalid(windowFrame.upper, isLower = false)
      if (invalidUpper.nonEmpty) {
        willNotWorkOnGpu(invalidUpper.get)
      }

      val invalidLower = checkIfInvalid(windowFrame.lower, isLower = true)
      if (invalidLower.nonEmpty) {
        willNotWorkOnGpu(invalidLower.get)
      }
    }

    if (windowFrame.frameType.equals(RowFrame)) {

      windowFrame.lower match {
        case literal : Literal =>
          if (!literal.value.isInstanceOf[Int]) {
            willNotWorkOnGpu(s"Literal Lower-bound of ROWS window-frame must be of INT type. " +
              s"Found ${literal.dataType}")
          }
          // We don't support a lower bound > 0 except for lead/lag where it is required
          // That check is done in GpuWindowExpressionMeta where it knows what type of operation
          // is being done
        case UnboundedPreceding =>
        case CurrentRow =>
        case _ =>
          willNotWorkOnGpu(s"Lower-bound of ROWS window-frame must be an INT literal," +
            s"UNBOUNDED PRECEDING, or CURRENT ROW. " +
            s"Found unexpected bound: ${windowFrame.lower.prettyName}")
      }

      windowFrame.upper match {
        case literal : Literal =>
          if (!literal.value.isInstanceOf[Int]) {
            willNotWorkOnGpu(s"Literal Upper-bound of ROWS window-frame must be of INT type. " +
              s"Found ${literal.dataType}")
          }
          // We don't support a upper bound < 0 except for lead/lag where it is required
          // That check is done in GpuWindowExpressionMeta where it knows what type of operation
          // is being done
        case UnboundedFollowing =>
        case CurrentRow =>
        case _ => willNotWorkOnGpu(s"Upper-bound of ROWS window-frame must be an INT literal," +
          s"UNBOUNDED FOLLOWING, or CURRENT ROW. " +
          s"Found unexpected bound: ${windowFrame.upper.prettyName}")
      }
    }
  }

  override def convertToGpu(): GpuExpression = {
    val Seq(left, right) = childExprs.map(_.convertToGpu())
    GpuSpecifiedWindowFrame(windowFrame.frameType, left, right)
  }
}

trait GpuWindowFrame extends GpuExpression with GpuUnevaluable {
  override def children: Seq[Expression] = Nil

  override def dataType: DataType = {
    // Note: WindowFrame has no dataType. Should throw UnsupportedOperationException.
    // Setting this to a concrete type to work around bug in SQL logging in certain
    // Spark versions, which mistakenly call `dataType()` on Unevaluable expressions.
    IntegerType
  }

  override def foldable: Boolean = false

  override def nullable: Boolean = false
}

case object GpuUnspecifiedFrame extends GpuWindowFrame // Placeholder, to handle UnspecifiedFrame

// This class closely follows what's done in SpecifiedWindowFrame.
case class GpuSpecifiedWindowFrame(
                                    frameType: FrameType,
                                    lower: Expression,
                                    upper: Expression)
  extends GpuWindowFrame {

  override def children: Seq[Expression] = lower :: upper :: Nil

  lazy val valueBoundary: Seq[Expression] =
    children.filterNot(_.isInstanceOf[GpuSpecialFrameBoundary])

  override def checkInputDataTypes(): TypeCheckResult = {
    // Check lower value.
    val lowerCheck = checkBoundary(lower, "lower")
    if (lowerCheck.isFailure) {
      return lowerCheck
    }

    // Check upper value.
    val upperCheck = checkBoundary(upper, "upper")
    if (upperCheck.isFailure) {
      return upperCheck
    }

    // Check combination (of expressions).
    (lower, upper) match {
      case (l: GpuExpression, u: GpuExpression) if !isValidFrameBoundary(l, u) =>
        TypeCheckFailure(s"Window frame upper bound '$upper' does not follow the lower bound " +
          s"'$lower'.")
      case (_: GpuSpecialFrameBoundary, _) => TypeCheckSuccess
      case (_, _: GpuSpecialFrameBoundary) => TypeCheckSuccess
      case (l: GpuExpression, u: GpuExpression) if l.dataType != u.dataType =>
        TypeCheckFailure(
          s"Window frame bounds '$lower' and '$upper' do no not have the same data type: " +
            s"'${l.dataType.catalogString}' <> '${u.dataType.catalogString}'")
      case (l: GpuExpression, u: GpuExpression) if isGreaterThan(l, u) =>
        TypeCheckFailure(
          "The lower bound of a window frame must be less than or equal to the upper bound")
      case _ => TypeCheckSuccess
    }
  }

  override def sql: String = {
    val lowerSql = boundarySql(lower)
    val upperSql = boundarySql(upper)
    s"${frameType.sql} BETWEEN $lowerSql AND $upperSql"
  }

  def isUnbounded: Boolean = {
    (lower, upper) match {
      case (l:GpuSpecialFrameBoundary, u:GpuSpecialFrameBoundary) =>
        l.boundary == UnboundedPreceding && u.boundary == UnboundedFollowing
      case _ => false
    }
  }

  def isValueBound: Boolean = valueBoundary.nonEmpty

  def isOffset: Boolean = (lower, upper) match {
    case (l: Expression, u: Expression) => frameType == RowFrame && l == u
    case _ => false
  }

  private def boundarySql(expr: Expression): String = expr match {
    case e: GpuSpecialFrameBoundary => e.sql
    case UnaryMinus(n) => n.sql + " PRECEDING"
    case e: Expression => e.sql + " FOLLOWING"
  }

  // Check whether the left boundary value is greater than the right boundary value. It's required
  // that the both expressions have the same data type.
  // Since CalendarIntervalType is not comparable, we only compare expressions that are AtomicType.
  //
  // Note: This check is currently skipped for GpuSpecifiedWindowFrame,
  // because: AtomicType has protected access in Spark. It is not available here.
  private def isGreaterThan(l: Expression, r: Expression): Boolean = l.dataType match {
    // case _: org.apache.spark.sql.types.AtomicType =>
    //   GreaterThan(l, r).eval().asInstanceOf[Boolean]
    case _ => false
  }

  private def checkBoundary(b: Expression, location: String): TypeCheckResult = b match {
    case _: GpuSpecialFrameBoundary => TypeCheckSuccess
    case e: Expression if !e.foldable =>
      TypeCheckFailure(s"Window frame $location bound '$e' is not a literal.")
    // Skipping type checks, because AbstractDataType::acceptsType() has protected access.
    // This should have been checked already.
    //
    // case e: Expression if !frameType.inputType.acceptsType(e.dataType) =>
    //   TypeCheckFailure(
    //     s"The data type of the $location bound '${e.dataType.catalogString}' does not match " +
    //       s"the expected data type '${frameType.inputType.simpleString}'.")
    case _ => TypeCheckSuccess
  }

  private def isValidFrameBoundary(l: GpuExpression, u: GpuExpression): Boolean = {
    (l, u) match {
      case (low: GpuSpecialFrameBoundary, _) if low.boundary == UnboundedFollowing => false
      case (_, up: GpuSpecialFrameBoundary)  if  up.boundary == UnboundedPreceding => false
      case _ => true
    }
  }
}

case class GpuSpecialFrameBoundary(boundary : SpecialFrameBoundary)
  extends GpuExpression with GpuUnevaluable {
  override def children : Seq[Expression] = Nil
  override def dataType: DataType = NullType
  override def foldable: Boolean = false
  override def nullable: Boolean = false

  def value : Int = {
    boundary match {
      case UnboundedPreceding => Int.MinValue
      case UnboundedFollowing => Int.MaxValue
      case CurrentRow => 0
      case anythingElse =>
        throw new UnsupportedOperationException(s"Unsupported window-bound $anythingElse!")
    }
  }
}

/**
 * GPU Counterpart of `AggregateWindowFunction`.
 * On the CPU this would extend `DeclarativeAggregate` and use the provided methods
 * to build up the expressions need to produce a result. For window operations we do it
 * in a single pass, where all of the data is available so instead we have out own set of
 * expressions.
 */
trait GpuAggregateWindowFunction[T <: Aggregation with RollingAggregation[T]]
    extends GpuUnevaluable {
  /**
   * Using child references, define the shape of the vectors sent to the window operations
   */
  val windowInputProjection: Seq[Expression]

  /**
   * Create the aggregation operation to perform for Windowing. The input to this method
   * is a sequence of (index, ColumnVector) that corresponds one to one with what was
   * returned by [[windowInputProjection]].  The index is the index into the Table for the
   * corresponding ColumnVector. Some aggregations need extra values.
   */
  def windowAggregation(inputs: Seq[(ColumnVector, Int)]): AggregationOnColumn[T]
}

case class GpuRowNumber() extends GpuAggregateWindowFunction[RowNumberAggregation] {
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = Nil

  override val windowInputProjection: Seq[Expression] = Nil

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): AggregationOnColumn[RowNumberAggregation] = {
    assert(inputs.isEmpty, inputs)
    Aggregation.rowNumber().onColumn(0)
  }
}

abstract class OffsetWindowFunctionMeta[INPUT <: OffsetWindowFunction] (
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends ExprMeta[INPUT](expr, conf, parent, rule) {
  lazy val input: BaseExprMeta[_] = GpuOverrides.wrapExpr(expr.input, conf, Some(this))
  lazy val offset: BaseExprMeta[_] = {
    expr match {
      case Lead(_,_,_) => // Supported.
      case Lag(_,_,_) =>  // Supported.
      case other =>
        throw new IllegalStateException(
          s"Only LEAD/LAG offset window functions are supported. Found: $other")
    }

    val literalOffset = GpuOverrides.extractLit(expr.offset) match {
      case Some(Literal(offset: Int, IntegerType)) =>
        Literal(offset, IntegerType)
      case _ =>
        throw new IllegalStateException(
          s"Only integer literal offsets are supported for LEAD/LAG. Found: ${expr.offset}")
    }

    GpuOverrides.wrapExpr(literalOffset, conf, Some(this))
  }
  lazy val default: BaseExprMeta[_] = GpuOverrides.wrapExpr(expr.default, conf, Some(this))

  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty

  override def tagExprForGpu(): Unit = {
    expr match {
      case Lead(_,_,_) => // Supported.
      case Lag(_,_,_) =>  // Supported.
      case other =>
        willNotWorkOnGpu( s"Only LEAD/LAG offset window functions are supported. Found: $other")
    }

    if (GpuOverrides.extractLit(expr.offset).isEmpty) { // Not a literal offset.
      willNotWorkOnGpu(
        s"Only integer literal offsets are supported for LEAD/LAG. Found: ${expr.offset}")
    }
  }
}

trait GpuOffsetWindowFunction[T <: Aggregation with RollingAggregation[T]]
    extends GpuAggregateWindowFunction[T] {
  protected val input: Expression
  protected val offset: Expression
  protected val default: Expression

  protected val parsedOffset: Int = offset match {
    case GpuLiteral(o: Int, IntegerType) => o
    case other =>
      throw new IllegalStateException(s"$other is not a supported offset type")
  }
  override def nullable: Boolean = default == null || default.nullable || input.nullable
  override def dataType: DataType = input.dataType

  override def children: Seq[Expression] = Seq(input, offset, default)

  override val windowInputProjection: Seq[Expression] = default match {
    case GpuLiteral(v, _) if v == null => Seq(input)
    case _ => Seq(input, default)
  }
}

case class GpuLead(input: Expression, offset: Expression, default: Expression)
    extends GpuOffsetWindowFunction[LeadAggregation] {

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): AggregationOnColumn[LeadAggregation] = {
    val in = inputs.toArray
    if (in.length > 1) {
      // Has a default
      Aggregation.lead(parsedOffset, in(1)._1).onColumn(in.head._2)
    } else {
      Aggregation.lead(parsedOffset).onColumn(in.head._2)
    }
  }
}

case class GpuLag(input: Expression, offset: Expression, default: Expression)
    extends GpuOffsetWindowFunction[LagAggregation] {

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): AggregationOnColumn[LagAggregation] = {
    val in = inputs.toArray
    if (in.length > 1) {
      // Has a default
      Aggregation.lag(parsedOffset, in(1)._1).onColumn(in.head._2)
    } else {
      Aggregation.lag(parsedOffset).onColumn(in.head._2)
    }
  }
}
