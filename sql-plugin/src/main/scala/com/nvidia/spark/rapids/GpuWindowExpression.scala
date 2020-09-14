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

package com.nvidia.spark.rapids

import ai.rapids.cudf.{DType, Table, WindowAggregate, WindowOptions}
import com.nvidia.spark.rapids.GpuOverrides.wrapExpr

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.CalendarInterval

class GpuWindowExpressionMeta(
    windowExpression: WindowExpression,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: ConfKeysAndIncompat)
  extends ExprMeta[WindowExpression](windowExpression, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {

    // Must have two children:
    //  1. An AggregateExpression as the window function: SUM, MIN, MAX, COUNT
    //  2. A WindowSpecDefinition, defining the window-bounds, partitioning, and ordering.

    if (wrapped.children.size != 2) {
      willNotWorkOnGpu("Unsupported children in WindowExpression. " +
        "Expected only WindowFunction, and WindowSpecDefinition")
    }

    val windowFunction = wrapped.windowFunction

    windowFunction match {
      case aggregateExpression : AggregateExpression =>
        aggregateExpression.aggregateFunction match {
          case Count(exp) => {
            if (!exp.forall(x => x.isInstanceOf[Literal])) {
              willNotWorkOnGpu(s"Currently, only COUNT(1) and COUNT(*) are supported. " +
                s"COUNT($exp) is not supported in windowing.")
            }
          }
          case Sum(_) | Min(_) | Max(_) => // Supported.
          case other: AggregateFunction =>
            willNotWorkOnGpu(s"AggregateFunction ${other.prettyName} " +
              s"is not supported in windowing.")
          case anythingElse =>
            willNotWorkOnGpu(s"Expression not supported in windowing. " +
              s"Found ${anythingElse.prettyName}")
        }

      case RowNumber() =>

      case _ =>
        willNotWorkOnGpu("Only AggregateExpressions are supported on GPU as WindowFunctions. " +
        s"Found ${windowFunction.prettyName}")
    }

    val spec = wrapped.windowSpec
    if (!spec.frameSpecification.isInstanceOf[SpecifiedWindowFrame]) {
      willNotWorkOnGpu(s"Only SpecifiedWindowFrame is a supported window-frame specification. " +
        s"Found ${spec.frameSpecification.prettyName}")
    }
  }

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  override def convertToGpu(): GpuExpression =
    GpuWindowExpression(
      childExprs.head.convertToGpu(),
      childExprs(1).convertToGpu().asInstanceOf[GpuWindowSpecDefinition]
    )
}

case class GpuWindowExpression(windowFunction: Expression, windowSpec: GpuWindowSpecDefinition)
  extends GpuExpression {

  override def children: Seq[Expression] = windowFunction :: windowSpec :: Nil

  override def dataType: DataType = windowFunction.dataType

  override def foldable: Boolean = windowFunction.foldable

  override def nullable: Boolean = windowFunction.nullable

  override def toString: String = s"$windowFunction $windowSpec"

  override def sql: String = windowFunction.sql + " OVER " + windowSpec.sql

  private var boundAggCol : Expression = _
  private val frameType : FrameType =
    windowSpec.frameSpecification.asInstanceOf[GpuSpecifiedWindowFrame].frameType

  def setBoundAggCol(bound : Expression) : Unit = {
    boundAggCol = bound
  }

  override def columnarEval(cb: ColumnarBatch) : Any = {
    frameType match {
      case RowFrame   => evaluateRowBasedWindowExpression(cb)
      case RangeFrame => evaluateRangeBasedWindowExpression(cb)
      case allElse    =>
        throw new UnsupportedOperationException(
          s"Unsupported window expression frame type: $allElse")
    }
  }

  private def evaluateRowBasedWindowExpression(cb : ColumnarBatch) : GpuColumnVector = {

    var groupingColsCB : ColumnarBatch = null
    var aggregationColsCB : ColumnarBatch = null
    var groupingCols : Array[GpuColumnVector] = null
    var aggregationCols : Array[GpuColumnVector] = null
    var inputTable : Table = null
    var aggResultTable : Table = null

    try {
      // Project required column batches.
      groupingColsCB    = GpuProjectExec.project(cb, windowSpec.partitionSpec)
      aggregationColsCB = GpuProjectExec.project(cb, Seq(boundAggCol))
      // Extract required columns columns.
      groupingCols = GpuColumnVector.extractColumns(groupingColsCB)
      aggregationCols = GpuColumnVector.extractColumns(aggregationColsCB)

      inputTable = new Table( ( groupingCols ++ aggregationCols ).map(_.getBase) : _* )

      aggResultTable = inputTable.groupBy(0 until groupingColsCB.numCols(): _*)
        .aggregateWindows(
          GpuWindowExpression.getRowBasedWindowFrame(
            groupingColsCB.numCols(),
            windowFunction,
            windowSpec.frameSpecification.asInstanceOf[GpuSpecifiedWindowFrame]
          )
        )

      val aggColumn = windowFunction match {

        // Special-case handling for COUNT(1)/COUNT(*):
        // GpuCount aggregation expects to return LongType (INT64),
        // but CUDF returns IntType (INT32) for COUNT() window function. Must cast back up to INT64.
        case GpuAggregateExpression(GpuCount(_), _, _, _, _) =>
          aggResultTable.getColumn(0).castTo(DType.INT64) // Aggregation column is at index `0`.

        case _ =>
          val origAggColumn = aggResultTable.getColumn(0) // Aggregation column is at index `0`.
          origAggColumn.incRefCount()
          origAggColumn
      }

      GpuColumnVector.from(aggColumn)
    } finally {
      if (groupingColsCB != null) groupingColsCB.close()
      if (aggregationColsCB != null) aggregationColsCB.close()
      if (inputTable != null) inputTable.close()
      if (aggResultTable != null) aggResultTable.close()
    }
  }

  private def evaluateRangeBasedWindowExpression(cb : ColumnarBatch) : GpuColumnVector = {

    var groupingColsCB : ColumnarBatch = null
    var sortColsCB : ColumnarBatch = null
    var aggregationColsCB : ColumnarBatch = null
    var groupingCols : Array[GpuColumnVector] = null
    var sortCols : Array[GpuColumnVector] = null
    var aggregationCols : Array[GpuColumnVector] = null
    var inputTable : Table = null
    var aggResultTable : Table = null

    try {
      // Project required column batches.
      groupingColsCB = GpuProjectExec.project(cb, windowSpec.partitionSpec)
      assert(windowSpec.orderSpec.size == 1, "Expected a single sort column.")

      sortColsCB = GpuProjectExec.project(cb,
        windowSpec.orderSpec.map(_.child.asInstanceOf[GpuExpression]))
      aggregationColsCB = GpuProjectExec.project(cb, Seq(boundAggCol))

      // Extract required columns columns.
      groupingCols = GpuColumnVector.extractColumns(groupingColsCB)
      sortCols = GpuColumnVector.extractColumns(sortColsCB)
      aggregationCols = GpuColumnVector.extractColumns(aggregationColsCB)

      inputTable = new Table( ( groupingCols ++ sortCols ++ aggregationCols ).map(_.getBase) : _* )

      aggResultTable = inputTable.groupBy(0 until groupingColsCB.numCols(): _*)
        .aggregateWindowsOverTimeRanges(
          GpuWindowExpression.getRangeBasedWindowFrame(
            groupingColsCB.numCols() + sortColsCB.numCols(),
            groupingColsCB.numCols(),
            windowFunction,
            windowSpec.frameSpecification.asInstanceOf[GpuSpecifiedWindowFrame],
            windowSpec.orderSpec.head.isAscending
          )
        )

      val aggColumn = windowFunction match {

        // Special-case handling for COUNT(1)/COUNT(*):
        // GpuCount aggregation expects to return LongType (INT64),
        // but CUDF returns IntType (INT32) for COUNT() window function.
        // Must cast back up to INT64.
        case GpuAggregateExpression(GpuCount(_), _, _, _, _) =>
          aggResultTable.getColumn(0).castTo(DType.INT64) // Aggregation column is at index `0`

        case _ =>
          val origAggColumn = aggResultTable.getColumn(0) // Aggregation column is at index `0`
          origAggColumn.incRefCount()
          origAggColumn
      }

      GpuColumnVector.from(aggColumn)
    } finally {
      if (groupingColsCB != null) groupingColsCB.close()
      if (sortColsCB != null) sortColsCB.close()
      if (aggregationColsCB != null) aggregationColsCB.close()
      if (inputTable != null) inputTable.close()
      if (aggResultTable != null) aggResultTable.close()
    }
  }
}

object GpuWindowExpression {

  def getRowBasedWindowFrame(columnIndex : Int,
                             aggExpression : Expression,
                             windowSpec : GpuSpecifiedWindowFrame)
  : WindowAggregate = {

    // FIXME: Currently, only negative or 0 values are supported.
    var lower = getBoundaryValue(windowSpec.lower)
    if(lower > 0) {
      throw new IllegalStateException(
        s"Lower-bounds ahead of current row is not supported. Found $lower")
    }

    // Now, translate the lower bound value to CUDF semantics:
    //  1. CUDF requires lower bound value to include the current row.
    //     i.e. If Spark's lower bound == 3, CUDF's lower bound == 2.
    //  2. Spark's lower_bound (preceding CURRENT ROW) as a negative offset.
    //     CUDF requires a positive number
    //  Note: UNBOUNDED PRECEDING implies lower == Int.MinValue, which needs special handling
    //  for negation.
    //
    // The following covers both requirements:
    lower = Math.abs(lower-1)

    val upper = getBoundaryValue(windowSpec.upper)
    if (upper < 0) {
      throw new IllegalStateException(
        s"Upper-bounds behind of current row is not supported. Found $upper")
    }

    val windowOption = WindowOptions.builder().minPeriods(1)
      .window(lower, upper).build()

    aggExpression match {
      case gpuAggregateExpression : GpuAggregateExpression =>
        gpuAggregateExpression.aggregateFunction match {
          case GpuCount(_) => WindowAggregate.count(columnIndex, windowOption)
          case GpuSum(_) => WindowAggregate.sum(columnIndex, windowOption)
          case GpuMin(_) => WindowAggregate.min(columnIndex, windowOption)
          case GpuMax(_) => WindowAggregate.max(columnIndex, windowOption)
          case anythingElse =>
            throw new UnsupportedOperationException(
              s"Unsupported aggregation: ${anythingElse.prettyName}")
        }
      case _: GpuRowNumber =>
        // ROW_NUMBER does not depend on input column values.
        WindowAggregate.row_number(0, windowOption)
      case anythingElse =>
        throw new UnsupportedOperationException(
          s"Unsupported window aggregation: ${anythingElse.prettyName}")
    }
  }

  def getRangeBasedWindowFrame(aggColumnIndex : Int,
                               timeColumnIndex : Int,
                               aggExpression : Expression,
                               windowSpec : GpuSpecifiedWindowFrame,
                               timestampIsAscending : Boolean)
  : WindowAggregate = {

    // FIXME: Currently, only negative or 0 values are supported.
    var lower = getBoundaryValue(windowSpec.lower)
    if (lower > 0) {
      throw new IllegalStateException(
        s"Lower-bounds ahead of current row is not supported. Found: $lower")
    }

    // Now, translate the lower bound value to CUDF semantics:
    // Spark's lower_bound (preceding CURRENT ROW) as a negative offset.
    // CUDF requires a positive offset.
    // Note: UNBOUNDED PRECEDING implies lower == Int.MinValue, which needs special handling
    // for negation.
    lower = if (lower == Int.MinValue) Int.MaxValue else Math.abs(lower)

    val upper = getBoundaryValue(windowSpec.upper)
    if(upper < 0) {
      throw new IllegalStateException(
        s"Upper-bounds behind current row is not supported. Found: $upper")
    }

    val windowOptionBuilder = WindowOptions.builder()
      .minPeriods(1)
      .window(lower,upper)
      .timestampColumnIndex(timeColumnIndex)
    if (timestampIsAscending) {
      windowOptionBuilder.timestampAscending()
    }
    else {
      windowOptionBuilder.timestampDescending()
    }

    val windowOption = windowOptionBuilder.build()

    aggExpression match {
      case gpuAggExpression : GpuAggregateExpression => gpuAggExpression.aggregateFunction match {
        case GpuCount(_) => WindowAggregate.count(aggColumnIndex, windowOption)
        case GpuSum(_) => WindowAggregate.sum(aggColumnIndex, windowOption)
        case GpuMin(_) => WindowAggregate.min(aggColumnIndex, windowOption)
        case GpuMax(_) => WindowAggregate.max(aggColumnIndex, windowOption)
        case anythingElse =>
          throw new UnsupportedOperationException(
            s"Unsupported aggregation: ${anythingElse.prettyName}")
      }
      case anythingElse =>
        throw new UnsupportedOperationException(
          s"Unsupported window aggregation: ${anythingElse.prettyName}")
    }
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
}

class GpuWindowSpecDefinitionMeta(
    windowSpec: WindowSpecDefinition,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: ConfKeysAndIncompat)
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
    rule: ConfKeysAndIncompat)
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

        if (!bounds.dataType.equals(CalendarIntervalType)) {
          return Some(s"Bounds for Range-based window frames must be specified in DAYS. " +
            s"Found ${bounds.dataType}")
        }

        val interval = bounds.asInstanceOf[Literal].value.asInstanceOf[CalendarInterval]
        if (interval.microseconds != 0 || interval.months != 0) { // DAYS == 0 is permitted.
          return Some(s"Bounds for Range-based window frames must be specified only in DAYS. " +
            s"Found $interval")
        }

        if (isLower && interval.days > 0) {
          Some(s"Lower-bounds ahead of current row is not supported. Found: ${interval.days}")
        }
        else if (!isLower && interval.days < 0) {
          Some(s"Upper-bounds behind current row is not supported. Found: ${interval.days}")
        }
        else {
          None
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
          else if (literal.value.asInstanceOf[Int] > 0) {
            willNotWorkOnGpu(s"Lower-bounds ahead of current row is not supported. " +
              s"Found ${literal.value}")
          }
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
          else if (literal.value.asInstanceOf[Int] < 0) {
            willNotWorkOnGpu(s"Upper-bounds behind of current row is not supported. " +
              s"Found ${literal.value}")
          }
        case UnboundedFollowing =>
        case CurrentRow =>
        case _ => willNotWorkOnGpu(s"Upper-bound of ROWS window-frame must be an INT literal," +
          s"UNBOUNDED FOLLOWING, or CURRENT ROW. " +
          s"Found unexpected bound: ${windowFrame.upper.prettyName}")
      }

    }
  }

  override def convertToGpu(): GpuExpression =
    GpuSpecifiedWindowFrame(windowFrame.frameType, childExprs.head.convertToGpu(),
      childExprs(1).convertToGpu())
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
      case (l: GpuSpecialFrameBoundary, _) => TypeCheckSuccess
      case (_, u: GpuSpecialFrameBoundary) => TypeCheckSuccess
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

// GPU Counterpart of AggregateWindowFunction.
// All windowing specific functions are expected to extend from this.
trait GpuAggregateWindowFunction extends GpuDeclarativeAggregate with GpuUnevaluable {
  override lazy val mergeExpressions: Seq[GpuExpression]
  = throw new UnsupportedOperationException("Window Functions do not support merging.")
}

case class GpuRowNumber() extends GpuAggregateWindowFunction {
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = Nil
  protected val zero: GpuLiteral = GpuLiteral(0, IntegerType)
  protected val one : GpuLiteral = GpuLiteral(1, IntegerType)

  protected val rowNumber : AttributeReference =
    AttributeReference("rowNumber", IntegerType)()
  override def aggBufferAttributes: Seq[AttributeReference] =  rowNumber :: Nil
  override val initialValues: Seq[GpuExpression] = zero :: Nil
  override val updateExpressions: Seq[Expression] = rowNumber :: one :: Nil
  override val evaluateExpression: Expression = rowNumber
  override val inputProjection: Seq[GpuExpression] = Nil
}
