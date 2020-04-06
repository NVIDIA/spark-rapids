/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import ai.rapids.spark.GpuOverrides.wrapExpr
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.expressions.{CurrentRow, Expression, FrameType, Literal, RangeFrame, RowFrame, SortOrder, SpecialFrameBoundary, SpecifiedWindowFrame, UnaryMinus, UnboundedFollowing, UnboundedPreceding, WindowExpression, WindowFrame, WindowSpecDefinition}
import org.apache.spark.sql.rapids.{GpuAggregateExpression, GpuCount}
import org.apache.spark.sql.types.{CalendarIntervalType, DataType, DateType, IntegerType, NullType, TimestampType}

class GpuWindowExpressionMeta(
          windowExpression: WindowExpression,
          conf: RapidsConf,
          parent: Option[RapidsMeta[_,_,_]],
          rule: ConfKeysAndIncompat) extends ExprMeta[WindowExpression](windowExpression, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {

    // Must have two children:
    //  1. An AggregateExpression as the window function: SUM, MIN, MAX, COUNT
    //  2. A WindowSpecDefinition, defining the window-bounds, partitioning, and ordering.

    if (wrapped.children.size != 2) {
      willNotWorkOnGpu("Unsupported children in WindowExpression. " +
        "Expected only WindowFunction, and WindowSpecDefinition")
      return
    }

    if (!wrapped.windowFunction.isInstanceOf[AggregateExpression]) {
      willNotWorkOnGpu("Only AggregateExpressions are supported on GPU as WindowFunctions. " +
        s"Found ${wrapped.windowFunction.prettyName}")
    }

    wrapped.windowFunction.asInstanceOf[AggregateExpression].aggregateFunction match {
      case Count(_) | Sum(_) | Min(_) | Max(_) => // Supported.
      case other: AggregateFunction => willNotWorkOnGpu(s"AggregateFunction ${other.prettyName} " +
        s"is not supported in windowing.")
      case _ => willNotWorkOnGpu(s"Expression not supported in windowing.")
    }

    if (!wrapped.windowSpec.frameSpecification.isInstanceOf[SpecifiedWindowFrame]) {
      willNotWorkOnGpu(s"Only SpecifiedWindowFrame is a supported window-frame specification. " +
        s"Found ${wrapped.windowSpec.frameSpecification.prettyName}")
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

case class GpuWindowExpression(
                                windowFunction: GpuExpression,
                                windowSpec: GpuWindowSpecDefinition
                              )
  extends GpuExpression with GpuUnevaluable {

  override def children: Seq[Expression] = windowFunction :: windowSpec :: Nil

  // Special-case for COUNT(1)/COUNT(*).
  // GpuCount aggregation expects to return LongType, but CUDF returns IntType for COUNT() window function.
  override def dataType: DataType
    = if (windowFunction.asInstanceOf[GpuAggregateExpression].aggregateFunction.isInstanceOf[GpuCount])
        IntegerType
      else windowFunction.dataType

  override def foldable: Boolean = windowFunction.foldable

  override def nullable: Boolean = windowFunction.nullable

  override def toString: String = s"$windowFunction $windowSpec"

  override def sql: String = windowFunction.sql + " OVER " + windowSpec.sql
}

class GpuWindowSpecDefinitionMeta(
          windowSpec: WindowSpecDefinition,
          conf: RapidsConf,
          parent: Option[RapidsMeta[_,_,_]],
          rule: ConfKeysAndIncompat) extends ExprMeta[WindowSpecDefinition](windowSpec, conf, parent, rule) {

  val partitionSpec: Seq[ExprMeta[Expression]] =
    windowSpec.partitionSpec.map(wrapExpr(_, conf, Some(this)))
  val orderSpec: Seq[ExprMeta[SortOrder]] =
    windowSpec.orderSpec.map(wrapExpr(_, conf, Some(this)))
  val windowFrame: ExprMeta[WindowFrame] =
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
      orderSpec.map(_.convertToGpu().asInstanceOf[GpuSortOrder]),
      windowFrame.convertToGpu().asInstanceOf[GpuWindowFrame])
  }
}

case class GpuWindowSpecDefinition(partitionSpec: Seq[GpuExpression],
                                   orderSpec: Seq[GpuSortOrder],
                                   frameSpecification: GpuWindowFrame)
  extends GpuExpression
    with GpuUnevaluable {

  override def children: Seq[Expression] = partitionSpec ++ orderSpec :+ frameSpecification

  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess &&
      frameSpecification.isInstanceOf[GpuSpecifiedWindowFrame]

  override def nullable: Boolean = true

  override def foldable: Boolean = false

  override def dataType: DataType = throw new UnsupportedOperationException("dataType")

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
          rule: ConfKeysAndIncompat) extends ExprMeta[SpecifiedWindowFrame](windowFrame, conf, parent, rule) {

  override val ignoreUnsetDataTypes: Boolean = true // SpecifiedWindowFrame has no associated dataType.

  override def tagExprForGpu(): Unit = {
    if (windowFrame.frameType.equals(RangeFrame)) {
      // Expect either SpecialFrame (UNBOUNDED PRECEDING/FOLLOWING, or CURRENT ROW),
      // or CalendarIntervalType in days.
      
      val upper = windowFrame.upper
      upper match {
        case literal: Literal 
          if !(upper.dataType.equals(CalendarIntervalType) 
            && literal.value.toString.toLowerCase.endsWith("days"))
              => willNotWorkOnGpu("Range-based window-frames must be specified in DAYS")
        case _ =>
      }
      
      val lower = windowFrame.lower
      lower match {
        case literal: Literal
          if !(lower.dataType.equals(CalendarIntervalType)
            && literal.value.toString.toLowerCase.endsWith("days"))
        => willNotWorkOnGpu("Range-based window-frames must be specified in DAYS")
        case _ =>
      }
    }

    if (windowFrame.frameType.equals(RowFrame)) {
      if (!windowFrame.lower.isInstanceOf[Literal]
        || !windowFrame.lower.asInstanceOf[Literal].value.isInstanceOf[Int]) {
        willNotWorkOnGpu("Lower-bound of window-frame should be an INT literal. " +
          s"Found ${windowFrame.lower.prettyName}")
      }

      if (!windowFrame.upper.isInstanceOf[Literal]
        || !windowFrame.upper.asInstanceOf[Literal].value.isInstanceOf[Int]) {
        willNotWorkOnGpu("Upper-bound of window-frame should be an INT literal. " +
          s"Found ${windowFrame.upper.prettyName}")
      }
    }

    // TODO: Add protections for foldable expressions with l>u, etc.
  }

  override def convertToGpu(): GpuExpression =
    GpuSpecifiedWindowFrame(windowFrame.frameType, childExprs.head.convertToGpu(), childExprs(1).convertToGpu())
}

trait GpuWindowFrame extends GpuExpression with GpuUnevaluable {
  override def children: Seq[Expression] = Nil

  override def dataType: DataType = throw new UnsupportedOperationException("GpuWindowFrame::dataType")
  override def foldable: Boolean = false

  override def nullable: Boolean = false
}

case object GpuUnspecifiedFrame extends GpuWindowFrame // Placeholder, to handle UnspecifiedFrame

case class GpuSpecifiedWindowFrame(
                                    frameType: FrameType,
                                    lower: GpuExpression,
                                    upper: GpuExpression)
  extends GpuWindowFrame {

  override def children: Seq[Expression] = lower :: upper :: Nil

  lazy val valueBoundary: Seq[Expression] =
    children.filterNot(_.isInstanceOf[SpecialFrameBoundary])

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
      case (l: Expression, u: Expression) if !isValidFrameBoundary(l, u) =>
        TypeCheckFailure(s"Window frame upper bound '$upper' does not follow the lower bound " +
          s"'$lower'.")
      case (l: GpuSpecialFrameBoundary, _) => TypeCheckSuccess
      case (_, u: GpuSpecialFrameBoundary) => TypeCheckSuccess
      case (l: Expression, u: Expression) if l.dataType != u.dataType =>
        TypeCheckFailure(
          s"Window frame bounds '$lower' and '$upper' do no not have the same data type: " +
            s"'${l.dataType.catalogString}' <> '${u.dataType.catalogString}'")
      case (l: Expression, u: Expression) if isGreaterThan(l, u) =>
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

  def isUnbounded: Boolean = lower == UnboundedPreceding && upper == UnboundedFollowing

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
  private def isGreaterThan(l: Expression, r: Expression): Boolean = l.dataType match {
    // TODO: Uncomment. AtomicType is protected-access.
    // TODO: Expressions may be Unevaluable. Figure out how to evaluate `GreaterThan`.
    // case _: org.apache.spark.sql.types.AtomicType => GreaterThan(l, r).eval().asInstanceOf[Boolean]
    case _ => false
  }

  private def checkBoundary(b: Expression, location: String): TypeCheckResult = b match {
    case _: GpuSpecialFrameBoundary => TypeCheckSuccess
    case e: Expression if !e.foldable =>
      TypeCheckFailure(s"Window frame $location bound '$e' is not a literal.")
    // TODO: Uncomment. AbstractDataType::acceptsType() has protected access.
    // case e: Expression if !frameType.inputType.acceptsType(e.dataType) =>
    //   TypeCheckFailure(
    //     s"The data type of the $location bound '${e.dataType.catalogString}' does not match " +
    //       s"the expected data type '${frameType.inputType.simpleString}'.")
    case _ => TypeCheckSuccess
  }

  private def isValidFrameBoundary(l: Expression, u: Expression): Boolean = {
    (l, u) match {
      case (UnboundedFollowing, _) => false
      case (_, UnboundedPreceding) => false
      case _ => true
    }
  }
}

case class GpuSpecialFrameBoundary(boundary : SpecialFrameBoundary) extends GpuExpression with GpuUnevaluable {
  override def children : Seq[Expression] = Nil
  override def dataType: DataType = NullType
  override def foldable: Boolean = false
  override def nullable: Boolean = false

  def value : Int = {
    boundary match {
      case UnboundedPreceding => Int.MinValue
      case UnboundedFollowing => Int.MaxValue
      case CurrentRow => 0
      case anythingElse =>  throw new UnsupportedOperationException(s"Unsupported window-bound ${anythingElse}!")
    }
  }
}

