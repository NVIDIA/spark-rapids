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

import java.util.concurrent.TimeUnit

import scala.language.{existentials, implicitConversions}

import ai.rapids.cudf
import ai.rapids.cudf.{Aggregation, BinaryOp, ColumnVector, DType, GroupByScanAggregation, RollingAggregation, RollingAggregationOnColumn, Scalar, ScanAggregation}
import com.nvidia.spark.rapids.GpuOverrides.wrapExpr

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.rapids.{GpuAggregateExpression, GpuCreateNamedStruct}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

class GpuWindowExpressionMeta(
    windowExpression: WindowExpression,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: DataFromReplacementRule)
  extends ExprMeta[WindowExpression](windowExpression, conf, parent, rule) {

  private def getAndCheckRowBoundaryValue(boundary: Expression) : Int = boundary match {
    case literal: Literal =>
      literal.dataType match {
        case IntegerType =>
          literal.value.asInstanceOf[Int]
          literal.value.asInstanceOf[Int]
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
            val lower = getAndCheckRowBoundaryValue(spec.lower)
            val upper = getAndCheckRowBoundaryValue(spec.upper)
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
            // even for columns that are not time type columns. We can switch this to row
            // based iff the ranges we are looking at both unbounded.
            if (spec.isUnbounded) {
              // this is okay because we will translate it to be a row query
            } else {
              // check whether order by column is supported or not
              val orderSpec = wrapped.windowSpec.orderSpec
              if (orderSpec.length > 1) {
                // We only support a single order by column
                willNotWorkOnGpu("only a single date/time or integral (Boolean exclusive)" +
                  "based column in window range functions is supported")
              }
              val orderByTypeSupported = orderSpec.forall { so =>
                so.dataType match {
                  case ByteType | ShortType | IntegerType | LongType |
                       DateType | TimestampType => true
                  case _ => false
                }
              }
              if (!orderByTypeSupported) {
                willNotWorkOnGpu(s"the type of orderBy column is not supported in a window" +
                  s" range function, found ${orderSpec.head.dataType}")
              }

              def checkRangeBoundaryConfig(dt: DataType): Unit = {
                dt match {
                  case ByteType => if (!conf.isRangeWindowByteEnabled) willNotWorkOnGpu(
                    s"Range window frame is not 100% compatible when the order by type is " +
                      s"byte and the range value calculated has overflow. " +
                      s"To enable it please set ${RapidsConf.ENABLE_RANGE_WINDOW_BYTES} to true.")
                  case ShortType => if (!conf.isRangeWindowShortEnabled) willNotWorkOnGpu(
                    s"Range window frame is not 100% compatible when the order by type is " +
                      s"short and the range value calculated has overflow. " +
                      s"To enable it please set ${RapidsConf.ENABLE_RANGE_WINDOW_SHORT} to true.")
                  case IntegerType => if (!conf.isRangeWindowIntEnabled) willNotWorkOnGpu(
                    s"Range window frame is not 100% compatible when the order by type is " +
                      s"int and the range value calculated has overflow. " +
                      s"To enable it please set ${RapidsConf.ENABLE_RANGE_WINDOW_INT} to true.")
                  case LongType => if (!conf.isRangeWindowLongEnabled) willNotWorkOnGpu(
                    s"Range window frame is not 100% compatible when the order by type is " +
                      s"long and the range value calculated has overflow. " +
                      s"To enable it please set ${RapidsConf.ENABLE_RANGE_WINDOW_LONG} to true.")
                  case _ => // never reach here
                }
              }

              // check whether the boundaries are supported or not.
              Seq(spec.lower, spec.upper).foreach {
                case l @ Literal(_, ByteType | ShortType | IntegerType | LongType) =>
                  checkRangeBoundaryConfig(l.dataType)
                case Literal(ci: CalendarInterval, CalendarIntervalType) =>
                  // interval is only working for TimeStampType
                  if (ci.months != 0) {
                    willNotWorkOnGpu("interval months isn't supported")
                  }
                case UnboundedFollowing | UnboundedPreceding | CurrentRow =>
                case anythings =>
                  willNotWorkOnGpu(s"the type of boundary is not supported in a window range" +
                  s" function, found $anythings")
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
  extends GpuUnevaluable {

  override def children: Seq[Expression] = windowFunction :: windowSpec :: Nil

  override def dataType: DataType = windowFunction.dataType

  override def foldable: Boolean = windowFunction.foldable

  override def nullable: Boolean = windowFunction.nullable

  override def toString: String = s"$windowFunction $windowSpec"

  override def sql: String = windowFunction.sql + " OVER " + windowSpec.sql

  lazy val normalizedFrameSpec: GpuSpecifiedWindowFrame = {
    val fs = windowFrameSpec.canonicalized.asInstanceOf[GpuSpecifiedWindowFrame]
    fs.frameType match {
      case RangeFrame if fs.isUnbounded =>
        GpuSpecifiedWindowFrame(RowFrame, fs.lower, fs.upper)
      case _ => fs
    }
  }

  private val windowFrameSpec = windowSpec.frameSpecification.asInstanceOf[GpuSpecifiedWindowFrame]
  lazy val wrappedWindowFunc: GpuWindowFunction = windowFunction match {
    case func: GpuWindowFunction => func
    case agg: GpuAggregateExpression => agg.aggregateFunction match {
      case func: GpuWindowFunction => func
      case other =>
        throw new IllegalStateException(s"${other.getClass} is not a supported window aggregation")
    }
    case other =>
      throw new IllegalStateException(s"${other.getClass} is not a supported window function")
  }

  private[this] lazy val optimizedRunningWindow: Option[GpuRunningWindowFunction] = {
    if (normalizedFrameSpec.frameType == RowFrame &&
        GpuWindowExec.isRunningWindow(windowSpec) &&
        wrappedWindowFunc.isInstanceOf[GpuRunningWindowFunction]) {
      val runningWin = wrappedWindowFunc.asInstanceOf[GpuRunningWindowFunction]
      val isSupported = if (windowSpec.partitionSpec.isEmpty) {
        runningWin.isScanSupported
      } else {
        runningWin.isGroupByScanSupported
      }
      if (isSupported) {
        Some(runningWin)
      } else {
        None
      }
    } else {
      None
    }
  }

  lazy val isOptimizedRunningWindow: Boolean = optimizedRunningWindow.isDefined

  def initialProjections(isRunningBatched: Boolean): Seq[Expression] = {
    val running = optimizedRunningWindow
    if (running.isDefined) {
      val r = running.get
      if (windowSpec.partitionSpec.isEmpty) {
        r.scanInputProjection(isRunningBatched)
      } else {
        r.groupByScanInputProjection(isRunningBatched)
      }
    } else {
      wrappedWindowFunc.asInstanceOf[GpuAggregateWindowFunction].windowInputProjection
    }
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

        val value: Long = bounds match {
          case Literal(value, ByteType) => value.asInstanceOf[Byte].toLong
          case Literal(value, ShortType) => value.asInstanceOf[Short].toLong
          case Literal(value, IntegerType) => value.asInstanceOf[Int].toLong
          case Literal(value, LongType) => value.asInstanceOf[Long]
          case Literal(ci: CalendarInterval, CalendarIntervalType) =>
            if (ci.months != 0) {
              willNotWorkOnGpu("interval months isn't supported")
            }
            // return the total microseconds
            try {
              Math.addExact(
                Math.multiplyExact(ci.days.toLong, TimeUnit.DAYS.toMicros(1)),
                ci.microseconds)
            } catch {
              case _: ArithmeticException =>
                willNotWorkOnGpu("windows over timestamps are converted to microseconds " +
                  s"and $ci is too large to fit")
                if (isLower) -1 else 1 // not check again
            }
          case _ =>
            willNotWorkOnGpu(s"Bounds for Range-based window frames must be specified in Integral" +
            s" type (Boolean exclusive) or CalendarInterval. Found ${bounds.dataType}")
            if (isLower) -1 else 1 // not check again
        }

        if (isLower && value > 0) {
          Some(s"Lower-bounds ahead of current row is not supported. Found: $value")
        } else if (!isLower && value < 0) {
          Some(s"Upper-bounds behind current row is not supported. Found: $value")
        } else {
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
    case u: UnaryMinus => u.child.sql + " PRECEDING"
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

  /**
   * Maps boundary to an Int value that in some cases can be used to build up the window options
   * for a window aggregation. UnboundedPreceding and UnboundedFollowing produce Int.MinValue and
   * Int.MaxValue respectively. In row based operations this should be fine because we cannot have
   * a batch with that many rows in it anyways. For range based queries isUnbounded should be
   * called too, to properly interpret the data. CurrentRow produces 0 which works for both row and
   * range based queries.
   */
  def value : Int = {
    boundary match {
      case UnboundedPreceding => Int.MinValue
      case UnboundedFollowing => Int.MaxValue
      case CurrentRow => 0
      case anythingElse =>
        throw new UnsupportedOperationException(s"Unsupported window-bound $anythingElse!")
    }
  }

  def isUnbounded: Boolean = {
    boundary match {
      case UnboundedPreceding | UnboundedFollowing => true
      case _ => false
    }
  }
}

// This is here for now just to tag an expression as being a GpuWindowFunction and match
// Spark. This may expand in the future if other types of window functions show up.
trait GpuWindowFunction extends GpuUnevaluable

/**
 * GPU Counterpart of `AggregateWindowFunction`.
 * On the CPU this would extend `DeclarativeAggregate` and use the provided methods
 * to build up the expressions need to produce a result. For window operations we do it
 * in a single pass, where all of the data is available so instead we have out own set of
 * expressions.
 */
trait GpuAggregateWindowFunction extends GpuWindowFunction {
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
  def windowAggregation(inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn
}

/**
 * A window function that is optimized for running windows using the cudf scan and group by
 * scan operations. In some cases, like row number and rank, Spark only supports them as running
 * window operations. This is why it directly extends GpuWindowFunction because it can be a stand
 * alone window function. In all other cases it should be combined with GpuAggregateWindowFunction
 * to provide a fully functional window operation. It should be noted that WindowExec tries to
 * deduplicate input projections and aggregations to reduce memory usage. Because of tracking
 * requirements it is required that there is a one to one relationship between an input projection
 * and a corresponding aggregation.
 */
trait GpuRunningWindowFunction extends GpuWindowFunction {
  /**
   * Get the input projections for a group by scan. This corresponds to a running window with
   * a partition by clause. The partition keys will be used as the grouping keys.
   * @param isRunningBatched is this for a batched running window that will use a fixer or not?
   * @return the input expressions that will be aggregated using the result from
   *         `groupByScanAggregation`
   */
  def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression]

  /**
   * Get the aggregations to perform on the results of `groupByScanInputProjection`. The
   * aggregations will be zipped with the values to produce the output.
   * @param isRunningBatched is this for a batched running window that will use a fixer or not?
   * @return the aggregations to perform as a group by scan.
   */
  def groupByScanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]]

  /**
   * Should a group by scan be run or not. This should never return false unless this is also an
   * instance of `GpuAggregateWindowFunction` so the window code can fall back to it for
   * computation.
   */
  def isGroupByScanSupported = true

  /**
   * Get the input projections for a scan. This corresponds to a running window without a
   * partition by clause.
   * @param isRunningBatched is this for a batched running window that will use a fixer or not?
   * @return the input expressions that will be aggregated using the result from
   *         `scanAggregation`
   */
  def scanInputProjection(isRunningBatched: Boolean): Seq[Expression]

  /**
   * Get the aggregations to perform on the results of `scanInputProjection`. The
   * aggregations will be zipped with the values to produce the output.
   * @param isRunningBatched is this for a batched running window that will use a fixer or not?
   * @return the aggregations to perform as a group by scan.
   */
  def scanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]]

  /**
   * Should a group by scan be run or not. This should never return false unless this is also an
   * instance of `GpuAggregateWindowFunction` so the window code can fall back to it for
   * computation.
   */
  def isScanSupported = true

  /**
   * Provides a way to combine the result of multiple aggregations into a final value. By
   * default it requires that there is a single aggregation and works as just a pass through.
   * @param isRunningBatched is this for a batched running window that will use a fixer or not?
   * @param cols the columns to be combined
   * @return the result of combining these together.
   */
  def scanCombine(isRunningBatched: Boolean, cols: Seq[ColumnVector]): ColumnVector = {
    require(cols.length == 1, "Only one column is supported fro the default scan combine")
    cols.head.incRefCount()
  }
}

/**
 * Provides a way to process running window operations without needing to buffer and split the
 * batches on partition by boundaries. When this happens part of a partition by key set may
 * have been processed in the last batch, and the rest of it will need to be updated. For example
 * if we are doing a running min operation. We may first get in something like
 * <code>
 * PARTS:  1, 1,  2, 2
 * VALUES: 2, 3, 10, 9
 * </code>
 *
 * The output of processing this would result in a new column that would look like
 * <code>
 *   MINS: 2, 2, 10, 9
 * </code>
 *
 * But we don't know if the group with 2 in PARTS is done or not. So the fixer saved
 * the last value in MINS, which is a 9. When the next batch shows up
 *
 * <code>
 *  PARTS:  2,  2,  3,  3
 * VALUES: 11,  5, 13, 14
 * </code>
 *
 * We generate the window result again and get
 *
 * <code>
 *    MINS: 11, 5, 13, 13
 * </code>
 *
 * But we cannot output this yet because there may have been overlap with the previous batch.
 * The framework will figure that out and pass data into `fixUp` to do the fixing. It will
 * pass in MINS, and also a column of boolean values `true, true, false, false` to indicate
 * which rows overlapped with the previous batch.  In our min example `fixUp` will do a min
 * between the last value in the previous batch and the values that could overlap with it.
 *
 * <code>
 * RESULT: 9, 5, 13, 13
 * </code>
 * which can be output.
 */
trait BatchedRunningWindowFixer extends AutoCloseable {
  /**
   * Fix up `windowedColumnOutput` with any stored state from previous batches.
   * Like all window operations the input data will have been sorted by the partition
   * by columns and the order by columns.
   *
   * @param samePartitionMask a mask that uses `true` to indicate the row
   *                          is for the same partition by keys that was the last row in the
   *                          previous batch or `false` to indicate it is not. If this is known
   *                          to be all true or all false values a single boolean is used. If
   *                          it can change for different rows than a column vector is provided.
   *                          Only values that are for the same partition by keys should be
   *                          modified. Because the input data is sorted by the partition by
   *                          columns the boolean values will be grouped together.
   * @param sameOrderMask a mask just like `samePartitionMask` but for ordering. This happens
   *                      for some operations like `rank` and `dense_rank` that use the ordering
   *                      columns in a row based query. This is not needed for all fixers and is not
   *                      free to calculate, so you must set `needsOrderMask` to true if you are
   *                      going to use it.
   * @param windowedColumnOutput the output of the windowAggregation without anything
   *                             fixed/modified. This should not be closed by `fixUp` as it will be
   *                             handled by the framework.
   * @return a fixed ColumnVector that was with outputs updated for items that were in the same
   *         group by key as the last row in the previous batch.
   */
  def fixUp(
      samePartitionMask: Either[cudf.ColumnVector, Boolean],
      sameOrderMask: Option[Either[cudf.ColumnVector, Boolean]],
      windowedColumnOutput: cudf.ColumnView): cudf.ColumnVector

  def needsOrderMask: Boolean = false

  protected def incRef(col: cudf.ColumnView): cudf.ColumnVector = col match {
    case cv: cudf.ColumnVector => cv.incRefCount()
    case _ => col.copyToColumnVector()
  }
}

/**
 * For many operations a running window (unbounded preceding to current row) can
 * process the data without dividing the data up into batches that contain all of the data
 * for a given group by key set. Instead we store a small amount of state from a previous result
 * and use it to fix the final result. This is a memory optimization.
 */
trait GpuBatchedRunningWindowWithFixer {

  /**
   * Get a new class that can be used to fix up batched RunningWindowOperations.
   */
  def newFixer(): BatchedRunningWindowFixer
}

/**
 * This class fixes up batched running windows by performing a binary op on the previous value and
 * those in the the same partition by key group. It does not deal with nulls, so it works for things
 * like row_number and count, that cannot produce nulls, or for NULL_MIN and NULL_MAX that do the
 * right thing when they see a null.
 */
class BatchedRunningWindowBinaryFixer(val binOp: BinaryOp, val name: String)
    extends BatchedRunningWindowFixer with Arm with Logging {
  private var previousResult: Option[Scalar] = None

  def getPreviousResult: Option[Scalar] = previousResult

  def updateState(finalOutputColumn: cudf.ColumnVector): Unit = {
    logDebug(s"$name: updateState from $previousResult to...")
    previousResult.foreach(_.close)
    previousResult =
      Some(finalOutputColumn.getScalarElement(finalOutputColumn.getRowCount.toInt - 1))
    logDebug(s"$name: ... $previousResult")
  }

  override def fixUp(samePartitionMask: Either[cudf.ColumnVector, Boolean],
      sameOrderMask: Option[Either[cudf.ColumnVector, Boolean]],
      windowedColumnOutput: cudf.ColumnView): cudf.ColumnVector = {
    logDebug(s"$name: fix up $previousResult $samePartitionMask")
    val ret = (previousResult, samePartitionMask) match {
      case (None, _) => incRef(windowedColumnOutput)
      case (Some(prev), scala.util.Right(mask)) =>
        if (mask) {
          windowedColumnOutput.binaryOp(binOp, prev, prev.getType)
        } else {
          // The mask is all false so do nothing
          incRef(windowedColumnOutput)
        }
      case (Some(prev), scala.util.Left(mask)) =>
        withResource(windowedColumnOutput.binaryOp(binOp, prev, prev.getType)) { updated =>
          mask.ifElse(updated, windowedColumnOutput)
        }
    }
    updateState(ret)
    ret
  }

  override def close(): Unit = {
    previousResult.foreach(_.close())
    previousResult = None
  }
}

/**
 * This class fixes up batched running windows for sum. Sum is a lot like other binary op
 * fixers, but it has to special case nulls and that is not super generic.  In the future we
 * might be able to make this more generic but we need to see what the use case really is.
 */
class SumBinaryFixer extends BatchedRunningWindowFixer with Arm with Logging {
  private val name = "sum"
  private val binOp = BinaryOp.ADD
  private var previousResult: Option[Scalar] = None

  def updateState(finalOutputColumn: cudf.ColumnVector): Unit = {
    logDebug(s"$name: updateState from $previousResult to...")
    previousResult.foreach(_.close)
    previousResult =
      Some(finalOutputColumn.getScalarElement(finalOutputColumn.getRowCount.toInt - 1))
    logDebug(s"$name: ... $previousResult")
  }

  private def makeZeroScalar(dt: DType): Scalar = dt match {
    case DType.INT8 => Scalar.fromByte(0.toByte)
    case DType.INT16 => Scalar.fromShort(0.toShort)
    case DType.INT32 => Scalar.fromInt(0)
    case DType.INT64=> Scalar.fromLong(0)
    case DType.FLOAT32 => Scalar.fromFloat(0.0f)
    case DType.FLOAT64 => Scalar.fromDouble(0.0)
    case dec if dec.isDecimalType =>
      if (dec.getTypeId == DType.DTypeEnum.DECIMAL32) {
        Scalar.fromDecimal(dec.getScale, 0)
      } else {
        Scalar.fromDecimal(dec.getScale, 0L)
      }
    case other =>
      throw new IllegalArgumentException(s"Making a zero scalar for $other is not supported")
  }

  override def fixUp(samePartitionMask: Either[cudf.ColumnVector, Boolean],
      sameOrderMask: Option[Either[cudf.ColumnVector, Boolean]],
      windowedColumnOutput: cudf.ColumnView): cudf.ColumnVector = {
    logDebug(s"$name: fix up $previousResult $samePartitionMask")
    val ret = (previousResult, samePartitionMask) match {
      case (None, _) => incRef(windowedColumnOutput)
      case (Some(prev), scala.util.Right(mask)) =>
        if (mask) {
          // ADD is not null safe, so we have to replace NULL with 0 if and only if prev is also
          // not null
          if (prev.isValid) {
            val nullsReplaced = withResource(windowedColumnOutput.isNull) { nulls =>
              withResource(makeZeroScalar(windowedColumnOutput.getType)) { zero =>
                nulls.ifElse(zero, windowedColumnOutput)
              }
            }
            withResource(nullsReplaced) { nullsReplaced =>
              nullsReplaced.binaryOp(binOp, prev, prev.getType)
            }
          } else {
            // prev is NULL but NULL + something == NULL which we don't want
            incRef(windowedColumnOutput)
          }
        } else {
          // The mask is all false so do nothing
          incRef(windowedColumnOutput)
        }
      case (Some(prev), scala.util.Left(mask)) =>
        if (prev.isValid) {
          val nullsReplaced = withResource(windowedColumnOutput.isNull) { nulls =>
            withResource(nulls.and(mask)) { shouldReplace =>
              withResource(makeZeroScalar(windowedColumnOutput.getType)) { zero =>
                shouldReplace.ifElse(zero, windowedColumnOutput)
              }
            }
          }
          withResource(nullsReplaced) { nullsReplaced =>
            withResource(nullsReplaced.binaryOp(binOp, prev, prev.getType)) { updated =>
              mask.ifElse(updated, windowedColumnOutput)
            }
          }
        } else {
          // prev is NULL but NULL + something == NULL which we don't want
          incRef(windowedColumnOutput)
        }
    }
    updateState(ret)
    ret
  }

  override def close(): Unit = {
    previousResult.foreach(_.close())
    previousResult = None
  }
}

/**
 * Rank is more complicated than DenseRank to fix. This is because there are gaps in the
 * rank values. The rank value of each group is row number of the first row in the group.
 * So values in the same partition group but not the same ordering are fixed by adding
 * the row number from the previous batch to them. If they are a part of the same ordering and
 * part of the same partition, then we need to just put in the previous rank value.
 *
 * Because we need both a rank and a row number to fix things up the input to this is a struct
 * containing a rank column as the first entry and a row number column as the second entry. This
 * happens in the `scanCombine` method for GpuRank.  It is a little ugly but it works to maintain
 * the requirement that the input to the fixer is a single column.
 */
class RankFixer extends BatchedRunningWindowFixer with Arm with Logging {
  import RankFixer._

  // We have to look at row number as well as rank.  This fixer is the same one that `GpuRowNumber`
  // uses.
  private[this] val rowNumFixer = new BatchedRunningWindowBinaryFixer(BinaryOp.ADD, "row_number")
  // convenience method to get access to the previous row number.
  private[this] def previousRow: Option[Scalar] = rowNumFixer.getPreviousResult
  // The previous rank value
  private[this] var previousRank: Option[Scalar] = None

  override def needsOrderMask: Boolean = true

  override def fixUp(
      samePartitionMask: Either[cudf.ColumnVector, Boolean],
      sameOrderMask: Option[Either[cudf.ColumnVector, Boolean]],
      windowedColumnOutput: cudf.ColumnView): cudf.ColumnVector = {
    assert(windowedColumnOutput.getType == DType.STRUCT)
    assert(windowedColumnOutput.getNumChildren == 2)
    val initialRank = windowedColumnOutput.getChildColumnView(0)
    val initialRowNum = windowedColumnOutput.getChildColumnView(1)
    val ret = (previousRank, samePartitionMask) match {
      case (None, _) => incRef(initialRank)
      case (Some(prevRank), scala.util.Right(partMask)) =>
        if (partMask) {
          // We are in the same partition as the last part of the batch so we have to look at the
          // ordering to know what to do.
          sameOrderMask.get match {
            case scala.util.Left(orderMask) =>
              fixRankSamePartition(initialRank, orderMask, prevRank, previousRow)
            case scala.util.Right(orderMask) =>
              // Technically I think this code is unreachable because the only time a constant
              // true or false is returned is if the order by column is empty or if the parts mask
              // is false. Spark requires there to be order by columns and we already know that
              // the partition mask is true. But it is small so just to be on the safe side.
              if (orderMask) {
                // it is all for the same partition and order so it is the same value as the
                // previous rank
                cudf.ColumnVector.fromScalar(prevRank, initialRank.getRowCount.toInt)
              } else {
                fixRankSamePartDifferentOrdering(initialRank, previousRow)
              }
          }
        } else {
          incRef(initialRank)
        }
      case (Some(prevRank), scala.util.Left(partMask)) =>
        sameOrderMask.get match {
          case scala.util.Left(orderMask) =>
            // Fix up the data for the same partition and keep the rest unchanged.
            val samePart = fixRankSamePartition(initialRank, orderMask, prevRank, previousRow)
            withResource(samePart) { samePart =>
              partMask.ifElse(samePart, initialRank)
            }
          case scala.util.Right(_) =>
            // The framework guarantees that the order by mask is a subset of the group by mask
            // So if the group by mask in not a constant, then the order by mask also cannot be
            // a constant
            throw new IllegalStateException(
              "Internal Error the order mask is not a subset of the part mask")
        }
    }

    // We just want to update the state for row num
    rowNumFixer.fixUp(samePartitionMask, sameOrderMask, initialRowNum).close()
    logDebug(s"rank: updateState from $previousRank to...")
    previousRank.foreach(_.close)
    previousRank = Some(ret.getScalarElement(ret.getRowCount.toInt - 1))
    logDebug(s"rank/row: ... $previousRank $previousRow")
    ret
  }

  override def close(): Unit = {
    previousRank.foreach(_.close())
    previousRank = None
  }
}

object RankFixer extends Arm {
  private def fixRankSamePartDifferentOrdering(rank: cudf.ColumnView,
      previousRow: Option[Scalar]): cudf.ColumnVector =
    rank.add(previousRow.get, rank.getType)

  private def fixRankSamePartition(rank: cudf.ColumnView,
      orderMask: cudf.ColumnView,
      prevRank: Scalar,
      previousRow: Option[Scalar]): cudf.ColumnVector = {
    withResource(fixRankSamePartDifferentOrdering(rank, previousRow)) { partlyFixed =>
      orderMask.ifElse(prevRank, partlyFixed)
    }
  }
}

/**
 * Fix up dense rank batches. A dense rank has no gaps in the rank values.
 * The rank corresponds to the ordering columns(s) equality. So when a batch
 * finishes and another starts that split can either be at the beginning of a
 * new order by section or part way through one. If it is at the beginning, then
 * like row number we want to just add in the previous value and go on. If
 * it was part way through, then we want to add in the previous value minus 1.
 * The minus one is to pick up where we left off.
 * If anything is outside of a continues partition by group then we just keep
 * those values unchanged.
 */
class DenseRankFixer extends BatchedRunningWindowFixer with Arm with Logging {
  import DenseRankFixer._

  private var previousRank: Option[Scalar] = None

  override def needsOrderMask: Boolean = true

  override def fixUp(
      samePartitionMask: Either[cudf.ColumnVector, Boolean],
      sameOrderMask: Option[Either[cudf.ColumnVector, Boolean]],
      windowedColumnOutput: cudf.ColumnView): cudf.ColumnVector = {
    val ret = (previousRank, samePartitionMask) match {
      case (None, _) => incRef(windowedColumnOutput)
      case (Some(prevRank), scala.util.Right(partMask)) =>
        if (partMask) {
          // We are in the same partition as the last part of the batch so we have to look at the
          // ordering to know what to do.
          sameOrderMask.get match {
            case scala.util.Left(orderMask) =>
              // It is all in the same partition so just fix it for that.
              fixRankInSamePartition(windowedColumnOutput, orderMask, prevRank)
            case scala.util.Right(orderMask) =>
              // Technically I think this code is unreachable because the only time a constant
              // true or false is returned is if the order by column is empty or if the parts mask
              // is false. Spark requires there to be order by columns and we already know that
              // the partition mask is true. But it is small so just to be on the safe side.
              if (orderMask) {
                // Everything in this batch is part of the same ordering group too.
                // We don't add previous rank - 1, because the current value for everything is 1
                // so rank - 1 + 1 == rank.
                cudf.ColumnVector.fromScalar(prevRank, windowedColumnOutput.getRowCount.toInt)
              } else {
                // Same partition but hit an order by boundary.
                addPrevRank(windowedColumnOutput, prevRank)
              }
          }
        } else {
          // Different partition by group so this is a NOOP
          incRef(windowedColumnOutput)
        }
      case (Some(prevRank), scala.util.Left(partMask)) =>
        sameOrderMask.get match {
          case scala.util.Left(orderMask) =>
            // Fix up the data for the same partition and keep the rest unchanged.
            val samePart = fixRankInSamePartition(windowedColumnOutput, orderMask, prevRank)
            withResource(samePart) { samePart =>
              partMask.ifElse(samePart, windowedColumnOutput)
            }
          case scala.util.Right(_) =>
            // The framework guarantees that the order by mask is a subset of the group by mask
            // So if the group by mask in not a constant, then the order by mask also cannot be
            // a constant
            throw new IllegalStateException(
              "Internal Error the order mask is not a subset of the part mask")
        }
    }

    logDebug(s"dense rank: updateState from $previousRank to...")
    previousRank.foreach(_.close)
    previousRank = Some(ret.getScalarElement(ret.getRowCount.toInt - 1))
    logDebug(s"dense rank: ... $previousRank")

    ret
  }

  override def close(): Unit = {
    previousRank.foreach(_.close())
    previousRank = None
  }
}

object DenseRankFixer extends Arm {
  private[this] def isFirstTrue(cv: cudf.ColumnView): Boolean = {
    withResource(cv.getScalarElement(0)) { scalar =>
      scalar.getBoolean
    }
  }

  private def addPrevRank(cv: cudf.ColumnView, prevRank: Scalar): cudf.ColumnVector =
    cv.add(prevRank)

  private[this] def addPrevRankMinusOne(cv: cudf.ColumnView,
      prevRank: Scalar): cudf.ColumnVector = {
    withResource(addPrevRank(cv, prevRank)) { prev =>
      withResource(Scalar.fromInt(1)) { one =>
        prev.sub(one)
      }
    }
  }

  private def fixRankInSamePartition(
      rank: cudf.ColumnView,
      orderMask: cudf.ColumnView,
      prevRank: Scalar): cudf.ColumnVector = {
    // This is a little ugly, but the only way to tell if we are part of the previous order by
    // group or not is to look at the orderMask. In this case we check if the first value in the
    // mask is true.
    val added = if (isFirstTrue(orderMask)) {
      addPrevRankMinusOne(rank, prevRank)
    } else {
      addPrevRank(rank, prevRank)
    }
    withResource(added) { added =>
      orderMask.ifElse(prevRank, added)
    }
  }
}

/**
 * Rank is a special window operation where it is only supported as a running window. In cudf
 * it is only supported as a scan and a group by scan. But there are special requirements beyond
 * that when doing the computation as a running batch. To fix up each batch it needs both the rank
 * and the row number. To make this work and be efficient there is different behavior for batched
 * running window vs non-batched. If it is for a running batch we include the row number values,
 * in both the initial projections and in the corresponding aggregations. Then we combine them
 * into a struct column in `scanCombine` before it is passed on to the `RankFixer`. If it is not
 * a running batch, then we drop the row number part because it is just not needed.
 * @param children the order by columns.
 * @note this is a running window only operator.
 */
case class GpuRank(children: Seq[Expression]) extends GpuRunningWindowFunction
    with GpuBatchedRunningWindowWithFixer {
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression] = {
    // The requirement is that each input projection has a corresponding aggregation
    // associated with it. This also fits with how rank works in cudf, where the input is also
    // a single column. If there are multiple order by columns we wrap them in a struct.
    // This is not ideal from a memory standpoint, and in the future we might be able to fix this
    // with a ColumnView, but for now with how the Java cudf APIs work it would be hard to work
    // around.
    val orderedBy = if (children.length == 1) {
      children.head
    } else {
      val childrenWithNames = children.zipWithIndex.flatMap {
        case (expr, idx) => Seq(GpuLiteral(idx.toString, StringType), expr)
      }
      GpuCreateNamedStruct(childrenWithNames)
    }

    // If the computation is for a batched running window then we need a row number too
    if (isRunningBatched) {
      Seq(orderedBy, GpuLiteral(1, IntegerType))
    } else {
      Seq(orderedBy)
    }
  }

  override def groupByScanAggregation(
      isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] = {
    if (isRunningBatched) {
      // We are computing both rank and row number so we can fix it up at the end
      Seq(AggAndReplace(GroupByScanAggregation.rank(), None),
        AggAndReplace(GroupByScanAggregation.sum(), None))
    } else {
      // Not batched just do the rank
      Seq(AggAndReplace(GroupByScanAggregation.rank(), None))
    }
  }

  override def scanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    groupByScanInputProjection(isRunningBatched)
  override def scanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]] = {
    if (isRunningBatched) {
      // We are computing both rank and row number so we can fix it up at the end
      Seq(AggAndReplace(ScanAggregation.rank(), None), AggAndReplace(ScanAggregation.sum(), None))
    } else {
      // Not batched just do the rank
      Seq(AggAndReplace(ScanAggregation.rank(), None))
    }
  }

  override def scanCombine(isRunningBatched: Boolean, cols: Seq[ColumnVector]): ColumnVector = {
    if (isRunningBatched) {
      // When the data is batched we are using the fixer, and it needs rank and row number
      // to calculate the final value
      assert(cols.length == 2)
      ColumnVector.makeStruct(cols: _*)
    } else {
      assert(cols.length == 1)
      cols.head.incRefCount()
    }
  }

  override def newFixer(): BatchedRunningWindowFixer = new RankFixer()
}

/**
 * Dense Rank is a special window operation where it is only supported as a running window. In cudf
 * it is only supported as a scan and a group by scan.
 * @param children the order by columns.
 * @note this is a running window only operator
 */
case class GpuDenseRank(children: Seq[Expression]) extends GpuRunningWindowFunction
    with GpuBatchedRunningWindowWithFixer {
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression] = {
    // The requirement is that each input projection has a corresponding aggregation
    // associated with it. This also fits with how rank works in cudf, where the input is also
    // a single column. If there are multiple order by columns we wrap them in a struct.
    // This is not ideal from a memory standpoint, and in the future we might be able to fix this
    // with a ColumnView, but for now with how the Java cudf APIs work it would be hard to work
    // around.
    if (children.length == 1) {
      Seq(children.head)
    } else {
      val childrenWithNames = children.zipWithIndex.flatMap {
        case (expr, idx) => Seq(GpuLiteral(idx.toString, StringType), expr)
      }
      Seq(GpuCreateNamedStruct(childrenWithNames))
    }
  }

  override def groupByScanAggregation(
      isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] =
    Seq(AggAndReplace(GroupByScanAggregation.denseRank(), None))

  override def scanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    groupByScanInputProjection(isRunningBatched)

  override def scanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]] =
    Seq(AggAndReplace(ScanAggregation.denseRank(), None))

  override def newFixer(): BatchedRunningWindowFixer = new DenseRankFixer()
}

/**
 * The row number in the window.
 * @note this is a running window only operator
 */
case object GpuRowNumber extends GpuRunningWindowFunction
    with GpuBatchedRunningWindowWithFixer {
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = Nil

  override def newFixer(): BatchedRunningWindowFixer =
    new BatchedRunningWindowBinaryFixer(BinaryOp.ADD, "row_number")

  // For group by scans cudf does not support ROW_NUMBER so we will do a SUM
  // on a column of 1s. We could do a COUNT_ALL too, but it would not be as consistent
  // with the non group by scan
  override def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    Seq(GpuLiteral(1, IntegerType))

  override def groupByScanAggregation(
      isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] =
    Seq(AggAndReplace(GroupByScanAggregation.sum(), None))

  // For regular scans cudf does not support ROW_NUMBER, nor does it support COUNT_ALL
  // so we will do a SUM on a column of 1s
  override def scanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    groupByScanInputProjection(isRunningBatched)
  override def scanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]] =
    Seq(AggAndReplace(ScanAggregation.sum(), None))
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

  override val childExprs: Seq[BaseExprMeta[_]] = Seq(input, offset, default)

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

trait GpuOffsetWindowFunction extends GpuAggregateWindowFunction {
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
    extends GpuOffsetWindowFunction {

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn = {
    val in = inputs.toArray
    if (in.length > 1) {
      // Has a default
      RollingAggregation.lead(parsedOffset, in(1)._1).onColumn(in.head._2)
    } else {
      RollingAggregation.lead(parsedOffset).onColumn(in.head._2)
    }
  }
}

case class GpuLag(input: Expression, offset: Expression, default: Expression)
    extends GpuOffsetWindowFunction {

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn = {
    val in = inputs.toArray
    if (in.length > 1) {
      // Has a default
      RollingAggregation.lag(parsedOffset, in(1)._1).onColumn(in.head._2)
    } else {
      RollingAggregation.lag(parsedOffset).onColumn(in.head._2)
    }
  }
}
