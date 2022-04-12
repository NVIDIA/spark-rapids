/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import java.util.concurrent.TimeUnit

import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, ExprMeta, GpuOverrides, RapidsConf, RapidsMeta}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

abstract class GpuWindowExpressionMetaBase(
    windowExpression: WindowExpression,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_]],
    rule: DataFromReplacementRule)
  extends ExprMeta[WindowExpression](windowExpression, conf, parent, rule) {

  private def getAndCheckRowBoundaryValue(boundary: Expression) : Int = boundary match {
    case literal: Literal =>
      literal.dataType match {
        case IntegerType =>
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

  /** Tag if RangeFrame expression is supported */
  def tagOtherTypesForRangeFrame(bounds: Expression): Unit = {
    willNotWorkOnGpu(s"the type of boundary is not supported in a window range" +
      s" function, found $bounds")
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
              case _: Lead | _: Lag => // ignored we are good
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
                case anythings => tagOtherTypesForRangeFrame(anythings)
              }
            }
        }
      case other =>
        willNotWorkOnGpu(s"only SpecifiedWindowFrame is a supported window-frame specification. " +
            s"Found ${other.prettyName}")
    }
  }
}

abstract class GpuSpecifiedWindowFrameMetaBase(
    windowFrame: SpecifiedWindowFrame,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_]],
    rule: DataFromReplacementRule)
  extends ExprMeta[SpecifiedWindowFrame](windowFrame, conf, parent, rule) {

  // SpecifiedWindowFrame has no associated dataType.
  override val ignoreUnsetDataTypes: Boolean = true

  /**
   * Tag RangeFrame for other types and get the value
   */
  def getAndTagOtherTypesForRangeFrame(bounds : Expression, isLower : Boolean): Long = {
    willNotWorkOnGpu(s"Bounds for Range-based window frames must be specified in Integral" +
      s" type (Boolean exclusive) or CalendarInterval. Found ${bounds.dataType}")
    if (isLower) -1 else 1 // not check again
  }

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
              // Spark 2.x different - no days, just months and microseconds
              // could remove this catch but leaving for now
              /*
              Math.addExact(
                Math.multiplyExact(ci.days.toLong, TimeUnit.DAYS.toMicros(1)),
                ci.microseconds)
              */
              ci.microseconds
            } catch {
              case _: ArithmeticException =>
                willNotWorkOnGpu("windows over timestamps are converted to microseconds " +
                  s"and $ci is too large to fit")
                if (isLower) -1 else 1 // not check again
            }
          case _ => getAndTagOtherTypesForRangeFrame(bounds, isLower)
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
}

class GpuSpecifiedWindowFrameMeta(
    windowFrame: SpecifiedWindowFrame,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_]],
    rule: DataFromReplacementRule)
  extends GpuSpecifiedWindowFrameMetaBase(windowFrame, conf, parent, rule) {}

class GpuWindowExpressionMeta(
    windowExpression: WindowExpression,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_]],
    rule: DataFromReplacementRule)
  extends GpuWindowExpressionMetaBase(windowExpression, conf, parent, rule) {}

object GpuWindowUtil {

  /**
   * Check if the type of RangeFrame is valid in GpuWindowSpecDefinition
   * @param orderSpecType the first order by data type
   * @param ft the first frame boundary data type
   * @return true to valid, false to invalid
   */
  def isValidRangeFrameType(orderSpecType: DataType, ft: DataType): Boolean = {
    (orderSpecType, ft) match {
      case (DateType, IntegerType) => true
      case (TimestampType, CalendarIntervalType) => true
      case (a, b) => a == b
    }
  }

  def getRangeBoundaryValue(boundary: Expression): ParsedBoundary = boundary match {
    case anything => throw new UnsupportedOperationException("Unsupported window frame" +
      s" expression $anything")
  }
}

case class ParsedBoundary(isUnbounded: Boolean, valueAsLong: Long)

class GpuWindowSpecDefinitionMeta(
    windowSpec: WindowSpecDefinition,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_]],
    rule: DataFromReplacementRule)
  extends ExprMeta[WindowSpecDefinition](windowSpec, conf, parent, rule) {

  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    windowSpec.partitionSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    windowSpec.orderSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val windowFrame: BaseExprMeta[WindowFrame] =
    GpuOverrides.wrapExpr(windowSpec.frameSpecification, conf, Some(this))

  override val ignoreUnsetDataTypes: Boolean = true

  override def tagExprForGpu(): Unit = {
    if (!windowSpec.frameSpecification.isInstanceOf[SpecifiedWindowFrame]) {
      willNotWorkOnGpu(s"WindowFunctions without a SpecifiedWindowFrame are unsupported.")
    }
  }
}
