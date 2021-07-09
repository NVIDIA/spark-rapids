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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{Aggregation, AggregationOnColumn, AggregationOverWindow, DType, GroupByOptions, NullPolicy, NvtxColor, ReplacePolicy, ReplacePolicyWithColumn, Scalar, ScanType, Table, WindowOptions}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, AttributeSeq, AttributeSet, CurrentRow, Expression, FrameType, NamedExpression, RangeFrame, RowFrame, SortOrder, UnboundedPreceding}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.rapids.GpuAggregateExpression
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, CalendarIntervalType, DataType, IntegerType, LongType, ShortType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Base class for GPU Execs that implement window functions. This abstracts the method
 * by which the window function's input expressions, partition specs, order-by specs, etc.
 * are extracted from the specific WindowExecType.
 *
 * @tparam WindowExecType The Exec class that implements window functions
 *                        (E.g. o.a.s.sql.execution.window.WindowExec.)
 */
abstract class GpuBaseWindowExecMeta[WindowExecType <: SparkPlan] (windowExec: WindowExecType,
                        conf: RapidsConf,
                        parent: Option[RapidsMeta[_, _, _]],
                        rule: DataFromReplacementRule)
  extends SparkPlanMeta[WindowExecType](windowExec, conf, parent, rule) with Logging {

  /**
   * Extracts window-expression from WindowExecType.
   * The implementation varies, depending on the WindowExecType class.
   */
  def getInputWindowExpressions: Seq[NamedExpression]

  /**
   * Extracts partition-spec from WindowExecType.
   * The implementation varies, depending on the WindowExecType class.
   */
  def getPartitionSpecs: Seq[Expression]

  /**
   * Extracts order-by spec from WindowExecType.
   * The implementation varies, depending on the WindowExecType class.
   */
  def getOrderSpecs: Seq[SortOrder]

  /**
   * Indicates the output column semantics for the WindowExecType,
   * i.e. whether to only return the window-expression result columns (as in some Spark
   * distributions) or also include the input columns (as in Apache Spark).
   */
  def getResultColumnsOnly: Boolean

  val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
    getInputWindowExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    getPartitionSpecs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    getOrderSpecs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  lazy val inputFields: Seq[BaseExprMeta[Attribute]] =
    windowExec.children.head.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {
    // Implementation depends on receiving a `NamedExpression` wrapped WindowExpression.
    windowExpressions.map(meta => meta.wrapped)
      .filter(expr => !expr.isInstanceOf[NamedExpression])
      .foreach(_ => willNotWorkOnGpu(because = "Unexpected query plan with Windowing functions; " +
        "cannot convert for GPU execution. " +
        "(Detail: WindowExpression not wrapped in `NamedExpression`.)"))
  }

  override def convertToGpu(): GpuExec = {
    val resultColumnsOnly = getResultColumnsOnly
    val gpuWindowExpressions = if (resultColumnsOnly) {
      windowExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression])
    } else {
      (inputFields ++ windowExpressions).map(_.convertToGpu().asInstanceOf[NamedExpression])
    }

    val (pre, windowOps, post) = GpuWindowExec.splitAndDedup(gpuWindowExpressions)
    // Order is not important for pre. It is unbound and we are inserting it in.
    val isPreNeeded =
      (AttributeSet(pre.map(_.toAttribute)) -- windowExec.children.head.output).nonEmpty
    // To check if post is needed we first have to remove a layer of indirection that
    // might not be needed. Here we want to maintain order, just to match Spark as closely
    // as possible
    val remappedWindowOps = GpuWindowExec.remapAttributes(windowOps, post)
    val isPostNeeded = remappedWindowOps.length != post.length ||
        remappedWindowOps.zip(post).exists {
          case (w, p) => w.exprId != p.exprId
        }
    val fixedUpWindowOps = if(isPostNeeded) {
      windowOps
    } else {
      remappedWindowOps
    }

    // When we support multiple ways to avoid batching the input data like with
    // https://github.com/NVIDIA/spark-rapids/issues/1860 we should check if all of
    // the operations fit into one of the supported groups and then split them up into
    // multiple execs if they do, so that we can avoid batching on all of them.
    val allBatchedRunning = fixedUpWindowOps.forall {
      case GpuAlias(GpuWindowExpression(func, spec), _) =>
        val isRunningFunc = func match {
          case _: GpuBatchedRunningWindowWithFixer => true
          case GpuAggregateExpression(_: GpuBatchedRunningWindowWithFixer, _, _, _ , _) => true
          case _ => false
        }
        // Running windows are limited to row based queries with a few changes we could make this
        // work for range based queries too https://github.com/NVIDIA/spark-rapids/issues/2708
        isRunningFunc && GpuWindowExec.isRunningWindow(spec)
      case GpuAlias(_: AttributeReference, _) | _: AttributeReference =>
        // We allow pure result columns for running windows
        true
      case other =>
        // This should only happen if we did something wrong in splitting/deduping
        // the window expressions.
        throw new IllegalArgumentException(
          s"Found unexpected expression $other in window exec ${other.getClass}")
    }

    val input = if (isPreNeeded) {
      GpuProjectExec(pre.toList, childPlans.head.convertIfNeeded())
    } else {
      childPlans.head.convertIfNeeded()
    }

    val windowExpr = if (allBatchedRunning) {
      GpuRunningWindowExec(
        fixedUpWindowOps,
        partitionSpec.map(_.convertToGpu()),
        orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
        input)
    } else {
      GpuWindowExec(
        fixedUpWindowOps,
        partitionSpec.map(_.convertToGpu()),
        orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
        input)
    }

    if (isPostNeeded) {
      GpuProjectExec(post.toList, windowExpr)
    } else {
      windowExpr
    }
  }
}

/**
 * Specialization of GpuBaseWindowExecMeta for org.apache.spark.sql.window.WindowExec.
 * This class implements methods to extract the window-expressions, partition columns,
 * order-by columns, etc. from WindowExec.
 */
class GpuWindowExecMeta(windowExec: WindowExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBaseWindowExecMeta[WindowExec](windowExec, conf, parent, rule) {

  /**
   * Fetches WindowExpressions in input `windowExec`, via reflection.
   * As a byproduct, determines whether to return the original input columns,
   * as part of the output.
   *
   * (Spark versions that use `projectList` expect result columns
   * *not* to include the input columns.
   * Apache Spark expects the input columns, before the aggregation output columns.)
   *
   * @return WindowExpressions within windowExec,
   *         and a boolean, indicating the result column semantics
   *         (i.e. whether result columns should be returned *without* including the
   *         input columns).
   */
  def getWindowExpression: (Seq[NamedExpression], Boolean) = {
    var resultColumnsOnly : Boolean = false
    val expr = try {
      val resultMethod = windowExec.getClass.getMethod("windowExpression")
      resultMethod.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
    } catch {
      case _: NoSuchMethodException =>
        resultColumnsOnly = true
        val winExpr = windowExec.getClass.getMethod("projectList")
        winExpr.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
    }
    (expr, resultColumnsOnly)
  }

  private lazy val (inputWindowExpressions, resultColumnsOnly) = getWindowExpression

  override def getInputWindowExpressions: Seq[NamedExpression] = inputWindowExpressions
  override def getPartitionSpecs: Seq[Expression] = windowExec.partitionSpec
  override def getOrderSpecs: Seq[SortOrder] = windowExec.orderSpec
  override def getResultColumnsOnly: Boolean = resultColumnsOnly
}

object GpuWindowExec extends Arm {
  /**
   * As a part of `splitAndDedup` the dedup part adds a layer of indirection. This attempts to
   * remove that layer of indirection.
   * @param windowOps the windowOps output of splitAndDedup
   * @param post the post output of splitAndDedup
   * @return a version of windowOps that has removed as many un-needed temp aliases as possible.
   */
  def remapAttributes(windowOps: Seq[NamedExpression],
      post: Seq[NamedExpression]): Seq[NamedExpression] = {
    val postRemapping = post.flatMap {
      case a @ GpuAlias(attr: AttributeReference, _) => Some((attr.exprId, a))
      case _ => None
    }.groupBy(_._1)
    windowOps.map {
      case a @ GpuAlias(child, _)
        // We can only replace the mapping if there is one thing to map it to.
        if postRemapping.get(a.exprId).exists(_.length == 1) =>
        val attr = postRemapping(a.exprId).head._2
        GpuAlias(child, attr.name)(attr.exprId, attr.qualifier)
      case other => other
    }
  }

  private def hasGpuWindowFunction(expr: Expression): Boolean =
    expr.find(_.isInstanceOf[GpuWindowExpression]).isDefined

  private def extractAndSave(expr: Expression,
      saved: ArrayBuffer[NamedExpression],
      deduped: mutable.HashMap[Expression, Attribute]): Expression =
    expr match {
      // Don't rename an already named expression
      case ne: NamedExpression =>
        if (!saved.exists(_.exprId == ne.exprId)) {
          saved += ne
        }
        ne.toAttribute
      case e: Expression if e.foldable =>
        e // No need to create an attribute reference if it will be evaluated as a Literal.
      case e: Expression =>
        // For other expressions, we extract it and replace it with an AttributeReference (with
        // an internal column name, e.g. "_gpu_w0"). Deduping it as we go.
        deduped.getOrElseUpdate(e, {
          val withName = GpuAlias(e, s"_gpu_w${saved.length}")()
          saved += withName
          withName.toAttribute
        })
    }

  /**
   * In some distributions expressions passed into WindowExec can have more operations
   * in them than just a WindowExpression wrapped in an GpuAlias. This is a problem if we
   * want to try and do multiple window operations in a single pass to speed things up
   * or if we need to add new transitive window functions when we are doing some memory
   * optimizations, like running window. This will split the input expressions
   * into three sets of expressions. The first set is a project with no window expressions in it at
   * all. The second takes the first as input and will only have aliases to columns in the first or
   * named expressions wrapping a single window function in it. The third uses the second as
   * input and will do any final steps to combine window functions together.
   *
   * For example `SUM(a) - SUM(b + c) over (PARTITION BY d ORDER BY e) as result` would be
   * transformed into
   * <pre>
   * Phase 1 (Pre project):
   * a, b + c as _tmp0, d, e
   *
   * Phase 2 (Window Operations):
   * SUM(a) over (PARTITION BY d ORDER BY e) as _tmp1,
   * SUM(_tmp0) over (PARTITION BY d ORDER BY e) as _tmp2
   *
   * Phase 3 (Post Project):
   * (_tmp1 - _tmp2) as result
   * </pre>
   *
   * This assumes that there is not a window function of another window function, like
   * `LAG(SUM(a), 2)` which appears to be something all distros split apart into separate
   * window operations, so we are good.
   * @param exprs the input expressions to a GpuWindowExec
   */
  def splitAndDedup(exprs: Seq[NamedExpression]):
  (Seq[NamedExpression], Seq[NamedExpression], Seq[NamedExpression]) = {
    // This is based off of similar code in Apache Spark's `ExtractWindowExpressions.extract` but
    // has been highly modified
    val preProject = ArrayBuffer[NamedExpression]()
    val preDedupe = mutable.HashMap[Expression, Attribute]()
    val windowOps = ArrayBuffer[NamedExpression]()
    val windowDedupe = mutable.HashMap[Expression, Attribute]()
    val postProject = ArrayBuffer[NamedExpression]()

    val shims = ShimLoader.getSparkShims

    exprs.foreach { expr =>
      if (hasGpuWindowFunction(expr)) {
        // First pass looks for GpuWindowFunctions and GpuWindowSpecDefinitions to build up
        // the preProject phase
        val firstPass = expr.transformDown {
          case wf: GpuWindowFunction =>
            // All window functions, including those that are also aggregation functions, are
            // wrapped in a GpuWindowExpression, so dedup and save their children into the pre
            // stage, replacing them with aliases.
            val newChildren = wf.children.map(extractAndSave(_, preProject, preDedupe))
            wf.withNewChildren(newChildren)
          case wsc @ GpuWindowSpecDefinition(partitionSpec, orderSpec, _) =>
            // Extracts expressions from the partition spec and order spec to be sure that they
            // show up in the pre stage.  Because map is lazy we are going to force it to be
            // materialized, by forcing it to go through an array that cannot be lazily created
            val newPartitionSpec = partitionSpec.map(
              extractAndSave(_, preProject, preDedupe)).toArray.toSeq
            val newOrderSpec = orderSpec.map { so =>
              val newChild = extractAndSave(so.child, preProject, preDedupe)
              shims.sortOrder(newChild, so.direction, so.nullOrdering)
            }.toArray.toSeq
            wsc.copy(partitionSpec = newPartitionSpec, orderSpec = newOrderSpec)
        }
        val secondPass = firstPass.transformDown {
          case we: GpuWindowExpression =>
            // A window Expression holds a window function or an aggregate function, so put it into
            // the windowOps phase, and create a new alias for it for the post phase
            extractAndSave(we, windowOps, windowDedupe)
        }.asInstanceOf[NamedExpression]

        postProject += secondPass
      } else {
        // There is no window function so pass the result through all of the phases (with deduping)
        postProject += extractAndSave(
          extractAndSave(expr, preProject, preDedupe), windowOps, windowDedupe)
            .asInstanceOf[NamedExpression]
      }
    }
    (preProject, windowOps, postProject)
  }

  def isRunningWindow(spec: GpuWindowSpecDefinition): Boolean = spec match {
    case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(RowFrame,
    GpuSpecialFrameBoundary(UnboundedPreceding), GpuSpecialFrameBoundary(CurrentRow))) => true
    case _ => false
  }
}

trait GpuWindowBaseExec extends UnaryExecNode with GpuExec {
  val windowOps: Seq[NamedExpression]
  val partitionSpec: Seq[Expression]
  val orderSpec: Seq[SortOrder]

  import GpuMetric._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME)
  )

  override def output: Seq[Attribute] = windowOps.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
          + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(partitionSpec) :: Nil
  }

  lazy val partitionOrdering: Seq[SortOrder] = {
    val shims = ShimLoader.getSparkShims
    partitionSpec.map(shims.sortOrder(_, Ascending))
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionOrdering ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not happen, in $this.")
}

/**
 * The class represents a window function and the locations of its deduped inputs after an initial
 * projection.
 */
case class BoundGpuWindowFunction(
    windowFunc: GpuWindowFunction,
    boundInputLocations: Array[Int]) extends Arm {

  def scanAggregation: Aggregation = {
    val aggFunc = windowFunc.asInstanceOf[GpuRunningWindowFunction]
    aggFunc.scanAggregation
  }

  def scanReplaceNulls: Option[ReplacePolicy] = {
    val aggFunc = windowFunc.asInstanceOf[GpuRunningWindowFunction]
    aggFunc.scanReplaceNulls
  }

  def groupByScan(cb: ColumnarBatch): AggregationOnColumn[Nothing] = {
    val aggFunc = windowFunc.asInstanceOf[GpuRunningWindowFunction]
    val inputs = boundInputLocations.map { pos =>
      (cb.column(pos).asInstanceOf[GpuColumnVector].getBase, pos)
    }
    aggFunc.groupByScanAggregation(inputs).asInstanceOf[AggregationOnColumn[Nothing]]
  }

  def groupByReplaceNulls(index: Int): Option[ReplacePolicyWithColumn] = {
    val aggFunc = windowFunc.asInstanceOf[GpuRunningWindowFunction]
    aggFunc.groupByReplaceNulls(index)
  }


  def aggOverWindow(cb: ColumnarBatch,
      windowOpts: WindowOptions): AggregationOverWindow[Nothing] = {
    val aggFunc = windowFunc.asInstanceOf[GpuAggregateWindowFunction[_]]
    val inputs = boundInputLocations.map { pos =>
      (cb.column(pos).asInstanceOf[GpuColumnVector].getBase, pos)
    }
    aggFunc.windowAggregation(inputs).overWindow(windowOpts)
  }

  val dataType: DataType = windowFunc.dataType
}

case class ParsedBoundary(isUnbounded: Boolean, valueAsLong: Long)

object GroupedAggregations extends Arm {
  // In some cases a scan or a group by scan produces a different type than window would for the
  // same aggregation. A lot of this is because scan has a limited set of aggregations so we can
  // end up using a SUM aggregation to work around other issues, and cudf rightly makes the output
  // an INT64 instead of an INT32. This is here to fix that up.
  private def castIfNeeded(
      col: ai.rapids.cudf.ColumnVector,
      dataType: DataType): GpuColumnVector = {
    dataType match {
      case _: ArrayType | _: StructType =>
        GpuColumnVector.from(col, dataType).incRefCount()
      case other =>
        val dtype = GpuColumnVector.getNonNestedRapidsType(other)
        GpuColumnVector.from(col.castTo(dtype), dataType)
    }
  }

  /**
   * Get the window options for an aggregation
   * @param orderSpec the order by spec
   * @param orderPositions the positions of the order by columns
   * @param frame the frame to translate
   * @return the options to use when doing the aggregation.
   */
  private def getWindowOptions(
      orderSpec: Seq[SortOrder],
      orderPositions: Seq[Int],
      frame: GpuSpecifiedWindowFrame): WindowOptions = {
    frame.frameType match {
      case RowFrame =>
        withResource(getRowBasedLower(frame)) { lower =>
          withResource(getRowBasedUpper(frame)) { upper =>
            WindowOptions.builder()
                .minPeriods(1)
                .window(lower, upper).build()
          }
        }
      case RangeFrame =>
        // This gets to be a little more complicated

        // We only support a single column to order by right now, so just verify that.
        require(orderSpec.length == 1)
        require(orderPositions.length == orderSpec.length)
        val orderExpr = orderSpec.head

        // We only support basic types for now too
        val orderType = GpuColumnVector.getNonNestedRapidsType(orderExpr.dataType)

        val orderByIndex = orderPositions.head
        val lower = getRangeBoundaryValue(frame.lower)
        val upper = getRangeBoundaryValue(frame.upper)

        withResource(asScalarRangeBoundary(orderType, lower)) { preceding =>
          withResource(asScalarRangeBoundary(orderType, upper)) { following =>
            val windowOptionBuilder = WindowOptions.builder()
                .minPeriods(1)
                .orderByColumnIndex(orderByIndex)

            if (preceding.isEmpty) {
              windowOptionBuilder.unboundedPreceding()
            } else {
              windowOptionBuilder.preceding(preceding.get)
            }

            if (following.isEmpty) {
              windowOptionBuilder.unboundedFollowing()
            } else {
              windowOptionBuilder.following(following.get)
            }

            if (orderExpr.isAscending) {
              windowOptionBuilder.orderByAscending()
            } else {
              windowOptionBuilder.orderByDescending()
            }

            windowOptionBuilder.build()
          }
        }
    }
  }

  private def getRowBasedLower(windowFrameSpec : GpuSpecifiedWindowFrame): Scalar = {
    val lower = getRowBoundaryValue(windowFrameSpec.lower)

    // Translate the lower bound value to CUDF semantics:
    // In spark 0 is the current row and lower bound is negative relative to that
    // In CUDF the preceding window starts at the current row with 1 and up from there the
    // further from the current row.
    val ret = if (lower >= Int.MaxValue) {
      Int.MinValue
    } else if (lower <= Int.MinValue) {
      Int.MaxValue
    } else {
      -(lower-1)
    }
    Scalar.fromInt(ret)
  }

  private def getRowBasedUpper(windowFrameSpec : GpuSpecifiedWindowFrame): Scalar =
    Scalar.fromInt(getRowBoundaryValue(windowFrameSpec.upper))

  private def getRowBoundaryValue(boundary : Expression) : Int = boundary match {
    case literal: GpuLiteral if literal.dataType.equals(IntegerType) =>
      literal.value.asInstanceOf[Int]
    case special: GpuSpecialFrameBoundary =>
      special.value
    case anythingElse =>
      throw new UnsupportedOperationException(s"Unsupported window frame expression $anythingElse")
  }

  /**
   * Create a Scalar from boundary value according to order by column type.
   *
   * Timestamp types will be converted into interval types.
   *
   * @param orderByType the type of order by column
   * @param bound boundary value
   * @return a Scalar holding boundary value or None if the boundary is unbounded.
   */
  private def asScalarRangeBoundary(orderByType: DType, bound: ParsedBoundary): Option[Scalar] = {
    if (bound.isUnbounded) {
      None
    } else {
      val value = bound.valueAsLong
      val s = orderByType match {
        case DType.INT8 => Scalar.fromByte(value.toByte)
        case DType.INT16 => Scalar.fromShort(value.toShort)
        case DType.INT32 => Scalar.fromInt(value.toInt)
        case DType.INT64 => Scalar.fromLong(value)
        // Interval is not working for DateType
        case DType.TIMESTAMP_DAYS => Scalar.durationFromLong(DType.DURATION_DAYS, value)
        case DType.TIMESTAMP_MICROSECONDS =>
          Scalar.durationFromLong(DType.DURATION_MICROSECONDS, value)
        case _ => throw new RuntimeException(s"Not supported order by type, Found $orderByType")
      }
      Some(s)
    }
  }

  private def getRangeBoundaryValue(boundary: Expression): ParsedBoundary = boundary match {
    case special: GpuSpecialFrameBoundary =>
      val isUnBounded = special.isUnbounded
      ParsedBoundary(isUnBounded, special.value)
    case GpuLiteral(ci: CalendarInterval, CalendarIntervalType) =>
      // Get the total microseconds for TIMESTAMP_MICROSECONDS
      var x = TimeUnit.DAYS.toMicros(ci.days) + ci.microseconds
      if (x == Long.MinValue) x = Long.MaxValue
      ParsedBoundary(isUnbounded = false, Math.abs(x))
    case GpuLiteral(value, ByteType) =>
      var x = value.asInstanceOf[Byte]
      if (x == Byte.MinValue) x = Byte.MaxValue
      ParsedBoundary(isUnbounded = false, Math.abs(x))
    case GpuLiteral(value, ShortType) =>
      var x = value.asInstanceOf[Short]
      if (x == Short.MinValue) x = Short.MaxValue
      ParsedBoundary(isUnbounded = false, Math.abs(x))
    case GpuLiteral(value, IntegerType) =>
      var x = value.asInstanceOf[Int]
      if (x == Int.MinValue) x = Int.MaxValue
      ParsedBoundary(isUnbounded = false, Math.abs(x))
    case GpuLiteral(value, LongType) =>
      var x = value.asInstanceOf[Long]
      if (x == Long.MinValue) x = Long.MaxValue
      ParsedBoundary(isUnbounded = false, Math.abs(x))
    case anything => throw new UnsupportedOperationException("Unsupported window frame" +
        s" expression $anything")
  }
}

/**
 * Window aggregations that are grouped together. It holds the aggregation and the offsets of
 * its input columns, along with the output columns it should write the result to.
 */
class GroupedAggregations extends Arm {
  import GroupedAggregations._

  // The window frame to a map of the window function to the output locations for the result
  private val data = mutable.HashMap[GpuSpecifiedWindowFrame,
      mutable.HashMap[BoundGpuWindowFunction, ArrayBuffer[Int]]]()

  // This is similar to data but specific to running windows. We don't divide it up by the
  // window frame because the frame is the same for all of them unbounded rows preceding to
  // the current row.
  private val runningWindowOptimizedData =
    mutable.HashMap[BoundGpuWindowFunction, ArrayBuffer[Int]]()

  /**
   * Add an aggregation.
   * @param win the window this aggregation is over.
   * @param inputLocs the locations of the input columns for this aggregation.
   * @param outputIndex the output index this will write to in the final output.
   */
  def addAggregation(win: GpuWindowExpression, inputLocs: Array[Int], outputIndex: Int): Unit = {
    val forSpec = if (win.isOptimizedRunningWindow) {
      runningWindowOptimizedData
    } else {
      data.getOrElseUpdate(win.normalizedFrameSpec, mutable.HashMap.empty)
    }

    forSpec.getOrElseUpdate(BoundGpuWindowFunction(win.wrappedWindowFunc, inputLocs),
      ArrayBuffer.empty) += outputIndex
  }

  private def copyResultToFinalOutput(result: Table,
      functions: mutable.HashMap[BoundGpuWindowFunction, ArrayBuffer[Int]],
      outputColumns: Array[ColumnVector]): Unit = {
    functions.zipWithIndex.foreach {
      case ((winFunc, outputIndexes), resultIndex) =>
        val aggColumn = result.getColumn(resultIndex)
        // For nested type, do not cast
        val finalCol = aggColumn.getType match {
          case dType if dType.isNestedType =>
            GpuColumnVector.from(aggColumn.incRefCount(), winFunc.dataType)
          case _ =>
            val expectedType = GpuColumnVector.getNonNestedRapidsType(winFunc.dataType)
            // The API 'castTo' will take care of the 'from' type and 'to' type, and
            // just increase the reference count by one when they are the same.
            // so it is OK to always call it here.
            GpuColumnVector.from(aggColumn.castTo(expectedType), winFunc.dataType)
        }

        withResource(finalCol) { finalCol =>
          outputIndexes.foreach { outIndex =>
            outputColumns(outIndex) = finalCol.incRefCount()
          }
        }
    }
  }

  private def doAggInternal(
      frameType: FrameType,
      boundOrderSpec: Seq[SortOrder],
      orderByPositions: Array[Int],
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[ColumnVector],
      aggIt: (Table.GroupByOperation, Seq[AggregationOverWindow[Nothing]]) => Table): Unit = {
    data.foreach {
      case (frameSpec, functions) =>
        if (frameSpec.frameType == frameType) {
          // For now I am going to assume that we don't need to combine calls across frame specs
          // because it would just not help that much
          val result = withResource(
            getWindowOptions(boundOrderSpec, orderByPositions, frameSpec)) { windowOpts =>
            val allAggs = functions.map {
              case (winFunc, _) => winFunc.aggOverWindow(inputCb, windowOpts)
            }.toSeq
            withResource(GpuColumnVector.from(inputCb)) { initProjTab =>
              aggIt(initProjTab.groupBy(partByPositions: _*), allAggs)
            }
          }
          withResource(result) { result =>
            copyResultToFinalOutput(result, functions, outputColumns)
          }
        }
    }
  }

  private def doRowAggs(boundOrderSpec: Seq[SortOrder],
      orderByPositions: Array[Int],
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[ColumnVector]): Unit = {
    doAggInternal(
      RowFrame, boundOrderSpec, orderByPositions, partByPositions, inputCb, outputColumns,
      (groupBy, aggs) => groupBy.aggregateWindows(aggs: _*))
  }

  private def doRangeAggs(boundOrderSpec: Seq[SortOrder],
      orderByPositions: Array[Int],
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[ColumnVector]): Unit = {
    doAggInternal(
      RangeFrame, boundOrderSpec, orderByPositions, partByPositions, inputCb, outputColumns,
      (groupBy, aggs) => groupBy.aggregateWindowsOverRanges(aggs: _*))
  }

  private final def doRunningWindowScan(
      inputCb: ColumnarBatch,
      outputColumns: Array[ColumnVector]): Unit = {
    runningWindowOptimizedData.foreach {
      case (func, outputIndexes) =>
        val agg = func.scanAggregation
        val replace = func.scanReplaceNulls
        require(func.boundInputLocations.length == 1)
        val inputColIndex = func.boundInputLocations.head
        val inputCol = inputCb.column(inputColIndex).asInstanceOf[GpuColumnVector].getBase
        val replaced = withResource(inputCol.scan(agg, ScanType.INCLUSIVE, NullPolicy.EXCLUDE)) {
          scanned =>
            // For scans when nulls are excluded then each input row that has a null in it the
            // output row also has a null in it. Typically this is not what we want, because
            // for windows that only happens if the first values are nulls. So we will then call
            // replace nulls as needed to fix that up. Typically the replacement policy is
            // preceding.
            if (replace.isDefined) {
              scanned.replaceNulls(replace.get)
            } else {
              scanned.incRefCount()
            }
        }

        withResource(replaced) { replaced =>
          withResource(castIfNeeded(replaced, func.dataType)) { retCol =>
            outputIndexes.foreach { outIndex =>
              outputColumns(outIndex) = retCol.incRefCount()
            }
          }
        }
    }
  }

  private final def doRunningWindowGroupedScan(
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[ColumnVector]): Unit = {
    val allAggs = runningWindowOptimizedData.map {
      case (func, _) =>
        func.groupByScan(inputCb)
    }.toSeq

    // Part by is always ascending with nulls first, which is the default for group by options too
    val sortedGroupingOpts = GroupByOptions.builder()
        .withKeysSorted(true)
        .build()

    val scanned = withResource(GpuColumnVector.from(inputCb)) { initProjTab =>
      initProjTab.groupBy(sortedGroupingOpts, partByPositions: _*).scan(allAggs: _*)
    }
    withResource(scanned) { scanned =>
      // This gets a little complicated, because scan does not typically treat nulls the
      // way window treats nulls. So in some cases we need to do another group by and replace
      // the nulls to make them match what we want. But this is not all of the time, so we
      // keep track of which aggregations need to have a replace called on them, and where
      // we need to copy the results back out to. This is a little hard because the output of
      // scan has the group by columns first followed by the scan columns, and the output of
      // replace has the group by columns first followed by the replaced columns.
      val allReplace = ArrayBuffer[ReplacePolicyWithColumn]()
      val copyOutAfterReplace = ArrayBuffer[(Int, DataType, ArrayBuffer[Int])]()
      var afterReplaceIndex = partByPositions.length
      runningWindowOptimizedData.zipWithIndex.foreach {
        case ((func, finalOutputIndices), zipIndex) =>
          val inputIndex = zipIndex + partByPositions.length
          val replace = func.groupByReplaceNulls(inputIndex)
          if (replace.isDefined) {
            allReplace.append(replace.get)
            copyOutAfterReplace.append((afterReplaceIndex, func.dataType, finalOutputIndices))
            afterReplaceIndex += 1
          } else {
            withResource(castIfNeeded(scanned.getColumn(inputIndex), func.dataType)) { col =>
              finalOutputIndices.foreach { i =>
                outputColumns(i) = col.incRefCount()
              }
            }
          }
      }

      if (allReplace.nonEmpty) {
        withResource(scanned
            .groupBy(sortedGroupingOpts, partByPositions.indices: _*)
            .replaceNulls(allReplace: _*)) { replaced =>
          copyOutAfterReplace.foreach {
            case (inputIndex, dt, outputIndices) =>
              withResource(castIfNeeded(replaced.getColumn(inputIndex), dt)) { col =>
                outputIndices.foreach { i =>
                  outputColumns(i) = col.incRefCount()
                }
              }
          }
        }
      }
    }
  }

  private def doRunningWindowOptimizedAggs(
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[ColumnVector]): Unit = {
    if (runningWindowOptimizedData.nonEmpty) {
      if (partByPositions.isEmpty) {
        // This is implemented in terms of a scan on a column
        doRunningWindowScan(inputCb, outputColumns)
      } else {
        doRunningWindowGroupedScan(partByPositions, inputCb, outputColumns)
      }
    }
  }

  def doAggs(boundOrderSpec: Seq[SortOrder],
      orderByPositions: Array[Int],
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[ColumnVector]): Unit = {
    doRunningWindowOptimizedAggs(partByPositions, inputCb, outputColumns)
    doRowAggs(boundOrderSpec, orderByPositions, partByPositions, inputCb, outputColumns)
    doRangeAggs(boundOrderSpec, orderByPositions, partByPositions, inputCb, outputColumns)
  }
}

/**
 * Calculates the results of window operations. It assumes that any batching of the data
 * or fixups after the fact to get the right answer is done outside of this.
 */
trait BasicWindowCalc extends Arm {
  val boundWindowOps: Seq[GpuExpression]
  val boundPartitionSpec: Seq[GpuExpression]
  val boundOrderSpec: Seq[SortOrder]

  // In order to dedupe aggregations we take a slightly different approach from
  // group by aggregations. Instead of using named expressions to line up different
  // parts of the aggregation (pre-processing, aggregation, post-processing) we
  // keep track of the offsets directly. This is quite a bit more complex, but lets us
  // see that 5 aggregations want a column of just 1 and we dedupe it so it is only
  // materialized once.
  // `initialProjections` are a list of projections that provide the inputs to the `aggregations`
  // The order of these matter and `aggregations` is keeping track of them
  // `passThrough` are columns that go directly from the input to the output. The first value
  // is the index in the original input batch. The second value is the index in the final output
  // batch
  // `orderByPositions`  and `partByPositions` are the positions in `initialProjections` for
  // the order by columns and the part by columns respectively.
  private val (initialProjections,
  passThrough,
  aggregations,
  orderByPositions,
  partByPositions) = {
    val initialProjections = ArrayBuffer[Expression]()
    val dedupedInitialProjections = mutable.HashMap[Expression, Int]()

    def getOrAddInitialProjectionIndex(expr: Expression): Int =
      dedupedInitialProjections.getOrElseUpdate(expr, {
        val at = initialProjections.length
        initialProjections += expr
        at
      })

    val passThrough = ArrayBuffer[(Int, Int)]()
    val aggregations = new GroupedAggregations()

    boundWindowOps.zipWithIndex.foreach {
      case (GpuAlias(GpuBoundReference(inputIndex, _, _), _), outputIndex) =>
        passThrough.append((inputIndex, outputIndex))
      case (GpuBoundReference(inputIndex, _, _), outputIndex) =>
        passThrough.append((inputIndex, outputIndex))
      case (GpuAlias(win: GpuWindowExpression, _), outputIndex) =>
        val inputLocations = win.initialProjections
            .map(getOrAddInitialProjectionIndex).toArray
        aggregations.addAggregation(win, inputLocations, outputIndex)
      case _ =>
        throw new IllegalArgumentException("Unexpected operation found in window expression")
    }

    val partByPositions =  boundPartitionSpec.map(getOrAddInitialProjectionIndex).toArray
    val orderByPositions = boundOrderSpec.map { so =>
      getOrAddInitialProjectionIndex(so.child)
    }.toArray

    (initialProjections, passThrough, aggregations, orderByPositions, partByPositions)
  }

  def computeBasicWindow(cb: ColumnarBatch): ColumnarBatch = {
    closeOnExcept(new Array[ColumnVector](boundWindowOps.length)) { outputColumns =>
      // First the pass through unchanged columns
      passThrough.foreach {
        case (inputIndex, outputIndex) =>
          outputColumns(outputIndex) =
            cb.column(inputIndex).asInstanceOf[GpuColumnVector].incRefCount()
      }

      withResource(GpuProjectExec.project(cb, initialProjections)) { initProjCb =>
        aggregations.doAggs(boundOrderSpec, orderByPositions,
          partByPositions, initProjCb, outputColumns)
      }

      new ColumnarBatch(outputColumns, cb.numRows())
    }
  }
}

/**
 * An Iterator that performs window operations on the input data. It is required that the input
 * data is batched so all of the data for a given key is in the same batch. The input data must
 * also be sorted by both partition by keys and order by keys.
 */
class GpuWindowIterator(
    input: Iterator[ColumnarBatch],
    override val boundWindowOps: Seq[GpuExpression],
    override val boundPartitionSpec: Seq[GpuExpression],
    override val boundOrderSpec: Seq[SortOrder],
    numOutputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] with BasicWindowCalc {

  override def hasNext: Boolean = input.hasNext

  override def next(): ColumnarBatch = {
    withResource(input.next()) { cb =>
      withResource(new NvtxWithMetrics("window", NvtxColor.CYAN, opTime)) { _ =>
        val ret = computeBasicWindow(cb)
        numOutputBatches += 1
        numOutputRows += ret.numRows()
        ret
      }
    }
  }
}

object GpuRunningWindowIterator extends Arm {
  private def cudfAnd(lhs: ai.rapids.cudf.ColumnVector,
      rhs: ai.rapids.cudf.ColumnVector): ai.rapids.cudf.ColumnVector = {
    withResource(lhs) { lhs =>
      withResource(rhs) { rhs =>
        lhs.and(rhs)
      }
    }
  }

  private def arePartsEqual(
      scalars: Seq[Scalar],
      columns: Seq[ai.rapids.cudf.ColumnVector]): Either[GpuColumnVector, Boolean] = {
    if (scalars.length != columns.length) {
      scala.util.Right(false)
    } else if (scalars.isEmpty && columns.isEmpty) {
      scala.util.Right(true)
    } else {
      val ret = scalars.zip(columns).map {
        case (scalar, column) => scalar.equalToNullAware(column)
      }.reduce(cudfAnd)
      scala.util.Left(GpuColumnVector.from(ret, BooleanType))
    }
  }

  private def getScalarRow(index: Int, columns: Seq[ai.rapids.cudf.ColumnVector]): Array[Scalar] =
    columns.map(_.getScalarElement(index)).toArray
}

/**
 * An iterator that can do row based aggregations on running window queries (Unbounded preceding to
 * current row) if and only if the aggregations are instances of GpuBatchedRunningWindowFunction
 * which can fix up the window output when an aggregation is only partly done in one batch of data.
 * Because of this there is no requirement about how the input data is batched, but it  must
 * be sorted by both partitioning and ordering.
 */
class GpuRunningWindowIterator(
    input: Iterator[ColumnarBatch],
    override val boundWindowOps: Seq[GpuExpression],
    override val boundPartitionSpec: Seq[GpuExpression],
    override val boundOrderSpec: Seq[SortOrder],
    numOutputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] with BasicWindowCalc {
  import GpuRunningWindowIterator._
  TaskContext.get().addTaskCompletionListener[Unit](_ => close())

  // This should only ever be cached in between calls to `hasNext` and `next`. This is just
  // to let us filter out empty batches.
  private var cachedBatch: Option[ColumnarBatch] = None
  private var lastParts: Array[Scalar] = Array.empty
  private var isClosed: Boolean = false

  private def saveLastParts(newLastParts: Array[Scalar]): Unit = {
    lastParts.foreach(_.close())
    lastParts = newLastParts
  }

  def close(): Unit = {
    if (!isClosed) {
      isClosed = true
      fixerIndexMap.values.foreach(_.close())
      saveLastParts(Array.empty)
    }
  }

  private lazy val fixerIndexMap: Map[Int, BatchedRunningWindowFixer] =
    boundWindowOps.zipWithIndex.flatMap {
      case (GpuAlias(GpuWindowExpression(func, _), _), index) =>
        func match {
          case f: GpuBatchedRunningWindowWithFixer =>
            Some((index, f.newFixer()))
          case GpuAggregateExpression(f: GpuBatchedRunningWindowWithFixer, _, _, _, _) =>
            Some((index, f.newFixer()))
          case _ => None
        }
      case _ => None
    }.toMap

  private def fixUpAll(computedWindows: ColumnarBatch,
      fixers: Map[Int, BatchedRunningWindowFixer],
      samePartitionMask: Either[GpuColumnVector, Boolean]): ColumnarBatch = {
    closeOnExcept(ArrayBuffer[ColumnVector]()) { newColumns =>
      boundWindowOps.indices.foreach { idx =>
        val column = computedWindows.column(idx).asInstanceOf[GpuColumnVector]
        fixers.get(idx) match {
          case Some(fixer) =>
            closeOnExcept(fixer.fixUp(samePartitionMask, column)) { finalOutput =>
              fixer.updateState(finalOutput)
              newColumns += finalOutput
            }
          case None =>
            newColumns += column.incRefCount()
        }
      }
      new ColumnarBatch(newColumns.toArray, computedWindows.numRows())
    }
  }

  def computeRunning(cb: ColumnarBatch): ColumnarBatch = {
    val fixers = fixerIndexMap
    val numRows = cb.numRows()
    withResource(computeBasicWindow(cb)) { basic =>
      withResource(GpuProjectExec.project(cb, boundPartitionSpec)) { parts =>
        val partColumns = GpuColumnVector.extractBases(parts)
        // We need to fix up the rows that are part of the same batch as the end of the
        // last batch
        withResourceIfAllowed(arePartsEqual(lastParts, partColumns)) { partsEqual =>
          val ret = fixUpAll(basic, fixers, partsEqual)
          saveLastParts(getScalarRow(numRows - 1, partColumns))
          ret
        }
      }
    }
  }

  private def cacheBatchIfNeeded(): Unit = {
    while (cachedBatch.isEmpty && input.hasNext) {
      closeOnExcept(input.next()) { cb =>
        if (cb.numRows() > 0) {
          cachedBatch = Some(cb)
        } else {
          cb.close()
        }
      }
    }
  }

  def readNextInputBatch(): ColumnarBatch = {
    cacheBatchIfNeeded()
    val ret = cachedBatch.getOrElse {
      throw new NoSuchElementException()
    }
    cachedBatch = None
    ret
  }

  override def hasNext: Boolean = {
    cacheBatchIfNeeded()
    cachedBatch.isDefined
  }

  override def next(): ColumnarBatch = {
    withResource(readNextInputBatch()) { cb =>
      withResource(new NvtxWithMetrics("RunningWindow", NvtxColor.CYAN, opTime)) { _ =>
        val ret = computeRunning(cb)
        numOutputBatches += 1
        numOutputRows += ret.numRows()
        ret
      }
    }
  }
}

case class GpuRunningWindowExec(
    windowOps: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan
) extends GpuWindowBaseExec {

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val boundWindowOps = GpuBindReferences.bindGpuReferences(windowOps, child.output)
    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(partitionSpec, child.output)
    val boundOrderSpec = GpuBindReferences.bindReferences(orderSpec, child.output)

    child.executeColumnar().mapPartitions { iter =>
      new GpuRunningWindowIterator(iter, boundWindowOps, boundPartitionSpec, boundOrderSpec,
        numOutputBatches, numOutputRows, opTime)
    }
  }
}

case class GpuWindowExec(
    windowOps: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan
  ) extends GpuWindowBaseExec {

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(outputBatching)

  override def outputBatching: CoalesceGoal = if (partitionSpec.isEmpty) {
    RequireSingleBatch
  } else {
    BatchedByKey(partitionOrdering)
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val boundWindowOps = GpuBindReferences.bindGpuReferences(windowOps, child.output)
    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(partitionSpec, child.output)
    val boundOrderSpec = GpuBindReferences.bindReferences(orderSpec, child.output)

    child.executeColumnar().mapPartitions { iter =>
        new GpuWindowIterator(iter, boundWindowOps, boundPartitionSpec, boundOrderSpec,
          numOutputBatches, numOutputRows, opTime)
    }
  }
}
