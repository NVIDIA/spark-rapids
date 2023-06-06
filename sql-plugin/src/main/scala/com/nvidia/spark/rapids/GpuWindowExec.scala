/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf
import ai.rapids.cudf.{AggregationOverWindow, DType, GroupByOptions, GroupByScanAggregation, NullPolicy, NvtxColor, ReplacePolicy, ReplacePolicyWithColumn, Scalar, ScanAggregation, ScanType, Table, WindowOptions}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{withRestoreOnRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.shims.{GpuWindowUtil, ShimUnaryExecNode}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, AttributeSeq, AttributeSet, CurrentRow, Expression, FrameType, NamedExpression, RangeFrame, RowFrame, SortOrder, UnboundedFollowing, UnboundedPreceding}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.rapids.GpuAggregateExpression
import org.apache.spark.sql.types._
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
  extends SparkPlanMeta[WindowExecType](windowExec, conf, parent, rule) {

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

  /**
   * Define all dependency expressions as `childExprs`.
   * This ensures that they are tagged for GPU execution.
   */
  override val childExprs: Seq[BaseExprMeta[_]] =
        windowExpressions ++ partitionSpec ++ orderSpec ++ inputFields

  override def namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] = Map(
    "partitionSpec" -> partitionSpec
  )

  override def tagPlanForGpu(): Unit = {
    // Implementation depends on receiving a `NamedExpression` wrapped WindowExpression.
    windowExpressions.map(meta => meta.wrapped)
        .filter(expr => !expr.isInstanceOf[NamedExpression])
        .foreach(_ => willNotWorkOnGpu("Unexpected query plan with Windowing functions; " +
            "cannot convert for GPU execution. " +
            "(Detail: WindowExpression not wrapped in `NamedExpression`.)"))
  }

  override def convertToGpu(): GpuExec = {
    // resultColumnsOnly specifies that we should only return the values of result columns for 
    // this WindowExec (applies to some Spark distributions that use `projectList` and combine
    // ProjectExec with WindowExec)
    val resultColumnsOnly = getResultColumnsOnly
    // Keep the converted input fields and input window expressions separate 
    // to handle the resultColumnsOnly case in GpuWindowExec.splitAndDedup
    val inputFieldExpressions = inputFields.map(_.convertToGpu().asInstanceOf[NamedExpression])
    val gpuWindowExpressions = windowExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression])
    val (pre, windowOps, post) = GpuWindowExec.splitAndDedup(
        inputFieldExpressions,
        gpuWindowExpressions,
        resultColumnsOnly)
    // Order is not important for pre. It is unbound and we are inserting it in.
    val isPreNeeded =
      (AttributeSet(pre.map(_.toAttribute)) -- windowExec.children.head.output).nonEmpty
    // To check if post is needed we first have to remove a layer of indirection that
    // might not be needed. Here we want to maintain order, just to match Spark as closely
    // as possible
    val remappedWindowOps = GpuWindowExec.remapAttributes(windowOps, post)
    // isPostNeeded is determined when there is a difference between the output of the WindowExec
    // computation and the output ultimately desired by this WindowExec or whether an additional
    // post computation is needed. Ultimately this is used to add an additional ProjectExec 
    // if that is needed to return the correct output
    val isPostNeeded = remappedWindowOps.length != post.length ||
        remappedWindowOps.zip(post).exists {
          case (w, p) => w.exprId != p.exprId
        }
    val fixedUpWindowOps = if(isPostNeeded) {
      windowOps
    } else {
      remappedWindowOps
    }

    val allBatched = fixedUpWindowOps.forall {
      case GpuAlias(GpuWindowExpression(func, spec), _) =>
        GpuWindowExec.isBatchedFunc(func, spec)
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
      GpuProjectExec(pre.toList, childPlans.head.convertIfNeeded())()
    } else {
      childPlans.head.convertIfNeeded()
    }

    val windowExpr = if (allBatched) {
      val batchedOps = GpuWindowExec.splitBatchedOps(fixedUpWindowOps)
      batchedOps.getWindowExec(
        partitionSpec.map(_.convertToGpu()),
        orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
        input,
        getPartitionSpecs,
        getOrderSpecs)
    } else {
      GpuWindowExec(
        fixedUpWindowOps,
        partitionSpec.map(_.convertToGpu()),
        orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
        input)(getPartitionSpecs, getOrderSpecs)
    }

    if (isPostNeeded) {
      GpuProjectExec(post.toList, windowExpr)()
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

case class BatchedOps(running: Seq[NamedExpression],
    unboundedToUnbounded: Seq[NamedExpression],
    passThrough: Seq[NamedExpression]) {
  def getRunningExpressionsWithPassthrough: Seq[NamedExpression] =
    passThrough ++ running

  private def getRunningWindowExec(
      gpuPartitionSpec: Seq[Expression],
      gpuOrderSpec: Seq[SortOrder],
      child: SparkPlan,
      cpuPartitionSpec: Seq[Expression],
      cpuOrderSpec: Seq[SortOrder]): GpuExec =
    GpuRunningWindowExec(
      getRunningExpressionsWithPassthrough,
      gpuPartitionSpec,
      gpuOrderSpec,
      child)(cpuPartitionSpec, cpuOrderSpec)

  private def getDoublePassWindowExec(
      gpuPartitionSpec: Seq[Expression],
      gpuOrderSpec: Seq[SortOrder],
      child: SparkPlan,
      cpuPartitionSpec: Seq[Expression],
      cpuOrderSpec: Seq[SortOrder]): GpuExec =
    GpuCachedDoublePassWindowExec(
      getDoublePassExpressionsWithRunningAsPassthrough,
      gpuPartitionSpec,
      gpuOrderSpec,
      child)(cpuPartitionSpec, cpuOrderSpec)

  def getWindowExec(
      gpuPartitionSpec: Seq[Expression],
      gpuOrderSpec: Seq[SortOrder],
      child: SparkPlan,
      cpuPartitionSpec: Seq[Expression],
      cpuOrderSpec: Seq[SortOrder]): GpuExec = {
    // The order of these matter so we can pass the output of the first through the second one
    if (hasRunning) {
      val running = getRunningWindowExec(gpuPartitionSpec, gpuOrderSpec, child,
        cpuPartitionSpec, cpuOrderSpec)
      if (hasDoublePass) {
        getDoublePassWindowExec(gpuPartitionSpec, gpuOrderSpec, running,
          cpuPartitionSpec, cpuOrderSpec)
      } else {
        running
      }
    } else {
      getDoublePassWindowExec(gpuPartitionSpec, gpuOrderSpec, child,
        cpuPartitionSpec, cpuOrderSpec)
    }
  }

  def hasRunning: Boolean = running.nonEmpty

  def getDoublePassExpressionsWithRunningAsPassthrough: Seq[NamedExpression] =
    passThrough ++ unboundedToUnbounded ++ running.map(_.toAttribute)

  def hasDoublePass: Boolean = unboundedToUnbounded.nonEmpty
}

object GpuWindowExec {
  /**
   * As a part of `splitAndDedup` the dedup part adds a layer of indirection. This attempts to
   * remove that layer of indirection.
   *
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
   * To handle special cases (like window function of another window function eg `LAG(SUM(a), 2)`, 
   * distros should split apart those into separate window operations. However, we will not 
   * see all of these in just the input window expressions of the WindowExec, so we process 
   * both the input fields and input window expressions, and handle whether we *only* want result
   * columns using the Post Project stage.
   * @param inputFieldExprs the input fields converted to input expressions
   * @param windowExprs the input window expressions to a GpuWindowExec
   * @param resultColumnsOnly whether the output of the window operation only desires result
   * columns or the output of all input expressions
   */
  def splitAndDedup(inputFieldExprs: Seq[NamedExpression],
      windowExprs: Seq[NamedExpression],
      resultColumnsOnly: Boolean):
  (Seq[NamedExpression], Seq[NamedExpression], Seq[NamedExpression]) = {
    // This is based off of similar code in Apache Spark's `ExtractWindowExpressions.extract` but
    // has been highly modified
    val preProject = ArrayBuffer[NamedExpression]()
    val preDedupe = mutable.HashMap[Expression, Attribute]()
    val windowOps = ArrayBuffer[NamedExpression]()
    val windowDedupe = mutable.HashMap[Expression, Attribute]()
    val postProject = ArrayBuffer[NamedExpression]()

    // Process input field expressions first. There are no window functions here, so
    // all of these should pass at least to pre and window stages
    inputFieldExprs.foreach { expr =>
        // If the Spark distribution only wants to output result columns (ie, ones that
        // use projectList), then pass the input field to pre and window stages, but
        // do not pass to the post project stage (as those are specifically given in
        // the projectList set)
        if (resultColumnsOnly) {
          extractAndSave(
            extractAndSave(expr, preProject, preDedupe), windowOps, windowDedupe)
              .asInstanceOf[NamedExpression]
        } else {
          // If the WindowExec returns everything, then pass the input fields through all the
          // phases (with deduping)
          postProject += extractAndSave(
            extractAndSave(expr, preProject, preDedupe), windowOps, windowDedupe)
              .asInstanceOf[NamedExpression]
        }
    }

    // Now split and dedup the input window expressions
    windowExprs.foreach { expr =>
      if (hasGpuWindowFunction(expr)) {
        // First pass replace any operations that should be totally replaced.
        val replacePass = expr.transformDown {
          case GpuWindowExpression(
          GpuAggregateExpression(rep: GpuReplaceWindowFunction, _, _, _, _), spec)
            if rep.shouldReplaceWindow(spec) =>
            // We don't actually care about the GpuAggregateExpression because it is ignored
            // by our GPU window operations anyways.
            rep.windowReplacement(spec)
          case GpuWindowExpression(rep: GpuReplaceWindowFunction, spec)
            if rep.shouldReplaceWindow(spec) =>
            rep.windowReplacement(spec)
        }
        // Second pass looks for GpuWindowFunctions and GpuWindowSpecDefinitions to build up
        // the preProject phase
        val secondPass = replacePass.transformDown {
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
              SortOrder(newChild, so.direction, so.nullOrdering, Seq.empty)
            }.toArray.toSeq
            wsc.copy(partitionSpec = newPartitionSpec, orderSpec = newOrderSpec)
        }
        // Final pass is to extract, dedup, and save the results.
        val finalPass = secondPass.transformDown {
          case we: GpuWindowExpression =>
            // A window Expression holds a window function or an aggregate function, so put it into
            // the windowOps phase, and create a new alias for it for the post phase
            extractAndSave(we, windowOps, windowDedupe)
        }.asInstanceOf[NamedExpression]

        postProject += finalPass
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
    case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(RowFrame,
    GpuSpecialFrameBoundary(UnboundedPreceding), GpuLiteral(value, _))) if value == 0 => true
    case _ => false
  }

  def isUnboundedToUnboundedWindow(spec: GpuWindowSpecDefinition): Boolean = spec match {
    case GpuWindowSpecDefinition(_, _, GpuSpecifiedWindowFrame(_,
    GpuSpecialFrameBoundary(UnboundedPreceding),
    GpuSpecialFrameBoundary(UnboundedFollowing))) => true
    case _ => false
  }

  def isBatchedRunningFunc(func: Expression, spec: GpuWindowSpecDefinition): Boolean = {
    val isSpecOkay = isRunningWindow(spec)
    val isFuncOkay = func match {
      case _: GpuBatchedRunningWindowWithFixer => true
      case GpuAggregateExpression(_: GpuBatchedRunningWindowWithFixer, _, _, _ , _) => true
      case _ => false
    }
    isSpecOkay && isFuncOkay
  }

  def isBatchedUnboundedToUnboundedFunc(func: Expression, spec: GpuWindowSpecDefinition): Boolean =
    func match {
      case _: GpuUnboundToUnboundWindowWithFixer
        if GpuWindowExec.isUnboundedToUnboundedWindow(spec) => true
      case GpuAggregateExpression(_: GpuUnboundToUnboundWindowWithFixer, _, _, _ , _)
        if GpuWindowExec.isUnboundedToUnboundedWindow(spec) => true
      case _ => false
    }

  def isBatchedFunc(func: Expression, spec: GpuWindowSpecDefinition): Boolean =
    isBatchedRunningFunc(func, spec) || isBatchedUnboundedToUnboundedFunc(func, spec)

  def splitBatchedOps(windowOps: Seq[NamedExpression]): BatchedOps = {
    val running = ArrayBuffer[NamedExpression]()
    val doublePass = ArrayBuffer[NamedExpression]()
    val passThrough = ArrayBuffer[NamedExpression]()
    windowOps.foreach {
      case expr@GpuAlias(GpuWindowExpression(func, spec), _) =>
        if (isBatchedRunningFunc(func, spec)) {
          running.append(expr)
        } else if (isBatchedUnboundedToUnboundedFunc(func, spec)) {
          doublePass.append(expr)
        } else {
          throw new IllegalArgumentException(
            s"Found unexpected expression $expr in window exec ${expr.getClass}")
        }
      case expr@(GpuAlias(_: AttributeReference, _) | _: AttributeReference) =>
        passThrough.append(expr)
      case other =>
        // This should only happen if we did something wrong in splitting/deduping
        // the window expressions.
        throw new IllegalArgumentException(
          s"Found unexpected expression $other in window exec ${other.getClass}")
    }
    BatchedOps(running, doublePass, passThrough)
  }
}

trait GpuWindowBaseExec extends ShimUnaryExecNode with GpuExec {
  val windowOps: Seq[NamedExpression]
  val gpuPartitionSpec: Seq[Expression]
  val gpuOrderSpec: Seq[SortOrder]
  val cpuPartitionSpec: Seq[Expression]
  val cpuOrderSpec: Seq[SortOrder]

  import GpuMetric._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME))

  override def output: Seq[Attribute] = windowOps.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (cpuPartitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
          + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(cpuPartitionSpec) :: Nil
  }

  lazy val gpuPartitionOrdering: Seq[SortOrder] = {
    gpuPartitionSpec.map(SortOrder(_, Ascending))
  }

  lazy val cpuPartitionOrdering: Seq[SortOrder] = {
    cpuPartitionSpec.map(SortOrder(_, Ascending))
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(cpuPartitionOrdering ++ cpuOrderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not happen, in $this.")
}

/**
 * For Scan and GroupBy Scan aggregations nulls are not always treated the same way as they are
 * in window operations. Often we have to run a post processing step and replace them. This
 * groups those two together so we can have a complete picture of how to perform these types of
 * aggregations.
 */
case class AggAndReplace[T](agg: T, nullReplacePolicy: Option[ReplacePolicy])

/**
 * The class represents a window function and the locations of its deduped inputs after an initial
 * projection.
 */
case class BoundGpuWindowFunction(
    windowFunc: GpuWindowFunction,
    boundInputLocations: Array[Int]) {

  /**
   * Get the operations to perform a scan aggregation.
   * @param isRunningBatched is this for a batched running window operation?
   * @return the sequence of aggregation operators to do. There will be one `AggAndReplace`
   *         for each value in `boundInputLocations` so that they can be zipped together.
   */
  def scan(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]] = {
    val aggFunc = windowFunc.asInstanceOf[GpuRunningWindowFunction]
    aggFunc.scanAggregation(isRunningBatched)
  }

  /**
   * Get the operations to perform a group by scan aggregation.
   * @param isRunningBatched is this for a batched running window operation?
   * @return the sequence of aggregation operators to do. There will be one `AggAndReplace`
   *         for each value in `boundInputLocations` so that they can be zipped together.
   */
  def groupByScan(isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] = {
    val aggFunc = windowFunc.asInstanceOf[GpuRunningWindowFunction]
    aggFunc.groupByScanAggregation(isRunningBatched)
  }

  /**
   * After a scan or group by scan if there are multiple columns they need to be combined together
   * into a single final output column. This does that job.
   * @param isRunningBatched is this for a batched running window operation?
   * @param cols the columns to be combined. This should not close them.
   * @return a single result column.
   */
  def scanCombine(isRunningBatched: Boolean,
      cols: Seq[cudf.ColumnVector]): cudf.ColumnVector = {
    val aggFunc = windowFunc.asInstanceOf[GpuRunningWindowFunction]
    aggFunc.scanCombine(isRunningBatched, cols)
  }

  def aggOverWindow(cb: ColumnarBatch,
      windowOpts: WindowOptions): AggregationOverWindow = {
    val aggFunc = windowFunc.asInstanceOf[GpuAggregateWindowFunction]
    val inputs = boundInputLocations.map { pos =>
      (cb.column(pos).asInstanceOf[GpuColumnVector].getBase, pos)
    }
    aggFunc.windowAggregation(inputs).overWindow(windowOpts)
  }

  def windowOutput(cv: cudf.ColumnVector): cudf.ColumnVector = {
    val aggFunc = windowFunc.asInstanceOf[GpuAggregateWindowFunction]
    aggFunc.windowOutput(cv)
  }

  val dataType: DataType = windowFunc.dataType
}

case class ParsedBoundary(isUnbounded: Boolean, value: Either[BigInt, Long])

object GroupedAggregations {
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
        val lower = getRangeBoundaryValue(frame.lower, orderType)
        val upper = getRangeBoundaryValue(frame.upper, orderType)

        withResource(asScalarRangeBoundary(orderType, lower)) { preceding =>
          withResource(asScalarRangeBoundary(orderType, upper)) { following =>
            val windowOptionBuilder = WindowOptions.builder()
                .minPeriods(1)
                .orderByColumnIndex(orderByIndex)

            if (preceding.isEmpty) {
              windowOptionBuilder.unboundedPreceding()
            } else {
              if (orderType == DType.STRING) { // Bounded STRING bounds can only mean "CURRENT ROW".
                windowOptionBuilder.currentRowPreceding()
              } else {
                windowOptionBuilder.preceding(preceding.get)
              }
            }

            if (following.isEmpty) {
              windowOptionBuilder.unboundedFollowing()
            } else {
              if (orderType == DType.STRING) { // Bounded STRING bounds can only mean "CURRENT ROW".
                windowOptionBuilder.currentRowFollowing()
              } else {
                windowOptionBuilder.following(following.get)
              }
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
      val valueLong = bound.value.right // Used for all cases except DECIMAL128.
      val s = orderByType match {
        case DType.INT8 => Scalar.fromByte(valueLong.get.toByte)
        case DType.INT16 => Scalar.fromShort(valueLong.get.toShort)
        case DType.INT32 => Scalar.fromInt(valueLong.get.toInt)
        case DType.INT64 => Scalar.fromLong(valueLong.get)
        // Interval is not working for DateType
        case DType.TIMESTAMP_DAYS => Scalar.durationFromLong(DType.DURATION_DAYS, valueLong.get)
        case DType.TIMESTAMP_MICROSECONDS =>
          Scalar.durationFromLong(DType.DURATION_MICROSECONDS, valueLong.get)
        case x if x.getTypeId == DType.DTypeEnum.DECIMAL32 =>
          Scalar.fromDecimal(x.getScale, valueLong.get.toInt)
        case x if x.getTypeId == DType.DTypeEnum.DECIMAL64 =>
          Scalar.fromDecimal(x.getScale, valueLong.get)
        case x if x.getTypeId == DType.DTypeEnum.DECIMAL128 =>
          Scalar.fromDecimal(x.getScale, bound.value.left.get.underlying())
        case x if x.getTypeId == DType.DTypeEnum.STRING =>
          // Not UNBOUNDED. The only other supported boundary for String is CURRENT ROW, i.e. 0.
          Scalar.fromString("")
        case _ => throw new RuntimeException(s"Not supported order by type, Found $orderByType")
      }
      Some(s)
    }
  }

  private def getRangeBoundaryValue(boundary: Expression, orderByType: DType): ParsedBoundary =
    boundary match {
    case special: GpuSpecialFrameBoundary =>
      val isUnBounded = special.isUnbounded
      val isDecimal128 = orderByType.getTypeId == DType.DTypeEnum.DECIMAL128
      ParsedBoundary(isUnBounded, if (isDecimal128) Left(special.value) else Right(special.value))
    case GpuLiteral(ci: CalendarInterval, CalendarIntervalType) =>
      // Get the total microseconds for TIMESTAMP_MICROSECONDS
      var x = TimeUnit.DAYS.toMicros(ci.days) + ci.microseconds
      if (x == Long.MinValue) x = Long.MaxValue
      ParsedBoundary(isUnbounded = false, Right(Math.abs(x)))
    case GpuLiteral(value, ByteType) =>
      var x = value.asInstanceOf[Byte]
      if (x == Byte.MinValue) x = Byte.MaxValue
      ParsedBoundary(isUnbounded = false, Right(Math.abs(x)))
    case GpuLiteral(value, ShortType) =>
      var x = value.asInstanceOf[Short]
      if (x == Short.MinValue) x = Short.MaxValue
      ParsedBoundary(isUnbounded = false, Right(Math.abs(x)))
    case GpuLiteral(value, IntegerType) =>
      var x = value.asInstanceOf[Int]
      if (x == Int.MinValue) x = Int.MaxValue
      ParsedBoundary(isUnbounded = false, Right(Math.abs(x)))
    case GpuLiteral(value, LongType) =>
      var x = value.asInstanceOf[Long]
      if (x == Long.MinValue) x = Long.MaxValue
      ParsedBoundary(isUnbounded = false, Right(Math.abs(x)))
    case GpuLiteral(value: Decimal, DecimalType()) =>
      orderByType.getTypeId match {
        case DType.DTypeEnum.DECIMAL32 | DType.DTypeEnum.DECIMAL64 =>
          ParsedBoundary(isUnbounded = false, Right(Math.abs(value.toUnscaledLong)))
        case DType.DTypeEnum.DECIMAL128 =>
          ParsedBoundary(isUnbounded = false, Left(value.toJavaBigDecimal.unscaledValue().abs))
        case anythingElse =>
          throw new UnsupportedOperationException(s"Unexpected Decimal type: $anythingElse")
      }
    case anything => GpuWindowUtil.getRangeBoundaryValue(anything)
  }
}

/**
 * Window aggregations that are grouped together. It holds the aggregation and the offsets of
 * its input columns, along with the output columns it should write the result to.
 */
class GroupedAggregations {
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

  private def doAggInternal(
      frameType: FrameType,
      boundOrderSpec: Seq[SortOrder],
      orderByPositions: Array[Int],
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector],
      aggIt: (Table.GroupByOperation, Seq[AggregationOverWindow]) => Table): Unit = {
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
            functions.zipWithIndex.foreach {
              case ((func, outputIndexes), resultIndex) =>
                val aggColumn = result.getColumn(resultIndex)

                outputIndexes.foreach { outIndex =>
                  require(outputColumns(outIndex) == null,
                    "Attempted to overwrite a window output column!!")
                  outputColumns(outIndex) = func.windowOutput(aggColumn)
                }
            }
          }
        }
    }
  }

  private def doRowAggs(boundOrderSpec: Seq[SortOrder],
      orderByPositions: Array[Int],
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    doAggInternal(
      RowFrame, boundOrderSpec, orderByPositions, partByPositions, inputCb, outputColumns,
      (groupBy, aggs) => groupBy.aggregateWindows(aggs: _*))
  }

  private def doRangeAggs(boundOrderSpec: Seq[SortOrder],
      orderByPositions: Array[Int],
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    doAggInternal(
      RangeFrame, boundOrderSpec, orderByPositions, partByPositions, inputCb, outputColumns,
      (groupBy, aggs) => groupBy.aggregateWindowsOverRanges(aggs: _*))
  }

  private final def doRunningWindowScan(
      isRunningBatched: Boolean,
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    runningWindowOptimizedData.foreach {
      case (func, outputIndexes) =>
        val aggAndReplaces = func.scan(isRunningBatched)
        // For now we need at least one column. For row number in the future we might be able
        // to change that, but I think this is fine.
        require(func.boundInputLocations.length == aggAndReplaces.length,
          s"Input locations for ${func.windowFunc} do not match aggregations " +
              s"${func.boundInputLocations.toSeq} vs $aggAndReplaces")
        val combined = withResource(
          new ArrayBuffer[cudf.ColumnVector](aggAndReplaces.length)) { replacedCols =>
          func.boundInputLocations.indices.foreach { aggIndex =>
            val inputColIndex = func.boundInputLocations(aggIndex)
            val inputCol = inputCb.column(inputColIndex).asInstanceOf[GpuColumnVector].getBase
            val anr = aggAndReplaces(aggIndex)
            val agg = anr.agg
            val replacePolicy = anr.nullReplacePolicy
            replacedCols +=
                withResource(inputCol.scan(agg, ScanType.INCLUSIVE, NullPolicy.EXCLUDE)) {
                  scanned =>
                    // For scans when nulls are excluded then each input row that has a null in it
                    // the output row also has a null in it. Typically this is not what we want,
                    // because for windows that only happens if the first values are nulls. So we
                    // will then call replace nulls as needed to fix that up. Typically the
                    // replacement policy is preceding.
                    replacePolicy.map(scanned.replaceNulls).getOrElse(scanned.incRefCount())
                }
          }
          func.scanCombine(isRunningBatched, replacedCols)
        }

        withResource(combined) { combined =>
          outputIndexes.foreach { outIndex =>
            require(outputColumns(outIndex) == null,
              "Attempted to overwrite a window output column!!")
            outputColumns(outIndex) = combined.incRefCount()
          }
        }
    }
  }

  // Part by is always ascending with nulls first, which is the default for group by options too
  private[this] val sortedGroupingOpts = GroupByOptions.builder()
      .withKeysSorted(true)
      .build()

  /**
   * Do just the grouped scan portion of a grouped scan aggregation.
   * @param isRunningBatched is this optimized for a running batch?
   * @param partByPositions what are the positions of the part by columns.
   * @param inputCb the input data to process
   * @return a Table that is the result of the aggregations. The partition
   *         by columns will be first, followed by one column for each aggregation in the order of
   *         `runningWindowOptimizedData`.
   */
  private final def justGroupedScan(
      isRunningBatched: Boolean,
      partByPositions: Array[Int],
      inputCb: ColumnarBatch): Table = {
    val allAggsWithInputs = runningWindowOptimizedData.map { case (func, _) =>
      func.groupByScan(isRunningBatched).zip(func.boundInputLocations)
    }.toArray

    val allAggs = allAggsWithInputs.flatMap { aggsWithInputs =>
      aggsWithInputs.map { case (aggAndReplace, index) =>
        aggAndReplace.agg.onColumn(index)
      }
    }

    val unoptimizedResult = withResource(GpuColumnVector.from(inputCb)) { initProjTab =>
      initProjTab.groupBy(sortedGroupingOpts, partByPositions: _*).scan(allAggs: _*)
    }
    // Our scan is sorted, but to comply with the API requirements of a non-sorted scan
    // the group/partition by columns are copied out. This is more memory then we want,
    // so we will replace them in the result with the same columns from the input batch
    withResource(unoptimizedResult) { unoptimizedResult =>
      withResource(new Array[cudf.ColumnVector](unoptimizedResult.getNumberOfColumns)) { cols =>
        // First copy over the part by columns
        partByPositions.zipWithIndex.foreach { case (inPos, outPos) =>
          cols(outPos) = inputCb.column(inPos).asInstanceOf[GpuColumnVector].getBase.incRefCount()
        }

        // Now copy over the scan results
        (partByPositions.length until unoptimizedResult.getNumberOfColumns).foreach { pos =>
          cols(pos) = unoptimizedResult.getColumn(pos).incRefCount()
        }
        new Table(cols: _*)
      }
    }
  }

  private final def groupedReplace(
      isRunningBatched: Boolean,
      partByPositions: Array[Int],
      tabFromScan: Table): Table = {
    // This gets a little complicated, because scan does not typically treat nulls the
    // way window treats nulls. So in some cases we need to do another group by and replace
    // the nulls to make them match what we want. But this is not all of the time, so we
    // keep track of which aggregations need to have a replace called on them, and where
    // we need to copy the results back out to. This is a little hard, but to try and help keep
    // track of it all the output of scan has the group by columns first followed by the scan
    // result columns in the order of `runningWindowOptimizedData`, and the output of
    // replace has the group by columns first followed by the replaced columns. So scans that
    // don't need a replace don't show up in the output of the replace call.
    val allReplace = ArrayBuffer[ReplacePolicyWithColumn]()
    val copyFromScan = ArrayBuffer[Int]()
    // We will not drop the partition by columns
    copyFromScan.appendAll(partByPositions.indices)

    // Columns to copy from the output of replace in the format of (fromReplaceIndex, toOutputIndex)
    val copyFromReplace = ArrayBuffer[(Int, Int)]()
    // Index of a column after it went through replace
    var afterReplaceIndex = partByPositions.length
    // Index of a column before it went through replace (this should be the same as the scan input
    // and the final output)
    var beforeReplaceIndex = partByPositions.length
    runningWindowOptimizedData.foreach { case (func, _) =>
      func.groupByScan(isRunningBatched).foreach { aggAndReplace =>
        val replace = aggAndReplace.nullReplacePolicy
        if (replace.isDefined) {
          allReplace.append(replace.get.onColumn(beforeReplaceIndex))
          copyFromReplace.append((afterReplaceIndex, beforeReplaceIndex))
          afterReplaceIndex += 1
        } else {
          copyFromScan.append(beforeReplaceIndex)
        }
        beforeReplaceIndex += 1
      }
    }

    withResource(new Array[cudf.ColumnVector](tabFromScan.getNumberOfColumns)) { columns =>
      copyFromScan.foreach { index =>
        columns(index) = tabFromScan.getColumn(index).incRefCount()
      }
      if (allReplace.nonEmpty) {
        // Don't bother to do the replace if none of them want anything replaced
        withResource(tabFromScan
            .groupBy(sortedGroupingOpts, partByPositions.indices: _*)
            .replaceNulls(allReplace: _*)) { replaced =>
          copyFromReplace.foreach { case (from, to) =>
            columns(to) = replaced.getColumn(from).incRefCount()
          }
        }
      }
      new Table(columns: _*)
    }
  }

  /**
   * Take the aggregation results and run `scanCombine` on them if needed before copying them to
   * the output location.
   */
  private final def combineAndOutput(isRunningBatched: Boolean,
      partByPositions: Array[Int],
      scannedAndReplaced: Table,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    var readIndex = partByPositions.length
    runningWindowOptimizedData.foreach { case (func, outputLocations) =>
      val numScans = func.boundInputLocations.length
      val columns =
        (readIndex until (readIndex + numScans)).map(scannedAndReplaced.getColumn).toArray
      withResource(func.scanCombine(isRunningBatched, columns)) { col =>
        outputLocations.foreach { outIndex =>
          require(outputColumns(outIndex) == null,
            "Attempted to overwrite a window output column!!")
          outputColumns(outIndex) = col.incRefCount()
        }
      }
      readIndex += numScans
    }
  }

  /**
   * Do any running window grouped scan aggregations.
   */
  private final def doRunningWindowGroupedScan(
      isRunningBatched: Boolean,
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    val replaced =
      withResource(justGroupedScan(isRunningBatched, partByPositions, inputCb)) { scanned =>
        groupedReplace(isRunningBatched, partByPositions, scanned)
      }
    withResource(replaced) { replaced =>
      combineAndOutput(isRunningBatched, partByPositions, replaced, outputColumns)
    }
  }

  /**
   * Do any running window optimized aggregations.
   */
  private def doRunningWindowOptimizedAggs(
      isRunningBatched: Boolean,
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    if (runningWindowOptimizedData.nonEmpty) {
      if (partByPositions.isEmpty) {
        // This is implemented in terms of a scan on a column
        doRunningWindowScan(isRunningBatched, inputCb, outputColumns)
      } else {
        doRunningWindowGroupedScan(isRunningBatched, partByPositions, inputCb, outputColumns)
      }
    }
  }

  /**
   * Do all of the aggregations and put them in the output columns. There may be extra processing
   * after this before you get to a final result.
   */
  def doAggs(isRunningBatched: Boolean,
      boundOrderSpec: Seq[SortOrder],
      orderByPositions: Array[Int],
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    doRunningWindowOptimizedAggs(isRunningBatched, partByPositions, inputCb, outputColumns)
    doRowAggs(boundOrderSpec, orderByPositions, partByPositions, inputCb, outputColumns)
    doRangeAggs(boundOrderSpec, orderByPositions, partByPositions, inputCb, outputColumns)
  }

  /**
   * Turn the final result of the aggregations into a ColumnarBatch.
   */
  def convertToColumnarBatch(dataTypes: Array[DataType],
      aggOutputColumns: Array[cudf.ColumnVector]): ColumnarBatch = {
    assert(dataTypes.length == aggOutputColumns.length)
    val numRows = aggOutputColumns.head.getRowCount.toInt
    closeOnExcept(new Array[ColumnVector](aggOutputColumns.length)) { finalOutputColumns =>
      dataTypes.indices.foreach { index =>
        val dt = dataTypes(index)
        val col = aggOutputColumns(index)
        finalOutputColumns(index) = GpuColumnVector.from(col, dt).incRefCount()
      }
      new ColumnarBatch(finalOutputColumns, numRows)
    }
  }
}

/**
 * Calculates the results of window operations. It assumes that any batching of the data
 * or fixups after the fact to get the right answer is done outside of this.
 */
trait BasicWindowCalc {
  val boundWindowOps: Seq[GpuExpression]
  val boundPartitionSpec: Seq[GpuExpression]
  val boundOrderSpec: Seq[SortOrder]

  /**
   * Is this going to do a batched running window optimization or not.
   */
  def isRunningBatched: Boolean

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
        val inputLocations = win.initialProjections(isRunningBatched)
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

  /**
   * Compute the basic aggregations. In some cases the resulting columns may not be the expected
   * types.  This could be caused by cudf type differences and can be fixed by calling
   * `castResultsIfNeeded` or it could be different because the window operations know about a
   * post processing step that needs to happen prior to `castResultsIfNeeded`.
   * @param cb the batch to do window aggregations on.
   * @return the cudf columns that are the results of doing the aggregations.
   */
  def computeBasicWindow(cb: ColumnarBatch): Array[cudf.ColumnVector] = {
    closeOnExcept(new Array[cudf.ColumnVector](boundWindowOps.length)) { outputColumns =>

      withResource(GpuProjectExec.project(cb, initialProjections)) { proj =>
        aggregations.doAggs(
          isRunningBatched,
          boundOrderSpec,
          orderByPositions,
          partByPositions,
          proj,
          outputColumns)
      }

      // if the window aggregates were successful, lets splice the passThrough
      // columns
      passThrough.foreach {
        case (inputIndex, outputIndex) =>
          outputColumns(outputIndex) =
            cb.column(inputIndex).asInstanceOf[GpuColumnVector].getBase.incRefCount()
      }

      outputColumns
    }
  }

  def convertToBatch(dataTypes: Array[DataType],
      cols: Array[cudf.ColumnVector]): ColumnarBatch =
    aggregations.convertToColumnarBatch(dataTypes, cols)
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
    val outputTypes: Array[DataType],
    numOutputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] with BasicWindowCalc {

  override def isRunningBatched: Boolean = false

  override def hasNext: Boolean = onDeck.isDefined || input.hasNext

  var onDeck: Option[SpillableColumnarBatch] = None

  override def next(): ColumnarBatch = {
    val cbSpillable = onDeck match {
      case Some(x) =>
        onDeck = None
        x
      case _ =>
        getNext()
    }
    withRetryNoSplit(cbSpillable) { _ =>
      withResource(cbSpillable.getColumnarBatch()) { cb =>
        withResource(new NvtxWithMetrics("window", NvtxColor.CYAN, opTime)) { _ =>
          val ret = withResource(computeBasicWindow(cb)) { cols =>
            convertToBatch(outputTypes, cols)
          }
          numOutputBatches += 1
          numOutputRows += ret.numRows()
          ret
        }
      }
    }
  }

  def getNext(): SpillableColumnarBatch = {
    SpillableColumnarBatch(input.next(), SpillPriorities.ACTIVE_BATCHING_PRIORITY)
  }

}

object GpuBatchedWindowIterator {
  def cudfAnd(lhs: cudf.ColumnVector,
      rhs: cudf.ColumnVector): cudf.ColumnVector = {
    withResource(lhs) { lhs =>
      withResource(rhs) { rhs =>
        lhs.and(rhs)
      }
    }
  }

  def areRowPartsEqual(
      scalars: Seq[Scalar],
      columns: Seq[cudf.ColumnVector],
      indexes: Seq[Int]): Array[Boolean] = {
    withResourceIfAllowed(arePartsEqual(scalars, columns)) {
      case scala.util.Right(ret) => Seq.fill(indexes.length)(ret).toArray
      case scala.util.Left(column) =>
        indexes.map { index =>
          withResource(column.getScalarElement(index)) { scalar =>
            scalar.isValid && scalar.getBoolean
          }
        }.toArray
    }
  }

  def arePartsEqual(
      scalars: Seq[Scalar],
      columns: Seq[cudf.ColumnVector]): Either[cudf.ColumnVector, Boolean] = {
    if (scalars.length != columns.length) {
      scala.util.Right(false)
    } else if (scalars.isEmpty && columns.isEmpty) {
      scala.util.Right(true)
    } else {
      scala.util.Left(computeMask(scalars, columns))
    }
  }

  private def computeMask(
      scalars: Seq[Scalar],
      columns: Seq[cudf.ColumnVector]): cudf.ColumnVector = {
    val dType = scalars.head.getType
    if (dType == DType.FLOAT32 || dType == DType.FLOAT64) {
      // We need to handle nans and nulls
      scalars.zip(columns).map {
        case (scalar, column) =>
          withResource(scalar.equalToNullAware(column)) { eq =>
            dType match {
              case DType.FLOAT32 if scalar.getFloat.isNaN =>
                withResource(column.isNan) { isNan =>
                  isNan.or(eq)
                }
              case DType.FLOAT64 if scalar.getDouble.isNaN =>
                withResource(column.isNan) { isNan =>
                  isNan.or(eq)
                }
              case _ => eq.incRefCount()
            }
          }
      }.reduce(cudfAnd)
    } else {
      scalars.zip(columns).map {
        case (scalar, column) => scalar.equalToNullAware(column)
      }.reduce(cudfAnd)
    }
  }

  def areOrdersEqual(
      scalars: Seq[Scalar],
      columns: Seq[cudf.ColumnVector],
      partsEqual: Either[cudf.ColumnVector, Boolean]): Either[cudf.ColumnVector, Boolean] = {
    if (scalars.length != columns.length) {
      scala.util.Right(false)
    } else if (scalars.isEmpty && columns.isEmpty) {
      // they are equal but only so far as the parts are also equal
      partsEqual match {
        case r @ scala.util.Right(_) => r
        case scala.util.Left(mask) => scala.util.Left(mask.incRefCount())
      }
    } else {
      // Part mask and order by equality mask
      partsEqual match {
        case r @ scala.util.Right(false) => r
        case scala.util.Right(true) =>
          scala.util.Left(computeMask(scalars, columns))
        case scala.util.Left(partMask) =>
          withResource(computeMask(scalars, columns)) { orderMask =>
            scala.util.Left(orderMask.and(partMask))
          }
      }
    }
  }

  def getScalarRow(index: Int, columns: Seq[cudf.ColumnVector]): Array[Scalar] =
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
    val outputTypes: Array[DataType],
    numOutputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] with BasicWindowCalc {
  import GpuBatchedWindowIterator._

  override def isRunningBatched: Boolean = true

  // This should only ever be cached in between calls to `hasNext` and `next`. This is just
  // to let us filter out empty batches.
  private val boundOrderColumns = boundOrderSpec.map(_.child)
  private var cachedBatch: Option[ColumnarBatch] = None
  private var lastParts: Array[Scalar] = Array.empty
  private var lastOrder: Array[Scalar] = Array.empty
  private var isClosed: Boolean = false

  TaskContext.get().addTaskCompletionListener[Unit](_ => close())

  private def saveLastParts(newLastParts: Array[Scalar]): Unit = {
    lastParts.foreach(_.close())
    lastParts = newLastParts
  }

  private def saveLastOrder(newLastOrder: Array[Scalar]): Unit = {
    lastOrder.foreach(_.close())
    lastOrder = newLastOrder
  }

  def close(): Unit = {
    if (!isClosed) {
      isClosed = true
      fixerIndexMap.values.foreach(_.close())
      saveLastParts(Array.empty)
      saveLastOrder(Array.empty)
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

  private lazy val fixerNeedsOrderMask = fixerIndexMap.values.exists(_.needsOrderMask)

  private def fixUpAll(computedWindows: Array[cudf.ColumnVector],
      fixers: Map[Int, BatchedRunningWindowFixer],
      samePartitionMask: Either[cudf.ColumnVector, Boolean],
      sameOrderMask: Option[Either[cudf.ColumnVector, Boolean]]): Array[cudf.ColumnVector] = {
    closeOnExcept(ArrayBuffer[cudf.ColumnVector]()) { newColumns =>
      boundWindowOps.indices.foreach { idx =>
        val column = computedWindows(idx)
        fixers.get(idx) match {
          case Some(fixer) =>
            closeOnExcept(fixer.fixUp(samePartitionMask, sameOrderMask, column)) { finalOutput =>
              newColumns += finalOutput
            }
          case None =>
            newColumns += column.incRefCount()
        }
      }
      newColumns.toArray
    }
  }

  def computeRunning(input: ColumnarBatch): ColumnarBatch = {
    val fixers = fixerIndexMap
    val numRows = input.numRows()
    val cbSpillable = SpillableColumnarBatch(input, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    withRetryNoSplit(cbSpillable) { _ =>
      withResource(cbSpillable.getColumnarBatch()) { cb =>
        withResource(computeBasicWindow(cb)) { basic =>
          var newOrder: Option[Array[Scalar]] = None
          var newParts: Option[Array[Scalar]] = None
          val fixedUp = try {
            // we backup the fixers state and restore it in the event of a retry
            withRestoreOnRetry(fixers.values.toSeq) {
              withResource(GpuProjectExec.project(cb,
                boundPartitionSpec)) { parts =>
                val partColumns = GpuColumnVector.extractBases(parts)
                withResourceIfAllowed(arePartsEqual(lastParts, partColumns)) { partsEqual =>
                  val fixedUp = if (fixerNeedsOrderMask) {
                    withResource(GpuProjectExec.project(cb,
                      boundOrderColumns)) { order =>
                      val orderColumns = GpuColumnVector.extractBases(order)
                      // We need to fix up the rows that are part of the same batch as the end of
                      // the last batch
                      withResourceIfAllowed(areOrdersEqual(lastOrder, orderColumns, partsEqual)) {
                        orderEqual =>
                          closeOnExcept(fixUpAll(basic, fixers, partsEqual, Some(orderEqual))) {
                            fixedUp =>
                              newOrder = Some(getScalarRow(numRows - 1, orderColumns))
                              fixedUp
                          }
                      }
                    }
                  } else {
                    // No ordering needed
                    fixUpAll(basic, fixers, partsEqual, None)
                  }
                  newParts = Some(getScalarRow(numRows - 1, partColumns))
                  fixedUp
                }
              }
            }
          } catch {
            case t: Throwable =>
              // avoid leaking unused interim results
              newOrder.foreach(_.foreach(_.close()))
              newParts.foreach(_.foreach(_.close()))
              throw t
          }
          // this section is outside of the retry logic because the calls to saveLastParts
          // and saveLastOrders can potentially close GPU resources
          withResource(fixedUp) { _ =>
            newOrder.foreach(saveLastOrder)
            newParts.foreach(saveLastParts)
            convertToBatch(outputTypes, fixedUp)
          }
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
    val cb = readNextInputBatch()
    withResource(new NvtxWithMetrics("RunningWindow", NvtxColor.CYAN, opTime)) { _ =>
      val ret = computeRunning(cb) // takes ownership of cb
      numOutputBatches += 1
      numOutputRows += ret.numRows()
      ret
    }
  }
}

/**
 * This allows for batches of data to be processed without needing them to correspond to
 * the partition by boundaries, but only for window operations that are unbounded preceding
 * to current row (Running Window). This works because a small amount of data can be saved
 * from a previous batch and used to update the current batch.
 */
case class GpuRunningWindowExec(
    windowOps: Seq[NamedExpression],
    gpuPartitionSpec: Seq[Expression],
    gpuOrderSpec: Seq[SortOrder],
    child: SparkPlan)(
    override val cpuPartitionSpec: Seq[Expression],
    override val cpuOrderSpec: Seq[SortOrder]) extends GpuWindowBaseExec {

  override def otherCopyArgs: Seq[AnyRef] = cpuPartitionSpec :: cpuOrderSpec :: Nil

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val boundWindowOps = GpuBindReferences.bindGpuReferences(windowOps, child.output)
    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(gpuPartitionSpec, child.output)
    val boundOrderSpec = GpuBindReferences.bindReferences(gpuOrderSpec, child.output)

    child.executeColumnar().mapPartitions { iter =>
      new GpuRunningWindowIterator(iter, boundWindowOps, boundPartitionSpec, boundOrderSpec,
        output.map(_.dataType).toArray, numOutputBatches, numOutputRows, opTime)
    }
  }
}

class FixerPair(op: GpuUnboundToUnboundWindowWithFixer) extends AutoCloseable {
  var fixing: BatchedUnboundedToUnboundedWindowFixer = op.newUnboundedToUnboundedFixer
  var collecting: BatchedUnboundedToUnboundedWindowFixer = op.newUnboundedToUnboundedFixer

  def updateState(scalar: Scalar): Unit = {
    collecting.updateState(scalar)
  }

  def fixUp(samePartitionMask: Either[cudf.ColumnVector, Boolean],
      column: cudf.ColumnVector): cudf.ColumnVector =
    fixing.fixUp(samePartitionMask, column)

  def swap(): Unit = {
    val tmp = fixing
    tmp.reset()
    fixing = collecting
    collecting = tmp
  }

  override def close(): Unit = {
    fixing.close()
    collecting.close()
  }
}

/**
 * An iterator that can do aggregations on window queries that need a small amount of
 * information from all of the batches to update the result in a second pass. It does this by
 * having the aggregations be instances of GpuUnboundToUnboundWindowWithFixer
 * which can fix up the window output for unbounded to unbounded windows.
 * Because of this there is no requirement about how the input data is batched, but it  must
 * be sorted by both partitioning and ordering.
 */
class GpuCachedDoublePassWindowIterator(
    input: Iterator[ColumnarBatch],
    override val boundWindowOps: Seq[GpuExpression],
    override val boundPartitionSpec: Seq[GpuExpression],
    override val boundOrderSpec: Seq[SortOrder],
    val outputTypes: Array[DataType],
    numOutputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] with BasicWindowCalc {
  import GpuBatchedWindowIterator._

  override def isRunningBatched: Boolean = true

  private var readyForPostProcessing = mutable.Queue[SpillableColumnarBatch]()
  private var firstPassProcessed = mutable.Queue[SpillableColumnarBatch]()
  // This should only ever be cached in between calls to `hasNext` and `next`.
  // This is just to let us filter out empty batches.
  private var waitingForFirstPass: Option[ColumnarBatch] = None
  private var lastPartsCaching: Array[Scalar] = Array.empty
  private var lastPartsProcessing: Array[Scalar] = Array.empty
  private var isClosed: Boolean = false

  TaskContext.get().addTaskCompletionListener[Unit](_ => close())

  private def saveLastPartsCaching(newLastParts: Array[Scalar]): Unit = {
    lastPartsCaching.foreach(_.close())
    lastPartsCaching = newLastParts
  }

  def close(): Unit = {
    if (!isClosed) {
      isClosed = true

      fixerIndexMap.values.foreach(_.close())

      saveLastPartsCaching(Array.empty)

      lastPartsProcessing.foreach(_.close())
      lastPartsProcessing = Array.empty

      firstPassProcessed.foreach(_.close())
      firstPassProcessed = mutable.Queue[SpillableColumnarBatch]()

      readyForPostProcessing.foreach(_.close())
      readyForPostProcessing = mutable.Queue[SpillableColumnarBatch]()

      waitingForFirstPass.foreach(_.close())
      waitingForFirstPass = None
    }
  }

  private lazy val fixerIndexMap: Map[Int, FixerPair] =
    boundWindowOps.zipWithIndex.flatMap {
      case (GpuAlias(GpuWindowExpression(func, _), _), index) =>
        func match {
          case f: GpuUnboundToUnboundWindowWithFixer =>
            Some((index, new FixerPair(f)))
          case GpuAggregateExpression(f: GpuUnboundToUnboundWindowWithFixer, _, _, _, _) =>
            Some((index, new FixerPair(f)))
          case _ => None
        }
      case _ => None
    }.toMap

  // Do any post processing fixup for the batch before it is sent out the door
  def postProcess(cb: ColumnarBatch): ColumnarBatch = {
    val computedWindows = GpuColumnVector.extractBases(cb)
    withResource(GpuProjectExec.project(cb, boundPartitionSpec)) { parts =>
      val partColumns = GpuColumnVector.extractBases(parts)
      withResourceIfAllowed(arePartsEqual(lastPartsProcessing, partColumns)) { samePartitionMask =>
        withResource(ArrayBuffer[cudf.ColumnVector]()) { newColumns =>
          boundWindowOps.indices.foreach { idx =>
            val column = computedWindows(idx)
            fixerIndexMap.get(idx) match {
              case Some(fixer) =>
                closeOnExcept(fixer.fixUp(samePartitionMask, column)) { finalOutput =>
                  newColumns += finalOutput
                }
              case None =>
                newColumns += column.incRefCount()
            }
          }
          makeBatch(newColumns)
        }
      }
    }
  }

  def makeBatch(columns: Seq[cudf.ColumnVector]): ColumnarBatch = {
    withResource(new cudf.Table(columns: _*)) { table =>
      GpuColumnVector.from(table, outputTypes)
    }
  }

  def swapFirstPassIsReadyForPost(): Unit = {
    // Swap the caching so it is ready to be used for updating
    fixerIndexMap.values.foreach(_.swap())

    // Swap the parts so we know what mask to use for updating
    lastPartsProcessing.foreach(_.close())
    lastPartsProcessing = lastPartsCaching
    lastPartsCaching = Array.empty

    // Swap the queues so we are ready to dequeue the data
    // Before we swap this must be empty, or we are dropping data...
    assert(readyForPostProcessing.isEmpty)
    readyForPostProcessing = firstPassProcessed
    firstPassProcessed = mutable.Queue[SpillableColumnarBatch]()
  }

  // The last batch was already processed so everything in processed needs to be moved to
  // readyForPostProcessing
  def lastBatch(): Unit = swapFirstPassIsReadyForPost()

  private def cacheInFixers(computedWindows: Array[cudf.ColumnVector],
      fixers: Map[Int, FixerPair],
      rowIndex: Int): Unit =
    fixers.foreach {
      case (columnIndex, fixer) =>
        val column = computedWindows(columnIndex)
        withResource(column.getScalarElement(rowIndex)) { scalar =>
          fixer.updateState(scalar)
        }
    }

  def saveBatchForPostProcessing(batch: ColumnarBatch): Unit = {
    firstPassProcessed += SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
  }

  def saveBatchForPostProcessing(basic: Array[cudf.ColumnVector]): Unit = {
    closeOnExcept(makeBatch(basic)) { batch =>
      saveBatchForPostProcessing(batch)
    }
  }

  // Compute the window operation and cache/update caches as needed.
  // This method takes ownership of cb
  def firstPassComputeAndCache(cb: ColumnarBatch): Unit = {
    val fixers = fixerIndexMap
    val numRows = cb.numRows()

    val sp = SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
    val (basic, parts) = withRetryNoSplit(sp) { _ =>
      withResource(sp.getColumnarBatch()) { batch =>
        closeOnExcept(computeBasicWindow(batch)) { basic =>
          (basic, GpuProjectExec.project(batch, boundPartitionSpec))
        }
      }
    }

    withResource(basic) { _ =>
      withResource(parts) { _ =>
        val partColumns = GpuColumnVector.extractBases(parts)

        val firstLastEqual = areRowPartsEqual(lastPartsCaching, partColumns, Seq(0, numRows - 1))
        val firstEqual = firstLastEqual(0)
        val lastEqual = firstLastEqual(1)
        if (firstEqual) {
          // This batch is a continuation of the previous batch so we need to update the
          // fixer with info from it.
          // This assumes that the window is unbounded to unbounded. We will need to update
          // APIs in the future and rename things if we want to support more than this.
          cacheInFixers(basic, fixers, 0)
        }

        // If the last part entry in this batch does not match the last entry in the previous batch
        // then we need to start post-processing the batches.
        if (!lastEqual) {
          // We swap the fixers and queues so we are ready to start on the next partition by group
          swapFirstPassIsReadyForPost()
          // Collect/Cache the needed info from the end of this batch
          cacheInFixers(basic, fixers, numRows - 1)
          saveLastPartsCaching(getScalarRow(numRows - 1, partColumns))

          if (firstEqual) {
            // Process the batch now, but it will only be for the first part of the batch
            // the last part may need to be fixed again, so put it into the queue for
            // when the next round finishes.
            val processedBatch = withResource(makeBatch(basic)) { basicBatch =>
              postProcess(basicBatch)
            }
            closeOnExcept(processedBatch) { processedBatch =>
              saveBatchForPostProcessing(processedBatch)
            }
          } else {
            // We split on a partition boundary, so just need to save it
            saveBatchForPostProcessing(basic)
          }
        } else {
          // No need to save the parts, it was equal...
          saveBatchForPostProcessing(basic)
        }
      }
    }
  }

  private def cacheBatchIfNeeded(): Unit = {
    while (waitingForFirstPass.isEmpty && input.hasNext) {
      closeOnExcept(input.next()) { cb =>
        if (cb.numRows() > 0) {
          waitingForFirstPass = Some(cb)
        } else {
          cb.close()
        }
      }
    }
  }

  override def hasNext: Boolean = {
    if (readyForPostProcessing.nonEmpty || firstPassProcessed.nonEmpty) {
      true
    } else {
      cacheBatchIfNeeded()
      waitingForFirstPass.isDefined
    }
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    while (readyForPostProcessing.isEmpty) {
      // Keep reading and processing data until we have something to output
      cacheBatchIfNeeded()
      if (waitingForFirstPass.isEmpty) {
        lastBatch()
      } else {
        val cb = waitingForFirstPass.get
        waitingForFirstPass = None
        withResource(
            new NvtxWithMetrics("DoubleBatchedWindow_PRE", NvtxColor.CYAN, opTime)) { _ =>
          // firstPassComputeAndCache takes ownership of the batch passed to it
          firstPassComputeAndCache(cb)
        }
      }
    }
    withRetryNoSplit(readyForPostProcessing.dequeue()) { sb =>
      withResource(sb.getColumnarBatch()) { cb =>
        val ret = withResource(
          new NvtxWithMetrics("DoubleBatchedWindow_POST", NvtxColor.BLUE, opTime)) { _ =>
          postProcess(cb)
        }
        numOutputBatches += 1
        numOutputRows += ret.numRows()
        ret
      }
    }
  }
}

/**
 * This allows for batches of data to be processed without needing them to correspond to
 * the partition by boundaries. This is similar to GpuRunningWindowExec, but for operations
 * that need a small amount of information from all of the batches associated with a partition
 * instead of just the previous batch. It does this by processing a batch, collecting and
 * updating a small cache of information about the last partition in the batch, and then putting
 * that batch into a form that would let it be spilled if needed. A batch is released when the
 * last partition key in the batch is fully processed. Before it is released it will be updated
 * to include any information needed from the cached data.
 *
 * Currently this only works for unbounded to unbounded windows, but could be extended to more.
 */
case class GpuCachedDoublePassWindowExec(
    windowOps: Seq[NamedExpression],
    gpuPartitionSpec: Seq[Expression],
    gpuOrderSpec: Seq[SortOrder],
    child: SparkPlan)(
    override val cpuPartitionSpec: Seq[Expression],
    override val cpuOrderSpec: Seq[SortOrder]) extends GpuWindowBaseExec {

  override def otherCopyArgs: Seq[AnyRef] = cpuPartitionSpec :: cpuOrderSpec :: Nil

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val boundWindowOps = GpuBindReferences.bindGpuReferences(windowOps, child.output)
    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(gpuPartitionSpec, child.output)
    val boundOrderSpec = GpuBindReferences.bindReferences(gpuOrderSpec, child.output)

    child.executeColumnar().mapPartitions { iter =>
      new GpuCachedDoublePassWindowIterator(iter, boundWindowOps, boundPartitionSpec,
        boundOrderSpec, output.map(_.dataType).toArray, numOutputBatches, numOutputRows, opTime)
    }
  }
}

case class GpuWindowExec(
    windowOps: Seq[NamedExpression],
    gpuPartitionSpec: Seq[Expression],
    gpuOrderSpec: Seq[SortOrder],
    child: SparkPlan)(
    override val cpuPartitionSpec: Seq[Expression],
    override val cpuOrderSpec: Seq[SortOrder]) extends GpuWindowBaseExec {

  override def otherCopyArgs: Seq[AnyRef] = cpuPartitionSpec :: cpuOrderSpec :: Nil

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(outputBatching)

  override def outputBatching: CoalesceGoal = if (gpuPartitionSpec.isEmpty) {
    RequireSingleBatch
  } else {
    BatchedByKey(gpuPartitionOrdering)(cpuPartitionOrdering)
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val boundWindowOps = GpuBindReferences.bindGpuReferences(windowOps, child.output)
    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(gpuPartitionSpec, child.output)
    val boundOrderSpec = GpuBindReferences.bindReferences(gpuOrderSpec, child.output)

    child.executeColumnar().mapPartitions { iter =>
        new GpuWindowIterator(iter, boundWindowOps, boundPartitionSpec, boundOrderSpec,
          output.map(_.dataType).toArray, numOutputBatches, numOutputRows, opTime)
    }
  }
}
