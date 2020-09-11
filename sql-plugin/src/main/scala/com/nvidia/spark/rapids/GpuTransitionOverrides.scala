/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, SortOrder}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, CustomShuffleReaderExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.rapids.{GpuDataSourceScanExec, GpuFileSourceScanExec, GpuInputFileBlockLength, GpuInputFileBlockStart, GpuInputFileName}
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExecBase, GpuCustomShuffleReaderExec, GpuShuffleExchangeExecBase}

/**
 * Rules that run after the row to columnar and columnar to row transitions have been inserted.
 * These rules insert transitions to and from the GPU, and then optimize various transitions.
 */
class GpuTransitionOverrides extends Rule[SparkPlan] {
  var conf: RapidsConf = null

  def optimizeGpuPlanTransitions(plan: SparkPlan): SparkPlan = plan match {
    case HostColumnarToGpu(r2c: RowToColumnarExec, goal) =>
      GpuRowToColumnarExec(optimizeGpuPlanTransitions(r2c.child), goal)
    case ColumnarToRowExec(bb: GpuBringBackToHost) =>
      getColumnarToRowExec(optimizeGpuPlanTransitions(bb.child))
    case p =>
      p.withNewChildren(p.children.map(optimizeGpuPlanTransitions))
  }

  private def getColumnarToRowExec(plan: SparkPlan, exportColumnRdd: Boolean = false) = {
    ShimLoader.getSparkShims.getGpuColumnarToRowTransition(plan, exportColumnRdd)
  }

  def optimizeAdaptiveTransitions(
      plan: SparkPlan,
      parent: Option[SparkPlan]): SparkPlan = plan match {
    case HostColumnarToGpu(r2c: RowToColumnarExec, goal) =>
      GpuRowToColumnarExec(optimizeAdaptiveTransitions(r2c.child, Some(r2c)), goal)

    case ColumnarToRowExec(GpuBringBackToHost(
      GpuCoalesceBatches(e: GpuShuffleExchangeExecBase, _))) if parent.isEmpty =>
      // If this coalesced exchange has no parent it means that this plan is being created
      // as a query stage when AQE is on. Because we must return an operator that implements
      // ShuffleExchangeLike we need to remove the GpuCoalesceBatches operator here and
      // re-insert it around the GpuCustomShuffleReaderExec in the parent query stage when that
      // is created. Another option would be to create a custom operator that combines
      // GpuCoalesceBatches and GpuShuffleExchangeExec and return that here instead. See
      // https://github.com/NVIDIA/spark-rapids/issues/719 for more information.
      optimizeAdaptiveTransitions(e, Some(plan))

    case e: GpuCustomShuffleReaderExec =>
      // this is where we re-insert the GpuCoalesceBatches that we removed from the
      // shuffle exchange
      GpuCoalesceBatches(e.copy(child = optimizeAdaptiveTransitions(e.child, Some(e))),
          TargetSize(Long.MaxValue))

    // Query stages that have already executed on the GPU could be used by CPU operators
    // in future query stages. Note that because these query stages have already executed, we
    // don't need to recurse down and optimize them again
    case ColumnarToRowExec(e: BroadcastQueryStageExec) =>
      getColumnarToRowExec(e)
    case ColumnarToRowExec(e: ShuffleQueryStageExec) =>
      getColumnarToRowExec(e)

    case HostColumnarToGpu(e: BroadcastQueryStageExec, _) => e
    case HostColumnarToGpu(e: ShuffleQueryStageExec, _) => e

    case ColumnarToRowExec(bb: GpuBringBackToHost) =>
      optimizeAdaptiveTransitions(bb.child, Some(bb)) match {
        case e: GpuBroadcastExchangeExecBase => e
        case e: GpuShuffleExchangeExecBase => e
        case other => getColumnarToRowExec(other)
      }

    case p =>
      p.withNewChildren(p.children.map(c => optimizeAdaptiveTransitions(c, Some(p))))
  }

  def optimizeCoalesce(plan: SparkPlan): SparkPlan = plan match {
    case c2r: GpuColumnarToRowExecParent if c2r.child.isInstanceOf[GpuCoalesceBatches] =>
      // Don't build a batch if we are just going to go back to ROWS
      val co = c2r.child.asInstanceOf[GpuCoalesceBatches]
      c2r.withNewChildren(co.children.map(optimizeCoalesce))
    case GpuCoalesceBatches(r2c: GpuRowToColumnarExec, goal: TargetSize) =>
      // TODO in the future we should support this for all goals, but
      // GpuRowToColumnarExec preallocates all of the memory, and the builder does not
      // support growing the sizes dynamically....

      // Don't build batches and then coalesce, just build the right sized batch
      GpuRowToColumnarExec(optimizeCoalesce(r2c.child), CoalesceGoal.max(goal, r2c.goal))
    case GpuCoalesceBatches(co: GpuCoalesceBatches, goal) =>
      GpuCoalesceBatches(optimizeCoalesce(co.child), CoalesceGoal.max(goal, co.goal))
    case p =>
      p.withNewChildren(p.children.map(optimizeCoalesce))
  }

  private def insertCoalesce(plans: Seq[SparkPlan], goals: Seq[CoalesceGoal],
      disableUntilInput: Boolean): Seq[SparkPlan] = {
    plans.zip(goals).map {
      case (plan, null) =>
        // No coalesce requested
        insertCoalesce(plan, disableUntilInput)
      case (plan, goal @ RequireSingleBatch) =>
        // Even if coalesce is disabled a single batch is required to make this operator work
        // This should not cause bugs because we require a single batch in situations where
        // Spark also buffers data, so any operator that needs coalesce disabled would also
        // get an incorrect answer in regular Spark
        GpuCoalesceBatches(insertCoalesce(plan, disableUntilInput), goal)
      case (plan, _) if disableUntilInput =>
        // We wanted to coalesce the input but cannot because it could cause errors
        insertCoalesce(plan, disableUntilInput)
      case (plan, goal) =>
        GpuCoalesceBatches(insertCoalesce(plan, disableUntilInput), goal)
    }
  }

  /**
   * Essentially check if this plan is in the same task as a file input.
   */
  private def hasDirectLineToInput(plan: SparkPlan): Boolean = plan match {
    case _: Exchange => false
    case _: DataSourceScanExec => true
    case _: GpuDataSourceScanExec => true
    case _: DataSourceV2ScanExecBase => true
    case _: RDDScanExec => true // just in case an RDD was reading in data
    case p => p.children.exists(hasDirectLineToInput)
  }

  /**
   * Essentially check if we have hit a boundary of a task.
   */
  private def shouldEnableCoalesce(plan: SparkPlan): Boolean = plan match {
    case _: Exchange => true
    case _: DataSourceScanExec => true
    case _: GpuDataSourceScanExec => true
    case _: DataSourceV2ScanExecBase => true
    case _: RDDScanExec => true // just in case an RDD was reading in data
    case _ => false
  }

  /**
   * Because we cannot change the executors in spark itself we need to try and account for
   * the ones that might have issues with coalesce here.
   */
  private def disableCoalesceUntilInput(exec: Expression): Boolean = exec match {
    case _: InputFileName => true
    case _: InputFileBlockStart => true
    case _: InputFileBlockLength => true
    case e => e.children.exists(disableCoalesceUntilInput)
  }

  /**
   * Because we cannot change the executors in spark itself we need to try and account for
   * the ones that might have issues with coalesce here.
   */
  private def disableCoalesceUntilInput(plan: SparkPlan): Boolean = {
    plan.expressions.exists(disableCoalesceUntilInput)
  }

  // This walks from the output to the input so disableUntilInput can walk its way from when
  // we hit something that cannot allow for coalesce up until the input
  private def insertCoalesce(plan: SparkPlan,
      disableUntilInput: Boolean = false): SparkPlan = plan match {
    case exec: GpuExec =>
      // We will disable coalesce if it is already disabled and we cannot re-enable it
      val shouldDisable = (disableUntilInput && !shouldEnableCoalesce(exec)) ||
        //or if we should disable it and it is in a stage with a file input that would matter
        (exec.disableCoalesceUntilInput() && hasDirectLineToInput(exec))
      val tmp = exec.withNewChildren(insertCoalesce(exec.children, exec.childrenCoalesceGoal,
        shouldDisable))
      if (exec.coalesceAfter && !shouldDisable) {
        GpuCoalesceBatches(tmp, TargetSize(conf.gpuTargetBatchSizeBytes))
      } else {
        tmp
      }
    case p =>
      // We will disable coalesce if it is already disabled and we cannot re-enable it
      val shouldDisable = disableUntilInput && !shouldEnableCoalesce(p)  ||
        //or if we should disable it and it is in a stage with a file input that would matter
        (disableCoalesceUntilInput(p) && hasDirectLineToInput(p))
      p.withNewChildren(p.children.map(c => insertCoalesce(c, shouldDisable)))
  }

  /**
   * Inserts a transition to be running on the CPU columnar
   */
  private def insertColumnarFromGpu(plan: SparkPlan): SparkPlan = {
    if (plan.supportsColumnar && plan.isInstanceOf[GpuExec]) {
      GpuBringBackToHost(insertColumnarToGpu(plan))
    } else {
      plan.withNewChildren(plan.children.map(insertColumnarFromGpu))
    }
  }

  /**
   * Inserts a transition to be running on the GPU from CPU columnar
   */
  private def insertColumnarToGpu(plan: SparkPlan): SparkPlan = {
    if (plan.supportsColumnar && !plan.isInstanceOf[GpuExec]) {
      HostColumnarToGpu(insertColumnarFromGpu(plan), TargetSize(conf.gpuTargetBatchSizeBytes))
    } else {
      plan.withNewChildren(plan.children.map(insertColumnarToGpu))
    }
  }

  private def insertHashOptimizeSorts(plan: SparkPlan): SparkPlan = {
    if (conf.enableHashOptimizeSort) {
      // Insert a sort after the last hash-based op before the query result if there are no
      // intermediate nodes that have a specified sort order. This helps with the size of
      // Parquet and Orc files
      plan match {
        case s if ShimLoader.getSparkShims.isGpuHashJoin(s) =>
          val sortOrder = getOptimizedSortOrder(plan)
          GpuSortExec(sortOrder, false, plan, TargetSize(conf.gpuTargetBatchSizeBytes))
        case _: GpuHashAggregateExec =>
          val sortOrder = getOptimizedSortOrder(plan)
          GpuSortExec(sortOrder, false, plan, TargetSize(conf.gpuTargetBatchSizeBytes))
        case p =>
          if (p.outputOrdering.isEmpty) {
            plan.withNewChildren(plan.children.map(insertHashOptimizeSorts))
          } else {
            plan
          }
      }
    } else {
      plan
    }
  }

  private def getOptimizedSortOrder(plan: SparkPlan): Seq[SortOrder] = {
    plan.output.map { expr =>
      val wrapped = GpuOverrides.wrapExpr(expr, conf, None)
      wrapped.tagForGpu()
      assert(wrapped.canThisBeReplaced)
      SortOrder(
        wrapped.convertToGpu(),
        Ascending,
        Ascending.defaultNullOrdering,
        Set.empty
      )
    }
  }

  private def getBaseNameFromClass(planClassStr: String): String = {
    val firstDotIndex = planClassStr.lastIndexOf(".")
    if (firstDotIndex != -1) planClassStr.substring(firstDotIndex + 1) else planClassStr
  }

  def assertIsOnTheGpu(exp: Expression, conf: RapidsConf): Unit = {
    // There are no GpuAttributeReference or GpuSortOrder
    if (!exp.isInstanceOf[AttributeReference] &&
        !exp.isInstanceOf[SortOrder] &&
        !exp.isInstanceOf[GpuExpression] &&
      !conf.testingAllowedNonGpu.contains(getBaseNameFromClass(exp.getClass.toString))) {
      throw new IllegalArgumentException(s"The expression $exp is not columnar ${exp.getClass}")
    }
    exp.children.foreach(subExp => assertIsOnTheGpu(subExp, conf))
  }

  def assertIsOnTheGpu(plan: SparkPlan, conf: RapidsConf): Unit = {
    val isAdaptiveEnabled = plan.conf.adaptiveExecutionEnabled
    plan match {
      case e: Exchange if isAdaptiveEnabled &&
          ShimLoader.getSparkShims.isBroadcastExchangeLike(e) =>
        // broadcasts are left on CPU for now when AQE is enabled
      case _: BroadcastHashJoinExec | _: BroadcastNestedLoopJoinExec
          if isAdaptiveEnabled =>
        // broadcasts are left on CPU for now when AQE is enabled
      case _: AdaptiveSparkPlanExec | _: QueryStageExec | _: CustomShuffleReaderExec =>
        // we do not yet fully support GPU-acceleration when AQE is enabled, so we skip checking
        // the plan in this case - https://github.com/NVIDIA/spark-rapids/issues/5
      case lts: LocalTableScanExec =>
        if (!lts.expressions.forall(_.isInstanceOf[AttributeReference])) {
          throw new IllegalArgumentException("It looks like some operations were " +
            s"pushed down to LocalTableScanExec ${lts.expressions.mkString(",")}")
        }
      case imts: InMemoryTableScanExec =>
        if (!imts.expressions.forall(_.isInstanceOf[AttributeReference])) {
          throw new IllegalArgumentException("It looks like some operations were " +
            s"pushed down to InMemoryTableScanExec ${imts.expressions.mkString(",")}")
        }
      case _: GpuColumnarToRowExecParent => () // Ignored
      case _: ExecutedCommandExec => () // Ignored
      case _: RDDScanExec => () // Ignored
      case _: ShuffleExchangeExec => () // Ignored for now, we don't force it to the GPU if
                                        // children are not on the gpu
      case other =>
        if (!plan.supportsColumnar &&
          !conf.testingAllowedNonGpu.contains(getBaseNameFromClass(other.getClass.toString))) {
          throw new IllegalArgumentException(s"Part of the plan is not columnar " +
            s"${plan.getClass}\n${plan}")
        }
        // filter out the output expressions since those are not GPU expressions
        val planOutput = plan.output.toSet
        // avoid checking expressions of GpuFileSourceScanExec since all expressions are
        // processed by driver and not run on GPU.
        if (!plan.isInstanceOf[GpuFileSourceScanExec]) {
          plan.expressions.filter(_ match {
            case a: Attribute => !planOutput.contains(a)
            case _ => true
          }).foreach(assertIsOnTheGpu(_, conf))
        }
    }
    plan.children.foreach(assertIsOnTheGpu(_, conf))
  }

  def detectAndTagFinalColumnarOutput(plan: SparkPlan): SparkPlan = plan match {
    case d: DeserializeToObjectExec if d.child.isInstanceOf[GpuColumnarToRowExecParent] =>
      val gpuColumnar = d.child.asInstanceOf[GpuColumnarToRowExecParent]
      plan.withNewChildren(Seq(getColumnarToRowExec(gpuColumnar.child, true)))
    case _ => plan
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    this.conf = new RapidsConf(plan.conf)
    if (conf.isSqlEnabled) {
      var updatedPlan = insertHashOptimizeSorts(plan)
      updatedPlan = insertCoalesce(insertColumnarFromGpu(updatedPlan))
      updatedPlan = optimizeCoalesce(if (plan.conf.adaptiveExecutionEnabled) {
        optimizeAdaptiveTransitions(updatedPlan, None)
      } else {
        optimizeGpuPlanTransitions(updatedPlan)
      })
      if (conf.exportColumnarRdd) {
        updatedPlan = detectAndTagFinalColumnarOutput(updatedPlan)
      }
      if (conf.isTestEnabled) {
        assertIsOnTheGpu(updatedPlan, conf)
      }
      updatedPlan
    } else {
      plan
    }
  }
}
