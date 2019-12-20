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

import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.objects.CreateExternalRow
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarToRowExec, DeserializeToObjectExec, GpuBroadcastHashJoinExec, LocalTableScanExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

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
      GpuColumnarToRowExec(optimizeGpuPlanTransitions(bb.child))
    case p =>
      p.withNewChildren(p.children.map(optimizeGpuPlanTransitions))
  }

  def optimizeCoalesce(plan: SparkPlan): SparkPlan = plan match {
    case c2r: GpuColumnarToRowExec if c2r.child.isInstanceOf[GpuCoalesceBatches] =>
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

  private def insertCoalesce(plans: Seq[SparkPlan], goals: Seq[CoalesceGoal]): Seq[SparkPlan] = {
    plans.zip(goals).map {
      case (plan, null) => insertCoalesce(plan)
      case (plan, goal) => GpuCoalesceBatches(insertCoalesce(plan), goal)
    }
  }

  private def insertCoalesce(plan: SparkPlan): SparkPlan = plan match {
    case exec: GpuExec =>
      val tmp = exec.withNewChildren(insertCoalesce(exec.children, exec.childrenCoalesceGoal))
      if (exec.coalesceAfter) {
        GpuCoalesceBatches(tmp, TargetSize(conf.gpuTargetBatchSizeRows.toLong))
      } else {
        tmp
      }
    case p =>
      p.withNewChildren(p.children.map(insertCoalesce))
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
      HostColumnarToGpu(insertColumnarFromGpu(plan), TargetSize(conf.gpuTargetBatchSizeRows.toLong))
    } else {
      plan.withNewChildren(plan.children.map(insertColumnarToGpu))
    }
  }

  private def insertHashOptimizeSorts(plan: SparkPlan): SparkPlan = {
    if (conf.enableHashOptimizeSort) {
      // Insert a sort after the last hash join before the query result if there are no
      // intermediate nodes that have a specified sort order.
      plan match {
        case p: GpuBroadcastHashJoinExec =>
          val sortOrder = getOptimizedSortOrder(plan)
          GpuSortExec(sortOrder, false, plan, TargetSize(conf.gpuTargetBatchSizeRows.toLong))
        case p: GpuShuffledHashJoinExec =>
          val sortOrder = getOptimizedSortOrder(plan)
          GpuSortExec(sortOrder, false, plan, TargetSize(conf.gpuTargetBatchSizeRows.toLong))
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

  private def getOptimizedSortOrder(plan: SparkPlan): Seq[GpuSortOrder] = {
    plan.output.map { expr =>
      val wrapped = GpuOverrides.wrapExpr(expr, conf, None)
      wrapped.tagForGpu()
      assert(wrapped.canThisBeReplaced)
      GpuSortOrder(
        wrapped.convertToGpu(),
        Ascending,
        Ascending.defaultNullOrdering,
        Set.empty,
        expr
      )
    }
  }

  private def getBaseNameFromClass(planClassStr: String): String = {
    val firstDotIndex = planClassStr.lastIndexOf(".")
    if (firstDotIndex != -1) planClassStr.substring(firstDotIndex + 1) else planClassStr
  }

  def assertIsOnTheGpu(exp: Expression, conf: RapidsConf): Unit = {
    if (!exp.isInstanceOf[GpuExpression] &&
      !conf.testingAllowedNonGpu.contains(getBaseNameFromClass(exp.getClass.toString))) {
      throw new IllegalArgumentException(s"The expression ${exp} is not columnar ${exp.getClass}")
    }
  }

  def assertIsOnTheGpu(plan: SparkPlan, conf: RapidsConf): Unit = {
    plan match {
      case lts: LocalTableScanExec =>
        if (!lts.expressions.forall(_.isInstanceOf[AttributeReference])) {
          throw new IllegalArgumentException("It looks like some operations were " +
            s"pushed down to LocalTableScanExec ${lts.expressions.mkString(",")}")
        }
      case _: GpuColumnarToRowExec => () // Ignored
      case _: ShuffleExchangeExec => () // Ignored for now
      case other =>
        if (!plan.supportsColumnar &&
          !conf.testingAllowedNonGpu.contains(getBaseNameFromClass(other.getClass.toString))) {
          throw new IllegalArgumentException(s"Part of the plan is not columnar ${plan.getClass}\n${plan}")
        }
        // filter out the output expressions since those are not GPU expressions
        val planOutput = plan.output.toSet
        plan.expressions.filter(_  match {
          case a: Attribute => !planOutput.contains(a)
          case _ => true
        }).foreach(assertIsOnTheGpu(_, conf))
    }
    plan.children.foreach(assertIsOnTheGpu(_, conf))
  }

  def detectAndTagFinalColumnarOutput(plan: SparkPlan): SparkPlan = plan match {
    case DeserializeToObjectExec(_: CreateExternalRow, _, child: GpuColumnarToRowExec) =>
      plan.withNewChildren(Seq(GpuColumnarToRowExec(child.child, true)))
    case _ => plan
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    this.conf = new RapidsConf(plan.conf)
    if (conf.isSqlEnabled) {
      var updatedPlan = insertHashOptimizeSorts(plan)
      updatedPlan = insertCoalesce(insertColumnarFromGpu(updatedPlan))
      updatedPlan = optimizeCoalesce(optimizeGpuPlanTransitions(updatedPlan))
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
