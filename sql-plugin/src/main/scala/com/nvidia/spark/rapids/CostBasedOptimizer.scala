/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftAnti, LeftSemi}
import org.apache.spark.sql.execution.{GlobalLimitExec, LocalLimitExec, ProjectExec, SparkPlan, TakeOrderedAndProjectExec, UnionExec}
import org.apache.spark.sql.execution.adaptive.{CustomShuffleReaderExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

class CostBasedOptimizer(conf: RapidsConf) extends Logging {

  // the intention is to make the cost model pluggable since we are probably going to need to
  // experiment a fair bit with this part
  private val costModel = new DefaultCostModel(conf)

  /**
   * Walk the plan and determine CPU and GPU costs for each operator and then make decisions
   * about whether operators should run on CPU or GPU.
   *
   * @param plan The plan to optimize
   * @return A list of optimizations that were applied
   */
  def optimize(plan: SparkPlanMeta[SparkPlan]): Seq[Optimization] = {
    val optimizations = new ListBuffer[Optimization]()
    recursivelyOptimize(plan, optimizations, finalOperator = true)
    optimizations
  }


  /**
   * Walk the plan and determine CPU and GPU costs for each operator and then make decisions
   * about whether operators should run on CPU or GPU.
   *
   * @param plan The plan to optimize
   * @param optimizations Accumulator to store the optimizations that are applied
   * @param finalOperator Is this the final (root) operator? We have special behavior for this
   *                      case because we need the final output to be on the CPU in row format
   * @return Tuple containing (cpuCost, gpuCost) for the specified plan and the subset of the
   *         tree beneath it that is a candidate for optimization.
   */
  private def recursivelyOptimize(
      plan: SparkPlanMeta[SparkPlan],
      optimizations: ListBuffer[Optimization],
      finalOperator: Boolean): (Double, Double) = {

    // get the CPU and GPU cost of the child plan(s)
    val childCosts = plan.childPlans
        .map(child => recursivelyOptimize(
          child.asInstanceOf[SparkPlanMeta[SparkPlan]],
          optimizations,
          finalOperator = false))

    val (childCpuCosts, childGpuCosts) = childCosts.unzip

    // get the CPU and GPU cost of this operator (excluding cost of children)
    val (operatorCpuCost, operatorGpuCost) = costModel.applyCost(plan)

    // calculate total (this operator + children)
    val totalCpuCost = operatorCpuCost + childCpuCosts.sum
    var totalGpuCost = operatorGpuCost + childGpuCosts.sum

    plan.estimatedOutputRows = RowCountPlanVisitor.visit(plan)

    // determine how many transitions between CPU and GPU are taking place between
    // the child operators and this operator
    val numTransitions = plan.childPlans
      .count(_.canThisBeReplaced != plan.canThisBeReplaced)

    if (numTransitions > 0) {
      // there are transitions between CPU and GPU so we need to calculate the transition costs
      // and also make decisions based on those costs to see whether any parts of the plan would
      // have been better off just staying on the CPU

      // is this operator on the GPU?
      if (plan.canThisBeReplaced) {
        // at least one child is transitioning from CPU to GPU so we calculate the
        // transition costs
        val transitionCost = plan.childPlans.filter(!_.canThisBeReplaced)
            .map(costModel.transitionToGpuCost).sum

        // if the GPU cost including transition is more than the CPU cost then avoid this
        // transition and reset the GPU cost
        if (operatorGpuCost + transitionCost > operatorCpuCost && !consumesQueryStage(plan)) {
          // avoid transition and keep this operator on CPU
          optimizations.append(AvoidTransition(plan))
          plan.costPreventsRunningOnGpu()
          // reset GPU cost
          totalGpuCost = totalCpuCost;
        } else {
          // add transition cost to total GPU cost
          totalGpuCost += transitionCost
        }
      } else {
        // at least one child is transitioning from GPU to CPU so we evaulate each of this
        // child plans to see if it was worth running on GPU now that we have the cost of
        // transitioning back to CPU
        plan.childPlans.zip(childCosts).foreach {
          case (child, childCosts) =>
            val (childCpuCost, childGpuCost) = childCosts
            val transitionCost = costModel.transitionToCpuCost(child)
            val childGpuTotal = childGpuCost + transitionCost
            if (child.canThisBeReplaced && !consumesQueryStage(child)
                && childGpuTotal > childCpuCost) {
              // force this child plan back onto CPU
              optimizations.append(ReplaceSection(
                child.asInstanceOf[SparkPlanMeta[SparkPlan]], totalCpuCost, totalGpuCost))
              child.recursiveCostPreventsRunningOnGpu()
            }
        }

        // recalculate the transition costs because child plans may have changed
        val transitionCost = plan.childPlans
            .filter(_.canThisBeReplaced)
            .map(costModel.transitionToCpuCost).sum
        totalGpuCost += transitionCost
      }
    }

    // special behavior if this is the final operator in the plan because we always have the
    // cost of going back to CPU at the end
    if (finalOperator && plan.canThisBeReplaced) {
      totalGpuCost += costModel.transitionToCpuCost(plan)
    }

    if (totalGpuCost > totalCpuCost) {
      // we have reached a point where we have transitioned onto GPU for part of this
      // plan but with no benefit from doing so, so we want to undo this and go back to CPU
      if (plan.canThisBeReplaced && !consumesQueryStage(plan)) {
        // this plan would have been on GPU so we move it and onto CPU and recurse down
        // until we reach a part of the plan that is already on CPU and then stop
        optimizations.append(ReplaceSection(plan, totalCpuCost, totalGpuCost))
        plan.recursiveCostPreventsRunningOnGpu()
        // reset the costs because this section of the plan was not moved to GPU
        totalGpuCost = totalCpuCost
      }
    }

    if (!plan.canThisBeReplaced || consumesQueryStage(plan)) {
      // reset the costs because this section of the plan was not moved to GPU
      totalGpuCost = totalCpuCost
    }

    (totalCpuCost, totalGpuCost)
  }

  /**
   * Determines whether the specified operator will read from a query stage.
   */
  private def consumesQueryStage(plan: SparkPlanMeta[_]): Boolean = {
    // if the child query stage already executed on GPU then we need to keep the
    // next operator on GPU in these cases
    SQLConf.get.adaptiveExecutionEnabled && (plan.wrapped match {
        case _: CustomShuffleReaderExec
             | _: ShuffledHashJoinExec
             | _: BroadcastHashJoinExec
             | _: BroadcastNestedLoopJoinExec => true
        case _ => false
      })
  }
}

/**
 * The cost model is behind a trait so that we can consider making this pluggable in the future
 * so that users can override the cost model to suit specific use cases.
 */
trait CostModel {

  /**
   * Determine the CPU and GPU cost for an individual operator.
   * @param plan Operator
   * @return (cpuCost, gpuCost)
   */
  def applyCost(plan: SparkPlanMeta[_]): (Double, Double)

  /**
   * Determine the cost of transitioning data from CPU to GPU for a specific operator
   * @param plan Operator
   * @return Cost
   */
  def transitionToGpuCost(plan: SparkPlanMeta[_]): Double

  /**
   * Determine the cost of transitioning data from GPU to CPU for a specific operator
   */
  def transitionToCpuCost(plan: SparkPlanMeta[_]): Double
}

class DefaultCostModel(conf: RapidsConf) extends CostModel {

  def transitionToGpuCost(plan: SparkPlanMeta[_]) = {
    // this is a placeholder for now - we would want to try and calculate the transition cost
    // based on the data types and size (if known)
    conf.defaultTransitionToGpuCost
  }

  def transitionToCpuCost(plan: SparkPlanMeta[_]) = {
    // this is a placeholder for now - we would want to try and calculate the transition cost
    // based on the data types and size (if known)
    conf.defaultTransitionToCpuCost
  }

  override def applyCost(plan: SparkPlanMeta[_]): (Double, Double) = {

    // for now we have a constant cost for CPU operations and we make the GPU cost relative
    // to this but later we may want to calculate actual CPU costs
    val cpuCost = 1.0

    // always check for user overrides first
    val gpuCost = plan.conf.getOperatorCost(plan.wrapped.getClass.getSimpleName).getOrElse {
      plan.wrapped match {
        case _: ProjectExec =>
          // the cost of a projection is the average cost of its expressions
          plan.childExprs
              .map(expr => exprCost(expr.asInstanceOf[BaseExprMeta[Expression]]))
              .sum / plan.childExprs.length

        case _: ShuffleExchangeExec =>
          // setting the GPU cost of ShuffleExchangeExec to 1.0 avoids moving from CPU to GPU for
          // a shuffle. This must happen before the join consistency or we risk running into issues
          // with disabling one exchange that would make a join inconsistent
          1.0

        case _ => conf.defaultOperatorCost
      }
    }

    plan.cpuCost = cpuCost
    plan.gpuCost = gpuCost

    (cpuCost, gpuCost)
  }

  private def exprCost[INPUT <: Expression](expr: BaseExprMeta[INPUT]): Double = {
    // always check for user overrides first
    expr.conf.getExpressionCost(expr.getClass.getSimpleName).getOrElse {
      expr match {
        case cast: CastExprMeta[_] =>
          // different CAST operations have different costs, so we allow these to be configured
          // based on the data types involved
          expr.conf.getExpressionCost(s"Cast${cast.fromType}To${cast.toType}")
              .getOrElse(conf.defaultExpressionCost)
        case _ =>
          // many of our BaseExprMeta implementations are anonymous classes so we look directly at
          // the wrapped expressions in some cases
          expr.wrapped match {
            case _: AttributeReference => 1.0 // no benefit on GPU
            case Alias(_: AttributeReference, _) => 1.0 // no benefit on GPU
            case _ => conf.defaultExpressionCost
          }
      }
    }
  }

}

/**
 * Estimate the number of rows that an operator will output. Note that these row counts are
 * the aggregate across all output partitions.
 *
 * Logic is based on Spark's SizeInBytesOnlyStatsPlanVisitor. which operates on logical plans
 * and only computes data sizes, not row counts.
 */
object RowCountPlanVisitor {

  def visit(plan: SparkPlanMeta[_]): Option[BigInt] = plan.wrapped match {
    case p: QueryStageExec =>
      ShimLoader.getSparkShims.getQueryStageRuntimeStatistics(p).rowCount
    case GlobalLimitExec(limit, _) =>
      visit(plan.childPlans.head).map(_.min(limit)).orElse(Some(limit))
    case LocalLimitExec(limit, _) =>
      // LocalLimit applies the same limit for each partition
      val n = limit * plan.wrapped.asInstanceOf[SparkPlan]
          .outputPartitioning.numPartitions
      visit(plan.childPlans.head).map(_.min(n)).orElse(Some(n))
    case p: TakeOrderedAndProjectExec =>
      visit(plan.childPlans.head).map(_.min(p.limit)).orElse(Some(p.limit))
    case p: HashAggregateExec if p.groupingExpressions.isEmpty =>
      Some(1)
    case p: SortMergeJoinExec =>
      estimateJoin(plan, p.joinType)
    case p: ShuffledHashJoinExec =>
      estimateJoin(plan, p.joinType)
    case p: BroadcastHashJoinExec =>
      estimateJoin(plan, p.joinType)
    case _: UnionExec =>
      Some(plan.childPlans.flatMap(visit).sum)
    case _ =>
      default(plan)
  }

  private def estimateJoin(plan: SparkPlanMeta[_], joinType: JoinType): Option[BigInt] = {
    joinType match {
      case LeftAnti | LeftSemi =>
        // LeftSemi and LeftAnti won't ever be bigger than left
        visit(plan.childPlans.head)
      case _ =>
        default(plan)
    }
  }

  /**
   * The default row count is the product of the row count of all child plans.
   */
  private def default(p: SparkPlanMeta[_]): Option[BigInt] = {
    val one = BigInt(1)
    val product = p.childPlans.map(visit)
        .filter(_.exists(_ > 0L))
        .map(_.get)
        .product
    if (product == one) {
      // product will be 1 when there are no child plans
      None
    } else {
      Some(product)
    }
  }
}

sealed abstract class Optimization

case class AvoidTransition[INPUT <: SparkPlan](plan: SparkPlanMeta[INPUT]) extends Optimization {
  override def toString: String = s"It is not worth moving to GPU for operator: " +
      s"${Explain.format(plan)}"
}

case class ReplaceSection[INPUT <: SparkPlan](
    plan: SparkPlanMeta[INPUT],
    totalCpuCost: Double,
    totalGpuCost: Double) extends Optimization {
  override def toString: String = s"It is not worth keeping this section on GPU; " +
      s"gpuCost=$totalGpuCost, cpuCost=$totalCpuCost:\n${Explain.format(plan)}"
}

object Explain {

  def format(plan: SparkPlanMeta[_]): String = {
    plan.wrapped match {
      case p: SparkPlan => p.simpleString(SQLConf.get.maxToStringFields)
      case other => other.toString
    }
  }

  def formatTree(plan: SparkPlanMeta[_]): String = {
    val b = new StringBuilder
    formatTree(plan, b, "")
    b.toString
  }

  def formatTree(plan: SparkPlanMeta[_], b: StringBuilder, indent: String): Unit = {
    b.append(indent)
    b.append(format(plan))
    b.append('\n')
    plan.childPlans.filter(_.canThisBeReplaced)
        .foreach(child => formatTree(child, b, indent + "  "))
  }

}