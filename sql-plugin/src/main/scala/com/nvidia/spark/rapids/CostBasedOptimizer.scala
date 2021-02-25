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
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
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
    recursivelyOptimize(plan, optimizations, finalOperator = true, "")
    optimizations
  }

  private def recursivelyOptimize(
      plan: SparkPlanMeta[SparkPlan],
      optimizations: ListBuffer[Optimization],
      finalOperator: Boolean,
      indent: String = ""): (Double, Double) = {

    // get the CPU and GPU cost of the child plan(s)
    val childCosts = plan.childPlans
        .map(child => recursivelyOptimize(
          child.asInstanceOf[SparkPlanMeta[SparkPlan]],
          optimizations,
          finalOperator = false,
          indent + "  "))

    val (childCpuCosts, childGpuCosts) = childCosts.unzip

    // get the CPU and GPU cost of this operator
    val (operatorCpuCost, operatorGpuCost) = costModel.applyCost(plan)

    // calculate total (this operator + children)
    val totalCpuCost = operatorCpuCost + childCpuCosts.sum
    var totalGpuCost = operatorGpuCost + childGpuCosts.sum

    // determine how many transitions between CPU and GPU are taking place between
    // the child operators and this operator
    val numTransitions = plan.childPlans
      .count(_.canThisBeReplaced != plan.canThisBeReplaced)

    if (numTransitions > 0) {
      if (plan.canThisBeReplaced) {
        // at least one child is transitioning from CPU to GPU
        val transitionCost = plan.childPlans.filter(!_.canThisBeReplaced)
            .map(costModel.transitionToGpuCost).sum
        val gpuCost = operatorGpuCost + transitionCost
        if (gpuCost > operatorCpuCost) {
          optimizations.append(AvoidTransition(plan))
          plan.costPreventsRunningOnGpu()
          // stay on CPU, so costs are same
          totalGpuCost = totalCpuCost;
        } else {
          totalGpuCost += transitionCost
        }
      } else {
        // at least one child is transitioning from GPU to CPU
        plan.childPlans.zip(childCosts).foreach {
          case (child, childCosts) =>
            val (childCpuCost, childGpuCost) = childCosts
            val transitionCost = costModel.transitionToCpuCost(child)
            val childGpuTotal = childGpuCost + transitionCost
            if (child.canThisBeReplaced && childGpuTotal > childCpuCost) {
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

    // special behavior if this is the final operator in the plan
    if (finalOperator && plan.canThisBeReplaced) {
      totalGpuCost += costModel.transitionToCpuCost(plan)
    }

    if (totalGpuCost > totalCpuCost) {
      // we have reached a point where we have transitioned onto GPU for part of this
      // plan but with no benefit from doing so, so we want to undo this and go back to CPU
      if (plan.canThisBeReplaced) {
        // this plan would have been on GPU so we move it and onto CPU and recurse down
        // until we reach a part of the plan that is already on CPU and then stop
        optimizations.append(ReplaceSection(plan, totalCpuCost, totalGpuCost))
        plan.recursiveCostPreventsRunningOnGpu()
      }

      // reset the costs because this section of the plan was not moved to GPU
      totalGpuCost = totalCpuCost
    }

    if (!plan.canThisBeReplaced) {
      // reset the costs because this section of the plan was not moved to GPU
      totalGpuCost = totalCpuCost
    }

    (totalCpuCost, totalGpuCost)
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