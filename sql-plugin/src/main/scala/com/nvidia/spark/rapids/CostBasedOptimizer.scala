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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression}
import org.apache.spark.sql.execution.{ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

class CostBasedOptimizer(conf: RapidsConf) extends Logging {

  // the intention is to make the cost model pluggable since we are probably going to need to
  // experiment a fair bit with this part
  private val costModel = new DefaultCostModel(conf)

  /**
   * Walk the plan and determine CPU and GPU costs for each operator and then make decisions
   * about whether operators should run on CPU or GPU.
   */
  def optimize(
      plan: SparkPlanMeta[_],
      finalOperator: Boolean,
      indent: String = ""): (Double, Double) = {

    // get the CPU and GPU cost of the child plan(s)
    val childCosts = plan.childPlans
        .map(child => optimize(child, finalOperator = false, indent + "  "))

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
        // transition from CPU to GPU
        val transitionCost = plan.childPlans.filter(!_.canThisBeReplaced)
            .map(costModel.transitionToGpuCost).sum
        val gpuCost = operatorGpuCost + transitionCost
        if (gpuCost > operatorCpuCost) {
          plan.willNotWorkOnGpu(s"it is not worth moving the " +
              s"${plan.wrapped.getClass.getSimpleName} operator to GPU: " +
              s"cpuCost=$operatorCpuCost; gpuCost=$operatorGpuCost; transitionCost=$transitionCost")
          // stay on CPU, so costs are same
          totalGpuCost = totalCpuCost;
        } else {
          totalGpuCost += transitionCost
        }
      } else {
        //  transition one or more children from GPU to CPU
        plan.childPlans.zip(childCosts).foreach {
          case (child, childCosts) =>
            val (childCpuCost, childGpuCost) = childCosts
            val transitionCost = costModel.transitionToCpuCost(child)
            val childGpuTotal = childGpuCost + transitionCost
            if (childGpuTotal > childCpuCost) {
              forceOnCpuRecursively(child, s"Forcing ${child.wrapped.getClass.getSimpleName} " +
                  s"onto CPU: cpuCost=$totalCpuCost; gpuCost=$totalGpuCost", indent)
            }
        }

        val transitionCost = plan.childPlans.filter(_.canThisBeReplaced)
            .map(costModel.transitionToCpuCost).sum
        totalGpuCost += transitionCost
      }
    }

    // special behavior if this is the final operator in the plan
    if (finalOperator && plan.canThisBeReplaced) {
      totalGpuCost += costModel.transitionToCpuCost(plan)
    }

    cboDebug(indent, s"Considering section ${plan.wrapped.getClass.getSimpleName}" +
        s": cpuCost=$totalCpuCost; gpuCost=$totalGpuCost")

    if (totalGpuCost > totalCpuCost) {
      // we have reached a point where we have transitioned onto GPU for part of this
      // plan but with no benefit from doing so, so we want to undo this and go back to CPU
      if (plan.canThisBeReplaced) {
        // this plan would have been on GPU so we move it and onto CPU and recurse down
        // until we reach a part of the plan that is already on CPU and then stop
        forceOnCpuRecursively(plan, s"Forcing ${plan.wrapped.getClass.getSimpleName} " +
            s"onto CPU: cpuCost=$totalCpuCost; gpuCost=$totalGpuCost", indent)
      } else {
        // this plan would have not have been on GPU so this probably means that at
        // least one of the child plans is on GPU and the cost of transitioning back
        // to CPU has now made it not worth it, so we put the children back onto CPU
        // and recurse down until we reach a part of the plan that is already on CPU
        // and then stop
        plan.childPlans.foreach { child =>
          forceOnCpuRecursively(child, s"Forcing ${plan.wrapped.getClass.getSimpleName} " +
              s"onto CPU: cpuCost=$totalCpuCost; gpuCost=$totalGpuCost", indent)
        }
      }

      // reset the costs because this section of the plan was not moved to GPU
      totalGpuCost = totalCpuCost
    } else {
      if (plan.canThisBeReplaced) {
        cboDebug(indent, s"Keeping ${plan.wrapped.getClass.getSimpleName} " +
            s"on GPU: cpuCost=$totalCpuCost; gpuCost=$totalGpuCost")
      } else {
        totalGpuCost = totalCpuCost
        cboDebug(indent, s"Keeping ${plan.wrapped.getClass.getSimpleName} " +
            s"on CPU: cpuCost=$totalCpuCost; gpuCost=$totalGpuCost")
      }
    }

    if (!plan.canThisBeReplaced) {
      // reset the costs because this section of the plan was not moved to GPU
      totalGpuCost = totalCpuCost
    }

    (totalCpuCost, totalGpuCost)
  }

  def calculateFinalCost(
      plan: SparkPlanMeta[_],
      finalOperator: Boolean): (Double, Double) = {

    // get the CPU and GPU cost of the child plan(s)
    val (childCpuCosts, childGpuCosts) = plan.childPlans
        .map(child => optimize(child, finalOperator = false)).unzip

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
      val transitionCost = if (plan.canThisBeReplaced) {
        // transition from CPU to GPU
        plan.childPlans.filter(!_.canThisBeReplaced)
            .map(costModel.transitionToGpuCost).sum
      } else {
        //  transition from GPU to CPU
        plan.childPlans.filter(_.canThisBeReplaced)
            .map(costModel.transitionToCpuCost).sum
      }
      totalGpuCost += transitionCost
    }

    // special behavior if this is the final operator in the plan
    if (finalOperator && plan.canThisBeReplaced) {
      totalGpuCost += costModel.transitionToCpuCost(plan)
    }

    (totalCpuCost, totalGpuCost)
  }

  def forceOnCpuRecursively(plan: SparkPlanMeta[_], reason: String, indent: String): Unit = {
    // stop when we hit an operator that is already on the CPU
    if (plan.canThisBeReplaced) {
      cboDebug(indent, reason)
      plan.willNotWorkOnGpu(reason)
      plan.setGpuCost(1.0)
      plan.childPlans.foreach(plan => forceOnCpuRecursively(plan, reason, indent + "  "))
    }
  }

  def prettyPrint(plan: SparkPlanMeta[_], indent: String = ""): Unit = {
    println(s"$indent ${plan.wrapped.getClass.getSimpleName} " +
        s"cpuCost=${plan.cpuCost}; " +
        s"gpuCost=${plan.gpuCost}; " +
        s"canThisBeReplaced=${plan.canThisBeReplaced}")
    plan.childPlans.foreach(plan => prettyPrint(plan, indent + "  "))
  }

  private def cboDebug(indent: String, msg: String): Unit = {
    logDebug(s"$indent$msg")
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
    conf.cboDefaultTransitionToGpu
  }

  def transitionToCpuCost(plan: SparkPlanMeta[_]) = {
    // this is a placeholder for now - we would want to try and calculate the transition cost
    // based on the data types and size (if known)
    conf.cboDefaultTransitionToCpu
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

        case _ => conf.cboDefaultOperatorCost
      }
    }

    plan.setCpuCost(cpuCost)
    plan.setGpuCost(gpuCost)

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
              .getOrElse(conf.cboDefaultExpressionCost)
        case _ =>
          // many of our BaseExprMeta implementations are anonymous classes so we look directly at
          // the wrapped expressions in some cases
          expr.wrapped match {
            case _: AttributeReference => 1.0 // no benefit on GPU
            case Alias(_: AttributeReference, _) => 1.0 // no benefit on GPU
            case _ => conf.cboDefaultExpressionCost
          }
      }
    }
  }

}