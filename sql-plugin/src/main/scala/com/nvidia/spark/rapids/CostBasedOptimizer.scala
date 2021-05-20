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
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, GetStructField}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftAnti, LeftSemi}
import org.apache.spark.sql.execution.{GlobalLimitExec, LocalLimitExec, SparkPlan, TakeOrderedAndProjectExec, UnionExec}
import org.apache.spark.sql.execution.adaptive.{CustomShuffleReaderExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Optimizer that can operate on a physical query plan.
 */
trait Optimizer {

  /**
   * Apply optimizations to a query plan.
   *
   * @param conf Rapids configuration
   * @param plan The plan to optimize
   * @return A list of optimizations that were applied
   */
  def optimize(conf: RapidsConf, plan: SparkPlanMeta[SparkPlan]): Seq[Optimization]
}

/**
 * Experimental cost-based optimizer that aims to avoid moving sections of the plan to the GPU when
 * it would be better to keep that part of the plan on the CPU. For example, we don't want to move
 * data to the GPU just for a trivial projection and then have to move data back to the CPU on the
 * next step.
 */
class CostBasedOptimizer extends Optimizer with Logging {

  /**
   * Walk the plan and determine CPU and GPU costs for each operator and then make decisions
   * about whether operators should run on CPU or GPU.
   *
   * @param conf Rapids configuration
   * @param plan The plan to optimize
   * @return A list of optimizations that were applied
   */
  def optimize(conf: RapidsConf, plan: SparkPlanMeta[SparkPlan]): Seq[Optimization] = {
    val cpuCostModel = new CpuCostModel(conf)
    val gpuCostModel = new GpuCostModel(conf)
    val optimizations = new ListBuffer[Optimization]()
    recursivelyOptimize(conf, cpuCostModel, gpuCostModel, plan, optimizations, finalOperator = true)
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
      conf: RapidsConf,
      cpuCostModel: CostModel,
      gpuCostModel: CostModel,
      plan: SparkPlanMeta[SparkPlan],
      optimizations: ListBuffer[Optimization],
      finalOperator: Boolean): (Double, Double) = {

    // get the CPU and GPU cost of the child plan(s)
    val childCosts = plan.childPlans
        .map(child => recursivelyOptimize(
          conf,
          cpuCostModel,
          gpuCostModel,
          child,
          optimizations,
          finalOperator = false))

    val (childCpuCosts, childGpuCosts) = childCosts.unzip

    // get the CPU and GPU cost of this operator (excluding cost of children)
    val operatorCpuCost = cpuCostModel.getCost(plan)
    val operatorGpuCost = gpuCostModel.getCost(plan)

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
            .map(transitionToGpuCost(conf, _)).sum

        // if the GPU cost including transition is more than the CPU cost then avoid this
        // transition and reset the GPU cost
        if (operatorGpuCost + transitionCost > operatorCpuCost && !isExchangeOp(plan)) {
          // avoid transition and keep this operator on CPU
          optimizations.append(AvoidTransition(plan))
          plan.costPreventsRunningOnGpu()
          // reset GPU cost
          totalGpuCost = totalCpuCost
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
            val transitionCost = transitionToCpuCost(conf, child)
            val childGpuTotal = childGpuCost + transitionCost
            if (child.canThisBeReplaced && !isExchangeOp(child)
                && childGpuTotal > childCpuCost) {
              // force this child plan back onto CPU
              optimizations.append(ReplaceSection(
                child, totalCpuCost, totalGpuCost))
              child.recursiveCostPreventsRunningOnGpu()
            }
        }

        // recalculate the transition costs because child plans may have changed
        val transitionCost = plan.childPlans
            .filter(_.canThisBeReplaced)
            .map(transitionToCpuCost(conf, _)).sum
        totalGpuCost += transitionCost
      }
    }

    // special behavior if this is the final operator in the plan because we always have the
    // cost of going back to CPU at the end
    if (finalOperator && plan.canThisBeReplaced) {
      totalGpuCost += transitionToCpuCost(conf, plan)
    }

    if (totalGpuCost > totalCpuCost) {
      // we have reached a point where we have transitioned onto GPU for part of this
      // plan but with no benefit from doing so, so we want to undo this and go back to CPU
      if (plan.canThisBeReplaced && !isExchangeOp(plan)) {
        // this plan would have been on GPU so we move it and onto CPU and recurse down
        // until we reach a part of the plan that is already on CPU and then stop
        optimizations.append(ReplaceSection(plan, totalCpuCost, totalGpuCost))
        plan.recursiveCostPreventsRunningOnGpu()
        // reset the costs because this section of the plan was not moved to GPU
        totalGpuCost = totalCpuCost
      }
    }

    if (!plan.canThisBeReplaced || isExchangeOp(plan)) {
      // reset the costs because this section of the plan was not moved to GPU
      totalGpuCost = totalCpuCost
    }

    (totalCpuCost, totalGpuCost)
  }

  private def transitionToGpuCost(conf: RapidsConf, plan: SparkPlanMeta[SparkPlan]): Double = {
    val rowCount = RowCountPlanVisitor.visit(plan).map(_.toDouble)
      .getOrElse(conf.defaultRowCount.toDouble)
    val dataSize = GpuBatchUtils.estimateGpuMemory(plan.wrapped.schema, rowCount.toLong)
    conf.getGpuOperatorCost("GpuRowToColumnarExec").getOrElse(0d) * rowCount +
      MemoryCostHelper.calculateCost(dataSize, conf.cpuReadMemorySpeed) +
      MemoryCostHelper.calculateCost(dataSize, conf.gpuWriteMemorySpeed)
  }

  private def transitionToCpuCost(conf: RapidsConf, plan: SparkPlanMeta[SparkPlan]): Double = {
    val rowCount = RowCountPlanVisitor.visit(plan).map(_.toDouble)
      .getOrElse(conf.defaultRowCount.toDouble)
    val dataSize = GpuBatchUtils.estimateGpuMemory(plan.wrapped.schema, rowCount.toLong)
    conf.getGpuOperatorCost("GpuColumnarToRowExec").getOrElse(0d) * rowCount +
      MemoryCostHelper.calculateCost(dataSize, conf.gpuReadMemorySpeed) +
      MemoryCostHelper.calculateCost(dataSize, conf.cpuWriteMemorySpeed)
  }

  /**
   * Determines whether the specified operator is an exchange, or will read from an
   * exchange / query stage. CBO needs to avoid moving these operators back onto
   * CPU because it could result in an invalid plan.
   */
  private def isExchangeOp(plan: SparkPlanMeta[_]): Boolean = {
    // if the child query stage already executed on GPU then we need to keep the
    // next operator on GPU in these cases
    SQLConf.get.adaptiveExecutionEnabled && (plan.wrapped match {
        case _: CustomShuffleReaderExec
             | _: ShuffledHashJoinExec
             | _: BroadcastHashJoinExec
             | _: BroadcastExchangeExec
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
  def getCost(plan: SparkPlanMeta[_]): Double

}

class CpuCostModel(conf: RapidsConf) extends CostModel {

  def getCost(plan: SparkPlanMeta[_]): Double = {

    val rowCount = RowCountPlanVisitor.visit(plan).map(_.toDouble)
      .getOrElse(conf.defaultRowCount.toDouble)

    val operatorCost = plan.conf
      .getCpuOperatorCost(plan.wrapped.getClass.getSimpleName)
      .getOrElse(conf.defaultCpuOperatorCost) * rowCount

    val exprEvalCost = plan.childExprs
      .map(expr => exprCost(expr.asInstanceOf[BaseExprMeta[Expression]], rowCount))
      .sum

    operatorCost + exprEvalCost
  }

  private def exprCost[INPUT <: Expression](expr: BaseExprMeta[INPUT], rowCount: Double): Double = {

    val memoryReadCost = expr.wrapped match {
      case _: Alias =>
        // alias has no cost, we just evaluate the cost of the aliased expression
        exprCost(expr.childExprs.head.asInstanceOf[BaseExprMeta[Expression]], rowCount)

      case _: AttributeReference | _: GetStructField =>
        MemoryCostHelper.calculateCost(GpuBatchUtils.estimateGpuMemory(
          expr.dataType, nullable = false, rowCount.toLong), conf.cpuReadMemorySpeed)

      case _ =>
        expr.childExprs
          .map(e => exprCost(e.asInstanceOf[BaseExprMeta[Expression]], rowCount)).sum
    }

    // the output of evaluating the expression needs to be written out to rows
    val memoryWriteCost = MemoryCostHelper.calculateCost(GpuBatchUtils.estimateGpuMemory(
      expr.dataType, nullable = false, rowCount.toLong), conf.cpuWriteMemorySpeed)

    // optional additional per-row overhead of evaluating the expression
    val exprEvalCost = rowCount *
      expr.conf.getCpuExpressionCost(expr.getClass.getSimpleName)
        .getOrElse(conf.defaultCpuExpressionCost)

    exprEvalCost + memoryReadCost + memoryWriteCost
  }
}

class GpuCostModel(conf: RapidsConf) extends CostModel {

  def getCost(plan: SparkPlanMeta[_]): Double = {
    val rowCount = RowCountPlanVisitor.visit(plan).map(_.toDouble)
      .getOrElse(conf.defaultRowCount.toDouble)

    val operatorCost = plan.conf
      .getGpuOperatorCost(plan.wrapped.getClass.getSimpleName)
      .getOrElse(conf.defaultGpuOperatorCost) * rowCount

    val exprEvalCost = plan.childExprs
      .map(expr => exprCost(expr.asInstanceOf[BaseExprMeta[Expression]], rowCount))
      .sum

    operatorCost + exprEvalCost
  }

  private def exprCost[INPUT <: Expression](expr: BaseExprMeta[INPUT], rowCount: Double): Double = {

    var memoryReadCost = 0d
    var memoryWriteCost = 0d

    expr.wrapped match {
      case _: Alias =>
        // alias has no cost, we just evaluate the cost of the aliased expression
        exprCost(expr.childExprs.head.asInstanceOf[BaseExprMeta[Expression]], rowCount)

      case _: AttributeReference =>
        // referencing an existing column on GPU is almost free since we're
        // just increasing a reference count and not actually copying any data

      case _ =>
        memoryReadCost = expr.childExprs
          .map(e => exprCost(e.asInstanceOf[BaseExprMeta[Expression]], rowCount)).sum

        memoryWriteCost += MemoryCostHelper.calculateCost(GpuBatchUtils.estimateGpuMemory(
          expr.dataType, nullable = false, rowCount.toLong), conf.gpuWriteMemorySpeed)
    }

    // optional additional per-row overhead of evaluating the expression
    val exprEvalCost = rowCount *
      expr.conf.getGpuExpressionCost(expr.getClass.getSimpleName)
        .getOrElse(conf.defaultGpuExpressionCost)

    exprEvalCost + memoryReadCost + memoryWriteCost
  }
}

object MemoryCostHelper {
  private val GIGABYTE = 1024d * 1024d * 1024d

  /**
   * Calculate the cost (time) of transferring data at a given memory speed.
   *
   * @param dataSize Size of data to transfer, in bytes.
   * @param memorySpeed Memory speed, in GB/s.
   * @return Time in seconds.
   */
  def calculateCost(dataSize: Long, memorySpeed: Double): Double = {
    (dataSize / GIGABYTE) / memorySpeed
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
      p.getRuntimeStatistics.rowCount
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