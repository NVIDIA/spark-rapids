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

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

import org.apache.spark.sql.catalyst.expressions.{Expression, InputFileBlockLength, InputFileBlockStart, InputFileName}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

/**
 * InputFileBlockRule is to revert the converting between the SparkPlan with
 * input_file_name/InputFileBlockStart/InputFileBlockLength expression
 * and FileScan which can't be replaced.
 *
 * See https://github.com/NVIDIA/spark-rapids/issues/3333
 */
object InputFileBlockRule {

  private def checkHasInputFileExpressions(exec: Expression): Boolean = exec match {
    case _: InputFileName => true
    case _: InputFileBlockStart => true
    case _: InputFileBlockLength => true
    case e => e.children.exists(checkHasInputFileExpressions)
  }

  private def checkHasInputFileExpressions(plan: SparkPlan): Boolean = {
    plan.expressions.exists(checkHasInputFileExpressions)
  }

  // Apply the rule on SparkPlanMeta
  def apply(plan: SparkPlanMeta[SparkPlan]) = {
    // The key is the SparkPlanMeta where has the first InputFile expression
    // The value is an array with the SparkPlanMeta chain from SparkPlan (with first input_file_xxx)
    //    to FileScan (exclusive)
    val resultOps = LinkedHashMap[SparkPlanMeta[SparkPlan], ArrayBuffer[SparkPlanMeta[SparkPlan]]]()
    recursivelyResolve(plan, None, resultOps)

    // If we've found some chain, we should prevent the transition.
    resultOps.foreach { item =>
      item._2.foreach(p => p.inputFilePreventsRunningOnGpu())
    }
  }

  /**
   * Recursively to apply the rule on the plan
   * @param plan the plan to be resolved.
   * @param key  the SparkPlanMeta with the first input_file_xxx
   * @param resultOps
   */
  private def recursivelyResolve(
      plan: SparkPlanMeta[SparkPlan],
      key: Option[SparkPlanMeta[SparkPlan]],
      resultOps: LinkedHashMap[SparkPlanMeta[SparkPlan],
        ArrayBuffer[SparkPlanMeta[SparkPlan]]]): Unit = {

    plan.wrapped match {
      case _: ShuffleExchangeExec => // Exchange will invalid the input_file_xxx
        key.map(p => resultOps.remove(p)) // Remove the chain from Map
        plan.childPlans.foreach(p => recursivelyResolve(p, None, resultOps))
      case _: FileSourceScanExec | _: BatchScanExec =>
        if (plan.canThisBeReplaced) { // FileScan can be replaced
          key.map(p => resultOps.remove(p)) // Remove the chain from Map
        }
      case _: LeafExecNode => // We've reached the LeafNode but without any FileScan
        key.map(p => resultOps.remove(p)) // Remove the chain from Map
      case _ =>
        val newKey = if (key.isDefined) {
          // The node is in the chain of input_file_xx to FileScan
          resultOps.getOrElseUpdate(key.get,  new ArrayBuffer[SparkPlanMeta[SparkPlan]]) += plan
          key
        } else { // There is no parent Node who has input_file_xx
          if (checkHasInputFileExpressions(plan.wrapped)) {
            // Current node has input_file_xxx. Mark it as the first Node with input_file_xxx
            resultOps.getOrElseUpdate(plan, new ArrayBuffer[SparkPlanMeta[SparkPlan]]) += plan
            Some(plan)
          } else {
            None
          }
        }

        plan.childPlans.foreach(p => recursivelyResolve(p, newKey, resultOps))
    }
  }

}
