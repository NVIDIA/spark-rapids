/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import java.lang.reflect.Method

import com.nvidia.spark.rapids.{GpuColumnarToRowExec, GpuExec, GpuRowToColumnarExec}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * This operator will attempt to optimize the case when we are writing the results of
 * an adaptive query to disk so that we remove the redundant transitions from columnar
 * to row within AdaptiveSparkPlanExec followed by a row to columnar transition.
 *
 * Specifically, this is the plan we see in this case:
 *
 * {{{
 * GpuRowToColumnar(AdaptiveSparkPlanExec(GpuColumnarToRow(child))
 * }}}
 *
 * We perform this optimization at runtime rather than during planning, because when the adaptive
 * plan is being planned and executed, we don't know whether it is being called from an operation
 * that wants rows (such as CollectTailExec) or from an operation that wants columns (such as
 * GpuDataWritingCommandExec).
 *
 * Spark does not provide a mechanism for executing an adaptive plan and retrieving columnar
 * results and the internal methods that we need to call are private, so we use reflection to
 * call them.
 *
 * @param child The plan to execute
 */
case class AvoidAdaptiveTransitionToRow(child: SparkPlan) extends ShimUnaryExecNode with GpuExec {

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def output: Seq[Attribute] = child.output

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = child match {
    case GpuRowToColumnarExec(a: AdaptiveSparkPlanExec, _, _) =>
      val getFinalPhysicalPlan = getPrivateMethod("getFinalPhysicalPlan")
      val plan = getFinalPhysicalPlan.invoke(a)
      val rdd = plan match {
        case t: GpuColumnarToRowExec =>
          t.child.executeColumnar()
        case _ =>
          child.executeColumnar()
      }

      // final UI update
      val finalPlanUpdate = getPrivateMethod("finalPlanUpdate")
      finalPlanUpdate.invoke(a)

      rdd

    case _ =>
      child.executeColumnar()
  }

  private def getPrivateMethod(name: String): Method = {
    val m = classOf[AdaptiveSparkPlanExec].getDeclaredMethod(name)
    m.setAccessible(true)
    m
  }
}