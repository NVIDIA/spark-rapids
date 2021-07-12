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

package com.nvidia.spark.rapids.shims.spark313

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.spark312.Spark312Shims
import com.nvidia.spark.rapids.spark313.RapidsShuffleManager

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

class Spark313Shims extends Spark312Shims {

  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  override def getRapidsShuffleManagerClass: String = {
    classOf[RapidsShuffleManager].getCanonicalName
  }

  override def injectRules(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(_ => ColumnarOverrideRules())
    extensions.injectQueryStagePrepRule(_ => GpuQueryStagePrepOverrides())
    // injectFinalStagePrepRule is proposed to be added in Spark version 3.2.0 and
    // possibly back-ported to 3.0.4 and 3.1.3. We use this to make the final query
    // stage columnar in adaptive queries, where possible
    extensions.injectFinalStagePrepRule(_ => GpuFinalStagePrepOverrides())
  }

  override def adaptivePlanToRow(plan: AdaptiveSparkPlanExec): SparkPlan =
    GpuColumnarToRowExec(plan)

  override def createGpuRowToColumnarTransition(
      optimizedChild: SparkPlan,
      r2c: RowToColumnarExec,
      goal: CoalesceSizeGoal): SparkPlan = {
    // with earlier Spark releases, we would need to insert an
    // AvoidAdaptiveTransitions operator here but this is no longer required
    // once SPARK-35881 is implemented
    optimizedChild match {
      case GpuColumnarToRowExec(child, _) =>
        // avoid a redundant GpuRowToColumnarExec(GpuColumnarToRowExec((_))
        GpuRowToColumnarExec(child, goal)
      case _ =>
        GpuRowToColumnarExec(optimizedChild, goal)
    }
  }

  override def isAdaptiveFinalPlanColumnar(plan: AdaptiveSparkPlanExec): Boolean =
    plan.finalPlanSupportsColumnar()

}
