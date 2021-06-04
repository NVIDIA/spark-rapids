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

package com.nvidia.spark.rapids.shims.spark311db

import com.databricks.sql.execution.window.RunningWindowFunctionExec
import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, GpuExec, GpuOverrides, GpuWindowExec, RapidsConf, RapidsMeta, SparkPlanMeta}

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, SortOrder}

/**
 * GPU-based window-exec implementation, analogous to RunningWindowFunctionExec.
 */
class GpuRunningWindowExecMeta(runningWindowFunctionExec: RunningWindowFunctionExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[RunningWindowFunctionExec](runningWindowFunctionExec, conf, parent, rule) {

  val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
    runningWindowFunctionExec.windowExpressionList.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    runningWindowFunctionExec.partitionSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    runningWindowFunctionExec.orderSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {
    windowExpressions.map(meta => meta.wrapped)
        .filter(expr => !expr.isInstanceOf[NamedExpression])
        .foreach(_ => willNotWorkOnGpu(because = "Unexpected query plan with Windowing functions; " +
            "cannot convert for GPU execution. " +
            "(Detail: WindowExpression not wrapped in `NamedExpression`.)"))
  }

  override def convertToGpu(): GpuExec = {
    GpuWindowExec(
      windowExpressions.map(_.convertToGpu()),
      partitionSpec.map(_.convertToGpu()),
      orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
      childPlans.head.convertIfNeeded(),
      true
    )
  }
}

