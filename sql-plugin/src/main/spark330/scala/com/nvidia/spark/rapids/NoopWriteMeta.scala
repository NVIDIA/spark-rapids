/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, OverwriteByExpressionExec}
import org.apache.spark.sql.execution.datasources.v2.noop.NoopTable
import org.apache.spark.sql.rapids.ExternalSource

class OverwriteByExpressionExecMeta(
    wrapped: OverwriteByExpressionExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[OverwriteByExpressionExec](wrapped, conf, parent, rule)
  with HasCustomTaggingData {

  override def tagPlanForGpu(): Unit = {
    if (wrapped.table.isInstanceOf[NoopTable]) {
      // This is a no-op write, we can handle it on the GPU by just consuming the data
    } else {
      ExternalSource.tagForGpu(wrapped, this)
    }
  }

  override def convertToGpu(): GpuExec = {
    if (wrapped.table.isInstanceOf[NoopTable]) {
      GpuOverwriteByExpressionExec(childPlans.head.convertIfNeeded())
    } else {
      ExternalSource.convertToGpu(wrapped, this)
    }
  }
}

class AppendDataExecMeta(
    wrapped: AppendDataExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[AppendDataExec](wrapped, conf, parent, rule)
  with HasCustomTaggingData {

  override def tagPlanForGpu(): Unit = {
    if (wrapped.table.isInstanceOf[NoopTable]) {
      // This is a no-op write, we can handle it on the GPU by just consuming the data
    } else {
      ExternalSource.tagForGpu(wrapped, this)
    }
  }

  override def convertToGpu(): GpuExec = {
    if (wrapped.table.isInstanceOf[NoopTable]) {
      GpuAppendDataExec(childPlans.head.convertIfNeeded())
    } else {
      ExternalSource.convertToGpu(wrapped, this)
    }
  }
}
