/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec}
import org.apache.spark.sql.rapids.ExternalSource

class AtomicCreateTableAsSelectExecMeta(
    wrapped: AtomicCreateTableAsSelectExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[AtomicCreateTableAsSelectExec](wrapped, conf, parent, rule) {

  override def tagPlanForGpu(): Unit = {
    ExternalSource.tagForGpu(wrapped, this)
  }

  override def convertToGpu(): GpuExec = {
    ExternalSource.convertToGpu(wrapped, this)
  }
}

class AtomicReplaceTableAsSelectExecMeta(
    wrapped: AtomicReplaceTableAsSelectExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[AtomicReplaceTableAsSelectExec](wrapped, conf, parent, rule) {

  override def tagPlanForGpu(): Unit = {
    ExternalSource.tagForGpu(wrapped, this)
  }

  override def convertToGpu(): GpuExec = {
    ExternalSource.convertToGpu(wrapped, this)
  }
}

class AppendDataExecV1Meta(
    wrapped: AppendDataExecV1,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[AppendDataExecV1](wrapped, conf, parent, rule) {

  private var customData: Option[Object] = None

  def setCustomTaggingData(data: Object): Unit = {
    assert(customData.isEmpty, "custom tagging data already exists")
    customData = Some(data)
  }

  def getCustomTaggingData: Option[Object] = customData

  override def tagPlanForGpu(): Unit = {
    ExternalSource.tagForGpu(wrapped, this)
  }

  override def convertToGpu(): GpuExec = {
    ExternalSource.convertToGpu(wrapped, this)
  }
}
