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

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.rapids.execution._

class GpuCustomShuffleReaderMeta(reader: CustomShuffleReaderExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[CustomShuffleReaderExec](reader, conf, parent, rule) {


  override def checkExistingTags(): Unit = {
    // CoalesceShufflePartitions performs a transformUp and may replace ShuffleQueryStageExec
    // with CustomShuffleReaderExec, causing tags to be copied from ShuffleQueryStageExec to
    // CustomShuffleReaderExec, including the "no need to replace ShuffleQueryStageExec" tag.
    wrapped.getTagValue(RapidsMeta.gpuSupportedTag)
      .foreach(_.diff(cannotBeReplacedReasons.get)
      .filterNot(_ == s"there is no need to replace ${classOf[ShuffleQueryStageExec]}")
      .foreach(willNotWorkOnGpu))
  }

  override def tagPlanForGpu(): Unit = {
    if (!reader.child.supportsColumnar) {
      willNotWorkOnGpu(
        "Unable to replace CustomShuffleReader due to child not being columnar")
    }
    val shuffleEx = reader.child.asInstanceOf[ShuffleQueryStageExec].plan
    shuffleEx.getTagValue(GpuOverrides.preRowToColProjection).foreach { r2c =>
      wrapped.setTagValue(GpuOverrides.preRowToColProjection, r2c)
    }
  }

  override def convertToGpu(): GpuExec = {
    GpuCustomShuffleReaderExec(childPlans.head.convertIfNeeded(),
      reader.partitionSpecs)
  }

  // extract output attributes of the underlying ShuffleExchange
  override def outputAttributes: Seq[Attribute] = {
    val shuffleEx = reader.child.asInstanceOf[ShuffleQueryStageExec].plan
    shuffleEx.getTagValue(GpuShuffleMeta.shuffleExOutputAttributes)
        .getOrElse(shuffleEx.output)
  }

  // fetch availableRuntimeDataTransition of the underlying ShuffleExchange
  override val availableRuntimeDataTransition: Boolean = {
    val shuffleEx = reader.child.asInstanceOf[ShuffleQueryStageExec].plan
    shuffleEx.getTagValue(GpuShuffleMeta.availableRuntimeDataTransition)
        .getOrElse(false)
  }
}