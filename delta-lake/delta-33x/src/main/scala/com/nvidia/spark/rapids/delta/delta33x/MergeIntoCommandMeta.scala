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

package com.nvidia.spark.rapids.delta.delta33x

import com.nvidia.spark.rapids.{DataFromReplacementRule, RapidsConf, RapidsMeta, RunnableCommandMeta}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.commands.MergeIntoCommand
import org.apache.spark.sql.execution.command.RunnableCommand

class MergeIntoCommandMeta(
    mergeCmd: MergeIntoCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RunnableCommandMeta[MergeIntoCommand](mergeCmd, conf, parent, rule) with Logging {

  override def tagSelfForGpu(): Unit = {
    willNotWorkOnGpu("MergeIntoCommand is not supported for Delta Lake 3.3.0")
  }

  override def convertToGpu(): RunnableCommand = {
    throw new UnsupportedOperationException("MergeIntoCommand not implemented")
  }

}
