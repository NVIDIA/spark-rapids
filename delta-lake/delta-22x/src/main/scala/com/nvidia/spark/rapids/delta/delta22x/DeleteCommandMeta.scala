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

package com.nvidia.spark.rapids.delta.delta22x

import com.nvidia.spark.rapids.{DataFromReplacementRule, RapidsConf, RapidsMeta, RunnableCommandMeta}
import com.nvidia.spark.rapids.delta.RapidsDeltaUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.commands.DeleteCommand
import org.apache.spark.sql.delta.rapids.GpuDeltaLog
import org.apache.spark.sql.delta.rapids.delta22x.GpuDeleteCommand
import org.apache.spark.sql.execution.command.RunnableCommand

class DeleteCommandMeta(
    deleteCmd: DeleteCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RunnableCommandMeta[DeleteCommand](deleteCmd, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    RapidsDeltaUtils.tagForDeltaWrite(this, deleteCmd.target.schema, deleteCmd.deltaLog,
      Map.empty, SparkSession.active)
  }

  override def convertToGpu(): RunnableCommand = {
    GpuDeleteCommand(
      new GpuDeltaLog(deleteCmd.deltaLog, conf),
      deleteCmd.target,
      deleteCmd.condition)
  }
}
