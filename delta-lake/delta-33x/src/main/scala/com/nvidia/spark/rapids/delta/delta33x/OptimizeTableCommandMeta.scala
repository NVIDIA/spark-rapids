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
import com.nvidia.spark.rapids.delta.RapidsDeltaUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.commands.{DeletionVectorUtils, OptimizeTableCommand}
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.rapids.delta33x.GpuOptimizeTableCommand
import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.command.RunnableCommand

class OptimizeTableCommandMeta(
    cmd: OptimizeTableCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RunnableCommandMeta[OptimizeTableCommand](cmd, conf, parent, rule)
    with DeltaCommand {

  override def tagSelfForGpu(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }

    val table = getDeltaTable(cmd.child, "OPTIMIZE")

    // DV write unsupported on GPU
    if (DeletionVectorUtils.deletionVectorsWritable(table.deltaLog.unsafeVolatileSnapshot) &&
        cmd.conf.getConf(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS)) {
      willNotWorkOnGpu("Deletion vectors are not supported on GPU")
    }

    // Z-Order or Clustered tables unsupported
    if (cmd.zOrderBy.nonEmpty) {
      willNotWorkOnGpu("Z-Order optimize is not supported on GPU")
    }
    val isClustered = ClusteredTableUtils.getClusterBySpecOptional(
      table.deltaLog.unsafeVolatileSnapshot).isDefined
    if (isClustered) {
      willNotWorkOnGpu("Liquid clustering is not supported on GPU")
    }

    // Ensure write path generally OK
    RapidsDeltaUtils.tagForDeltaWrite(this,
      table.deltaLog.unsafeVolatileSnapshot.schema, Some(table.deltaLog), Map.empty,
      SparkSession.active)
  }

  override def convertToGpu(): RunnableCommand = {
    GpuOptimizeTableCommand(cmd.child, cmd.userPartitionPredicates, cmd.optimizeContext)(
      cmd.zOrderBy)
  }
}
