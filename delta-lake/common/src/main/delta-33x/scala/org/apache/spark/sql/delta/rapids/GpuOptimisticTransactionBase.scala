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

package org.apache.spark.sql.delta.rapids

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.{GpuColumnarToRowExec, GpuExec, RapidsConf, TargetSize}
import com.nvidia.spark.rapids.delta.DeltaWriteUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions, Snapshot}
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.delta.{DeltaShufflePartitionsUtil, GpuOptimizeWriteExchangeExec, OptimizeWriteExchangeExec}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Clock

abstract class GpuOptimisticTransactionBase(
    deltaLog: DeltaLog,
    catalog: Option[CatalogTable],
    snapshot: Snapshot,
    rapidsConf: RapidsConf)
    (implicit clock: Clock)
  extends AbstractGpuOptimisticTransactionBase(deltaLog, catalog, snapshot, rapidsConf)(clock) {

  protected def applyOptimizeWriteIfNeeded(
      spark: SparkSession,
      physicalPlan: SparkPlan,
      partitionSchema: StructType,
      isOptimize: Boolean,
      writeOptions: Option[DeltaOptions]): SparkPlan = {
    val optimizeWriteEnabled = !isOptimize &&
      DeltaWriteUtils.shouldOptimizeWrite(writeOptions, spark.sessionState.conf, metadata)
    if (optimizeWriteEnabled) {
      val planWithoutTopRepartition =
        DeltaShufflePartitionsUtil.removeTopRepartition(physicalPlan)
      val partitioning = DeltaShufflePartitionsUtil.partitioningForRebalance(
        physicalPlan.output, partitionSchema, spark.sessionState.conf.numShufflePartitions)
      planWithoutTopRepartition match {
        case p: GpuExec =>
          val partMeta = GpuOverrides.wrapPart(partitioning, rapidsConf, None)
          partMeta.tagForGpu()
          if (partMeta.canThisBeReplaced) {
            val plan = GpuOptimizeWriteExchangeExec(partMeta.convertToGpu(), p)
            if (GpuShuffleEnv.useGPUShuffle(rapidsConf)) {
              GpuCoalesceBatches(plan, TargetSize(rapidsConf.gpuTargetBatchSizeBytes))
            } else {
              GpuShuffleCoalesceExec(plan, rapidsConf.gpuTargetBatchSizeBytes)
            }
          } else {
            GpuColumnarToRowExec(OptimizeWriteExchangeExec(partitioning, p))
          }
        case p =>
          OptimizeWriteExchangeExec(partitioning, p)
      }
    } else {
      physicalPlan
    }
  }

  protected def isOptimizeCommand(plan: LogicalPlan): Boolean = {
    val leaves = plan.collectLeaves()
    leaves.size == 1 && leaves.head.collect {
      case LogicalRelation(HadoopFsRelation(
      index: TahoeBatchFileIndex, _, _, _, _, _), _, _, _) =>
        index.actionType.equals("Optimize")
    }.headOption.getOrElse(false)
  }
}
