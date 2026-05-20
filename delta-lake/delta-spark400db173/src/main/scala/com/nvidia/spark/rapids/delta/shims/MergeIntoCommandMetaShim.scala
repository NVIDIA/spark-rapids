/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta.shims

import com.databricks.sql.transaction.tahoe.commands.{DeletionVectorUtils, MergeIntoCommand,
  MergeIntoCommandEdge}
import com.databricks.sql.transaction.tahoe.rapids.{GpuDeltaLog, GpuMergeIntoCommand}
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.delta.{MergeIntoCommandEdgeMeta, MergeIntoCommandMeta}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand

object MergeIntoCommandMetaShim {
  private val MERGE_MATERIALIZE_SOURCE_CONF = "spark.databricks.delta.merge.materializeSource"

  def tagForGpu(meta: MergeIntoCommandMeta, mergeCmd: MergeIntoCommand): Unit = {
    // see https://github.com/NVIDIA/spark-rapids/issues/8415 for more information
    if (mergeCmd.notMatchedBySourceClauses.nonEmpty) {
      meta.willNotWorkOnGpu("notMatchedBySourceClauses not supported on GPU")
    }
    tagPersistentDeletionVectorFallback(meta, mergeCmd)
    tagMaterializedSourceFallback(meta, mergeCmd.source)
  }

  def tagForGpu(meta: MergeIntoCommandEdgeMeta, mergeCmd: MergeIntoCommandEdge): Unit = {
    // see https://github.com/NVIDIA/spark-rapids/issues/8415 for more information
    if (mergeCmd.notMatchedBySourceClauses.nonEmpty) {
      meta.willNotWorkOnGpu("notMatchedBySourceClauses not supported on GPU")
    }
    tagPersistentDeletionVectorFallback(meta, mergeCmd)
    tagMaterializedSourceFallback(meta, mergeCmd.source)
  }

  private def tagPersistentDeletionVectorFallback(
      meta: MergeIntoCommandMeta,
      mergeCmd: MergeIntoCommand): Unit = {
    if (shouldWritePersistentDeletionVectors(mergeCmd)) {
      meta.willNotWorkOnGpu("Deletion vectors are not supported on GPU")
    }
  }

  private def tagPersistentDeletionVectorFallback(
      meta: MergeIntoCommandEdgeMeta,
      mergeCmd: MergeIntoCommandEdge): Unit = {
    if (shouldWritePersistentDeletionVectors(mergeCmd)) {
      meta.willNotWorkOnGpu("Deletion vectors are not supported on GPU")
    }
  }

  private def tagMaterializedSourceFallback(
      meta: MergeIntoCommandMeta,
      source: LogicalPlan): Unit = {
    if (isMaterializeSourceForced) {
      meta.willNotWorkOnGpu("MERGE source materialization is not supported on GPU")
    } else if (hasNondeterministicSource(source)) {
      meta.willNotWorkOnGpu(
        "nondeterministic MERGE source materialization is not supported on GPU")
    }
  }

  private def tagMaterializedSourceFallback(
      meta: MergeIntoCommandEdgeMeta,
      source: LogicalPlan): Unit = {
    if (isMaterializeSourceForced) {
      meta.willNotWorkOnGpu("MERGE source materialization is not supported on GPU")
    } else if (hasNondeterministicSource(source)) {
      meta.willNotWorkOnGpu(
        "nondeterministic MERGE source materialization is not supported on GPU")
    }
  }

  private def isMaterializeSourceForced: Boolean = {
    SparkSession.active.conf.get(MERGE_MATERIALIZE_SOURCE_CONF, "auto").equalsIgnoreCase("all")
  }

  private def hasNondeterministicSource(source: LogicalPlan): Boolean = {
    source.exists(_.expressions.exists(expr => !expr.deterministic))
  }

  private def shouldWritePersistentDeletionVectors(mergeCmd: MergeIntoCommand): Boolean = {
    val deltaLog = mergeCmd.targetFileIndex.deltaLog
    DeletionVectorUtils.deletionVectorsWritable(deltaLog.unsafeVolatileSnapshot) &&
      mergeCmd.conf.getConf(DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS)
  }

  private def shouldWritePersistentDeletionVectors(mergeCmd: MergeIntoCommandEdge): Boolean = {
    val deltaLog = mergeCmd.targetFileIndex.deltaLog
    DeletionVectorUtils.deletionVectorsWritable(deltaLog.unsafeVolatileSnapshot) &&
      mergeCmd.conf.getConf(DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS)
  }

  def convertToGpu(mergeCmd: MergeIntoCommand, conf: RapidsConf): RunnableCommand = {
    GpuMergeIntoCommand(
      mergeCmd.source,
      mergeCmd.target,
      mergeCmd.catalogTable,
      mergeCmd.targetFileIndex,
      new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
      mergeCmd.condition,
      mergeCmd.matchedClauses,
      mergeCmd.notMatchedClauses,
      mergeCmd.notMatchedBySourceClauses,
      mergeCmd.migratedSchema,
      mergeCmd.trackHighWaterMarks,
      mergeCmd.schemaEvolutionEnabled)(conf)
  }

  def convertToGpu(mergeCmd: MergeIntoCommandEdge, conf: RapidsConf): RunnableCommand = {
    GpuMergeIntoCommand(
      mergeCmd.source,
      mergeCmd.target,
      mergeCmd.catalogTable,
      mergeCmd.targetFileIndex,
      new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
      mergeCmd.condition,
      mergeCmd.matchedClauses,
      mergeCmd.notMatchedClauses,
      mergeCmd.notMatchedBySourceClauses,
      mergeCmd.migratedSchema,
      mergeCmd.trackHighWaterMarks,
      mergeCmd.schemaEvolutionEnabled,
      // This is safe to forward as-is because DBR analysis has already encoded snapshot reuse
      // eligibility in this Option: Some(snapshot) means the Edge command may reuse the analyzed
      // snapshot, while None makes GpuDeltaLog open the transaction on the latest snapshot.
      mergeCmd.snapshotAtAnalysis)(conf)
  }
}
