/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import com.databricks.sql.transaction.tahoe.commands.{MergeIntoCommand, MergeIntoCommandEdge}
import com.databricks.sql.transaction.tahoe.rapids.{GpuDeltaLog, GpuLowShuffleMergeCommand, GpuMergeIntoCommand}
import com.nvidia.spark.rapids.{RapidsConf, RapidsReaderType}
import com.nvidia.spark.rapids.delta.{MergeIntoCommandEdgeMeta, MergeIntoCommandMeta}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.command.RunnableCommand

object MergeIntoCommandMetaShim extends Logging {
  def tagForGpu(meta: MergeIntoCommandMeta, mergeCmd: MergeIntoCommand): Unit = {
    // see https://github.com/NVIDIA/spark-rapids/issues/8415 for more information
    if (mergeCmd.notMatchedBySourceClauses.nonEmpty) {
      meta.willNotWorkOnGpu("notMatchedBySourceClauses not supported on GPU")
    }
  }

  def tagForGpu(meta: MergeIntoCommandEdgeMeta, mergeCmd: MergeIntoCommandEdge): Unit = {
    // see https://github.com/NVIDIA/spark-rapids/issues/8415 for more information
    if (mergeCmd.notMatchedBySourceClauses.nonEmpty) {
      meta.willNotWorkOnGpu("notMatchedBySourceClauses not supported on GPU")
    }
  }

  def convertToGpu(mergeCmd: MergeIntoCommand, conf: RapidsConf): RunnableCommand = {
    // TODO: Currently we only support low shuffler merge only when parquet per file read is enabled
    // due to the limitation of implementing row index metadata column.
    if (conf.isDeltaLowShuffleMergeEnabled) {
      if (conf.isParquetPerFileReadEnabled) {
        GpuLowShuffleMergeCommand(
          mergeCmd.source,
          mergeCmd.target,
          new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
          mergeCmd.condition,
          mergeCmd.matchedClauses,
          mergeCmd.notMatchedClauses,
          mergeCmd.notMatchedBySourceClauses,
          mergeCmd.migratedSchema)(conf)
      } else {
        logWarning(s"""Low shuffle merge disabled since ${RapidsConf.PARQUET_READER_TYPE} is
          not set to ${RapidsReaderType.PERFILE}. Falling back to classic merge.""")
        GpuMergeIntoCommand(
          mergeCmd.source,
          mergeCmd.target,
          new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
          mergeCmd.condition,
          mergeCmd.matchedClauses,
          mergeCmd.notMatchedClauses,
          mergeCmd.notMatchedBySourceClauses,
          mergeCmd.migratedSchema)(conf)
      }
    } else {
      GpuMergeIntoCommand(
        mergeCmd.source,
        mergeCmd.target,
        new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
        mergeCmd.condition,
        mergeCmd.matchedClauses,
        mergeCmd.notMatchedClauses,
        mergeCmd.notMatchedBySourceClauses,
        mergeCmd.migratedSchema)(conf)
    }
  }

  def convertToGpu(mergeCmd: MergeIntoCommandEdge, conf: RapidsConf): RunnableCommand = {
    // TODO: Currently we only support low shuffler merge only when parquet per file read is enabled
    // due to the limitation of implementing row index metadata column.
    if (conf.isDeltaLowShuffleMergeEnabled) {
      if (conf.isParquetPerFileReadEnabled) {
        GpuLowShuffleMergeCommand(
          mergeCmd.source,
          mergeCmd.target,
          new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
          mergeCmd.condition,
          mergeCmd.matchedClauses,
          mergeCmd.notMatchedClauses,
          mergeCmd.notMatchedBySourceClauses,
          mergeCmd.migratedSchema)(conf)
      } else {
        logWarning(s"""Low shuffle merge is still disable since ${RapidsConf.PARQUET_READER_TYPE} is
          not set to ${RapidsReaderType.PERFILE}. Falling back to classic merge.""")
        GpuMergeIntoCommand(
          mergeCmd.source,
          mergeCmd.target,
          new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
          mergeCmd.condition,
          mergeCmd.matchedClauses,
          mergeCmd.notMatchedClauses,
          mergeCmd.notMatchedBySourceClauses,
          mergeCmd.migratedSchema)(conf)
      }
    } else {
      GpuMergeIntoCommand(
        mergeCmd.source,
        mergeCmd.target,
        new GpuDeltaLog(mergeCmd.targetFileIndex.deltaLog, conf),
        mergeCmd.condition,
        mergeCmd.matchedClauses,
        mergeCmd.notMatchedClauses,
        mergeCmd.notMatchedBySourceClauses,
        mergeCmd.migratedSchema)(conf)
    }
  }
}
