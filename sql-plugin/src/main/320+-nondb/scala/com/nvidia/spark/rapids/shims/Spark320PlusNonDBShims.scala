/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.SparkShims
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.python.WindowInPandasExec

/**
 * Shim methods that can be compiled with every supported 3.2.0+ except Databricks versions
 */
trait Spark320PlusNonDBShims extends SparkShims {

  override final def broadcastModeTransform(mode: BroadcastMode, rows: Array[InternalRow]): Any =
    mode.transform(rows)

  override final def newBroadcastQueryStageExec(
      old: BroadcastQueryStageExec,
      newPlan: SparkPlan): BroadcastQueryStageExec =
    BroadcastQueryStageExec(old.id, newPlan, old._canonicalized)

  override final def filesFromFileIndex(fileIndex: PartitioningAwareFileIndex): Seq[FileStatus] = {
    fileIndex.allFiles()
  }

  override def alluxioReplacePathsPartitionDirectory(
      pd: PartitionDirectory,
      replaceFunc: Option[Path => Path]): (Seq[FileStatus], PartitionDirectory) = {
    val updatedFileStatus = pd.files.map { f =>
      val replaced = replaceFunc.get(f.getPath)
      // Alluxio caches the entire file, so the size should be the same.
      // Just hardcode block replication to 1 to make sure nothing weird happens but
      // I haven't seen it used by splits. The modification time shouldn't be
      // affected by Alluxio. Blocksize is also not used. Note that we will not
      // get new block locations with this so if Alluxio would return new ones
      // this isn't going to get them. From my current experiments, Alluxio is not
      // returning the block locations of the cached blocks anyway.
      new FileStatus(f.getLen, f.isDirectory, 1, f.getBlockSize, f.getModificationTime, replaced)
    }
    (updatedFileStatus, PartitionDirectory(pd.values, updatedFileStatus))
  }

  def getWindowExpressions(winPy: WindowInPandasExec): Seq[NamedExpression] = winPy.windowExpression

  /**
   * Case class ShuffleQueryStageExec holds an additional field shuffleOrigin
   * affecting the unapply method signature
   */
  override def reusedExchangeExecPfn: PartialFunction[SparkPlan, ReusedExchangeExec] = {
    case ShuffleQueryStageExec(_, e: ReusedExchangeExec, _) => e
    case BroadcastQueryStageExec(_, e: ReusedExchangeExec, _) => e
  }
}
