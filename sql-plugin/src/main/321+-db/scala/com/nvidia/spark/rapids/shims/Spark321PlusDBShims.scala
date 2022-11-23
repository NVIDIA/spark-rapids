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

import com.nvidia.spark.rapids._
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.rapids.shims._
import org.apache.spark.sql.types._


trait Spark321PlusDBShims extends SparkShims {
  override def broadcastModeTransform(mode: BroadcastMode, rows: Array[InternalRow]): Any = {
    // In some cases we can be asked to transform when there's no task context, which appears to
    // be new behavior since Databricks 10.4. A task memory manager must be passed, so if one is
    // not available we construct one from the main memory manager using a task attempt ID of 0.
    val memoryManager = Option(TaskContext.get).map(_.taskMemoryManager()).getOrElse {
      new TaskMemoryManager(SparkEnv.get.memoryManager, 0)
    }
    mode.transform(rows, memoryManager)
  }

  def getWindowExpressions(winPy: WindowInPandasExec): Seq[NamedExpression] = {
    winPy.projectList
  }

  override def filesFromFileIndex(fileCatalog: PartitioningAwareFileIndex): Seq[FileStatus] = {
    fileCatalog.allFiles().map(_.toFileStatus)
  }

  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference]): RDD[InternalRow] = {
    new GpuFileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan] = null

  override def newBroadcastQueryStageExec(
      old: BroadcastQueryStageExec,
      newPlan: SparkPlan): BroadcastQueryStageExec = {
    BroadcastQueryStageExec(old.id, newPlan, old.originalPlan, old.isSparkExchange)
  }

  override def reusedExchangeExecPfn: PartialFunction[SparkPlan, ReusedExchangeExec] = {
    case ShuffleQueryStageExec(_, e: ReusedExchangeExec, _, _) => e
    case BroadcastQueryStageExec(_, e: ReusedExchangeExec, _, _) => e
  }
}
