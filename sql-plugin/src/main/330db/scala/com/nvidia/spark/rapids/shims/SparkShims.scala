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

import org.apache.parquet.schema.MessageType

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.rapids.execution.python._

object SparkShimImpl extends Spark320PlusShims
  with Spark321PlusDBShims
  with RebaseShims {
  // AnsiCast is removed from Spark3.4.0
  override def ansiCastRule: ExprRule[_ <: Expression] = null

  override def getParquetFilters(
      schema: MessageType,
      pushDownDate: Boolean,
      pushDownTimestamp: Boolean,
      pushDownDecimal: Boolean,
      pushDownStartWith: Boolean,
      pushDownInFilterThreshold: Int,
      caseSensitive: Boolean,
      lookupFileMeta: String => String,
      dateTimeRebaseModeFromConf: String): ParquetFilters = {
    val datetimeRebaseMode = DataSourceUtils
      .datetimeRebaseSpec(lookupFileMeta, dateTimeRebaseModeFromConf)
    new ParquetFilters(schema, pushDownDate, pushDownTimestamp, pushDownDecimal, pushDownStartWith,
      pushDownInFilterThreshold, caseSensitive, datetimeRebaseMode)
  }

  // RebaseShims.parquetRebaseReadKey
  // override def parquetRebaseReadKey: String = super.parquetRebaseReadKey

  // RebaseShims.parquetRebaseWriteKey
  // override def parquetRebaseWriteKey: String = ???

  // RebaseShims.avroRebaseReadKey$
  // override def avroRebaseReadKey: String = ???

  // RebaseShims.avroRebaseWriteKey$
  // override def avroRebaseWriteKey: String = ???

  // RebaseShims.parquetRebaseRead$
  // override def parquetRebaseRead(conf: org.apache.spark.sql.internal.SQLConf): String = ???

  // RebaseShims.parquetRebaseWrite$
  // override def parquetRebaseWrite(conf: org.apache.spark.sql.internal.SQLConf): String = ???

  // Spark320PlusShims.v1RepairTableCommand$
  // override def v1RepairTableCommand(tableName: TableIdentifier): org.apache.spark.sql.execution.command.RunnableCommand = ???

  // RebaseShims.int96ParquetRebaseRead$
  // override def int96ParquetRebaseRead(conf: org.apache.spark.sql.internal.SQLConf): String = ???

  // RebaseShims.int96ParquetRebaseWrite$
  // override def int96ParquetRebaseWrite(conf: org.apache.spark.sql.internal.SQLConf): String = ???

  // RebaseShims.int96ParquetRebaseReadKey$
  // override def int96ParquetRebaseReadKey: String = ???

  // RebaseShims.int96ParquetRebaseWriteKey$
  // override def int96ParquetRebaseWriteKey: String = ???

  // Spark330PlusShims.getParquetFilters$
  // override def getParquetFilters(schema: org.apache.parquet.schema.MessageType, pushDownDate: Boolean, pushDownTimestamp: Boolean, pushDownDecimal: Boolean, pushDownStartWith: Boolean, pushDownInFilterThreshold: Int, caseSensitive: Boolean, lookupFileMeta: String => String, dateTimeRebaseModeFromConf: String): org.apache.spark.sql.execution.datasources.parquet.ParquetFilters = ???

  // Spark320PlusShims.isWindowFunctionExec$
  // override def isWindowFunctionExec(plan: SparkPlan): Boolean = ???

  // Spark320PlusShims.getExprs$
  // Spark330PlusShims.getExprs$:
  //  Spark331PlusShims.getExprs$
  // override def getExprs: Map[Class[_ <: Expression],ExprRule[_ <: Expression]] = ???

  // Spark320PlusShims.getScans$
  // override def getScans: Map[Class[_ <: Scan],ScanRule[_ <: Scan]] = ???

  // Spark321PlusDBShims.newBroadcastQueryStageExec$
  // override def newBroadcastQueryStageExec(old: BroadcastQueryStageExec, newPlan: SparkPlan): BroadcastQueryStageExec = ???

  // Spark321PlusDBShims.getFileScanRDD$:
  // override def getFileScanRDD(sparkSession: SparkSession, readFunction: PartitionedFile => Iterator[InternalRow], filePartitions: Seq[FilePartition], readDataSchema: StructType, metadataColumns: Seq[AttributeReference]): RDD[InternalRow] = ???

  // Spark320PlusShims.shouldFailDivOverflow$
  // override def shouldFailDivOverflow: Boolean = ???

  // Spark321PlusDBShims.reusedExchangeExecPfn$
  // override def reusedExchangeExecPfn: PartialFunction[SparkPlan,ReusedExchangeExec] = ???

  // Spark320PlusShims.attachTreeIfSupported$
  // override def attachTreeIfSupported[TreeType <: TreeNode[_], A](tree: TreeType, msg: String)(f: => A): A = ???

  // Spark320PlusShims.hasAliasQuoteFix$
  // override def hasAliasQuoteFix: Boolean = ???

  // Spark320PlusShims.hasCastFloatTimestampUpcast$
  // override def hasCastFloatTimestampUpcast: Boolean = ???

  // Spark321PlusDBShims.filesFromFileIndex$
  // override def filesFromFileIndex(fileCatalog: PartitioningAwareFileIndex): Seq[FileStatus] = ???

  // Spark320PlusShims.isEmptyRelation$
  // override def isEmptyRelation(relation: Any): Boolean = ???

  // Spark321PlusDBShims.broadcastModeTransform$
  // override def broadcastModeTransform(mode: BroadcastMode, toArray: Array[InternalRow]): Any = ???

  // Spark320PlusShims.tryTransformIfEmptyRelation$
  // override def tryTransformIfEmptyRelation(mode: BroadcastMode): Option[Any] = ???

  // Spark320PlusShims.isAqePlan$
  // override def isAqePlan(p: SparkPlan): Boolean = ???

  // Spark320PlusShims.isExchangeOp$
  // override def isExchangeOp(plan: SparkPlanMeta[_]): Boolean = ???

  // Spark320PlusShims.getDateFormatter$
  // override def getDateFormatter(): DateFormatter = ???

  // Spark320PlusShims.sessionFromPlan$:
  // override def sessionFromPlan(plan: SparkPlan): SparkSession = ???

  // Spark320PlusShims.isCustomReaderExec$
  // override def isCustomReaderExec(x: SparkPlan): Boolean = ???

  // Spark320PlusShims.aqeShuffleReaderExec$
  // override def aqeShuffleReaderExec: ExecRule[_ <: SparkPlan] = ???

  // Spark320PlusShims.findOperators$:
  // override def findOperators(plan: SparkPlan, predicate: SparkPlan => Boolean): Seq[SparkPlan] = ???

  // Spark320PlusShims.skipAssertIsOnTheGpu$
  // override def skipAssertIsOnTheGpu(plan: SparkPlan): Boolean = ???

  // Spark320PlusShims.leafNodeDefaultParallelism$
  // override def leafNodeDefaultParallelism(ss: SparkSession): Int = ???

  // Spark320PlusShims.getAdaptiveInputPlan$
  // override def getAdaptiveInputPlan(adaptivePlan: AdaptiveSparkPlanExec): SparkPlan = ???

  // Spark321PlusDBShims.neverReplaceShowCurrentNamespaceCommand$
  // override def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan] = ???

  // Spark320PlusShims.supportsColumnarAdaptivePlans$
  // override def supportsColumnarAdaptivePlans: Boolean = ???

  // Spark320PlusShims.columnarAdaptivePlan$
  // override def columnarAdaptivePlan(a: AdaptiveSparkPlanExec, goal: CoalesceSizeGoal): SparkPlan = ???
}

// Fallback to the default definition of `deterministic`
trait GpuDeterministicFirstLastCollectShim extends Expression
