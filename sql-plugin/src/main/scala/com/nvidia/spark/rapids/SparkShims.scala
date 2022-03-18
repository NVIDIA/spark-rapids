/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.net.URI
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.Kryo
import org.apache.arrow.memory.ReferenceManager
import org.apache.arrow.vector.ValueVector
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.schema.MessageType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ExprId, NullOrdering, SortDirection, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.{FileIndex, FilePartition, HadoopFsRelation, PartitionDirectory, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.storage.{BlockId, BlockManagerId}

/**
 * Spark BuildSide, BuildRight, BuildLeft moved packages in Spark 3.1
 * so create GPU versions of these that can be agnostic to Spark version.
 */
sealed abstract class GpuBuildSide

case object GpuBuildRight extends GpuBuildSide

case object GpuBuildLeft extends GpuBuildSide

sealed abstract class ShimVersion

case class SparkShimVersion(major: Int, minor: Int, patch: Int) extends ShimVersion {
  override def toString(): String = s"$major.$minor.$patch"
}

case class ClouderaShimVersion(major: Int, minor: Int, patch: Int, clouderaVersion: String)
  extends ShimVersion {
  override def toString(): String = s"$major.$minor.$patch.$clouderaVersion"
}

case class DatabricksShimVersion(
    major: Int,
    minor: Int,
    patch: Int,
    dbver: String = "") extends ShimVersion {
  override def toString(): String = s"$major.$minor.$patch-databricks$dbver"
}

case class EMRShimVersion(major: Int, minor: Int, patch: Int) extends ShimVersion {
  override def toString(): String = s"$major.$minor.$patch-amzn"
}

trait SparkShims {
  def getSparkShimVersion: ShimVersion
  def parquetRebaseReadKey: String
  def parquetRebaseWriteKey: String
  def avroRebaseReadKey: String
  def avroRebaseWriteKey: String
  def parquetRebaseRead(conf: SQLConf): String
  def parquetRebaseWrite(conf: SQLConf): String
  def v1RepairTableCommand(tableName: TableIdentifier): RunnableCommand
  def hasSeparateINT96RebaseConf: Boolean
  def int96ParquetRebaseRead(conf: SQLConf): String
  def int96ParquetRebaseWrite(conf: SQLConf): String
  def int96ParquetRebaseReadKey: String
  def int96ParquetRebaseWriteKey: String
  def isCastingStringToNegDecimalScaleSupported: Boolean = true

  def getParquetFilters(
    schema: MessageType,
    pushDownDate: Boolean,
    pushDownTimestamp: Boolean,
    pushDownDecimal: Boolean,
    pushDownStartWith: Boolean,
    pushDownInFilterThreshold: Int,
    caseSensitive: Boolean,
    lookupFileMeta: String => String,
    dateTimeRebaseModeFromConf: String): ParquetFilters

  def isWindowFunctionExec(plan: SparkPlan): Boolean
  def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]]
  def getGpuColumnarToRowTransition(plan: SparkPlan,
     exportColumnRdd: Boolean): GpuColumnarToRowExecParent
  def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]]
  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]]
  def getFileFormats: Map[FileFormatType, Map[FileFormatOp, FileFormatChecks]] = Map()

  def getScalaUDFAsExpression(
    function: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil,
    outputEncoder: Option[ExpressionEncoder[_]] = None,
    udfName: Option[String] = None,
    nullable: Boolean = true,
    udfDeterministic: Boolean = true): Expression

  def getGpuShuffleExchangeExec(
      gpuOutputPartitioning: GpuPartitioning,
      child: SparkPlan,
      cpuOutputPartitioning: Partitioning,
      cpuShuffle: Option[ShuffleExchangeExec] = None): GpuShuffleExchangeExecBase

  def getGpuShuffleExchangeExec(
      queryStage: ShuffleQueryStageExec): GpuShuffleExchangeExecBase

  def newBroadcastQueryStageExec(
      old: BroadcastQueryStageExec,
      newPlan: SparkPlan): BroadcastQueryStageExec

  def getMapSizesByExecutorId(
    shuffleId: Int,
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])]

  def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: (PartitionedFile) => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference] = Seq.empty): RDD[InternalRow]

  def getFileSourceMaxMetadataValueLength(sqlConf: SQLConf): Int

  def sortOrder(child: Expression, direction: SortDirection): SortOrder = {
    sortOrder(child, direction, direction.defaultNullOrdering)
  }

  def sortOrder(
      child: Expression,
      direction: SortDirection,
      nullOrdering: NullOrdering): SortOrder

  def copySortOrderWithNewChild(s: SortOrder, child: Expression): SortOrder

  def alias(child: Expression, name: String)(
      exprId: ExprId,
      qualifier: Seq[String] = Seq.empty,
      explicitMetadata: Option[Metadata] = None): Alias

  def shouldIgnorePath(path: String): Boolean

  def getLegacyComplexTypeToString(): Boolean

  def getArrowDataBuf(vec: ValueVector): (ByteBuffer, ReferenceManager)
  def getArrowValidityBuf(vec: ValueVector): (ByteBuffer, ReferenceManager)
  def getArrowOffsetsBuf(vec: ValueVector): (ByteBuffer, ReferenceManager)

  def replaceWithAlluxioPathIfNeeded(
      conf: RapidsConf,
      relation: HadoopFsRelation,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): FileIndex

  def replacePartitionDirectoryFiles(
    partitionDir: PartitionDirectory,
    replaceFunc: Path => Path): Seq[Path]

  def shouldFailDivByZero(): Boolean

  def shouldFailDivOverflow: Boolean

  /**
   * This is specifically in relation to SPARK-33498 which went into 3.1.0. We cannot fully support
   * it right now, so we fall back to the CPU in those cases.
   */
  def shouldFallbackOnAnsiTimestamp(): Boolean

  /**
   * This is to support ANSI mode: optionally return null result if element not exists
   * in array/map.
   */
  def shouldFailOnElementNotExists(): Boolean = false

  def createTable(table: CatalogTable,
    sessionCatalog: SessionCatalog,
    tableLocation: Option[URI],
    result: BaseRelation): Unit

  def reusedExchangeExecPfn: PartialFunction[SparkPlan, ReusedExchangeExec]

  /** dropped by SPARK-34234 */
  def attachTreeIfSupported[TreeType <: TreeNode[_], A](
    tree: TreeType,
    msg: String = "")(
    f: => A
  ): A

  def hasAliasQuoteFix: Boolean

  def hasCastFloatTimestampUpcast: Boolean

  def filesFromFileIndex(fileCatalog: PartitioningAwareFileIndex): Seq[FileStatus]

  def isEmptyRelation(relation: Any): Boolean

  def broadcastModeTransform(mode: BroadcastMode, toArray: Array[InternalRow]): Any

  /**
   * This call can produce an `EmptyHashedRelation` or an empty array,
   * allowing the AQE rule `EliminateJoinToEmptyRelation` in Spark 3.1.x
   * to optimize certain joins.
   *
   * In Spark 3.2.0, the optimization is still performed (under `AQEPropagateEmptyRelation`),
   * but the AQE optimizer is looking at the metrics for the query stage to determine
   * if numRows == 0, and if so it can eliminate certain joins.
   *
   * The call is implemented only for Spark 3.1.x+. It is disabled in
   * Databricks because it requires a task context to perform the
   * `BroadcastMode.transform` call, but we'd like to call this from the driver.
   */
  def tryTransformIfEmptyRelation(mode: BroadcastMode): Option[Any]

  def isAqePlan(p: SparkPlan): Boolean

  def isExchangeOp(plan: SparkPlanMeta[_]): Boolean

  def getDateFormatter(): DateFormatter

  def sessionFromPlan(plan: SparkPlan): SparkSession

  def isCustomReaderExec(x: SparkPlan): Boolean

  def aqeShuffleReaderExec: ExecRule[_ <: SparkPlan]

  /**
   * Walk the plan recursively and return a list of operators that match the predicate
   */
  def findOperators(plan: SparkPlan, predicate: SparkPlan => Boolean): Seq[SparkPlan]

  /**
   * Our tests, by default, will check that all operators are running on the GPU, but
   * there are some operators that we do not translate to GPU plans, so we need a way
   * to bypass the check for those.
   */
  def skipAssertIsOnTheGpu(plan: SparkPlan): Boolean

  def leafNodeDefaultParallelism(ss: SparkSession): Int

  def registerKryoClasses(kryo: Kryo): Unit

  def getAdaptiveInputPlan(adaptivePlan: AdaptiveSparkPlanExec): SparkPlan

  def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan]

  /**
   * Determine if the Spark version allows the supportsColumnar flag to be overridden
   * in AdaptiveSparkPlanExec. This feature was introduced in Spark 3.2 as part of
   * SPARK-35881.
   */
  def supportsColumnarAdaptivePlans: Boolean

  def columnarAdaptivePlan(a: AdaptiveSparkPlanExec, goal: CoalesceSizeGoal): SparkPlan
}
