/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.datasources.{FilePartition, HadoopFsRelation, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.rapids.{GpuFileSourceScanExec, ShuffleManagerShimBase}
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExecBase, GpuBroadcastNestedLoopJoinExecBase, GpuShuffleExchangeExecBase}
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

case class DatabricksShimVersion(major: Int, minor: Int, patch: Int) extends ShimVersion {
  override def toString(): String = s"$major.$minor.$patch-databricks"
}

case class EMRShimVersion(major: Int, minor: Int, patch: Int) extends ShimVersion {
  override def toString(): String = s"$major.$minor.$patch-amzn"
}

trait SparkShims {
  def getSparkShimVersion: ShimVersion
  def isGpuHashJoin(plan: SparkPlan): Boolean
  def isGpuBroadcastHashJoin(plan: SparkPlan): Boolean
  def isGpuShuffledHashJoin(plan: SparkPlan): Boolean
  def isBroadcastExchangeLike(plan: SparkPlan): Boolean
  def isShuffleExchangeLike(plan: SparkPlan): Boolean
  def getRapidsShuffleManagerClass: String
  def getBuildSide(join: HashJoin): GpuBuildSide
  def getBuildSide(join: BroadcastNestedLoopJoinExec): GpuBuildSide
  def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]]
  def getGpuColumnarToRowTransition(plan: SparkPlan,
     exportColumnRdd: Boolean): GpuColumnarToRowExecParent
  def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]]
  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]]

  def getScalaUDFAsExpression(
    function: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil,
    outputEncoder: Option[ExpressionEncoder[_]] = None,
    udfName: Option[String] = None,
    nullable: Boolean = true,
    udfDeterministic: Boolean = true): Expression

  def getGpuBroadcastNestedLoopJoinShim(
    left: SparkPlan,
    right: SparkPlan,
    join: BroadcastNestedLoopJoinExec,
    joinType: JoinType,
    condition: Option[Expression],
    targetSizeBytes: Long): GpuBroadcastNestedLoopJoinExecBase


  def getGpuBroadcastExchangeExec(
      mode: BroadcastMode,
      child: SparkPlan): GpuBroadcastExchangeExecBase

  def getGpuShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan,
      cpuShuffle: Option[ShuffleExchangeExec] = None): GpuShuffleExchangeExecBase

  def getGpuShuffleExchangeExec(
      queryStage: ShuffleQueryStageExec): GpuShuffleExchangeExecBase

  def getMapSizesByExecutorId(
    shuffleId: Int,
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])]

  def injectQueryStagePrepRule(
      extensions: SparkSessionExtensions,
      rule: SparkSession => Rule[SparkPlan])

  def getShuffleManagerShims(): ShuffleManagerShimBase

  def createFilePartition(index: Int, files: Array[PartitionedFile]): FilePartition

  def getPartitionFileNames(partitions: Seq[PartitionDirectory]): Seq[String]
  def getPartitionFileStatusSize(partitions: Seq[PartitionDirectory]): Long
  def getPartitionedFiles(partitions: Array[PartitionDirectory]): Array[PartitionedFile]
  def getPartitionSplitFiles(
      partitions: Array[PartitionDirectory],
      maxSplitBytes: Long,
      relation: HadoopFsRelation): Array[PartitionedFile]
  def getFileScanRDD(
    sparkSession: SparkSession,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    filePartitions: Seq[FilePartition]): RDD[InternalRow]

  def copyParquetBatchScanExec(
      batchScanExec: GpuBatchScanExec,
      queryUsesInputFile: Boolean): GpuBatchScanExec

  def copyFileSourceScanExec(
      scanExec: GpuFileSourceScanExec,
      queryUsesInputFile: Boolean): GpuFileSourceScanExec

  def checkColumnNameDuplication(
      schema: StructType,
      colType: String,
      resolver: Resolver): Unit
}

