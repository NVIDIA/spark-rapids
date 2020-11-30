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

package com.nvidia.spark.rapids.shims.spark310

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.spark301.Spark301Shims
import com.nvidia.spark.rapids.spark310.RapidsShuffleManager

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, HashJoin, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.rapids.{GpuFileSourceScanExec, GpuStringReplace, ShuffleManagerShimBase}
import org.apache.spark.sql.rapids.execution.{GpuBroadcastNestedLoopJoinExecBase, GpuShuffleExchangeExecBase}
import org.apache.spark.sql.rapids.shims.spark310._
import org.apache.spark.sql.types._
import org.apache.spark.storage.{BlockId, BlockManagerId}

class Spark310Shims extends Spark301Shims {

  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  override def getScalaUDFAsExpression(
      function: AnyRef,
      dataType: DataType,
      children: Seq[Expression],
      inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil,
      outputEncoder: Option[ExpressionEncoder[_]] = None,
      udfName: Option[String] = None,
      nullable: Boolean = true,
      udfDeterministic: Boolean = true): Expression = {
    ScalaUDF(function, dataType, children, inputEncoders, outputEncoder, udfName, nullable,
      udfDeterministic)
  }

  override def getMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(shuffleId,
      startMapIndex, endMapIndex, startPartition, endPartition)
  }

  override def getGpuBroadcastNestedLoopJoinShim(
      left: SparkPlan,
      right: SparkPlan,
      join: BroadcastNestedLoopJoinExec,
      joinType: JoinType,
      condition: Option[Expression],
      targetSizeBytes: Long): GpuBroadcastNestedLoopJoinExecBase = {
    GpuBroadcastNestedLoopJoinExec(left, right, join, joinType, condition, targetSizeBytes)
  }

  override def isGpuHashJoin(plan: SparkPlan): Boolean = {
    plan match {
      case _: GpuHashJoin => true
      case _ => false
    }
  }

  override def isGpuBroadcastHashJoin(plan: SparkPlan): Boolean = {
    plan match {
      case _: GpuBroadcastHashJoinExec => true
      case _ => false
    }
  }

  override def isGpuShuffledHashJoin(plan: SparkPlan): Boolean = {
    plan match {
      case _: GpuShuffledHashJoinExec => true
      case _ => false
    }
  }

  def exprs310: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[RegExpReplace](
      "RegExpReplace support for string literal input patterns",
      (a, conf, p, r) => new ExprMeta[RegExpReplace](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!GpuOverrides.isLit(a.rep)) {
            willNotWorkOnGpu("Only literal values are supported for replacement string")
          }
          if (GpuOverrides.isNullOrEmptyOrRegex(a.regexp)) {
            willNotWorkOnGpu(
              "Only non-null, non-empty String literals that are not regex patterns " +
                  "are supported by RegExpReplace on the GPU")
          }
          if (!a.pos.foldable) {
            willNotWorkOnGpu("Only foldable expressions are supported for the " +
            "starting search position")
          }
          val posEval = a.pos.eval()
          if (posEval.asInstanceOf[Int] != 1) {
            willNotWorkOnGpu("Only a search starting position of 1 is supported")
          }
        }
        override def convertToGpu(): GpuExpression = {
          GpuStringReplace(
            childExprs(0).convertToGpu(),
            childExprs(1).convertToGpu(),
            childExprs(2).convertToGpu())
        }
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    super.exprs301 ++ exprs310
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    super.getExecs ++ Seq(
      GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {
          override def isSupportedType(t: DataType): Boolean =
            GpuOverrides.isSupportedType(t,
              allowArray = true,
              allowMaps = true,
              allowStruct = true,
              allowNesting = true)

          // partition filters and data filters are not run on the GPU
          override val childExprs: Seq[ExprMeta[_]] = Seq.empty

          override def tagPlanForGpu(): Unit = GpuFileSourceScanExec.tagSupport(this)

          override def convertToGpu(): GpuExec = {
            val sparkSession = wrapped.relation.sparkSession
            val options = wrapped.relation.options
            val newRelation = HadoopFsRelation(
              wrapped.relation.location,
              wrapped.relation.partitionSchema,
              wrapped.relation.dataSchema,
              wrapped.relation.bucketSpec,
              GpuFileSourceScanExec.convertFileFormat(wrapped.relation.fileFormat),
              options)(sparkSession)

            GpuFileSourceScanExec(
              newRelation,
              wrapped.output,
              wrapped.requiredSchema,
              wrapped.partitionFilters,
              wrapped.optionalBucketSet,
              wrapped.optionalNumCoalescedBuckets,
              wrapped.dataFilters,
              wrapped.tableIdentifier,
              conf)
          }
        }),
      GpuOverrides.exec[InMemoryTableScanExec](
        "Implementation of InMemoryTableScanExec to use GPU accelerated Caching",
        (scan, conf, p, r) => new SparkPlanMeta[InMemoryTableScanExec](scan, conf, p, r) {
          override def tagPlanForGpu(): Unit = {
            if (!scan.relation.cacheBuilder.serializer.isInstanceOf[ParquetCachedBatchSerializer]) {
              willNotWorkOnGpu("ParquetCachedBatchSerializer is not being used")
            }
          }
          /**
           * Convert InMemoryTableScanExec to a GPU enabled version.
           */
          override def convertToGpu(): GpuExec = {
            GpuInMemoryTableScanExec(scan.attributes, scan.predicates, scan.relation)
          }
        }),
      GpuOverrides.exec[SortMergeJoinExec](
        "Sort merge join, replacing with shuffled hash join",
        (join, conf, p, r) => new GpuSortMergeJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[BroadcastHashJoinExec](
        "Implementation of join using broadcast data",
        (join, conf, p, r) => new GpuBroadcastHashJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[ShuffledHashJoinExec](
        "Implementation of join using hashed shuffled data",
        (join, conf, p, r) => new GpuShuffledHashJoinMeta(join, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r))
  }

  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Seq(
    GpuOverrides.scan[ParquetScan](
      "Parquet parsing",
      (a, conf, p, r) => new ScanMeta[ParquetScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = GpuParquetScanBase.tagSupport(this)

        override def convertToGpu(): Scan = {
          GpuParquetScan(a.sparkSession,
            a.hadoopConf,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.pushedFilters,
            a.options,
            a.partitionFilters,
            a.dataFilters,
            conf)
        }
      }),
    GpuOverrides.scan[OrcScan](
      "ORC parsing",
      (a, conf, p, r) => new ScanMeta[OrcScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit =
          GpuOrcScanBase.tagSupport(this)

        override def convertToGpu(): Scan =
          GpuOrcScan(a.sparkSession,
            a.hadoopConf,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.options,
            a.pushedFilters,
            a.partitionFilters,
            a.dataFilters,
            conf)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap


  override def getBuildSide(join: HashJoin): GpuBuildSide = {
    GpuJoinUtils.getGpuBuildSide(join.buildSide)
  }

  override def getBuildSide(join: BroadcastNestedLoopJoinExec): GpuBuildSide = {
    GpuJoinUtils.getGpuBuildSide(join.buildSide)
  }

  override def getRapidsShuffleManagerClass: String = {
    classOf[RapidsShuffleManager].getCanonicalName
  }

  override def getShuffleManagerShims(): ShuffleManagerShimBase = {
    new ShuffleManagerShim
  }

  override def copyParquetBatchScanExec(
      batchScanExec: GpuBatchScanExec,
      queryUsesInputFile: Boolean): GpuBatchScanExec = {
    val scan = batchScanExec.scan.asInstanceOf[GpuParquetScan]
    val scanCopy = scan.copy(queryUsesInputFile=queryUsesInputFile)
    batchScanExec.copy(scan=scanCopy)
  }

  override def copyFileSourceScanExec(
      scanExec: GpuFileSourceScanExec,
      queryUsesInputFile: Boolean): GpuFileSourceScanExec = {
    scanExec.copy(queryUsesInputFile=queryUsesInputFile)
  }

  override def getGpuColumnarToRowTransition(plan: SparkPlan,
     exportColumnRdd: Boolean): GpuColumnarToRowExecParent = {
    val serName = plan.conf.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER)
    val serClass = Class.forName(serName)
    if (serClass == classOf[ParquetCachedBatchSerializer]) {
      org.apache.spark.sql.rapids.shims.spark310.GpuColumnarToRowTransitionExec(plan)
    } else {
      GpuColumnarToRowExec(plan)
    }
  }

  override def checkColumnNameDuplication(
      schema: StructType,
      colType: String,
      resolver: Resolver): Unit = {
    GpuSchemaUtils.checkColumnNameDuplication(schema, colType, resolver)
  }

  override def getGpuShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan,
      cpuShuffle: Option[ShuffleExchangeExec]): GpuShuffleExchangeExecBase = {
    val shuffleOrigin = cpuShuffle.map(_.shuffleOrigin).getOrElse(ENSURE_REQUIREMENTS)
    GpuShuffleExchangeExec(outputPartitioning, child, shuffleOrigin)
  }
}
