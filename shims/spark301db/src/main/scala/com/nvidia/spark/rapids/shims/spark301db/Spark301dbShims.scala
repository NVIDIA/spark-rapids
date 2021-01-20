/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark301db

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.spark301.Spark301Shims
import org.apache.spark.sql.rapids.shims.spark301db._
import org.apache.hadoop.fs.{Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.datasources.{FileIndex, FilePartition, HadoopFsRelation, InMemoryFileIndex, PartitionDirectory, PartitionedFile, RapidsPartitioningUtils}
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, HashJoin, SortMergeJoinExec}
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.execution.python.WindowInPandasExec
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExecBase, GpuBroadcastNestedLoopJoinExecBase, GpuShuffleExchangeExecBase}
import org.apache.spark.sql.rapids.execution.python.GpuWindowInPandasExecMetaBase
import org.apache.spark.sql.types._

import scala.collection.mutable

class Spark301dbShims extends Spark301Shims {

  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  override def getGpuBroadcastNestedLoopJoinShim(
      left: SparkPlan,
      right: SparkPlan,
      join: BroadcastNestedLoopJoinExec,
      joinType: JoinType,
      condition: Option[Expression],
      targetSizeBytes: Long): GpuBroadcastNestedLoopJoinExecBase = {
    GpuBroadcastNestedLoopJoinExec(left, right, join, joinType, condition, targetSizeBytes)
  }

  override def getGpuBroadcastExchangeExec(
      mode: BroadcastMode,
      child: SparkPlan): GpuBroadcastExchangeExecBase = {
    GpuBroadcastExchangeExec(mode, child)
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

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    Seq(
      GpuOverrides.exec[WindowInPandasExec](
        "The backend for Window Aggregation Pandas UDF, Accelerates the data transfer between" +
          " the Java process and the Python process. It also supports scheduling GPU resources" +
          " for the Python process when enabled. For now it only supports row based window frame.",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested(TypeSig.commonCudfTypes),
          TypeSig.all),
        (winPy, conf, p, r) => new GpuWindowInPandasExecMetaBase(winPy, conf, p, r) {
          override def convertToGpu(): GpuExec = {
            GpuWindowInPandasExec(
              windowExpressions.map(_.convertToGpu()),
              partitionSpec.map(_.convertToGpu()),
              orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
              childPlans.head.convertIfNeeded()
            )
          }
        }).disabledByDefault("it only supports row based frame for now"),
      GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.ARRAY + TypeSig.DECIMAL).nested(), TypeSig.all),
        (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {
          // partition filters and data filters are not run on the GPU
          override val childExprs: Seq[ExprMeta[_]] = Seq.empty

          override def tagPlanForGpu(): Unit = {
            // this is very specific check to have any of the Delta log metadata queries
            // fallback and run on the CPU since there is some incompatibilities in
            // Databricks Spark and Apache Spark.
            if (wrapped.relation.fileFormat.isInstanceOf[JsonFileFormat] &&
              wrapped.relation.location.getClass.getCanonicalName() ==
                "com.databricks.sql.transaction.tahoe.DeltaLogFileIndex") {
              this.entirePlanWillNotWork("Plans that read Delta Index JSON files can not run " +
                "any part of the plan on the GPU!")
            }
            GpuFileSourceScanExec.tagSupport(this)
          }

          override def convertToGpu(): GpuExec = {
            val sparkSession = wrapped.relation.sparkSession
            val options = wrapped.relation.options

            val location = getLocationWithAlluxioReplace(
              conf,
              wrapped.relation,
              wrapped.partitionFilters,
              wrapped.dataFilters)

            val newRelation = HadoopFsRelation(
              location,
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
              // TODO: Does Databricks have coalesced bucketing implemented?
              None,
              wrapped.dataFilters,
              wrapped.tableIdentifier)(conf)
          }
        }),
      GpuOverrides.exec[SortMergeJoinExec](
        "Sort merge join, replacing with shuffled hash join",
        ExecChecks(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
        (join, conf, p, r) => new GpuSortMergeJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[BroadcastHashJoinExec](
        "Implementation of join using broadcast data",
        ExecChecks(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
        (join, conf, p, r) => new GpuBroadcastHashJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[ShuffledHashJoinExec](
        "Implementation of join using hashed shuffled data",
        ExecChecks(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
        (join, conf, p, r) => new GpuShuffledHashJoinMeta(join, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
  }

  override def getBuildSide(join: HashJoin): GpuBuildSide = {
    GpuJoinUtils.getGpuBuildSide(join.buildSide)
  }

  override def getBuildSide(join: BroadcastNestedLoopJoinExec): GpuBuildSide = {
    GpuJoinUtils.getGpuBuildSide(join.buildSide)
  }

  // Databricks has a different version of FileStatus
  override def getPartitionFileNames(
      partitions: Seq[PartitionDirectory]): Seq[String] = {
    val files = partitions.flatMap(partition => partition.files)
    files.map(_.getPath.getName)
  }

  override def getPartitionFileStatusSize(partitions: Seq[PartitionDirectory]): Long = {
    partitions.map(_.files.map(_.getLen).sum).sum
  }

  override def getPartitionedFiles(
      partitions: Array[PartitionDirectory]): Array[PartitionedFile] = {
    partitions.flatMap { p =>
      p.files.map { f =>
        PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values)
      }
    }
  }

  override def getPartitionSplitFiles(
      partitions: Array[PartitionDirectory],
      maxSplitBytes: Long,
      relation: HadoopFsRelation): Array[PartitionedFile] = {
    partitions.flatMap { partition =>
      partition.files.flatMap { file =>
        // getPath() is very expensive so we only want to call it once in this block:
        val filePath = file.getPath
        val isSplitable = relation.fileFormat.isSplitable(
          relation.sparkSession, relation.options, filePath)
        PartitionedFileUtil.splitFiles(
          sparkSession = relation.sparkSession,
          file = file,
          filePath = filePath,
          isSplitable = isSplitable,
          maxSplitBytes = maxSplitBytes,
          partitionValues = partition.values
        )
      }
    }
  }

  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: (PartitionedFile) => Iterator[InternalRow],
      filePartitions: Seq[FilePartition]): RDD[InternalRow] = {
    new GpuFileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def createFilePartition(index: Int, files: Array[PartitionedFile]): FilePartition = {
    FilePartition(index, files)
  }

  override def copyFileSourceScanExec(
      scanExec: GpuFileSourceScanExec,
      queryUsesInputFile: Boolean): GpuFileSourceScanExec = {
    scanExec.copy(queryUsesInputFile=queryUsesInputFile)(scanExec.rapidsConf)
  }

  override def getGpuShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan,
      cpuShuffle: Option[ShuffleExchangeExec]): GpuShuffleExchangeExecBase = {
    val canChangeNumPartitions = cpuShuffle.forall(_.canChangeNumPartitions)
    GpuShuffleExchangeExec(outputPartitioning, child, canChangeNumPartitions)
  }

  override def getGpuShuffleExchangeExec(
      queryStage: ShuffleQueryStageExec): GpuShuffleExchangeExecBase = {
    queryStage.shuffle.asInstanceOf[GpuShuffleExchangeExecBase]
  }

  // this function is copied from Spark300Shims
  override def getLocationWithAlluxioReplace(
      conf: RapidsConf,
      relation: HadoopFsRelation,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): FileIndex = {

    val alluxioFilesReplace: Option[Seq[String]] = conf.getAlluxioFilesReplace

    if (alluxioFilesReplace.isDefined) {
      // alluxioFilesReplace: Seq("key->value", "key1->value1")
      val replaceMap = new mutable.HashMap[String, String]()
      alluxioFilesReplace.get.foreach(rule => {
        val ruleSplit = rule.split("->")
        if (ruleSplit.size == 2) {
          replaceMap += ruleSplit(0).trim -> ruleSplit(1).trim
        } else {
          logWarning("Wrong set for spark.rapids.alluxio.pathsToReplace " + rule)
        }
      })

      if (replaceMap.size > 0) {
        val listFiles: Seq[PartitionDirectory] = relation.location.listFiles(
          partitionFilters, dataFilters)

        val replaceFunc = (f: Path) => {
          val matchedSet = replaceMap.keySet.filter(reg => f.toString.startsWith(reg))
          if (matchedSet.size > 0) {
            new Path(
              f.toString.replaceFirst(matchedSet.head, replaceMap(matchedSet.head)))
          } else {
            f
          }
        }

        val inputFiles: Seq[Path] = listFiles.flatMap(partitionDir => {
          partitionDir.files.map(f => replaceFunc(f.getPath))
        }).toSet.toSeq

        // get the leaf dir of inputFiles
        val leafDirs = inputFiles.map(_.getParent).toSet.toSeq

        val finalRootPaths = relation.location.rootPaths.map(replaceFunc)

        val partitionSpec = RapidsPartitioningUtils.inferPartitioning(
          relation.sparkSession,
          leafDirs,
          finalRootPaths.toSet,
          relation.options,
          Option(relation.dataSchema))

        new InMemoryFileIndex(
          relation.sparkSession,
          inputFiles,
          relation.options,
          Option(relation.dataSchema),
          userSpecifiedPartitionSpec = Some(partitionSpec))
      } else {
        relation.location
      }
    } else {
      relation.location
    }
  }
}
