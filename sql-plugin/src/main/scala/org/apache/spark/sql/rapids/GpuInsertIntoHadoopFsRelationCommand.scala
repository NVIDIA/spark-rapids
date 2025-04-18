/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.io.IOException

import com.nvidia.spark.rapids.{ColumnarFileFormat, GpuDataWritingCommand, RapidsConf}
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.getPartitionPathString
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableDropPartitionCommand, CommandUtils}
import org.apache.spark.sql.execution.datasources.{FileFormatWriter, FileIndex, PartitioningUtils}
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.rapids.shims.{RapidsErrorUtils, SchemaUtilsShims}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuInsertIntoHadoopFsRelationCommand(
    outputPath: Path,
    staticPartitions: TablePartitionSpec,
    ifPartitionNotExists: Boolean,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    fileFormat: ColumnarFileFormat,
    options: Map[String, String],
    query: LogicalPlan,
    mode: SaveMode,
    catalogTable: Option[CatalogTable],
    fileIndex: Option[FileIndex],
    outputColumnNames: Seq[String],
    useStableSort: Boolean,
    concurrentWriterPartitionFlushSize: Long,
    baseDebugOutputPath: Option[String])
  extends GpuDataWritingCommand {

  override def runColumnar(sparkSession: SparkSession, child: SparkPlan): Seq[ColumnarBatch] = {
    // Most formats don't do well with duplicate columns, so lets not allow that
    SchemaUtilsShims.checkColumnNameDuplication(
      outputColumnNames,
      s"when inserting into $outputPath",
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    val parameters = CaseInsensitiveMap(options)

    val partitionOverwriteMode = parameters.get("partitionOverwriteMode")
      // scalastyle:off caselocale
      .map(mode => PartitionOverwriteMode.withName(mode.toUpperCase))
      // scalastyle:on caselocale
      .getOrElse(sparkSession.sessionState.conf.partitionOverwriteMode)


    val enableDynamicOverwrite = partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC
    // This config only makes sense when we are overwriting a partitioned dataset with dynamic
    // partition columns.
    val dynamicPartitionOverwrite = enableDynamicOverwrite && mode == SaveMode.Overwrite &&
      staticPartitions.size < partitionColumns.length

    val jobId = java.util.UUID.randomUUID().toString

    // For dynamic partition overwrite, FileOutputCommitter's output path is staging path, files
    // will be renamed from staging path to final output path during commit job
    val committerOutputPath = if (dynamicPartitionOverwrite) {
      FileCommitProtocol.getStagingDir(outputPath.toString, jobId)
        .makeQualified(fs.getUri, fs.getWorkingDirectory)
    } else {
      qualifiedOutputPath
    }

    val partitionsTrackedByCatalog = sparkSession.sessionState.conf.manageFilesourcePartitions &&
      catalogTable.isDefined &&
      catalogTable.get.partitionColumnNames.nonEmpty &&
      catalogTable.get.tracksPartitionsInCatalog

    var initialMatchingPartitions: Seq[TablePartitionSpec] = Nil
    var customPartitionLocations: Map[TablePartitionSpec, String] = Map.empty
    var matchingPartitions: Seq[CatalogTablePartition] = Seq.empty

    // When partitions are tracked by the catalog, compute all custom partition locations that
    // may be relevant to the insertion job.
    if (partitionsTrackedByCatalog) {
      matchingPartitions = sparkSession.sessionState.catalog.listPartitions(
        catalogTable.get.identifier, Some(staticPartitions))
      initialMatchingPartitions = matchingPartitions.map(_.spec)
      customPartitionLocations = getCustomPartitionLocations(
        fs, catalogTable.get, qualifiedOutputPath, matchingPartitions)
    }

    val committer = FileCommitProtocol.instantiate(
      sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = jobId,
      outputPath = outputPath.toString,
      dynamicPartitionOverwrite = dynamicPartitionOverwrite)

    val doInsertion = if (mode == SaveMode.Append) {
      true
    } else {
      val pathExists = fs.exists(qualifiedOutputPath)
      (mode, pathExists) match {
        case (SaveMode.ErrorIfExists, true) =>
          throw RapidsErrorUtils.outputPathAlreadyExistsError(qualifiedOutputPath)
        case (SaveMode.Overwrite, true) =>
          if (ifPartitionNotExists && matchingPartitions.nonEmpty) {
            false
          } else if (dynamicPartitionOverwrite) {
            // For dynamic partition overwrite, do not delete partition directories ahead.
            true
          } else {
            deleteMatchingPartitions(fs, qualifiedOutputPath, customPartitionLocations, committer)
            true
          }
        case (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
          true
        case (SaveMode.Ignore, exists) =>
          !exists
        case (s, exists) =>
          throw new IllegalStateException(s"unsupported save mode $s ($exists)")
      }
    }

    if (doInsertion) {

      def refreshUpdatedPartitions(updatedPartitionPaths: Set[String]): Unit = {
        val updatedPartitions = updatedPartitionPaths.map(PartitioningUtils.parsePathFragment)
        if (partitionsTrackedByCatalog) {
          val newPartitions = updatedPartitions -- initialMatchingPartitions
          if (newPartitions.nonEmpty) {
            AlterTableAddPartitionCommand(
              catalogTable.get.identifier, newPartitions.toSeq.map(p => (p, None)),
              ifNotExists = true).run(sparkSession)
          }
          // For dynamic partition overwrite, we never remove partitions but only update existing
          // ones.
          if (mode == SaveMode.Overwrite && !dynamicPartitionOverwrite) {
            val deletedPartitions = initialMatchingPartitions.toSet -- updatedPartitions
            if (deletedPartitions.nonEmpty) {
              AlterTableDropPartitionCommand(
                catalogTable.get.identifier, deletedPartitions.toSeq,
                ifExists = true, purge = false,
                retainData = true /* already deleted */).run(sparkSession)
            }
          }
        }
      }
      val forceHiveHashForBucketing =
        RapidsConf.FORCE_HIVE_HASH_FOR_BUCKETED_WRITE.get(sparkSession.sessionState.conf)

      val updatedPartitionPaths =
        GpuFileFormatWriter.write(
          sparkSession = sparkSession,
          plan = child,
          fileFormat = fileFormat,
          committer = committer,
          outputSpec = FileFormatWriter.OutputSpec(
            committerOutputPath.toString, customPartitionLocations, outputColumns),
          hadoopConf = hadoopConf,
          partitionColumns = partitionColumns,
          bucketSpec = bucketSpec,
          statsTrackers = Seq(gpuWriteJobStatsTracker(hadoopConf)),
          options = options,
          useStableSort = useStableSort,
          concurrentWriterPartitionFlushSize = concurrentWriterPartitionFlushSize,
          forceHiveHashForBucketing = forceHiveHashForBucketing,
          numStaticPartitionCols = staticPartitions.size,
          baseDebugOutputPath = baseDebugOutputPath)


      // update metastore partition metadata
      if (updatedPartitionPaths.isEmpty && staticPartitions.nonEmpty
        && partitionColumns.length == staticPartitions.size) {
        // Avoid empty static partition can't loaded to datasource table.
        val staticPathFragment =
          PartitioningUtils.getPathFragment(staticPartitions, partitionColumns)
        refreshUpdatedPartitions(Set(staticPathFragment))
      } else {
        refreshUpdatedPartitions(updatedPartitionPaths)
      }

      // refresh cached files in FileIndex
      fileIndex.foreach(_.refresh())
      // refresh data cache if table is cached
      sparkSession.catalog.refreshByPath(outputPath.toString)

      if (catalogTable.nonEmpty) {
        CommandUtils.updateTableStats(sparkSession, catalogTable.get)
      }

    } else {
      logInfo("Skipping insertion into a relation that already exists.")
    }

    Seq.empty[ColumnarBatch]
  }

  /**
   * Deletes all partition files that match the specified static prefix. Partitions with custom
   * locations are also cleared based on the custom locations map given to this class.
   */
  private def deleteMatchingPartitions(
      fs: FileSystem,
      qualifiedOutputPath: Path,
      customPartitionLocations: Map[TablePartitionSpec, String],
      committer: FileCommitProtocol): Unit = {
    val staticPartitionPrefix = if (staticPartitions.nonEmpty) {
      "/" + partitionColumns.flatMap { p =>
        staticPartitions.get(p.name).map(getPartitionPathString(p.name, _))
      }.mkString("/")
    } else {
      ""
    }
    // first clear the path determined by the static partition keys (e.g. /table/foo=1)
    val staticPrefixPath = qualifiedOutputPath.suffix(staticPartitionPrefix)
    if (fs.exists(staticPrefixPath) && !committer.deleteWithJob(fs, staticPrefixPath, true)) {
      throw new IOException(s"Unable to clear output " +
        s"directory $staticPrefixPath prior to writing to it")
    }
    // now clear all custom partition locations (e.g. /custom/dir/where/foo=2/bar=4)
    for ((spec, customLoc) <- customPartitionLocations) {
      assert(
        (staticPartitions.toSet -- spec).isEmpty,
        "Custom partition location did not match static partitioning keys")
      val path = new Path(customLoc)
      if (fs.exists(path) && !committer.deleteWithJob(fs, path, true)) {
        throw new IOException(s"Unable to clear partition " +
          s"directory $path prior to writing to it")
      }
    }
  }

  /**
   * Given a set of input partitions, returns those that have locations that differ from the
   * Hive default (e.g. /k1=v1/k2=v2). These partitions were manually assigned locations by
   * the user.
   *
   * @return a mapping from partition specs to their custom locations
   */
  private def getCustomPartitionLocations(
      fs: FileSystem,
      table: CatalogTable,
      qualifiedOutputPath: Path,
      partitions: Seq[CatalogTablePartition]): Map[TablePartitionSpec, String] = {
    partitions.flatMap { p =>
      val defaultLocation = qualifiedOutputPath.suffix(
        "/" + PartitioningUtils.getPathFragment(p.spec, table.partitionSchema)).toString
      val catalogLocation = new Path(p.location).makeQualified(
        fs.getUri, fs.getWorkingDirectory).toString
      if (catalogLocation != defaultLocation) {
        Some(p.spec -> catalogLocation)
      } else {
        None
      }
    }.toMap
  }

  private val isPartitioned = partitionColumns.nonEmpty

  private val isBucketed = bucketSpec.nonEmpty

  private val needSort = isPartitioned || isBucketed

  // If need sort and use stable sort, require single batch.
  // If need sort and not use stable sort, use out-of-core sort which not requires single batch.
  override def requireSingleBatch: Boolean = needSort && useStableSort
}
