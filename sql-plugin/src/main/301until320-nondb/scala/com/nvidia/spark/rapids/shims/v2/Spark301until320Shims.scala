/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.v2

import java.net.URI
import java.nio.ByteBuffer

import scala.collection.mutable.ListBuffer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuOverrides.exec
import org.apache.arrow.memory.ReferenceManager
import org.apache.arrow.vector.ValueVector
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils}
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, CustomShuffleReaderExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.command.{AlterTableRecoverPartitionsCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.rapids.GpuPartitioningUtils
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.execution.{GpuCustomShuffleReaderExec, SerializeBatchDeserializeHostBuffer, SerializeConcatHostBuffersDeserializeBatch}
import org.apache.spark.sql.rapids.shims.v2.GpuSchemaUtils
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._

/**
* Shim base class that can be compiled with from 301 until 320
*/
trait Spark301until320Shims extends SparkShims {
  override def parquetRebaseReadKey: String =
    SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ.key
  override def parquetRebaseWriteKey: String =
    SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE.key
  override def avroRebaseReadKey: String =
    SQLConf.LEGACY_AVRO_REBASE_MODE_IN_READ.key
  override def avroRebaseWriteKey: String =
    SQLConf.LEGACY_AVRO_REBASE_MODE_IN_WRITE.key
  override def parquetRebaseRead(conf: SQLConf): String =
    conf.getConf(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ)
  override def parquetRebaseWrite(conf: SQLConf): String =
    conf.getConf(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE)

  override def sessionFromPlan(plan: SparkPlan): SparkSession = {
    plan.sqlContext.sparkSession
  }

  override def filesFromFileIndex(fileIndex: PartitioningAwareFileIndex): Seq[FileStatus] = {
    fileIndex.allFiles()
  }

  def broadcastModeTransform(mode: BroadcastMode, rows: Array[InternalRow]): Any =
    mode.transform(rows)

  override def newBroadcastQueryStageExec(
      old: BroadcastQueryStageExec,
      newPlan: SparkPlan): BroadcastQueryStageExec = BroadcastQueryStageExec(old.id, newPlan)

  override def getDateFormatter(): DateFormatter = {
    DateFormatter(DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone))
  }

  override def isExchangeOp(plan: SparkPlanMeta[_]): Boolean = {
    // if the child query stage already executed on GPU then we need to keep the
    // next operator on GPU in these cases
    SQLConf.get.adaptiveExecutionEnabled && (plan.wrapped match {
      case _: CustomShuffleReaderExec
           | _: ShuffledHashJoinExec
           | _: BroadcastHashJoinExec
           | _: BroadcastExchangeExec
           | _: BroadcastNestedLoopJoinExec => true
      case _ => false
    })
  }

  override def isAqePlan(p: SparkPlan): Boolean = p match {
    case _: AdaptiveSparkPlanExec |
         _: QueryStageExec |
         _: CustomShuffleReaderExec => true
    case _ => false
  }

  override def isCustomReaderExec(x: SparkPlan): Boolean = x match {
    case _: GpuCustomShuffleReaderExec | _: CustomShuffleReaderExec => true
    case _ => false
  }

  override def aqeShuffleReaderExec: ExecRule[_ <: SparkPlan] = exec[CustomShuffleReaderExec](
    "A wrapper of shuffle query stage",
    ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.ARRAY +
        TypeSig.STRUCT + TypeSig.MAP).nested(), TypeSig.all),
    (exec, conf, p, r) => new GpuCustomShuffleReaderMeta(exec, conf, p, r))

  override def findOperators(plan: SparkPlan, predicate: SparkPlan => Boolean): Seq[SparkPlan] = {
    def recurse(
        plan: SparkPlan,
        predicate: SparkPlan => Boolean,
        accum: ListBuffer[SparkPlan]): Seq[SparkPlan] = {
      if (predicate(plan)) {
        accum += plan
      }
      plan match {
        case a: AdaptiveSparkPlanExec => recurse(a.executedPlan, predicate, accum)
        case qs: BroadcastQueryStageExec => recurse(qs.broadcast, predicate, accum)
        case qs: ShuffleQueryStageExec => recurse(qs.shuffle, predicate, accum)
        case other => other.children.flatMap(p => recurse(p, predicate, accum)).headOption
      }
      accum
    }
    recurse(plan, predicate, new ListBuffer[SparkPlan]())
  }

  override def skipAssertIsOnTheGpu(plan: SparkPlan): Boolean = false

  override def shouldFailDivOverflow(): Boolean = false

  override def leafNodeDefaultParallelism(ss: SparkSession): Int = {
    ss.sparkContext.defaultParallelism
  }

  override def shouldFallbackOnAnsiTimestamp(): Boolean = false

  override def getLegacyStatisticalAggregate(): Boolean = true

  override def v1RepairTableCommand(tableName: TableIdentifier): RunnableCommand =
    AlterTableRecoverPartitionsCommand(tableName)

  override def isWindowFunctionExec(plan: SparkPlan): Boolean = plan.isInstanceOf[WindowExecBase]

  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Seq(
    GpuOverrides.scan[ParquetScan](
      "Parquet parsing",
      (a, conf, p, r) => new RapidsParquetScanMeta(a, conf, p, r)),
    GpuOverrides.scan[OrcScan](
      "ORC parsing",
      (a, conf, p, r) => new RapidsOrcScanMeta(a, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap

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
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference]): RDD[InternalRow] = {
    new FileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def createFilePartition(index: Int, files: Array[PartitionedFile]): FilePartition = {
    FilePartition(index, files)
  }

  override def copyBatchScanExec(
      batchScanExec: GpuBatchScanExec,
      queryUsesInputFile: Boolean): GpuBatchScanExec = {
    val scanCopy = batchScanExec.scan match {
      case parquetScan: GpuParquetScan =>
        parquetScan.copy(queryUsesInputFile = queryUsesInputFile)
      case orcScan: GpuOrcScan =>
        orcScan.copy(queryUsesInputFile = queryUsesInputFile)
      case _ => throw new RuntimeException("Wrong format") // never reach here
    }
    batchScanExec.copy(scan = scanCopy)
  }

  override def copyFileSourceScanExec(
      scanExec: GpuFileSourceScanExec,
      queryUsesInputFile: Boolean): GpuFileSourceScanExec = {
    scanExec.copy(queryUsesInputFile = queryUsesInputFile)(scanExec.rapidsConf)
  }

  override def checkColumnNameDuplication(
      schema: StructType,
      colType: String,
      resolver: Resolver): Unit = {
    GpuSchemaUtils.checkColumnNameDuplication(schema, colType, resolver)
  }

  override def alias(child: Expression, name: String)(
      exprId: ExprId,
      qualifier: Seq[String],
      explicitMetadata: Option[Metadata]): Alias = {
    Alias(child, name)(exprId, qualifier, explicitMetadata)
  }

  override def getArrowValidityBuf(vec: ValueVector): (ByteBuffer, ReferenceManager) = {
    val arrowBuf = vec.getValidityBuffer
    (arrowBuf.nioBuffer(), arrowBuf.getReferenceManager)
  }

  override def getArrowOffsetsBuf(vec: ValueVector): (ByteBuffer, ReferenceManager) = {
    val arrowBuf = vec.getOffsetBuffer
    (arrowBuf.nioBuffer(), arrowBuf.getReferenceManager)
  }

  override def replaceWithAlluxioPathIfNeeded(
      conf: RapidsConf,
      relation: HadoopFsRelation,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): FileIndex = {

    val alluxioPathsReplace: Option[Seq[String]] = conf.getAlluxioPathsToReplace

    if (alluxioPathsReplace.isDefined) {
      // alluxioPathsReplace: Seq("key->value", "key1->value1")
      // turn the rules to the Map with eg
      // { s3:/foo -> alluxio://0.1.2.3:19998/foo,
      //   gs:/bar -> alluxio://0.1.2.3:19998/bar,
      //   /baz -> alluxio://0.1.2.3:19998/baz }
      val replaceMapOption = alluxioPathsReplace.map(rules => {
        rules.map(rule => {
          val split = rule.split("->")
          if (split.size == 2) {
            split(0).trim -> split(1).trim
          } else {
            throw new IllegalArgumentException(s"Invalid setting for " +
              s"${RapidsConf.ALLUXIO_PATHS_REPLACE.key}")
          }
        }).toMap
      })

      replaceMapOption.map(replaceMap => {

        def isDynamicPruningFilter(e: Expression): Boolean =
          e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

        val partitionDirs = relation.location.listFiles(
          partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)

        // replacement func to check if the file path is prefixed with the string user configured
        // if yes, replace it
        val replaceFunc = (f: Path) => {
          val pathStr = f.toString
          val matchedSet = replaceMap.keySet.filter(reg => pathStr.startsWith(reg))
          if (matchedSet.size > 1) {
            // never reach here since replaceMap is a Map
            throw new IllegalArgumentException(s"Found ${matchedSet.size} same replacing rules " +
              s"from ${RapidsConf.ALLUXIO_PATHS_REPLACE.key} which requires only 1 rule for each " +
              s"file path")
          } else if (matchedSet.size == 1) {
            new Path(pathStr.replaceFirst(matchedSet.head, replaceMap(matchedSet.head)))
          } else {
            f
          }
        }

        // replace all of input files
        val inputFiles: Seq[Path] = partitionDirs.flatMap(partitionDir => {
          replacePartitionDirectoryFiles(partitionDir, replaceFunc)
        })

        // replace all of rootPaths which are already unique
        val rootPaths = relation.location.rootPaths.map(replaceFunc)

        val parameters: Map[String, String] = relation.options

        // infer PartitionSpec
        val partitionSpec = GpuPartitioningUtils.inferPartitioning(
          relation.sparkSession,
          rootPaths,
          inputFiles,
          parameters,
          Option(relation.dataSchema),
          replaceFunc)

        // generate a new InMemoryFileIndex holding paths with alluxio schema
        new InMemoryFileIndex(
          relation.sparkSession,
          inputFiles,
          parameters,
          Option(relation.dataSchema),
          userSpecifiedPartitionSpec = Some(partitionSpec))
      }).getOrElse(relation.location)

    } else {
      relation.location
    }
  }

  override def replacePartitionDirectoryFiles(partitionDir: PartitionDirectory,
      replaceFunc: Path => Path): Seq[Path] = {
    partitionDir.files.map(f => replaceFunc(f.getPath))
  }

  override def hasAliasQuoteFix: Boolean = false

  override def registerKryoClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[SerializeConcatHostBuffersDeserializeBatch],
      new KryoJavaSerializer())
    kryo.register(classOf[SerializeBatchDeserializeHostBuffer],
      new KryoJavaSerializer())
  }

  override def reusedExchangeExecPfn: PartialFunction[SparkPlan, ReusedExchangeExec] = {
    case ShuffleQueryStageExec(_, e: ReusedExchangeExec) => e
    case BroadcastQueryStageExec(_, e: ReusedExchangeExec) => e
  }

  override def createTable(table: CatalogTable,
      sessionCatalog: SessionCatalog,
      tableLocation: Option[URI],
      result: BaseRelation) = {
    val newTable = table.copy(
      storage = table.storage.copy(locationUri = tableLocation),
      // We will use the schema of resolved.relation as the schema of the table (instead of
      // the schema of df). It is important since the nullability may be changed by the relation
      // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
      schema = result.schema)
    // Table location is already validated. No need to check it again during table creation.
    sessionCatalog.createTable(newTable, ignoreIfExists = false, validateLocation = false)
  }
}
