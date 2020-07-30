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

package org.apache.spark.sql.rapids.shims.spark300

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.HashMap

import com.nvidia.spark.rapids._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, Expression, PlanExpression, Predicate, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.rapids.GpuFileSourceScanExecBase
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet

case class GpuFileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier])
    extends DataSourceScanExec with GpuFileSourceScanExecBase with GpuExec {

  override val nodeName: String = {
    s"GpuScan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  private[this] val wrapped: FileSourceScanExec = {
    val tclass = classOf[org.apache.spark.sql.execution.FileSourceScanExec]
    val constructors = tclass.getConstructors()
    if (constructors.size > 1) {
      throw new IllegalStateException(s"Only expected 1 constructor for FileSourceScanExec")
    }
    val constructor = constructors(0)
    val instance = if (constructor.getParameterCount() == 8) {
      // Some distributions of Spark modified FileSourceScanExec to take an additional parameter
      // that is the logicalRelation. We don't know what its used for exactly but haven't
      // run into any issues in testing using the one we create here.
      @transient val logicalRelation = LogicalRelation(relation)
      try {
        constructor.newInstance(relation, output, requiredSchema, partitionFilters,
          optionalBucketSet, dataFilters, tableIdentifier,
          logicalRelation).asInstanceOf[FileSourceScanExec]
      } catch {
        case il: IllegalArgumentException =>
          // TODO - workaround until https://github.com/NVIDIA/spark-rapids/issues/354
          constructor.newInstance(relation, output, requiredSchema, partitionFilters,
            optionalBucketSet, None, dataFilters, tableIdentifier).asInstanceOf[FileSourceScanExec]
      }
    } else {
      constructor.newInstance(relation, output, requiredSchema, partitionFilters,
        optionalBucketSet, dataFilters, tableIdentifier).asInstanceOf[FileSourceScanExec]
    }
    instance
  }

  override lazy val outputPartitioning: Partitioning  = wrapped.outputPartitioning

  override lazy val outputOrdering: Seq[SortOrder] = wrapped.outputOrdering

  override lazy val metadata: Map[String, String] = wrapped.metadata

  override lazy val metrics: Map[String, SQLMetric] = wrapped.metrics

  override def verboseStringWithOperatorId(): String = {
    val metadataStr = metadata.toSeq.sorted.filterNot {
      case (_, value) if (value.isEmpty || value.equals("[]")) => true
      case (key, _) if (key.equals("DataFilters") || key.equals("Format")) => true
      case (_, _) => false
    }.map {
      case (key, _) if (key.equals("Location")) =>
        val location = wrapped.relation.location
        val numPaths = location.rootPaths.length
        val abbreviatedLoaction = if (numPaths <= 1) {
          location.rootPaths.mkString("[", ", ", "]")
        } else {
          "[" + location.rootPaths.head + s", ... ${numPaths - 1} entries]"
        }
        s"$key: ${location.getClass.getSimpleName} ${redact(abbreviatedLoaction)}"
      case (key, value) => s"$key: ${redact(value)}"
    }

    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metadataStr.mkString("\n")}
       |""".stripMargin
  }

  val useSmallFiles = true

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    if (useSmallFiles) {
      inputRDD:: Nil
    } else {
      wrapped.inputRDD :: Nil
    }
  }

  private def toAttribute(colName: String): Option[Attribute] =
    output.find(_.name == colName)

  lazy val bucketedScan: Boolean = {
    if (relation.sparkSession.sessionState.conf.bucketingEnabled && relation.bucketSpec.isDefined) {
      val spec = relation.bucketSpec.get
      val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
      bucketColumns.size == spec.bucketColumnNames.size
    } else {
      false
    }
  }

  private lazy val driverMetrics: HashMap[String, Long] = HashMap.empty


  private def sendDriverMetrics(): Unit = {
    driverMetrics.foreach(e => metrics(e._1).add(e._2))
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      metrics.filter(e => driverMetrics.contains(e._1)).values.toSeq)
  }

  /** Helper for computing total number and size of files in selected partitions. */
  private def setFilesNumAndSizeMetric(
      partitions: Seq[PartitionDirectory],
      static: Boolean): Unit = {
    val filesNum = partitions.map(_.files.size.toLong).sum
    val filesSize = partitions.map(_.files.map(_.getLen).sum).sum
    if (!static || partitionFilters.filter(isDynamicPruningFilter).isEmpty) {
      driverMetrics("numFiles") = filesNum
      driverMetrics("filesSize") = filesSize
    } else {
      driverMetrics("staticFilesNum") = filesNum
      driverMetrics("staticFilesSize") = filesSize
    }
  }


  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined


  @transient lazy val selectedPartitions: Array[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret =
      relation.location.listFiles(
        partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)
    if (relation.partitionSchemaOption.isDefined) {
      driverMetrics("numPartitions") = ret.length
    }
    setFilesNumAndSizeMetric(ret, true)
    val timeTakenMs = NANOSECONDS.toMillis(
      (System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    driverMetrics("metadataTime") = timeTakenMs
    ret
  }.toArray


  // We can only determine the actual partitions at runtime when a dynamic partition filter is
  // present. This is because such a filter relies on information that is only available at run
  // time (for instance the keys used in the other side of a join).
  @transient private lazy val dynamicallySelectedPartitions: Array[PartitionDirectory] = {
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)

    if (dynamicPartitionFilters.nonEmpty) {
      val startTime = System.nanoTime()
      // call the file index for the files matching all filters except dynamic partition filters
      val predicate = dynamicPartitionFilters.reduce(And)
      val partitionColumns = relation.partitionSchema
      val boundPredicate = Predicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      }, Nil)
      val ret = selectedPartitions.filter(p => boundPredicate.eval(p.values))
      setFilesNumAndSizeMetric(ret, false)
      val timeTakenMs = (System.nanoTime() - startTime) / 1000 / 1000
      driverMetrics("pruningTime") = timeTakenMs
      ret
    } else {
      selectedPartitions
    }
  }

  @transient
  private lazy val pushedDownFilters = {
    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    dataFilters.flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }


  lazy val inputRDD: RDD[InternalRow] = {
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = relation.options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    val readRDD = if (bucketedScan) {
      throw new Exception("haven't implemented bucketting yet")
    } else {
      createNonBucketedReadRDD(readFile, dynamicallySelectedPartitions, relation)
    }
    sendDriverMetrics()
    readRDD
  }

  private def createNonBucketedReadRDD(
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Array[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes =
      FilePartition.maxSplitBytes(fsRelation.sparkSession, selectedPartitions)
    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    val splitFiles = selectedPartitions.flatMap { partition =>
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
    }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions =
      FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)

    val sparkSession = relation.sparkSession
    val sqlConf = sparkSession.sessionState.conf
    val hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val factory = GpuParquetMultiPartitionReaderFactory(
      sqlConf,
      broadcastedHadoopConf,
      relation.dataSchema,
      requiredSchema,
      relation.partitionSchema,
      pushedDownFilters.toArray,
      new RapidsConf(sqlConf),
      PartitionReaderIterator.buildScanMetrics(sparkSession.sparkContext))

    new DataSourceRDD(sparkSession.sparkContext, partitions, factory, supportsColumnar)
    // new FileScanRDD(fsRelation.sparkSession, readFile, partitions)
  }


  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          // The `FileScanRDD` returns an iterator which scans the file during the `hasNext` call.
          val startNs = System.nanoTime()
          val res = batches.hasNext
          scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
          res
        }

        override def next(): ColumnarBatch = {
          val batch = batches.next()
          numOutputRows += batch.numRows()
          batch
        }
      }
    }
  }

  override val nodeNamePrefix: String = "Gpu" + wrapped.nodeNamePrefix

  override def doCanonicalize(): GpuFileSourceScanExec = {
    val canonical = wrapped.doCanonicalize()
    GpuFileSourceScanExec(
      canonical.relation,
      canonical.output,
      canonical.requiredSchema,
      canonical.partitionFilters,
      canonical.optionalBucketSet,
      canonical.dataFilters,
      canonical.tableIdentifier)
  }
}

object GpuFileSourceScanExec {
  def tagSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    meta.wrapped.relation.fileFormat match {
      case _: CSVFileFormat => GpuReadCSVFileFormat.tagSupport(meta)
      case _: OrcFileFormat => GpuReadOrcFileFormat.tagSupport(meta)
      case _: ParquetFileFormat => GpuReadParquetFileFormat.tagSupport(meta)
      case f =>
        meta.willNotWorkOnGpu(s"unsupported file format: ${f.getClass.getCanonicalName}")
    }
  }

  def convertFileFormat(format: FileFormat): FileFormat = {
    format match {
      case _: CSVFileFormat => new GpuReadCSVFileFormat
      case _: OrcFileFormat => new GpuReadOrcFileFormat
      case _: ParquetFileFormat => new GpuReadParquetFileFormat
      case f =>
        throw new IllegalArgumentException(s"${f.getClass.getCanonicalName} is not supported")
    }
  }
}
