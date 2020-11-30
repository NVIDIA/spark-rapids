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

package org.apache.spark.sql.rapids

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.HashMap

import com.nvidia.spark.rapids.{GpuDataSourceRDD, GpuExec, GpuMetricNames, GpuParquetMultiFilePartitionReaderFactory, GpuReadCSVFileFormat, GpuReadFileFormatWithMetrics, GpuReadOrcFileFormat, GpuReadParquetFileFormat, RapidsConf, ShimLoader, SparkPlanMeta}
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{And, Ascending, Attribute, AttributeReference, BoundReference, DynamicPruningExpression, Expression, Literal, PlanExpression, Predicate, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{ExecSubqueryExpression, ExplainUtils, FileSourceScanExec, SQLExecution}
import org.apache.spark.sql.execution.datasources.{BucketingUtils, DataSourceStrategy, DataSourceUtils, FileFormat, FilePartition, HadoopFsRelation, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet

/**
 * GPU version of Spark's `FileSourceScanExec`
 *
 * @param relation The file-based relation to scan.
 * @param output Output attributes of the scan, including data attributes and partition attributes.
 * @param requiredSchema Required schema of the underlying relation, excluding partition columns.
 * @param partitionFilters Predicates to use for partition pruning.
 * @param optionalBucketSet Bucket ids for bucket pruning.
 * @param optionalNumCoalescedBuckets Number of coalesced buckets.
 * @param dataFilters Filters on non-partition columns.
 * @param tableIdentifier identifier for the table in the metastore.
 * @param rapidsConf Rapids conf
 * @param queryUsesInputFile This is a parameter to easily allow turning it
 *                               off in GpuTransitionOverrides if InputFileName,
 *                               InputFileBlockStart, or InputFileBlockLength are used
 */
case class GpuFileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    @transient rapidsConf: RapidsConf,
    queryUsesInputFile: Boolean = false)
    extends GpuDataSourceScanExec with GpuExec {

  private val isParquetFileFormat: Boolean = relation.fileFormat.isInstanceOf[ParquetFileFormat]
  private val isPerFileReadEnabled = rapidsConf.isParquetPerFileReadEnabled || !isParquetFileFormat

  override val nodeName: String = {
    s"GpuScan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  private lazy val driverMetrics: HashMap[String, Long] = HashMap.empty

  /**
   * Send the driver-side metrics. Before calling this function, selectedPartitions has
   * been initialized. See SPARK-26327 for more details.
   */
  private def sendDriverMetrics(): Unit = {
    driverMetrics.foreach(e => metrics(e._1).add(e._2))
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      metrics.filter(e => driverMetrics.contains(e._1)).values.toSeq)
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

  /**
   * [[partitionFilters]] can contain subqueries whose results are available only at runtime so
   * accessing [[selectedPartitions]] should be guarded by this method during planning
   */
  private def hasPartitionsAvailableAtRunTime: Boolean = {
    partitionFilters.exists(ExecSubqueryExpression.hasSubquery)
  }

  private def toAttribute(colName: String): Option[Attribute] =
    output.find(_.name == colName)

  // exposed for testing
  lazy val bucketedScan: Boolean = {
    if (relation.sparkSession.sessionState.conf.bucketingEnabled && relation.bucketSpec.isDefined) {
      val spec = relation.bucketSpec.get
      val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
      bucketColumns.size == spec.bucketColumnNames.size
    } else {
      false
    }
  }

  override lazy val (outputPartitioning, outputOrdering): (Partitioning, Seq[SortOrder]) = {
    if (bucketedScan) {
      // For bucketed columns:
      // -----------------------
      // `HashPartitioning` would be used only when:
      // 1. ALL the bucketing columns are being read from the table
      //
      // For sorted columns:
      // ---------------------
      // Sort ordering should be used when ALL these criteria's match:
      // 1. `HashPartitioning` is being used
      // 2. A prefix (or all) of the sort columns are being read from the table.
      //
      // Sort ordering would be over the prefix subset of `sort columns` being read
      // from the table.
      // eg.
      // Assume (col0, col2, col3) are the columns read from the table
      // If sort columns are (col0, col1), then sort ordering would be considered as (col0)
      // If sort columns are (col1, col0), then sort ordering would be empty as per rule #2
      // above
      val spec = relation.bucketSpec.get
      val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
      val numPartitions = optionalNumCoalescedBuckets.getOrElse(spec.numBuckets)
      val partitioning = HashPartitioning(bucketColumns, numPartitions)
      val sortColumns =
        spec.sortColumnNames.map(x => toAttribute(x)).takeWhile(x => x.isDefined).map(_.get)
      val shouldCalculateSortOrder =
        conf.getConf(SQLConf.LEGACY_BUCKETED_TABLE_SCAN_OUTPUT_ORDERING) &&
          sortColumns.nonEmpty &&
          !hasPartitionsAvailableAtRunTime

      val sortOrder = if (shouldCalculateSortOrder) {
        // In case of bucketing, its possible to have multiple files belonging to the
        // same bucket in a given relation. Each of these files are locally sorted
        // but those files combined together are not globally sorted. Given that,
        // the RDD partition will not be sorted even if the relation has sort columns set
        // Current solution is to check if all the buckets have a single file in it

        val filesPartNames = ShimLoader.getSparkShims.getPartitionFileNames(selectedPartitions)
        val bucketToFilesGrouping =
          filesPartNames.groupBy(file => BucketingUtils.getBucketId(file))
        val singleFilePartitions = bucketToFilesGrouping.forall(p => p._2.length <= 1)

        // TODO SPARK-24528 Sort order is currently ignored if buckets are coalesced.
        if (singleFilePartitions && optionalNumCoalescedBuckets.isEmpty) {
          // TODO Currently Spark does not support writing columns sorting in descending order
          // so using Ascending order. This can be fixed in future
          sortColumns.map(attribute => SortOrder(attribute, Ascending))
        } else {
          Nil
        }
      } else {
        Nil
      }
      (partitioning, sortOrder)
    } else {
      (UnknownPartitioning(0), Nil)
    }
  }

  @transient
  private lazy val pushedDownFilters = {
    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    dataFilters.flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }

  override lazy val metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")
    val location = relation.location
    val locationDesc =
      location.getClass.getSimpleName +
        GpuDataSourceScanExec.buildLocationMetadata(location.rootPaths, maxMetadataValueLength)
    val metadata =
      Map(
        "Format" -> relation.fileFormat.toString,
        "ReadSchema" -> requiredSchema.catalogString,
        "Batched" -> supportsColumnar.toString,
        "PartitionFilters" -> seqToString(partitionFilters),
        "PushedFilters" -> seqToString(pushedDownFilters),
        "DataFilters" -> seqToString(dataFilters),
        "Location" -> locationDesc)

    val withSelectedBucketsCount = relation.bucketSpec.map { spec =>
      val numSelectedBuckets = optionalBucketSet.map { b =>
        b.cardinality()
      } getOrElse {
        spec.numBuckets
      }
      metadata + ("SelectedBucketsCount" ->
        (s"$numSelectedBuckets out of ${spec.numBuckets}" +
          optionalNumCoalescedBuckets.map { b => s" (Coalesced to $b)"}.getOrElse("")))
    } getOrElse {
      metadata
    }

    withSelectedBucketsCount
  }

  override def verboseStringWithOperatorId(): String = {
    val metadataStr = metadata.toSeq.sorted.filterNot {
      case (_, value) if (value.isEmpty || value.equals("[]")) => true
      case (key, _) if (key.equals("DataFilters") || key.equals("Format")) => true
      case (_, _) => false
    }.map {
      case (key, _) if (key.equals("Location")) =>
        val location = relation.location
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

  /**
   * If the small file optimization is enabled then we read all the files before sending down
   * to the GPU. If it is disabled then we use the standard Spark logic of reading one file
   * at a time.
   */
  lazy val inputRDD: RDD[InternalRow] = {
    val readFile: Option[(PartitionedFile) => Iterator[InternalRow]] =
      if (isPerFileReadEnabled) {
        val fileFormat = relation.fileFormat.asInstanceOf[GpuReadFileFormatWithMetrics]
        val reader = fileFormat.buildReaderWithPartitionValuesAndMetrics(
          sparkSession = relation.sparkSession,
          dataSchema = relation.dataSchema,
          partitionSchema = relation.partitionSchema,
          requiredSchema = requiredSchema,
          filters = pushedDownFilters,
          options = relation.options,
          hadoopConf =
            relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options),
          metrics = metrics)
        Some(reader)
      } else {
        None
      }

    val readRDD = if (bucketedScan) {
      createBucketedReadRDD(relation.bucketSpec.get, readFile, dynamicallySelectedPartitions,
        relation)
    } else {
      createNonBucketedReadRDD(readFile, dynamicallySelectedPartitions,
        relation)
    }
    sendDriverMetrics()
    readRDD
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  /** SQL metrics generated only for scans using dynamic partition pruning. */
  private lazy val staticMetrics = if (partitionFilters.filter(isDynamicPruningFilter).nonEmpty) {
    Map("staticFilesNum" -> SQLMetrics.createMetric(sparkContext, "static number of files read"),
      "staticFilesSize" -> SQLMetrics.createSizeMetric(sparkContext, "static size of files read"))
  } else {
    Map.empty[String, SQLMetric]
  }

  /** Helper for computing total number and size of files in selected partitions. */
  private def setFilesNumAndSizeMetric(
      partitions: Seq[PartitionDirectory],
      static: Boolean): Unit = {
    val filesNum = partitions.map(_.files.size.toLong).sum
    val filesSize = ShimLoader.getSparkShims.getPartitionFileStatusSize(partitions)
    if (!static || partitionFilters.filter(isDynamicPruningFilter).isEmpty) {
      driverMetrics("numFiles") = filesNum
      driverMetrics("filesSize") = filesSize
    } else {
      driverMetrics("staticFilesNum") = filesNum
      driverMetrics("staticFilesSize") = filesSize
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
    "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
    "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "size of files read"),
    "pruningTime" ->
      SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time")
  ) ++ {
    // Tracking scan time has overhead, we can't afford to do it for each row, and can only do
    // it for each batch.
    if (supportsColumnar) {
      Some("scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))
    } else {
      None
    }
  } ++ {
    if (relation.partitionSchemaOption.isDefined) {
      Some("numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"))
    } else {
      None
    }
  } ++ staticMetrics ++ GpuMetricNames.buildGpuScanMetrics(sparkContext)

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

  override val nodeNamePrefix: String = "GpuFile"

  /**
   * Create an RDD for bucketed reads.
   * The non-bucketed variant of this function is [[createNonBucketedReadRDD]].
   *
   * The algorithm is pretty simple: each RDD partition being returned should include all the files
   * with the same bucket id from all the given Hive partitions.
   *
   * @param bucketSpec the bucketing spec.
   * @param readFile an optional function to read each (part of a) file. Used
   *                 when not using the small file optimization.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createBucketedReadRDD(
      bucketSpec: BucketSpec,
      readFile: Option[(PartitionedFile) => Iterator[InternalRow]],
      selectedPartitions: Array[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")

    val partitionedFiles =
      ShimLoader.getSparkShims.getPartitionedFiles(selectedPartitions)

    val filesGroupedToBuckets = partitionedFiles.groupBy { f =>
      BucketingUtils
        .getBucketId(new Path(f.filePath).getName)
        .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
    }

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter {
        f => bucketSet.get(f._1)
      }
    } else {
      filesGroupedToBuckets
    }

    val filePartitions = optionalNumCoalescedBuckets.map { numCoalescedBuckets =>
      logInfo(s"Coalescing to ${numCoalescedBuckets} buckets")
      val coalescedBuckets = prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
      Seq.tabulate(numCoalescedBuckets) { bucketId =>
        val partitionedFiles = coalescedBuckets.get(bucketId).map {
          _.values.flatten.toArray
        }.getOrElse(Array.empty)
        ShimLoader.getSparkShims.createFilePartition(bucketId, partitionedFiles)
      }
    }.getOrElse {
      Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
        ShimLoader.getSparkShims.createFilePartition(bucketId,
          prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
      }
    }
    if (isPerFileReadEnabled) {
      logInfo("Using the original per file parquet reader")
      ShimLoader.getSparkShims.getFileScanRDD(fsRelation.sparkSession, readFile.get, filePartitions)
    } else {
      // here we are making an optimization to read more then 1 file at a time on the CPU side
      // if they are small files before sending it down to the GPU
      val sqlConf = relation.sparkSession.sessionState.conf
      val hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
      val broadcastedHadoopConf =
        relation.sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
      val factory = GpuParquetMultiFilePartitionReaderFactory(
        sqlConf,
        broadcastedHadoopConf,
        relation.dataSchema,
        requiredSchema,
        relation.partitionSchema,
        pushedDownFilters.toArray,
        rapidsConf,
        metrics,
        queryUsesInputFile)

      // note we use the v2 DataSourceRDD instead of FileScanRDD so we don't have to copy more code
      new GpuDataSourceRDD(relation.sparkSession.sparkContext, filePartitions, factory)
    }
  }

  /**
   * Create an RDD for non-bucketed reads.
   * The bucketed variant of this function is [[createBucketedReadRDD]].
   *
   * @param readFile an optional function to read each (part of a) file. Used when
   *                 not using the small file optimization.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createNonBucketedReadRDD(
      readFile: Option[(PartitionedFile) => Iterator[InternalRow]],
      selectedPartitions: Array[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes =
      FilePartition.maxSplitBytes(fsRelation.sparkSession, selectedPartitions)
    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    val splitFiles = ShimLoader.getSparkShims
      .getPartitionSplitFiles(selectedPartitions, maxSplitBytes, relation)
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions =
      FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)

    if (isPerFileReadEnabled) {
      logInfo("Using the original per file parquet reader")
      ShimLoader.getSparkShims.getFileScanRDD(fsRelation.sparkSession, readFile.get, partitions)
    } else {
      // here we are making an optimization to read more then 1 file at a time on the CPU side
      // if they are small files before sending it down to the GPU
      val sqlConf = relation.sparkSession.sessionState.conf
      val hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
      val broadcastedHadoopConf =
        relation.sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
      val factory = GpuParquetMultiFilePartitionReaderFactory(
        sqlConf,
        broadcastedHadoopConf,
        relation.dataSchema,
        requiredSchema,
        relation.partitionSchema,
        pushedDownFilters.toArray,
        rapidsConf,
        metrics,
        queryUsesInputFile)

      // note we use the v2 DataSourceRDD instead of FileScanRDD so we don't have to copy more code
      new GpuDataSourceRDD(relation.sparkSession.sparkContext, partitions, factory)
    }
  }

  // Filters unused DynamicPruningExpression expressions - one which has been replaced
  // with DynamicPruningExpression(Literal.TrueLiteral) during Physical Planning
  private def filterUnusedDynamicPruningExpressions(
      predicates: Seq[Expression]): Seq[Expression] = {
    predicates.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral))
  }

  override def doCanonicalize(): GpuFileSourceScanExec = {
    GpuFileSourceScanExec(
      relation,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters), output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      rapidsConf,
      queryUsesInputFile)
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
