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

package org.apache.spark.sql.hive.rapids

import ai.rapids.cudf.{HostMemoryBuffer, Scalar, Schema, Table}
import com.nvidia.spark.RebaseHelper.withResource
import com.nvidia.spark.rapids.{ColumnarPartitionReaderWithPartitionValues, CSVPartitionReader, GpuColumnVector, GpuExec, PartitionReaderIterator, PartitionReaderWithBytesRead, RapidsConf}
import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.GpuMetric.{BUFFER_TIME, DEBUG_LEVEL, DESCRIPTION_BUFFER_TIME, DESCRIPTION_FILTER_TIME, DESCRIPTION_GPU_DECODE_TIME, DESCRIPTION_PEAK_DEVICE_MEMORY, ESSENTIAL_LEVEL, FILTER_TIME, GPU_DECODE_TIME, MODERATE_LEVEL, NUM_OUTPUT_ROWS, PEAK_DEVICE_MEMORY}
import com.nvidia.spark.rapids.shims.{ShimFilePartitionReaderFactory, ShimSparkPlan, SparkShimImpl}
import java.net.URI
import java.util.concurrent.TimeUnit.NANOSECONDS
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition}
import org.apache.hadoop.hive.ql.plan.TableDesc
import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet
import scala.collection.mutable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeReference, AttributeSeq, AttributeSet, BindReferences, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.{ExecSubqueryExpression, LeafExecNode, PartitionedFileUtil, SQLExecution}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * RAPIDS replacement for the [[org.apache.spark.sql.hive.execution.HiveTableScanExec]],
 * for supporting reading Hive delimited text format.
 *
 * While the HiveTableScanExec supports all the data formats that Hive does,
 * the GpuHiveTableScanExec currently supports only text tables.
 *
 * This GpuExec supports reading from Hive tables under the following conditions:
 *   1. The table is stored as TEXTFILE (i.e. input-format == TextInputFormat,
 *      serde == LazySimpleSerDe).
 *   2. The table contains only columns of primitive types. Specifically, STRUCT,
 *      ARRAY, MAP, and BINARY are not supported.
 *   3. The table uses Hive's default record delimiters ('Ctrl-A'),
 *      and line delimiters ('\n').
 *
 * @param requestedAttributes Columns to be read from the table
 * @param hiveTableRelation The Hive table to be scanned
 * @param partitionPruningPredicate Partition-pruning predicate for Hive partitioned tables
 */
case class GpuHiveTableScanExec(requestedAttributes: Seq[Attribute],
                                hiveTableRelation: HiveTableRelation,
                                partitionPruningPredicate: Seq[Expression])
  extends GpuExec with ShimSparkPlan with LeafExecNode with CastSupport {

  override def producedAttributes: AttributeSet = outputSet ++
    AttributeSet(partitionPruningPredicate.flatMap(_.references))

  private val originalAttributes = AttributeMap(hiveTableRelation.output.map(a => a -> a))

  override val output: Seq[Attribute] = {
    // Retrieve the original attributes based on expression ID so that capitalization matches.
    requestedAttributes.map(originalAttributes)
  }

  val partitionAttributes: Seq[AttributeReference] = hiveTableRelation.partitionCols

  // CPU expression to prune Hive partitions, based on [[partitionPruningPredicate]].
  // Bind all partition key attribute references in the partition pruning predicate for later
  // evaluation.
  private lazy val boundPartitionPruningPredOnCPU =
    partitionPruningPredicate.reduceLeftOption(And).map { pred =>
      require(pred.dataType == BooleanType,
        s"Data type of predicate $pred must be ${BooleanType.catalogString} rather than " +
          s"${pred.dataType.catalogString}.")

    BindReferences.bindReference(pred, hiveTableRelation.partitionCols)
  }

  @transient private lazy val hiveQlTable = HiveClientImpl.toHiveTable(hiveTableRelation.tableMeta)
  @transient private lazy val tableDesc = new TableDesc(
    hiveQlTable.getInputFormatClass,
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata)

  private def castFromString(value: String, dataType: DataType) = {
    cast(Literal(value), dataType).eval(null)
  }

  /**
   * Prunes partitions not involve the query plan.
   *
   * @param partitions All partitions of the relation.
   * @return Partitions that are involved in the query plan.
   */
  private[hive] def prunePartitions(partitions: Seq[HivePartition]): Seq[HivePartition] = {
    boundPartitionPruningPredOnCPU match {
      case None => partitions
      case Some(shouldKeep) => partitions.filter { part =>
        val dataTypes = hiveTableRelation.partitionCols.map(_.dataType)
        val castedValues = part.getValues.asScala.zip(dataTypes)
          .map { case (value, dataType) => castFromString(value, dataType) }

        // Only partitioned values are needed here, since the predicate has already been bound to
        // partition key attribute references.
        val row = InternalRow.fromSeq(castedValues)
        shouldKeep.eval(row).asInstanceOf[Boolean]
      }
    }
  }

  @transient lazy val prunedPartitions: Seq[HivePartition] = {
    if (hiveTableRelation.prunedPartitions.nonEmpty) {
      val hivePartitions =
        hiveTableRelation.prunedPartitions.get.map(HiveClientImpl.toHivePartition(_, hiveQlTable))
      if (partitionPruningPredicate.forall(!ExecSubqueryExpression.hasSubquery(_))) {
        hivePartitions
      } else {
        prunePartitions(hivePartitions)
      }
    } else {
      if (sparkSession.sessionState.conf.metastorePartitionPruning &&
        partitionPruningPredicate.nonEmpty) {
        rawPartitions
      } else {
        prunePartitions(rawPartitions)
      }
    }
  }

  // exposed for tests
  @transient lazy val rawPartitions: Seq[HivePartition] = {
    val prunedPartitions =
      if (sparkSession.sessionState.conf.metastorePartitionPruning &&
        partitionPruningPredicate.nonEmpty) {
        // Retrieve the original attributes based on expression ID so that capitalization matches.
        val normalizedFilters = partitionPruningPredicate.map(_.transform {
          case a: AttributeReference => originalAttributes(a)
        })
        sparkSession.sessionState.catalog
          .listPartitionsByFilter(hiveTableRelation.tableMeta.identifier, normalizedFilters)
      } else {
        sparkSession.sessionState.catalog.listPartitions(hiveTableRelation.tableMeta.identifier)
      }
    prunedPartitions.map(HiveClientImpl.toHivePartition(_, hiveQlTable))
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    "numFiles" -> createMetric(ESSENTIAL_LEVEL, "number of files read"),
    "metadataTime" -> createTimingMetric(ESSENTIAL_LEVEL, "metadata time"),
    "filesSize" -> createSizeMetric(ESSENTIAL_LEVEL, "size of files read"),
    GPU_DECODE_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_GPU_DECODE_TIME),
    BUFFER_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUFFER_TIME),
    FILTER_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_FILTER_TIME),
    PEAK_DEVICE_MEMORY -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_PEAK_DEVICE_MEMORY),
    "scanTime" -> createTimingMetric(ESSENTIAL_LEVEL, "scan time")
  ) ++ semaphoreMetrics

  private lazy val driverMetrics: mutable.HashMap[String, Long] = mutable.HashMap.empty

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

  private def buildReader(sqlConf: SQLConf,
                          broadcastConf: Broadcast[SerializableConfiguration],
                          rapidsConf: RapidsConf,
                          dataSchema: StructType,
                          partitionSchema: StructType,
                          readSchema: StructType,
                          options: Map[String, String]
              ): PartitionedFile => Iterator[InternalRow] = {
    val readerFactory = GpuHiveTextPartitionReaderFactory(
      sqlConf = sqlConf,
      broadcastConf = broadcastConf,
      inputFileSchema = dataSchema,
      partitionSchema = partitionSchema,
      requestedOutputDataSchema = readSchema,
      requestedAttributes = requestedAttributes,
      maxReaderBatchSizeRows = rapidsConf.maxReadBatchSizeRows,
      maxReaderBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes,
      metrics = allMetrics,
      params = options
    )

    PartitionReaderIterator.buildReader(readerFactory)
  }

  /**
   * Prune output projection to those columns that are to be read from
   * the input file/buffer, in the same order as [[requestedAttributes]]
   * Removes partition columns, and returns the resultant schema
   * as a [[StructType]].
   */
  private def getRequestedOutputDataSchema(tableSchema: StructType,
                                           partitionAttributes: Seq[Attribute],
                                           requestedAttributes: Seq[Attribute]): StructType = {
    // Read schema in the same order as requestedAttributes.
    // Note: This might differ from the column order in `tableSchema`.
    //       In fact, HiveTableScanExec specifies `requestedAttributes` alphabetically.
    val partitionKeys: HashSet[String] = HashSet() ++ partitionAttributes.map(_.name)
    val requestedCols = requestedAttributes.filter(a => !partitionKeys.contains(a.name))
                                           .toList
    val distinctColumns = requestedCols.distinct
    val distinctFields  = distinctColumns.map(a => tableSchema.apply(a.name))
    StructType(distinctFields)
  }

  private def createReadRDDForDirectories(readFile: PartitionedFile => Iterator[InternalRow],
                                          directories: Seq[(URI, InternalRow)],
                                          readSchema: StructType,
                                          sparkSession: SparkSession,
                                          hadoopConf: Configuration): RDD[ColumnarBatch] = {
    def isNonEmptyDataFile(f: FileStatus): Boolean = {
      if (!f.isFile || f.getLen == 0) {
        false
      } else {
        val name = f.getPath.getName
        !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
      }
    }

    val filePartitions: Seq[FilePartition] = directories.flatMap { case (directory, partValues) =>
      val path               = new Path(directory)
      val fs                 = path.getFileSystem(hadoopConf)
      val dirContents        = fs.listStatus(path).filter(isNonEmptyDataFile)
      val partitionDirectory = PartitionDirectory(partValues, dirContents)
      val maxSplitBytes      = FilePartition.maxSplitBytes(sparkSession, Array(partitionDirectory))

      val splitFiles: Seq[PartitionedFile] = partitionDirectory.files.flatMap { f =>
        PartitionedFileUtil.splitFiles(
          sparkSession,
          f,
          f.getPath,
          isSplitable = true,
          maxSplitBytes,
          partitionDirectory.values
        )
      }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
      FilePartition.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
    }

    // TODO [future]: Handle small-file optimization.
    //                (https://github.com/NVIDIA/spark-rapids/issues/7017)
    //                Currently assuming per-file reading.
    SparkShimImpl.getFileScanRDD(sparkSession, readFile, filePartitions, readSchema)
                 .asInstanceOf[RDD[ColumnarBatch]]
  }

  private def createReadRDDForTable(
                readFile: PartitionedFile => Iterator[InternalRow],
                hiveTableRelation: HiveTableRelation,
                readSchema: StructType,
                sparkSession: SparkSession,
                hadoopConf: Configuration
              ): RDD[ColumnarBatch] = {
    val tableLocation: URI = hiveTableRelation.tableMeta.storage.locationUri.getOrElse{
      throw new UnsupportedOperationException("Table path not set.")
    }
    // No need to check if table directory exists.
    // FileSystem.listStatus() handles this for GpuHiveTableScanExec,
    // just like for Apache Spark.
    createReadRDDForDirectories(readFile,
                                Array((tableLocation, InternalRow.empty)),
                                readSchema,
                                sparkSession,
                                hadoopConf)
  }

  private def createReadRDDForPartitions(
                readFile: PartitionedFile => Iterator[InternalRow],
                hiveTableRelation: HiveTableRelation,
                readSchema: StructType,
                sparkSession: SparkSession,
                hadoopConf: Configuration
              ): RDD[ColumnarBatch] = {
    val partitionColTypes = hiveTableRelation.partitionCols.map(_.dataType)
    val dirsWithPartValues = prunedPartitions.map { p =>
      // No need to check if partition directory exists.
      // FileSystem.listStatus() handles this for GpuHiveTableScanExec,
      // just like for Apache Spark.
      val uri = p.getDataLocation.toUri
      val partValues: Seq[Any] = {
        p.getValues.asScala.zip(partitionColTypes).map {
          case (value, dataType) => castFromString(value, dataType)
        }
      }
      val partValuesAsInternalRow = InternalRow.fromSeq(partValues)

      (uri, partValuesAsInternalRow)
    }

    createReadRDDForDirectories(readFile,
      dirsWithPartValues, readSchema, sparkSession, hadoopConf)
  }

  lazy val inputRDD: RDD[ColumnarBatch] = {
    // Assume Delimited text.
    // Note: The populated `options` aren't strictly required for text, currently.
    //       These are added in case they are required for table read in the future,
    //       (like with Hive ORC tables).
    val options                   = hiveTableRelation.tableMeta.properties ++
                                    hiveTableRelation.tableMeta.storage.properties
    val hadoopConf                = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val broadcastHadoopConf       = sparkSession.sparkContext.broadcast(
                                      new SerializableConfiguration(hadoopConf))
    val sqlConf                   = sparkSession.sessionState.conf
    val rapidsConf                = new RapidsConf(sqlConf)
    val requestedOutputDataSchema = getRequestedOutputDataSchema(hiveTableRelation.tableMeta.schema,
                                                                 partitionAttributes,
                                                                 requestedAttributes)
    val reader                    = buildReader(sqlConf,
                                                broadcastHadoopConf,
                                                rapidsConf,
                                                hiveTableRelation.tableMeta.dataSchema,
                                                hiveTableRelation.tableMeta.partitionSchema,
                                                requestedOutputDataSchema,
                                                options)
    val rdd = if (hiveTableRelation.isPartitioned) {
      createReadRDDForPartitions(reader, hiveTableRelation, requestedOutputDataSchema,
                                 sparkSession, hadoopConf)
    } else {
      createReadRDDForTable(reader, hiveTableRelation, requestedOutputDataSchema,
                            sparkSession, hadoopConf)
    }
    sendDriverMetrics()
    rdd
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val scanTime = gpuLongMetric("scanTime")
    inputRDD.mapPartitionsInternal { batches =>
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

  override def doCanonicalize(): GpuHiveTableScanExec = {
    val input: AttributeSeq = hiveTableRelation.output
    GpuHiveTableScanExec(
      requestedAttributes.map(QueryPlan.normalizeExpressions(_, input)),
      hiveTableRelation.canonicalized.asInstanceOf[HiveTableRelation],
      QueryPlan.normalizePredicates(partitionPruningPredicate, input))
  }
}

/**
 * Partition-reader styled similarly to `ColumnarPartitionReaderWithPartitionValues`,
 * but orders the output columns alphabetically.
 * This is required since the [[GpuHiveTableScanExec.requestedAttributes]] have the columns
 * ordered alphabetically by name, even though the table schema (and hence, the file-schema)
 * need not.
 */
class AlphabeticallyReorderingColumnPartitionReader(fileReader: PartitionReader[ColumnarBatch],
                                                    partitionValues: Array[Scalar],
                                                    partValueTypes: Array[DataType],
                                                    partitionSchema: StructType,
                                                    requestedAttributes: Seq[Attribute])
  extends ColumnarPartitionReaderWithPartitionValues(fileReader,
                                                     partitionValues,
                                                     partValueTypes) {
  override def get(): ColumnarBatch = {
    val fileBatch: ColumnarBatch = super.get()
    if (partitionValues.isEmpty) {
      return fileBatch
    }

    // super.get() returns columns specified in `requestedAttributes`,
    // but ordered according to `tableSchema`. Must reorder, based on `requestedAttributes`.
    // Also, super.get() appends *all* partition keys, even if they do not belong
    // in the output projection. Must discard unused partition keys here.
    withResource(fileBatch) { fileBatch =>
      var dataColumnIndex = 0
      val partitionColumnStartIndex = fileBatch.numCols() - partitionValues.length
      val partitionKeys = partitionSchema.map(_.name).toList
      val reorderedColumns = requestedAttributes.map { a =>
        val partIndex = partitionKeys.indexOf(a.name)
        if (partIndex == -1) {
          // Not a partition column.
          dataColumnIndex += 1
          fileBatch.column(dataColumnIndex - 1)
        }
        else {
          // Partition key.
          fileBatch.column(partitionColumnStartIndex + partIndex)
        }
      }.toArray
      for (col <- reorderedColumns) { col.asInstanceOf[GpuColumnVector].incRefCount() }
      new ColumnarBatch(reorderedColumns, fileBatch.numRows())
    }
  }
}

// Factory to build the columnar reader.
case class GpuHiveTextPartitionReaderFactory(sqlConf: SQLConf,
                                             broadcastConf: Broadcast[SerializableConfiguration],
                                             inputFileSchema: StructType,
                                             partitionSchema: StructType,
                                             requestedOutputDataSchema: StructType,
                                             requestedAttributes: Seq[Attribute],
                                             maxReaderBatchSizeRows: Integer,
                                             maxReaderBatchSizeBytes: Long,
                                             metrics: Map[String, GpuMetric],
                                             @transient params: Map[String, String])
  extends ShimFilePartitionReaderFactory(params) {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("Row-based text parsing is not supported on GPU.")
  }

  private val csvOptions = new CSVOptions(params,
                                          sqlConf.csvColumnPruning,
                                          sqlConf.sessionLocalTimeZone,
                                          sqlConf.columnNameOfCorruptRecord)

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastConf.value.value
    val reader = new PartitionReaderWithBytesRead(
                   new GpuHiveDelimitedTextPartitionReader(
                     conf, csvOptions, partFile, inputFileSchema,
                     requestedOutputDataSchema, maxReaderBatchSizeRows,
                     maxReaderBatchSizeBytes, metrics))

    val partValueScalars = ColumnarPartitionReaderWithPartitionValues.createPartitionValues(
      partFile.partitionValues.toSeq(partitionSchema),
      partitionSchema
    )
    new AlphabeticallyReorderingColumnPartitionReader(reader,
                                                      partValueScalars,
                                                      GpuColumnVector.extractTypes(partitionSchema),
                                                      partitionSchema,
                                                      requestedAttributes)
  }
}

// Reader that converts from chunked data buffers into cudf.Table.
class GpuHiveDelimitedTextPartitionReader(conf: Configuration,
                                          csvOptions: CSVOptions,
                                          partFile: PartitionedFile,
                                          inputFileSchema: StructType,
                                          requestedOutputDataSchema: StructType,
                                          maxRowsPerChunk: Integer,
                                          maxBytesPerChunk: Long,
                                          execMetrics: Map[String, GpuMetric]) extends
  CSVPartitionReader(conf, partFile, inputFileSchema, requestedOutputDataSchema,
                     csvOptions, maxRowsPerChunk, maxBytesPerChunk, execMetrics) {

  override def buildCsvOptions(parsedOptions: CSVOptions,
                               schema: StructType,
                               hasHeader: Boolean): ai.rapids.cudf.CSVOptions.Builder = {
    super.buildCsvOptions(parsedOptions, schema, hasHeader)
      .withDelim('\u0001') // Record field delimiter '^A'.
      .withNullValue("\\N")
  }

  override def readToTable(dataBuffer: HostMemoryBuffer,
                           dataSize: Long,
                           inputFileCudfSchema: Schema,
                           requestedOutputDataSchema: StructType,
                           isFirstChunk: Boolean): Table = {
    // inputFileCudfSchema       == Schema of the input file/buffer.
    //                              Presented in the order of input columns in the file.
    // requestedOutputDataSchema == Spark output schema. This is inexplicably sorted alphabetically
    //                              in HiveTSExec, unlike FileSourceScanExec (which has file-input
    //                              ordering).
    //                              This trips up the downstream string->numeric casts in
    //                              GpuTextBasedPartitionReader.readToTable().
    // Given that Table.readCsv presents the output columns in the order of the input file,
    // we need to reorder the table read from the input file in the order specified in
    // [[requestedOutputDataSchema]] (i.e. requiredAttributes).
    val table = super.readToTable(dataBuffer, dataSize, inputFileCudfSchema,
                                  requestedOutputDataSchema, isFirstChunk)
    withResource(table) { table =>
      val requiredColumnSequence = requestedOutputDataSchema.map(_.name).toList
      val requiredColumnSet      = requiredColumnSequence.toSet
      val outputColumnNames      = inputFileCudfSchema.getColumnNames
                                                      .filter(c => requiredColumnSet.contains(c))
      val reorderedColumns       = requiredColumnSequence.map(
        colName => table.getColumn(outputColumnNames.indexOf(colName)))

      new Table(reorderedColumns: _*)
    }
  }
}
