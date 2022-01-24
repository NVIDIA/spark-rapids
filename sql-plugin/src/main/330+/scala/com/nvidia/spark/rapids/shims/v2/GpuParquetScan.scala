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

package com.nvidia.spark.rapids.shims.v2

import java.net.URI

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{Arm, GpuMetric, GpuParquetFileFilterHandler, GpuParquetMultiFilePartitionReaderFactory, GpuParquetPartitionReaderFactory, GpuParquetScanBase, GpuRowToColumnConverter, RapidsConf}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER
import org.apache.parquet.hadoop.metadata.{FileMetaData, ParquetMetadata}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, DataSourceUtils, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFooterReader, ParquetOptions}
import org.apache.spark.sql.execution.datasources.parquet.rapids.shims.v2.GpuParquetUtils
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, FileScan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * Users should make sure that `aggregation` is valid when using this factory.
 */
case class GpuParquetPartitionReaderFactoryForAggregationPushdown(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    aggregation: Aggregation,
    parquetOptions: ParquetOptions,
    @transient rapidsConf: RapidsConf,
  metrics: Map[String, GpuMetric]) extends FilePartitionReaderFactory with Arm with Logging {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val debugDumpPrefix = rapidsConf.parquetDebugDumpPrefix
  private val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes
  private val datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead

  private val filterHandler = GpuParquetFileFilterHandler(sqlConf)

  private def getFooter(file: PartitionedFile): ParquetMetadata = {
    val conf = broadcastedConf.value.value
    val filePath = new Path(new URI(file.filePath))

    // For aggregate push down, we will get max/min/count from footer statistics.
    ParquetFooterReader.readFooter(conf, filePath, NO_FILTER)
  }

  private def getDatetimeRebaseSpec(
    footerFileMetaData: FileMetaData): RebaseSpec = {
    DataSourceUtils.datetimeRebaseSpec(
      footerFileMetaData.getKeyValueMetaData.get,
      datetimeRebaseModeInRead)
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def buildColumnarReader(
    partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    new PartitionReader[ColumnarBatch] {
      private var hasNext = true
      private val batch: ColumnarBatch = {
        val footer = getFooter(partitionedFile)
        if (footer != null && footer.getBlocks.size > 0) {
          val row = GpuParquetUtils.createAggInternalRowFromFooter(
            footer,
            partitionedFile.filePath,
            dataSchema,
            partitionSchema,
            aggregation,
            readDataSchema,
            partitionedFile.partitionValues,
            getDatetimeRebaseSpec(footer.getFileMetaData)
          )

          new GpuRowToColumnConverter(readDataSchema).convertBatch(Array(row), readDataSchema)
        } else {
          null
        }
      }
      override def next(): Boolean = {
        hasNext && batch != null
      }

      override def get(): ColumnarBatch = {
        hasNext = false
        batch
      }

      override def close(): Unit = {}
    }
  }
}

case class GpuParquetScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    pushedAggregate: Option[Aggregation] = None,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    rapidsConf: RapidsConf,
    queryUsesInputFile: Boolean = true)
  extends GpuParquetScanBase(sparkSession, hadoopConf, dataSchema,
    readDataSchema, readPartitionSchema, pushedFilters, rapidsConf,
    queryUsesInputFile) with FileScan {

  override def isSplitable(path: Path): Boolean = super.isSplitableBase(path)

  override def readSchema(): StructType = {
    // If aggregate is pushed down, schema has already been pruned in `ParquetScanBuilder`
    // and no need to call super.readSchema()
    if (pushedAggregate.nonEmpty) readDataSchema else super.readSchema()
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    if (pushedAggregate.nonEmpty){
      GpuParquetPartitionReaderFactoryForAggregationPushdown(
        sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, pushedAggregate.get,
        new ParquetOptions(
          options.asCaseSensitiveMap.asScala.toMap,
          sparkSession.sessionState.conf),
        rapidsConf, metrics)
    } else if (rapidsConf.isParquetPerFileReadEnabled) {
      logInfo("Using the original per file parquet reader")
      GpuParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics)
    } else {
      GpuParquetMultiFilePartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics,
        queryUsesInputFile)
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: GpuParquetScan =>
      val pushedDownAggEqual = if (pushedAggregate.nonEmpty && p.pushedAggregate.nonEmpty) {
        AggregatePushDownUtils.equivalentAggregations(pushedAggregate.get, p.pushedAggregate.get)
      } else {
        pushedAggregate.isEmpty && p.pushedAggregate.isEmpty
      }
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters) && pushedDownAggEqual
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  lazy private val (pushedAggregationsStr, pushedGroupByStr) = if (pushedAggregate.nonEmpty) {
    (seqToString(pushedAggregate.get.aggregateExpressions),
      seqToString(pushedAggregate.get.groupByColumns))
  } else {
    ("[]", "[]")
  }

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters) +
      ", PushedAggregation: " + pushedAggregationsStr +
      ", PushedGroupBy: " + pushedGroupByStr
  }

  // overrides nothing in 330
  def withFilters(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
}

