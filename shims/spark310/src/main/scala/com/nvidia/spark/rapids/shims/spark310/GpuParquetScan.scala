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

import java.io.OutputStream
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.{Collections, Locale}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.math.max

import ai.rapids.cudf.{ColumnVector, DType, HostMemoryBuffer, NvtxColor, ParquetOptions, Table}
import com.nvidia.spark.RebaseHelper
import com.nvidia.spark.rapids.GpuMetricNames._
import com.nvidia.spark.rapids.ParquetPartitionReader.CopyRange
import com.nvidia.spark.rapids.RapidsConf.ENABLE_SMALL_FILES_PARQUET
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.{CountingOutputStream, NullOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat}
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnPath, FileMetaData, ParquetMetadata}
import org.apache.parquet.schema.{GroupType, MessageType, Types}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, ParquetReadSupport}
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, FileScan}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

// FileScan changed in Spark 3.1.0 so need to compile in Shim
case class GpuParquetScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    rapidsConf: RapidsConf)
  extends FileScan with GpuParquetScanBase(sparkSession, hadoopConf, fileIndex, dataSchema,
     readDataSchema, readPartitionSchema, pushedFilters, options, partitionFilters,
     dataFilters, rapidsConf) {

  override def isSplitable(path: Path): Boolean = super.isSplitable

  override def createReaderFactory(): PartitionReaderFactory = super.createReaderFactory()

  override def equals(obj: Any): Boolean = super.equals(obj)

  override def description(): String = super.description()

  override def equals(obj: Any): Boolean = obj match {
    case p: GpuParquetScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters) && rapidsConf == p.rapidsConf
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }

  override def withFilters(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
}
