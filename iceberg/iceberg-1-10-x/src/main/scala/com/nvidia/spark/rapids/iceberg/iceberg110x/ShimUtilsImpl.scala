/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.iceberg110x

import java.{util => ju}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import com.nvidia.spark.rapids.iceberg.IcebergShimUtils
import com.nvidia.spark.rapids.iceberg.parquet.converter.ToIcebergShaded
import com.nvidia.spark.rapids.parquet.{HMBInputFile, ParquetFooterUtils}
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{ContentFile, FileScanTask, MetadataColumns, Partitioning, Schema, Table}
import org.apache.iceberg.io.{FileIO, SupportsStorageCredentials}
import org.apache.iceberg.parquet.GpuParquetIO
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.spark.SparkUtil
import org.apache.iceberg.util.PartitionUtil

/** Iceberg 1.10.x shim: uses `SparkUtil::internalToSpark` and a `FileCache`-aware footer path. */
class ShimUtilsImpl extends IcebergShimUtils {
  override def locationOf(f: ContentFile[_]): String = f.location()

  override def constantsMap(
      task: FileScanTask,
      readSchema: Schema,
      table: Table): ju.Map[Integer, _] = {
    if (readSchema.findField(MetadataColumns.PARTITION_COLUMN_ID) != null) {
      val partitionType = Partitioning.partitionType(table)
      PartitionUtil.constantsMap(task, partitionType, SparkUtil.internalToSpark _)
    } else {
      PartitionUtil.constantsMap(task, SparkUtil.internalToSpark _)
    }
  }

  override def storageCredentialOverlays(
      fileIO: FileIO): ju.Map[String, ju.Map[String, String]] = {
    fileIO match {
      case sc: SupportsStorageCredentials =>
        sc.credentials().asScala.map { c => c.prefix() -> c.config() }.toMap.asJava
      case _ => ju.Collections.emptyMap()
    }
  }

  /**
   * Cache-aware footer read: pull the framed footer buffer from `FileCache` (populating on
   * miss via the underlying input file), parse it once via `ParquetFileReader.readFooter`,
   * then construct the reader with the pre-parsed `ParquetMetadata` so it does not re-read
   * the footer from the file.
   */
  override def openParquetReader(
      inputFile: IcebergInputFile,
      filePath: Path,
      options: ParquetReadOptions,
      metrics: Map[String, GpuMetric]): ParquetFileReader = {
    val metadata = withResource(ParquetFooterUtils.getFooterBuffer(
        inputFile, metrics,
        ParquetFooterUtils.readFooterBufferFromInputFile(inputFile, filePath))) { hmb =>
      val shadedHmbFile = ToIcebergShaded.shade(new HMBInputFile(hmb))
      withResource(shadedHmbFile.newStream()) { hmbStream =>
        ParquetFileReader.readFooter(shadedHmbFile, options, hmbStream)
      }
    }
    val realFile = GpuParquetIO.file(inputFile.getDelegate)
    closeOnExcept(realFile.newStream()) { stream =>
      new ParquetFileReader(realFile, metadata, options, stream)
    }
  }
}
