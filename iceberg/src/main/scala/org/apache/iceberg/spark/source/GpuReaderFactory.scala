/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark.source

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{GpuMetric, MultiFileReaderUtils, RapidsConf, ResourcePoolConf}
import com.nvidia.spark.rapids.iceberg.parquet.{MultiFile, MultiThread, SingleFile, ThreadConf}
import org.apache.iceberg.{FileFormat, ScanTask, ScanTaskGroup}

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuReaderFactory(private val metrics: Map[String, GpuMetric],
    rapidsConf: RapidsConf,
    queryUsesInputFile: Boolean) extends PartitionReaderFactory {

  private val allCloudSchemes = rapidsConf.getCloudSchemes.toSet
  private val isParquetPerFileReadEnabled = rapidsConf.isParquetPerFileReadEnabled
  private val canUseParquetMultiThread = rapidsConf.isParquetMultiThreadReadEnabled
  // Here ignores the "ignoreCorruptFiles" comparing to the code in
  // "GpuParquetMultiFilePartitionReaderFactory", since "ignoreCorruptFiles" is
  // not honored by Iceberg.
  private val canUseParquetCoalescing = rapidsConf.isParquetCoalesceFileReadEnabled &&
    !queryUsesInputFile
  // Fetch the latest updated value of multiThreadMemoryLimit from the driver side.
  private val poolMemCapacity = rapidsConf.multiThreadMemoryLimit match {
    case v if v == 0 => None
    case v => Some(v)
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException("GpuReaderFactory does not support createReader()")

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    partition match {
      case gpuPartition: GpuSparkInputPartition =>
        val threadConf = calcThreadConf(gpuPartition)
        new GpuIcebergPartitionReader(gpuPartition, threadConf, metrics)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported partition type: ${partition.getClass}")
    }
  }

  override def supportColumnarReads(partition: InputPartition) = true

  private def calcThreadConf(partition: GpuSparkInputPartition): ThreadConf = {
    val scans = partition
      .cpuPartition
      .taskGroup()
      .asInstanceOf[ScanTaskGroup[ScanTask]]
      .tasks
      .asScala
      .map(_.asFileScanTask())

    val hasNoDeletes = scans.forall(_.deletes.isEmpty)

    val allParquet = scans.forall(_.file.format == FileFormat.PARQUET)

    if (allParquet) {
      if (isParquetPerFileReadEnabled) {
        // If per-file read is enabled, we can only use single threaded reading.
        return SingleFile
      }

      val canUseMultiThread = canUseParquetMultiThread
      val canUseCoalescing = canUseParquetCoalescing && hasNoDeletes

      val files = scans.map(_.file.path.toString).toArray

      val useMultiThread = MultiFileReaderUtils.useMultiThreadReader(canUseCoalescing,
        canUseMultiThread, files, allCloudSchemes)

      val poolConf = ResourcePoolConf
          .buildFromConf(rapidsConf)
          .setMemoryCapacity(
            // Set the appropriate capacity of the resource pool for this reader:
            // 1. Try to get the value from the latest user defined value from driver side
            // 2. If not set, figure out the value according to physical memory settings of current
            // executor via `initializePinnedPoolAndOffHeapLimits`
            poolMemCapacity.getOrElse(
              SparkEnv.get.conf.getLong(RapidsConf.MULTITHREAD_READ_MEM_LIMIT.key, 0L)
            )
          )
      if (useMultiThread) {
        MultiThread(poolConf, partition.maxNumParquetFilesParallel)
      } else {
        MultiFile(poolConf)
      }
    } else {
      throw new UnsupportedOperationException("Currently only parquet format is supported")
    }
  }
}