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

import com.nvidia.spark.rapids.RapidsConf
import org.apache.iceberg.{Schema, SchemaParser}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition}
import org.apache.spark.util.SerializableConfiguration

class GpuSparkInputPartition(val cpuInputPartition: SparkInputPartition,
    val rapidsConf: RapidsConf,
    val hadoopConf: Broadcast[SerializableConfiguration],
    val expectedSchemaStr: String) extends
  InputPartition with HasPartitionKey with Serializable {

  override def preferredLocations(): Array[String] = cpuInputPartition.preferredLocations()
  override def partitionKey(): InternalRow = cpuInputPartition.partitionKey()

  lazy val maxChunkedReaderMemoryUsageSizeBytes: Long = {
    if (rapidsConf.limitChunkedReaderMemoryUsage) {
      val limitRatio = rapidsConf.chunkedReaderMemoryUsageRatio
      (limitRatio * rapidsConf.gpuTargetBatchSizeBytes).toLong
    } else {
      0L
    }
  }

  @transient lazy val expectedSchema: Schema = {
    SchemaParser.fromJson(expectedSchemaStr)
  }
}
