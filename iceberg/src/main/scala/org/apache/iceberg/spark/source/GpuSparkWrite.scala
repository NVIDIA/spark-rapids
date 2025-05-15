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

import com.nvidia.spark.rapids.GpuWrite
import org.apache.hadoop.shaded.org.apache.commons.lang3.reflect.{FieldUtils, MethodUtils}
import org.apache.iceberg.{FileFormat, Table}

import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.write.{BatchWrite, RequiresDistributionAndOrdering, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite

class GpuSparkWrite(cpu: SparkWrite) extends GpuWrite with RequiresDistributionAndOrdering  {
  private val table: Table = FieldUtils.readField(cpu, "table", true).asInstanceOf[Table]
  private val format: FileFormat = FieldUtils.readField(cpu, "format", true)
    .asInstanceOf[FileFormat]

  override def toBatch: BatchWrite = throw new UnsupportedOperationException(
    "GpuSparkWrite does not support batch write")

  override def toStreaming: StreamingWrite = throw new UnsupportedOperationException(
    "GpuSparkWrite does not support streaming write")

  override def toString: String = s"GpuIcebergWrite(table=$table, format=$format)"

  private[source] def abort(messages: Array[WriterCommitMessage]): Unit = {
    MethodUtils.invokeMethod(cpu, true, "abort", messages)
  }

  override def distributionStrictlyRequired(): Boolean = cpu.distributionStrictlyRequired()

  override def requiredNumPartitions(): Int = cpu.requiredNumPartitions()

  override def advisoryPartitionSizeInBytes(): Long = cpu.advisoryPartitionSizeInBytes()

  override def requiredDistribution(): Distribution = cpu.requiredDistribution()

  override def requiredOrdering(): Array[SortOrder] = cpu.requiredOrdering()
}

