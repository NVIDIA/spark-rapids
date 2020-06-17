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

package com.nvidia.spark.rapids

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * An adaptor class that provides an Iterator interface for a PartitionReader.
 */
class PartitionReaderIterator(reader: PartitionReader[ColumnarBatch])
    extends Iterator[ColumnarBatch] with AutoCloseable {
  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  var hasNextResult: Option[Boolean] = None

  override def hasNext: Boolean = {
    if (hasNextResult.isEmpty) {
      hasNextResult = Some(reader.next())
    }
    hasNextResult.get
  }

  override def next(): ColumnarBatch = {
    if (hasNextResult.isEmpty) {
      hasNextResult = Some(reader.next())
    }
    hasNextResult = None
    reader.get()
  }

  override def close(): Unit = {
    reader.close()
  }
}

object PartitionReaderIterator {
  def buildReader(factory: FilePartitionReaderFactory): PartitionedFile => Iterator[InternalRow] = {
    file: PartitionedFile => {
      val reader = factory.buildColumnarReader(file)
      new PartitionReaderIterator(reader).asInstanceOf[Iterator[InternalRow]]
    }
  }

  // builds the map of metrics needed by the GPU batch scan code
  def buildScanMetrics(sparkContext: SparkContext): Map[String, SQLMetric] = {
    Map(
      GpuMetricNames.NUM_OUTPUT_ROWS -> SQLMetrics.createMetric(sparkContext,
        GpuMetricNames.DESCRIPTION_NUM_OUTPUT_ROWS),
      GpuMetricNames.NUM_OUTPUT_BATCHES -> SQLMetrics.createMetric(sparkContext,
        GpuMetricNames.DESCRIPTION_NUM_OUTPUT_BATCHES),
      GpuMetricNames.TOTAL_TIME -> SQLMetrics.createNanoTimingMetric(sparkContext,
        GpuMetricNames.DESCRIPTION_TOTAL_TIME),
      "bufferTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "buffer time"),
      GpuMetricNames.PEAK_DEVICE_MEMORY ->
        SQLMetrics.createSizeMetric(sparkContext, GpuMetricNames.DESCRIPTION_PEAK_DEVICE_MEMORY)
    )
  }
}
