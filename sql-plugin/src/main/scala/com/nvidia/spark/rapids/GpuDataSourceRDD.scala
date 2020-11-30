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

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDD, DataSourceRDDPartition}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A replacement for DataSourceRDD that does NOT compute the bytes read input metric.
 * DataSourceRDD assumes all reads occur on the task thread, and some GPU input sources
 * use multithreaded readers that cannot generate proper metrics with DataSourceRDD.
 * @note It is the responsibility of users of this RDD to generate the bytes read input
 *       metric explicitly!
 */
class GpuDataSourceRDD(
    sc: SparkContext,
    @transient private val inputPartitions: Seq[InputPartition],
    partitionReaderFactory: PartitionReaderFactory)
    extends DataSourceRDD(sc, inputPartitions, partitionReaderFactory, columnarReads = true) {

  private def castPartition(split: Partition): DataSourceRDDPartition = split match {
    case p: DataSourceRDDPartition => p
    case _ => throw new SparkException(s"[BUG] Not a DataSourceRDDPartition: $split")
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val inputPartition = castPartition(split).inputPartition
    val batchReader = partitionReaderFactory.createColumnarReader(inputPartition)
    val iter = new MetricsBatchIterator(new PartitionIterator[ColumnarBatch](batchReader))
    context.addTaskCompletionListener[Unit](_ => batchReader.close())
    // TODO: SPARK-25083 remove the type erasure hack in data source scan
    new InterruptibleIterator(context, iter.asInstanceOf[Iterator[InternalRow]])
  }
}

private class PartitionIterator[T](reader: PartitionReader[T]) extends Iterator[T] {
  private[this] var valuePrepared = false

  override def hasNext: Boolean = {
    if (!valuePrepared) {
      valuePrepared = reader.next()
    }
    valuePrepared
  }

  override def next(): T = {
    if (!hasNext) {
      throw new java.util.NoSuchElementException("End of stream")
    }
    valuePrepared = false
    reader.get()
  }
}

private class MetricsBatchIterator(iter: Iterator[ColumnarBatch]) extends Iterator[ColumnarBatch] {
  private[this] val inputMetrics = TaskContext.get().taskMetrics().inputMetrics

  override def hasNext: Boolean = iter.hasNext

  override def next(): ColumnarBatch = {
    val batch = iter.next()
    TrampolineUtil.incInputRecordsRows(inputMetrics, batch.numRows())
    batch
  }
}

/** Wraps a columnar PartitionReader to update bytes read metric based on filesystem statistics. */
class PartitionReaderWithBytesRead(reader: PartitionReader[ColumnarBatch])
    extends PartitionReader[ColumnarBatch] {
  private[this] val inputMetrics = TaskContext.get.taskMetrics().inputMetrics
  private[this] val getBytesRead = TrampolineUtil.getFSBytesReadOnThreadCallback()

  override def next(): Boolean = {
    val result = reader.next()
    TrampolineUtil.incBytesRead(inputMetrics, getBytesRead())
    result
  }

  override def get(): ColumnarBatch = reader.get()

  override def close(): Unit = reader.close()
}
