/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{MetricsBatchIterator, PartitionIterator}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDD, DataSourceRDDPartition}
import org.apache.spark.sql.execution.metric.SQLMetric
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
    partitionReaderFactory: PartitionReaderFactory
) extends DataSourceRDD(sc, inputPartitions, partitionReaderFactory, columnarReads = true,
  Map.empty[String, SQLMetric]) {

  private def castPartition(split: Partition): DataSourceRDDPartition = split match {
    case p: DataSourceRDDPartition => p
    case _ => throw new SparkException(s"[BUG] Not a DataSourceRDDPartition: $split")
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val inputPartition = castPartition(split).inputPartition
    val batchReader = partitionReaderFactory.createColumnarReader(inputPartition)
    val iter = new MetricsBatchIterator(new PartitionIterator[ColumnarBatch](batchReader))
    onTaskCompletion(batchReader.close())
    // TODO: SPARK-25083 remove the type erasure hack in data source scan
    new InterruptibleIterator(context, iter.asInstanceOf[Iterator[InternalRow]])
  }
}

object GpuDataSourceRDD {
  def apply(
      sc: SparkContext,
      inputPartitions: Seq[InputPartition],
      partitionReaderFactory: PartitionReaderFactory): GpuDataSourceRDD = {
    new GpuDataSourceRDD(sc, inputPartitions, partitionReaderFactory)
  }
}
