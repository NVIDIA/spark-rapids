package com.nvidia.spark.rapids.lore

import com.nvidia.spark.rapids.{DumpUtils, GpuExec}
import org.apache.hadoop.fs.Path

import org.apache.spark.{OneToOneDependency, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

case class GpuLoreDumpExec(child: SparkPlan, loreOutputInfo: LoreOutputInfo,
    hadoopConf: SerializableConfiguration) extends UnaryExecNode
  with GpuExec {
  override def output: Seq[Attribute] = child.output

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("GpuLoreDumpExec does not support row mode")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val input = child.executeColumnar()
    new GpuLoreDumpRDD(input, hadoopConf, loreOutputInfo)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    GpuLoreDumpExec(newChild, loreOutputInfo, hadoopConf)
}

class GpuLoreDumpRDD(input: RDD[ColumnarBatch], hadoopConf: SerializableConfiguration,
    loreOutputInfo: LoreOutputInfo) extends RDD[ColumnarBatch](input.context,
  Seq(new OneToOneDependency(input))) {
  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = input
    .compute(split, context).zipWithIndex.map { f =>
      val (batch, index) = f
      val outputPath = columnarBatchPath(split, index)
      val outputStream = outputPath.getFileSystem(hadoopConf.value).create(outputPath, false)
      DumpUtils.dumpToParquet(batch, outputStream)
      batch
    }

  override protected def getPartitions: Array[Partition] = input.getPartitions

  private def columnarBatchPath(partition: Partition, batchIndex: Int): Path = new Path(
    loreOutputInfo.path, s"part-${partition.index}/batch-$batchIndex.parquet")
}