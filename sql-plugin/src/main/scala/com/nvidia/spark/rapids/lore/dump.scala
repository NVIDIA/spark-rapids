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

package com.nvidia.spark.rapids.lore

import com.nvidia.spark.rapids.{DumpUtils, GpuColumnVector, GpuExec}
import org.apache.hadoop.fs.Path

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration


case class GpuLoreDumpExec(idxInParent: Int, child: SparkPlan, loreOutputInfo: LoreOutputInfo)
  extends UnaryExecNode with GpuExec {
  override def output: Seq[Attribute] = child.output

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("GpuLoreDumpExec does not support row mode")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val input = child.executeColumnar()
    val rdd = new GpuLoreDumpRDD(idxInParent, input, loreOutputInfo)
    rdd.saveMeta()
    rdd
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    GpuLoreDumpExec(idxInParent, newChild, loreOutputInfo)
}


class GpuLoreDumpRDD(idxInParent: Int, input: RDD[ColumnarBatch], loreOutputInfo: LoreOutputInfo)
  extends RDD[ColumnarBatch](input) with GpuLoreRDD {
  override val rootPath: Path = new Path(loreOutputInfo.path, s"input-$idxInParent")

  private val hadoopConf = new SerializableConfiguration(this.context.hadoopConfiguration)

  def saveMeta(): Unit = {
    val meta = LoreRDDMeta(input.getNumPartitions, this.getPartitions.map(_.index))
    GpuLore.dumpObject(meta, pathOfMeta, this.context.hadoopConfiguration)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    if (loreOutputInfo.outputLoreId.shouldOutputPartition(split.index)) {
      val originalIter = input.compute(split, context)
      new Iterator[ColumnarBatch] {
        var batchIdx: Int = -1
        var nextBatch: Option[ColumnarBatch] = None

        loadNextBatch()

        override def hasNext: Boolean = {
          nextBatch.isDefined
        }

        override def next(): ColumnarBatch = {
          val ret = dumpCurrentBatch()
          loadNextBatch()
          if (!hasNext) {
            // This is the last batch, save the partition meta
            val partitionMeta = LoreRDDPartitionMeta(batchIdx+1, GpuColumnVector.extractTypes(ret))
            GpuLore.dumpObject(partitionMeta, pathOfPartitionMeta(split.index), hadoopConf.value)
          }
          ret
        }

        private def dumpCurrentBatch(): ColumnarBatch = {
          val outputPath = pathOfBatch(split.index, batchIdx)
          val outputStream = outputPath.getFileSystem(hadoopConf.value).create(outputPath, false)
          DumpUtils.dumpToParquet(nextBatch.get, outputStream)
          nextBatch.get
        }

        private def loadNextBatch(): Unit = {
          if (originalIter.hasNext) {
            nextBatch = Some(originalIter.next())
            batchIdx += 1
          }
        }
      }
    } else {
      input.compute(split, context)
    }
  }

  override protected def getPartitions: Array[Partition] = {
    input.partitions
  }
}

