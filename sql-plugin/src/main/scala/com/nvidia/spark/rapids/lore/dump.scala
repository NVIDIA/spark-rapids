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

import com.nvidia.spark.rapids.{DumpUtils, GpuColumnVector}
import com.nvidia.spark.rapids.GpuCoalesceExec.EmptyPartition
import com.nvidia.spark.rapids.lore.GpuLore.pathOfChild
import org.apache.hadoop.fs.Path

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.rapids.execution.GpuBroadcastHelper
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration


case class LoreDumpRDDInfo(idxInParent: Int, loreOutputInfo: LoreOutputInfo, attrs: Seq[Attribute],
    hadoopConf: Broadcast[SerializableConfiguration])

class GpuLoreDumpRDD(info: LoreDumpRDDInfo, input: RDD[ColumnarBatch])
  extends RDD[ColumnarBatch](input) with GpuLoreRDD {
  override def rootPath: Path = pathOfChild(info.loreOutputInfo.path, info.idxInParent)

  def saveMeta(): Unit = {
    val meta = LoreRDDMeta(input.getNumPartitions, this.getPartitions.map(_.index), info.attrs)
    GpuLore.dumpObject(meta, pathOfMeta, this.context.hadoopConfiguration)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    if (info.loreOutputInfo.outputLoreId.shouldOutputPartition(split.index)) {
      val originalIter = input.compute(split, context)
      new Iterator[ColumnarBatch] {
        var batchIdx: Int = -1
        var nextBatch: Option[ColumnarBatch] = None

        override def hasNext: Boolean = {
          if (batchIdx == -1) {
            loadNextBatch()
          }
          nextBatch.isDefined
        }

        override def next(): ColumnarBatch = {
          val ret = dumpCurrentBatch()
          loadNextBatch()
          if (!hasNext) {
            // This is the last batch, save the partition meta
            val partitionMeta = LoreRDDPartitionMeta(batchIdx, GpuColumnVector.extractTypes(ret))
            GpuLore.dumpObject(partitionMeta, pathOfPartitionMeta(split.index),
              info.hadoopConf.value.value)
          }
          ret
        }

        private def dumpCurrentBatch(): ColumnarBatch = {
          val outputPath = pathOfBatch(split.index, batchIdx)
          val outputStream = outputPath.getFileSystem(info.hadoopConf.value.value)
            .create(outputPath, false)
          DumpUtils.dumpToParquet(nextBatch.get, outputStream)
          nextBatch.get
        }

        private def loadNextBatch(): Unit = {
          if (originalIter.hasNext) {
            nextBatch = Some(originalIter.next())
          } else {
            nextBatch = None
          }
          batchIdx += 1
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

class SimpleRDD(_sc: SparkContext, data: Broadcast[Any], schema: StructType) extends
  RDD[ColumnarBatch](_sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    Seq(GpuBroadcastHelper.getBroadcastBatch(data, schema)).iterator
  }

  override protected def getPartitions: Array[Partition] = Array(EmptyPartition(0))
}
