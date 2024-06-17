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

import scala.reflect.ClassTag

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuExec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DataType

case class LoreRDDMeta(numPartitions: Int, outputPartitions: Seq[Int])

case class LoreRDDPartitionMeta(numBatches: Int, dataType: Seq[DataType])

trait GpuLoreRDD {
  val rootPath: Path

  def pathOfMeta: Path = new Path(rootPath, "rdd.meta")

  def pathOfPartition(partitionIndex: Int): Path = {
    new Path(rootPath, s"partition-$partitionIndex")
  }

  def pathOfPartitionMeta(partitionIndex: Int): Path = {
    new Path(pathOfPartition(partitionIndex), "partition.meta")
  }

  def pathOfBatch(partitionIndex: Int, batchIndex: Int): Path = {
    new Path(pathOfPartition(partitionIndex), s"batch-$batchIndex.parquet")
  }
}


object GpuLore {
  def pathOfRootPlanMeta(rootPath: Path): Path = {
    new Path(rootPath, "plan.meta")
  }

  def dumpPlan[T <: SparkPlan : ClassTag](plan: T, rootPath: Path): Unit = {
    dumpObject(plan, pathOfRootPlanMeta(rootPath), plan.session.sparkContext.hadoopConfiguration)
  }

  def dumpObject[T: ClassTag](obj: T, path: Path, hadoopConf: Configuration): Unit = {
    withResource(path.getFileSystem(hadoopConf)) { fs =>
      withResource(fs.create(path, false)) { fout =>
        val serializerStream = SparkEnv.get.serializer.newInstance().serializeStream(fout)
        withResource(serializerStream) { ser =>
          ser.writeObject(obj)
        }
      }
    }
  }

  def loadObject[T: ClassTag](path: Path, hadoopConf: Configuration): T = {
    withResource(path.getFileSystem(hadoopConf)) { fs =>
      withResource(fs.open(path)) { fin =>
        val serializerStream = SparkEnv.get.serializer.newInstance().deserializeStream(fin)
        withResource(serializerStream) { ser =>
          ser.readObject().asInstanceOf[T]
        }
      }
    }
  }

  def pathOfChild(rootPath: Path, childIndex: Int): Path = {
    new Path(rootPath, s"child-$childIndex")
  }

  def restoreGpuExec(rootPath: Path, hadoopConf: Configuration): GpuExec = {
    val rootExec = loadObject[GpuExec](pathOfRootPlanMeta(rootPath), hadoopConf)
    // Load children
    rootExec.withNewChildren(rootExec.children.indices.map(GpuLoreReplayExec(_, rootPath)))
      .asInstanceOf[GpuExec]
  }
}
