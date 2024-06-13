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
package com.nvidia.spark.rapids.profiling

import java.io.File

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.DumpUtils.deserializeObject
import com.nvidia.spark.rapids.GpuColumnVector

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

class SimplePartition extends Partition {
  override def index: Int = 0
}

object ReplayDumpRDD {

}

class ReplayDumpRDD(
    @transient private val sparkSession: SparkSession,
    val path: String)
  extends RDD[ColumnarBatch](sparkSession.sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val rootFolder = new File(path)
    val subFolderIter = rootFolder.listFiles().filter(_.isDirectory).map(_.getPath).iterator
    val cbIter: Iterator[ColumnarBatch] = subFolderIter.map(replayDir => {

      val cbTypesPath = replayDir + "/col_types.meta"
      if (!(new File(cbTypesPath).exists() && new File(cbTypesPath).isFile)) {
        throw new IllegalStateException(s"There is no col_types.meta file in $replayDir")
      }

      val parquets = new File(replayDir).listFiles(f => f.getName.equals(s"cb_data.parquet"))
      if (parquets.size != 1) {
        throw new IllegalStateException(
          s"missing cb_data.parquet file in $replayDir")
      }
      val cbPath = parquets(0).getAbsolutePath

      // restore column types
      val restoredCbTypes = deserializeObject[Array[DataType]](cbTypesPath)

      // construct a column batch
      withResource(Table.readParquet(new File(cbPath))) { restoredTable =>
        println("a input batch with size " + restoredTable.getRowCount)
        GpuColumnVector.from(restoredTable, restoredCbTypes)
      }
    })
    cbIter
  }

  override protected def getPartitions: Array[Partition] = Seq(new SimplePartition()).toArray
}

