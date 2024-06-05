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

import com.nvidia.spark.rapids.DumpUtils.deserializeObject
import com.nvidia.spark.rapids.GpuExec

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.trees.UnaryLike

object DumpedExecReplayer extends Logging {

  def main(args: Array[String]): Unit = {
    // check arguments and get paths
    if (args.length != 1) {
      throw new IllegalStateException("Specify 1 args: <partition dump path>, " +
        "e.g. /tmp/lore/lore_id=41_plan_id=240/partition_id=0")
    }

    // start a Spark session with Spark-Rapids initialization
    SparkSession.builder()
      .master("local[*]")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .appName("Replaying Dumped Exec")
      .getOrCreate()

    val rootFolder: File = new File(args(0))
    val subFolders = rootFolder.listFiles().filter(_.isDirectory)
    if (subFolders.length < 1) {
      throw new IllegalStateException("There is no subfolder in the replay dir")
    }


    val planMetaPath = rootFolder.getParent + s"/plan.meta"
    if (!(new File(planMetaPath).exists() && new File(planMetaPath).isFile)) {
      throw new IllegalStateException(s"There is no plan.meta file in ${rootFolder.getParent}")
    }

    // restore SparkPlan
    val restoredExec = deserializeObject[GpuExec](planMetaPath)

    if (!restoredExec.isInstanceOf[UnaryLike[_]]) throw new IllegalStateException(
      s"For now, restored exec only supports UnaryLike: ${restoredExec.getClass}")
    val unaryLike = restoredExec.asInstanceOf[UnaryLike[_]]

    if (!unaryLike.child.isInstanceOf[GpuExec]) throw new IllegalStateException(
      s"For now, restored exec's child only supports GpuExec: " +
        s"${unaryLike.child.getClass}")
    val child = unaryLike.child.asInstanceOf[GpuExec]
    child.loreReplayInputDir = rootFolder.getPath
    restoredExec.loreIsReplayingOperator = true

    restoredExec.doExecuteColumnar().foreach(
      cb => {
        println(s"return ColumnarBatch with size: ${cb.numRows()}")
        cb.close()
      }
    )
  }
}