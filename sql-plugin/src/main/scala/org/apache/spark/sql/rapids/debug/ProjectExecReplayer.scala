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

package org.apache.spark.sql.rapids.debug

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}

import scala.reflect.ClassTag

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.{GpuColumnVector, GpuProjectExec, GpuTieredProject, NoopMetric}
import com.nvidia.spark.rapids.Arm.withResource
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType

/**
 * Replayer for dumped Project Exec runtime.
 * for how to dump, refer to dev doc `replay-exec.md`
 */
object ProjectExecReplayer extends Logging {
  private def deserializeObject[T: ClassTag](readPath: String): T = {
    val bytes = Files.readAllBytes(Paths.get(readPath))
    val buffer = ByteBuffer.wrap(bytes)
    SparkEnv.get.closureSerializer.newInstance().deserialize(buffer)
  }

  /**
   * Replay data dir should contains, e.g.::
   * - execUUID_xxx_meta_GpuTieredProject_randomID_xxx.meta
   * - execUUID_xxx_meta_cb_types_randomID_xxx.meta
   * - execUUID_xxx_cb_data_index_0_randomID_xxx_744305176.parquet
   * Here first xxx is UUID for a Project.
   * Only replay one Parquet in the replay path
   * @param args specify data path and project UUID, e.g.:
   *   /path/to/replay <UUID>
   */
  def main(args: Array[String]): Unit = {
    // check arguments and get paths
    if (args.length < 2) {
      logError("Project Exec replayer: Specify 2 args: data path and projectUUID")
      printUsage()
      return
    }
    var replayDir = args(0)
    val projectUUID = args(1)
    logWarning("Project Exec replayer: start running.")

    // start a Spark session with Spark-Rapids initialization
    SparkSession.builder()
        .master("local[*]")
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .appName("Replay")
        .getOrCreate()
    logWarning("Project Exec replayer: started a Spark session")

    // copy to local dir
    replayDir = copyToLocal(replayDir)

    // execUUID_${execUUID}_meta_${metaName}
    val cbTypesFiles = new File(replayDir).listFiles(
      f => f.getName.startsWith(s"execUUID_${projectUUID}_meta_cb_types_randomID_") &&
        f.getName.endsWith(".meta"))
    if (cbTypesFiles == null || cbTypesFiles.isEmpty) {
      logError(s"Project Exec replayer: " +
        s"there is no execUUID_xxx_meta_cb_types_randomID_xxx.meta file in $replayDir")
      printUsage()
      return
    }
    val cbTypesPath = replayDir + "/" + cbTypesFiles(0).getName

    val projectMetaFiles = new File(replayDir).listFiles(
      f => f.getName.startsWith(s"execUUID_${projectUUID}_meta_GpuTieredProject_randomID_") &&
        f.getName.endsWith(".meta"))
    if (projectMetaFiles == null || projectMetaFiles.isEmpty) {
      logError(s"Project Exec replayer: " +
        s"there is no execUUID_xxx_meta_GpuTieredProject_randomID_xxx.meta file in $replayDir")
      printUsage()
      return
    }
    val projectMetaPath = replayDir + "/" + projectMetaFiles(0).getName

    // find a Parquet file, e.g.: xxx_cb_data_101656570.parquet
    val parquets = new File(replayDir).listFiles(
      f => f.getName.startsWith(s"execUUID_${projectUUID}_cb_data_index_") &&
          f.getName.contains("randomID") &&
          f.getName.endsWith(".parquet"))
    if (parquets == null || parquets.isEmpty) {
      logError(s"Project Exec replayer: " +
          s"there is no execUUID_xxx_cb_data_index_x_randomID_xxx.parquet file in $replayDir")
      printUsage()
      return
    }
    // NOTE: only replay 1st parquet
    val cbPath = replayDir + "/" + parquets(0).getName

    // restore project meta
    val restoredProject: GpuTieredProject = deserializeObject[GpuTieredProject](projectMetaPath)
    // print expressions in project
    restoredProject.exprTiers.foreach { exprs =>
      exprs.foreach { expr =>
        logWarning(s"Project Exec replayer: Project expression: ${expr.sql}")
      }
    }
    logWarning("Project Exec replayer: restored Project Exec meta")

    // restore column batch data
    val restoredCbTypes = deserializeObject[Array[DataType]](cbTypesPath)

    // replay
    withResource(Table.readParquet(new File(cbPath))) { restoredTable =>
      // this `restoredCb` will be closed in the `projectCb`
      val restoredCb = GpuColumnVector.from(restoredTable, restoredCbTypes)
      logWarning("Project Exec replayer: restored column batch data")
      logWarning("Project Exec replayer: begin to replay")
      // execute project
      withResource(GpuProjectExec.projectCb(restoredProject, restoredCb, NoopMetric)) { retCB =>
        logWarning(s"Project Exec replayer: project result has ${retCB.numRows()} rows.")
      }
      logWarning("Project Exec replayer: project replay completed successfully!!!")
    }
  }

  private def printUsage(): Unit = {
    val usageString =
"""
Usage is:
$SPARK_HOME/bin/spark-submit \
  --class org.apache.spark.sql.rapids.test.ProjectExecReplayer \
  --conf spark.rapids.sql.explain=ALL \
  --master local[*] \
  --jars ${PLUGIN_JAR} \
  ${PLUGIN_JAR} <dumped path> <project UUID>
e.g.:
The files in dumped path(hdfs://host/path/to/dir) are:
  execUUID_xxx_meta_GpuTieredProject_randomID_xxx.meta
  execUUID_xxx_meta_cb_types_randomID_xxx.meta
  execUUID_xxx_cb_data_index_0_randomID_xxx_744305176.parquet
The project UUID is the `xxx` after execUUID
The command will be:
$SPARK_HOME/bin/spark-submit \
  --class org.apache.spark.sql.rapids.test.ProjectExecReplayer \
  --conf spark.rapids.sql.explain=ALL \
  --master local[*] \
  --jars ${PLUGIN_JAR} \
  ${PLUGIN_JAR} hdfs://host/path/to/dir 123e4567-e89b-12d3-a456-426655440000
Note:
  dumped path could be remote path or local path, e.g.:
   hdfs://host/path/to/dir
   file:/path/to/dir
  Replay will only run one parquet file in the replay dir.
"""
    logError(usageString)
  }

  private def copyToLocal(replayDir: String): String = {
    // used to access remote path
    val hadoopConf = SparkSession.active.sparkContext.hadoopConfiguration
    // copy from remote to local
    val replayDirPath = new Path(replayDir)
    val fs = replayDirPath.getFileSystem(hadoopConf)

    if (!fs.getScheme.equals("file")) {
      // remote file system; copy to local dir
      val uuid = java.util.UUID.randomUUID.toString
      val localReplayDir = s"/tmp/replay-exec-tmp-$uuid"
      fs.copyToLocalFile(replayDirPath, new Path(s"/tmp/replay-exec-tmp-$uuid"))
      logWarning(s"Copied from remote dir $replayDir to local dir $localReplayDir")
      localReplayDir
    } else {
      // Remove the 'file:' prefix. e.g.: file:/a/b/c => /a/b/c
      replayDirPath.toUri.getPath
    }
  }
}
