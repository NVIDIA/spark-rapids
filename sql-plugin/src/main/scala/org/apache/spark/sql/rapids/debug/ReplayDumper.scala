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

import java.io.{File, FileInputStream}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag

import com.nvidia.spark.rapids.{DumpUtils, GpuColumnVector, RapidsConf}
import com.nvidia.spark.rapids.Arm.withResource
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FSDataOutputStream, Path}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * Dump tool for replay feature, refer to dev doc `replay-exec.md`
 *
 */
case class ReplayDumper(
    hadoopConf: SerializableConfiguration,
    dumpDir: String,
    thresholdMS: Int,
    batchLimit: Int,
    execUUID: String
) extends Logging {

  // current dumped number
  private val currentNumOfColumnBatch: AtomicInteger = new AtomicInteger(0)

  private def getOutputStream(filePath: String): FSDataOutputStream = {
    val hadoopPath = new Path(filePath)
    val fs = hadoopPath.getFileSystem(hadoopConf.value)
    // multiple may call this concurrently, make overwrite as true
    fs.create(hadoopPath, false)
  }

  def dumpMeta[T: ClassTag](metaName: String, obj: T): Unit = {
    // use random Id in the file name, because there are multiple threads using the same
    // exec UUID, random Id will avoid file name collision
    val randomId = UUID.randomUUID().toString
    val byteBuff = SparkEnv.get.closureSerializer.newInstance().serialize[T](obj)
    val fos = getOutputStream(
      s"$dumpDir/execUUID_${execUUID}_meta_${metaName}_randomID_$randomId.meta")
    fos.write(byteBuff.array())
    fos.close()
    logWarning(s"dump project: dump project meta $metaName done")
  }

  def dumpColumnBatch(elapsedTimeNS: Long, cb: ColumnarBatch): Unit = {
    val elapsedTimeMS = elapsedTimeNS / 1000000L
    if (elapsedTimeMS > thresholdMS) {
      val currBatchIndex = currentNumOfColumnBatch.getAndIncrement()
      if (currBatchIndex < batchLimit) {
        logWarning(s"dump project: currentNumOfColumnBatch " +
            s"$currBatchIndex < batchLimit $batchLimit")

        logWarning(s"dump project: elapsedTime(MS) $elapsedTimeMS > thresholdMS $thresholdMS")
        logWarning(s"dump project: dump dir is $dumpDir")
        logWarning(s"dump project: threshold MS is $thresholdMS")
        logWarning(s"dump project: batch limit is $batchLimit")
        logWarning(s"dump project: execUUID $execUUID")

        // dump col types for column batch to remote storage
        val cbTypes = GpuColumnVector.extractTypes(cb)
        dumpMeta("cb_types", cbTypes)
        logWarning(s"dump project: dump column batch column types done")

        // dump data for column batch to /tmp dir
        val tmpDir = "/tmp"
        val randomId = UUID.randomUUID().toString
        val filePrefix = s"$tmpDir/" +
          s"execUUID_${execUUID}_cb_data_index_${currBatchIndex}_randomID_$randomId"
        val tmpParquetPathOpt = withResource(GpuColumnVector.from(cb)) { table =>
          DumpUtils.dumpToParquetFile(table, filePrefix)
        }
        // copy from /tmp dir to remote dir
        tmpParquetPathOpt.map { tmpParquetPath =>
          try {
            val tmpParquetName = new File(tmpParquetPath).getName
            val dataOutput = getOutputStream(s"$dumpDir/$tmpParquetName")
            val dataInput = new FileInputStream(tmpParquetPath)
            IOUtils.copy(dataInput, dataOutput)
            dataOutput.close()
            logWarning(s"dump project: dump column batch data done")
          } finally {
            // delete tmp file
            new File(tmpParquetPath).delete()
          }
        }.orElse {
          logError(s"dump project: dump column batch data failed !!! ")
          None
        }
      }
    }
  }
}

object ReplayDumper {
  def enabledReplayForProject(conf: RapidsConf): Boolean =
    conf.debugReplayExecType.contains("project")
}
