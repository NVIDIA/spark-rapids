/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids.tool

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * Class for writing local files, allows writing to distributed file systems.
 */
class ToolTextFileWriter(finalOutputDir: String, logFileName: String) extends Logging {

  private val textOutputPath = new Path(s"$finalOutputDir/$logFileName")
  private val fs = FileSystem.get(textOutputPath.toUri, new Configuration())
  // this overwrites existing path
  private var outFile: Option[FSDataOutputStream] = Some(fs.create(textOutputPath))
  logInfo(s"Output directory: $finalOutputDir")

  def write(stringToWrite: String): Unit = {
    outFile.foreach(_.writeBytes(stringToWrite))
  }

  def write(df: DataFrame, numOutputRows: Int): Unit = {
    outFile.foreach(_.writeBytes(ToolUtils.showString(df, numOutputRows)))
  }

  def close(): Unit = {
    outFile.foreach { file =>
      logInfo(s"Output location: $finalOutputDir")
      file.flush()
      file.close()
      outFile = None
    }
  }
}
