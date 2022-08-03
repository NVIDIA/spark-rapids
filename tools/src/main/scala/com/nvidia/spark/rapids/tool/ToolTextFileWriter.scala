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

import java.io.FileOutputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import org.apache.spark.internal.Logging

/**
 * Class for writing local files, allows writing to distributed file systems.
 */
class ToolTextFileWriter(finalOutputDir: String, logFileName: String,
    finalLocationText: String) extends Logging {

  private val textOutputPath = new Path(s"$finalOutputDir/$logFileName")
  logWarning("text output path is: " + textOutputPath)
  private val hadoopConf = new Configuration()

  val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
  val isDefaultLocal = defaultFs == null || defaultFs == "file"
  val uri = textOutputPath.toUri

  // The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
  // Therefore, for local files, use FileOutputStream instead.
  // this overwrites existing path
  private var outFile: Option[FSDataOutputStream] = {
    if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
      Some(new FSDataOutputStream(new FileOutputStream(uri.getPath), null))
    } else {
      val fs = FileSystem.get(uri, hadoopConf)
      Some(fs.create(textOutputPath))
    }
  }

  def write(stringToWrite: String): Unit = {
    outFile.foreach(_.writeBytes(stringToWrite))
    if (outFile.nonEmpty) {
      logWarning(s"write : $stringToWrite outfile is: $outFile")
    } else {
      logWarning("outFile is empty not writing")
    }
  }

  def flush(): Unit = {
    outFile.foreach { file =>
      file.flush()
      file.hflush()
    }
  }

  def close(): Unit = {
    outFile.foreach { file =>
      logInfo(s"$finalLocationText output location: $textOutputPath")
      file.flush()
      file.hflush()
      file.close()
      outFile = None
    }
  }
}
