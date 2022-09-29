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
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.internal.Logging

/**
 * Class for writing local files, allows writing to distributed file systems.
 */
class ToolTextFileWriter(
    finalOutputDir: String,
    logFileName: String,
    finalLocationText: String,
    hadoopConf: Option[Configuration] = None) extends Logging {

  // use same as Spark event log writer
  val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("660", 8).toShort)
  val LOG_FOLDER_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)
  private val textOutputPath = new Path(s"$finalOutputDir/$logFileName")
  logWarning("text output path is: " + textOutputPath)
  private val hadoopConfToUse = hadoopConf.getOrElse(new Configuration())

  private val defaultFs = FileSystem.getDefaultUri(hadoopConfToUse).getScheme
  private val isDefaultLocal = defaultFs == null || defaultFs == "file"
  private val uri = textOutputPath.toUri

  def getFileOutputPath: Path = textOutputPath

  // The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
  // Therefore, for local files, use FileOutputStream instead.
  // this overwrites existing path
  private var outFile: Option[FSDataOutputStream] = {
    logWarning(s"schem is: ${uri.getScheme} is default is $isDefaultLocal final output" +
      s" dir $finalOutputDir")
    val fs = FileSystem.get(uri, hadoopConfToUse)
    // TODO - test on dbfs, I don't think this works
    val outStream = if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
      logWarning("using local file system")
      FileSystem.mkdirs(fs, new Path(finalOutputDir), LOG_FOLDER_PERMISSIONS)
      Some(new FSDataOutputStream(new FileOutputStream(uri.getPath), null))
    } else {
      logWarning("using distributed file system")
      Some(fs.create(textOutputPath))
    }
    fs.setPermission(textOutputPath, LOG_FILE_PERMISSIONS)
    outStream
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
