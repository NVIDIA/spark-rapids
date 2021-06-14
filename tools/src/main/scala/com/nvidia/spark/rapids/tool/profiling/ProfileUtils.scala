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

package com.nvidia.spark.rapids.tool.profiling

import java.io.{FileNotFoundException, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.mutable.Map

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.EventLogFileWriter
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.tool.profiling.ToolUtils

/**
 * object Utils provides toolkit functions
 *
 */
object ProfileUtils extends Logging {

  // Create a SparkSession
  def createSparkSession: SparkSession = {
    SparkSession
        .builder()
        .appName("Rapids Spark Qualification/Profiling Tool")
        .getOrCreate()
  }

  // Convert a null-able String to Option[Long]
  def stringToLong(in: String): Option[Long] = try {
    Some(in.toLong)
  } catch {
    case _: NumberFormatException => None
  }

  // Convert Option[Long] to String
  def optionLongToString(in: Option[Long]): String = try {
    in.get.toString
  } catch {
    case _: NoSuchElementException => ""
  }

  // Check if the job/stage is GPU mode is on
  def isGPUMode(properties: collection.mutable.Map[String, String]): Boolean = {
    ToolUtils.isGPUMode(properties)
  }

  // Return None if either of them are None
  def optionLongMinusOptionLong(a: Option[Long], b: Option[Long]): Option[Long] =
    try Some(a.get - b.get) catch {
      case _: NoSuchElementException => None
    }

  // Return None if either of them are None
  def OptionLongMinusLong(a: Option[Long], b: Long): Option[Long] =
    try Some(a.get - b) catch {
      case _: NoSuchElementException => None
    }

  val EVENT_LOG_DIR_NAME_PREFIX = "eventlog_v2_"
  val EVENT_LOG_FILE_NAME_PREFIX = "events_"

  def isEventLogDir(status: FileStatus): Boolean = {
    status.isDirectory && status.getPath.getName.startsWith(EVENT_LOG_DIR_NAME_PREFIX)
  }

  // This only checks the name of the path
  def isEventLogDir(path: String): Boolean = {
    path.startsWith(EVENT_LOG_DIR_NAME_PREFIX)
  }

  // https://github.com/apache/spark/blob/0494dc90af48ce7da0625485a4dc6917a244d580/
  // core/src/main/scala/org/apache/spark/io/CompressionCodec.scala#L67
  val SPARK_SHORT_COMPRESSION_CODEC_NAMES = Set("lz4", "lzf", "snappy", "zstd")

  def eventLogNameFilter(logFile: Path): Boolean = {
    EventLogFileWriter.codecName(logFile)
      .forall(suffix => SPARK_SHORT_COMPRESSION_CODEC_NAMES.contains(suffix))
  }

  class GZIPCompressionCodec(conf: SparkConf) extends CompressionCodec {

    override def compressedOutputStream(s: OutputStream): OutputStream = {
      new GZIPOutputStream(s)
    }

    override def compressedInputStream(s: InputStream): InputStream = new GZIPInputStream(s)
  }

  // Return an Array(Path) and Timestamp Map based on input path string
  def stringToPath(pathString: String): Map[Path, Long] = {
    val inputPath = new Path(pathString)
    val uri = inputPath.toUri
    val fs = FileSystem.get(uri, new Configuration())
    val pathsWithTimestamp: Map[Path, Long] = Map.empty[Path, Long]
    try {
      val fileStatus = fs.getFileStatus(inputPath)
      val filePath = fileStatus.getPath()
      val fileName = filePath.getName()
      if (!eventLogNameFilter(filePath)) {
        logWarning(s"File: $fileName it not a supported file type. " +
          "Supported compression types are: " +
          s"${SPARK_SHORT_COMPRESSION_CODEC_NAMES.mkString(", ")}. " +
          "Skipping this file.")
      } else if (fileStatus.isDirectory && isEventLogDir(fileStatus)) {
        // either event logDir v2 directory or regular event log
        pathsWithTimestamp += (fileStatus.getPath -> fileStatus.getModificationTime)
      } else {
        // assume either single event log or directory with event logs in it, we don't
        // supported nested dirs, so if event log dir within another one we skip it
        val (validLogs, invalidLogs) = fs.listStatus(inputPath)
          .partition(s => {
            val name = s.getPath().getName()
            logWarning(s"s is: $name dir ${s.isDirectory} file: ${s.isFile}")
            (s.isFile || (s.isDirectory && isEventLogDir(name)))
          })
        val (logsSupported, unsupportLogs) =
          validLogs.partition(l => eventLogNameFilter(l.getPath()))
        logWarning("file status is are: " + logsSupported.map(_.getPath).mkString(", "))
        if (logsSupported != null) {
          logsSupported.map(a => pathsWithTimestamp += (a.getPath -> a.getModificationTime))
        }
        if (invalidLogs.nonEmpty) {
          logWarning("Skipping the following directories: " +
            s"${invalidLogs.map(_.getPath().getName()).mkString(", ")}")
        }
        if (unsupportLogs.nonEmpty) {
          logWarning(s"Files: ${unsupportLogs.map(_.getPath.getName).mkString(", ")} " +
            s"have unsupported file types. Supported compression types are: " +
            s"${SPARK_SHORT_COMPRESSION_CODEC_NAMES.mkString(", ")}. " +
            "Skipping these files.")
        }
      }
    } catch {
      case e: FileNotFoundException => logWarning(s"$pathString not found, skipping!")
    }
    pathsWithTimestamp
  }
}
