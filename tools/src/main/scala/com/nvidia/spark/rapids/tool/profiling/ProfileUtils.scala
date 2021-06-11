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

import java.io.FileNotFoundException

import scala.collection.mutable.Map

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.internal.Logging
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

  def isEventLogFile(fileName: String): Boolean = {
    fileName.startsWith(EVENT_LOG_FILE_NAME_PREFIX)
  }

  // Return an Array(Path) and Timestamp Map based on input path string
  def stringToPath(pathString: String): Map[Path, Long] = {
    val inputPath = new Path(pathString)
    val uri = inputPath.toUri
    val fs = FileSystem.get(uri, new Configuration())
    val pathsWithTimestamp: Map[Path, Long] = Map.empty[Path, Long]
    try {
      val fileStatus = fs.getFileStatus(inputPath)
      if ((fileStatus.isDirectory && isEventLogDir(fileStatus)) ||
        (fileStatus.isFile() && isEventLogFile(fileStatus.getPath().getName()))) {
        // either event logDir v2 directory or regular event log
        pathsWithTimestamp += (fileStatus.getPath -> fileStatus.getModificationTime)
      } else {
        // assume directory with event logs in it, we don't supported nested dirs, so
        // if event log dir within another one we skip it
        val (filesStatus, dirsStatus) = fs.listStatus(inputPath).partition(s => s.isFile)
        if (filesStatus != null) {
          filesStatus.map(a => pathsWithTimestamp += (a.getPath -> a.getModificationTime))
        }
        if (dirsStatus.nonEmpty) {
          logWarning("Skipping the following directories: " +
            s"${dirsStatus.map(_.getPath().getName()).mkString(", ")}")
        }
      }
    } catch {
      case e: FileNotFoundException => logWarning(s"$pathString not found, skipping!")
    }
    pathsWithTimestamp
  }


}
