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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.tool.profiling.ToolUtils

/**
 * object Utils provides toolkit functions
 *
 */
object ProfileUtils {

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

  // Return an Array(Path) based on input path string
  def stringToPath(pathString: String): ArrayBuffer[Path] = {
    val inputPath = new Path(pathString)
    val uri = inputPath.toUri
    val fs = FileSystem.get(uri, new Configuration())
    val allStatus = fs.listStatus(inputPath).filter(s => s.isFile)
    ArrayBuffer(FileUtil.stat2Paths(allStatus): _*)
  }
}
