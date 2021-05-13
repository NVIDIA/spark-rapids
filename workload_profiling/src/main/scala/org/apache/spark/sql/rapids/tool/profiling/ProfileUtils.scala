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

package org.apache.spark.sql.rapids.tool.profiling

import java.io.IOException

import org.apache.log4j._

import org.apache.spark.internal.config
import org.apache.spark.sql.SparkSession

/**
 * object Utils provides toolkit functions
 *
 */

private object ProfileUtils {

  // Create a logger
  def createLogger(outputDir: String, logFileName: String): Logger = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
    val logger = Logger.getLogger(this.getClass.getName)
    logger.setLevel(Level.INFO)

    val layout = new PatternLayout("[%t] %-5p %c %x - %m%n")
    rootLogger.addAppender(new ConsoleAppender(layout))

    try {
      val fileAppender = new RollingFileAppender(layout, s"$outputDir/$logFileName")
      logger.addAppender(fileAppender)
    }
    catch {
      case e: IOException =>
        println("ERROR: Failed to add appender! Exiting... " + e.toString)
        System.exit(1)
    }
    logger
  }

  // Create a SparkSession in local mode
  def createSparkSession: SparkSession = {
    SparkSession
        .builder()
        .master("local")
        .appName("Rapids Spark Profiling Tool")
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
    if (properties.getOrElse(config.PLUGINS.key, "").contains("com.nvidia.spark.SQLPlugin")
        && properties.getOrElse("spark.rapids.sql.enabled", "true").toBoolean) {
      true
    }
    else {
      false
    }
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
}