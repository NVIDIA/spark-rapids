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

import java.io.{File, FilenameFilter, FileNotFoundException}

import org.apache.spark.sql.{DataFrame, SparkSession}

object ToolTestUtils {

  def getTestResourceFile(file: String): File = {
    new File(getClass.getClassLoader.getResource(file).getFile)
  }

  def getTestResourcePath(file: String): String = {
    getTestResourceFile(file).getCanonicalPath
  }

  def generateEventLog(eventLogDir: File, appName: String)
      (fun: SparkSession => DataFrame): String = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(appName)
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", eventLogDir.getAbsolutePath)
      .getOrCreate()

    // execute the query and generate events
    val df = fun(spark)
    df.collect()

    // close the event log
    spark.close()

    // find the event log
    val files = listFilesMatching(eventLogDir, !_.startsWith("."))
    if (files.length != 1) {
      throw new FileNotFoundException(s"Could not find event log in ${eventLogDir.getAbsolutePath}")
    }
    files.head.getAbsolutePath
  }

  def listFilesMatching(dir: File, matcher: String => Boolean): Array[File] = {
    dir.listFiles(new FilenameFilter {
      override def accept(file: File, s: String): Boolean = matcher(s)
    })
  }
}
