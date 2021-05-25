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

import java.io.File
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo


/**
 * CollectInformation mainly print information based on this event log:
 * Such as executors, parameters, etc.
 */
class CollectInformation(apps: ArrayBuffer[ApplicationInfo]) {

  require(apps.nonEmpty)
  private val fileWriter = apps.head.fileWriter

  // Print Application Information
  def printAppInfo(): Unit = {
    val messageHeader = "Application Information:\n"
    for (app <- apps) {
      app.runQuery(query = app.generateAppInfo, writeToFile = true, messageHeader = messageHeader)
    }
  }

  // Print rapids-4-spark and cuDF jar if CPU Mode is on.
  def printRapidsJAR(): Unit = {
    for (app <- apps) {
      if (app.gpuMode) {
        fileWriter.write(s"Application ${app.appId} (index=${app.index}) 's" +
            " Rapids Accelerator Jar and cuDF Jar:")
        // Look for rapids-4-spark and cuDF jar
        val rapidsJar = app.classpathEntries.filterKeys(_ matches ".*rapids-4-spark.*jar")
        val cuDFJar = app.classpathEntries.filterKeys(_ matches ".*cudf.*jar")
        if (rapidsJar.nonEmpty) {
          rapidsJar.keys.foreach(k => fileWriter.write(k))
        }
        if (cuDFJar.nonEmpty) {
          cuDFJar.keys.foreach(k => fileWriter.write(k))
        }
      }
    }
  }

  // Print executor related information
  def printExecutorInfo(): Unit = {
    val messageHeader = "\n\nExecutor Information:\n"
    for (app <- apps) {
      app.runQuery(query = app.generateExecutorInfo + " order by cast(executorID as long)",
        writeToFile = true, messageHeader = messageHeader)
    }
  }

  // Print Rapids related Spark Properties
  def printRapidsProperties(): Unit = {
    val messageHeader = "\n\nSpark Rapids parameters set explicitly:\n"
    for (app <- apps) {
      app.runQuery(query = app.generateRapidsProperties + " order by key", writeToFile = true,
        messageHeader = messageHeader)
    }
  }

  def generateDot(): Unit = {
    for (app <- apps) {
      val requiredDataFrames = Seq("sqlMetricsDF", "driverAccumDF",
          "taskStageAccumDF", "taskStageAccumDF")
        .map(name => s"${name}_${app.index}")
      if (requiredDataFrames.forall(app.allDataFrames.contains)) {
        val accums = app.runQuery(app.generateSQLAccums)
        val start = System.nanoTime()
        val accumSummary = accums
          .select(col("sqlId"), col("accumulatorId"), col("max_value"))
          .collect()
        val map = new mutable.HashMap[Long, ArrayBuffer[(Long,Long)]]()
        for (row <- accumSummary) {
          val list = map.getOrElseUpdate(row.getLong(0), new ArrayBuffer[(Long, Long)]())
          list += row.getLong(1) -> row.getLong(2)
        }
        val outDir = new File(app.args.outputDirectory())
        for ((sqlID, planInfo) <- app.sqlPlan) {
          val fileDir = new File(outDir, s"${app.appId}-query-$sqlID")
          fileDir.mkdirs()
          val metrics = map.getOrElse(sqlID, Seq.empty).toMap
          GenerateDot.generateDotGraph(
            QueryPlanWithMetrics(planInfo, metrics), None, fileDir, sqlID + ".dot")
        }
        val duration = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        fileWriter.write(s"Generated DOT graphs for app ${app.appId} " +
          s"to ${outDir.getAbsolutePath} in $duration second(s)\n")
      } else {
        val missingDataFrames = requiredDataFrames.filterNot(app.allDataFrames.contains)
        fileWriter.write(s"Could not generate DOT graph for app ${app.appId} " +
          s"because of missing data frames: ${missingDataFrames.mkString(", ")}\n")
      }
    }
  }
}