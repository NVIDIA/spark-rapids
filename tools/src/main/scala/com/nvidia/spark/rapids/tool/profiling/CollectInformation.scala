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

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

case class StageMetrics(numTasks: Int, duration: String)

/**
 * CollectInformation mainly print information based on this event log:
 * Such as executors, parameters, etc.
 */
class CollectInformation(apps: Seq[ApplicationInfo],
    fileWriter: Option[ToolTextFileWriter]) {

  require(apps.nonEmpty)

  // Print Application Information
  def printAppInfo(): Unit = {
    val messageHeader = "\nApplication Information:\n"
    for (app <- apps) {
      if (app.allDataFrames.contains(s"appDF_${app.index}")) {
        app.runQuery(query = app.generateAppInfo, fileWriter = fileWriter,
          messageHeader = messageHeader)
      } else {
        fileWriter.foreach(_.write("No Application Information Found!\n"))
      }
    }
  }

  // Print rapids-4-spark and cuDF jar if CPU Mode is on.
  def printRapidsJAR(): Unit = {
    for (app <- apps) {
      if (app.gpuMode) {
        fileWriter.foreach(_.write("\nRapids Accelerator Jar and cuDF Jar:\n"))
        // Look for rapids-4-spark and cuDF jar
        val rapidsJar = app.classpathEntries.filterKeys(_ matches ".*rapids-4-spark.*jar")
        val cuDFJar = app.classpathEntries.filterKeys(_ matches ".*cudf.*jar")
        if (rapidsJar.nonEmpty) {
          rapidsJar.keys.foreach(k => fileWriter.foreach(_.write(s"$k\n")))
        }
        if (cuDFJar.nonEmpty) {
          cuDFJar.keys.foreach(k => fileWriter.foreach(_.write(s"$k\n")))
        }
      }
    }
  }

  // Print executor related information
  def printExecutorInfo(): Unit = {
    val messageHeader = "\nExecutor Information:\n"
    for (app <- apps) {
      if (app.allDataFrames.contains(s"executorsDF_${app.index}")) {
        app.runQuery(query = app.generateExecutorInfo + " order by cast(numExecutors as long)",
          fileWriter = fileWriter, messageHeader = messageHeader)
      } else {
        fileWriter.foreach(_.write("No Executor Information Found!\n"))
      }
    }
  }

  // Print job related information
  def printJobInfo(): Unit = {
    val messageHeader = "\nJob Information:\n"
    for (app <- apps) {
      if (app.allDataFrames.contains(s"jobDF_${app.index}")) {
        app.runQuery(query = app.jobtoStagesSQL,
        fileWriter = fileWriter, messageHeader = messageHeader)
      } else {
        fileWriter.foreach(_.write("No Job Information Found!\n"))
      }
    }
  }

  // Print Rapids related Spark Properties
  def printRapidsProperties(): Unit = {
    val messageHeader = "\nSpark Rapids parameters set explicitly:\n"
    for (app <- apps) {
      if (app.allDataFrames.contains(s"propertiesDF_${app.index}")) {
        app.runQuery(query = app.generateRapidsProperties + " order by propertyName",
          fileWriter = fileWriter, messageHeader = messageHeader)
      } else {
        fileWriter.foreach(_.write("No Spark Rapids parameters Found!\n"))
      }
    }
  }

  def printSQLPlans(outputDirectory: String): Unit = {
    for (app <- apps) {
      val planFileWriter = new ToolTextFileWriter(outputDirectory,
        s"planDescriptions-${app.appId}")
      try {
        for ((sqlID, planDesc) <- app.physicalPlanDescription.toSeq.sortBy(_._1)) {
          planFileWriter.write("\n=============================\n")
          planFileWriter.write(s"Plan for SQL ID : $sqlID")
          planFileWriter.write("\n=============================\n")
          planFileWriter.write(planDesc)
        }
      } finally {
        planFileWriter.close()
      }
    }
  }

  def generateDot(outputDirectory: String, accumsOpt: Option[DataFrame]): Unit = {
    for (app <- apps) {
      val requiredDataFrames = Seq("sqlMetricsDF", "driverAccumDF",
          "taskStageAccumDF", "taskStageAccumDF")
        .map(name => s"${name}_${app.index}")
      if (requiredDataFrames.forall(app.allDataFrames.contains)) {
        val start = System.nanoTime()
        val accums = accumsOpt.getOrElse(app.runQuery(app.generateSQLAccums))

        val accumIdToStageId = app.runQuery(
          s"""
             | select
             | stageId,
             | accumulatorId
             | from taskStageAccumDF_${app.index}
             | """.stripMargin).groupBy("accumulatorId").agg(
          max(col("stageId")).alias("stageId")).collect().map { row =>
          (row.getLong(0), row.getInt(1))
        }.toMap

        val formatter = java.text.NumberFormat.getIntegerInstance

        val stageIdToStageMetrics = app.runQuery(
          s"""
             |select
             | stageId,
             | duration
             | from taskDF_${app.index}
             |""".stripMargin).groupBy(col("stageId"))
            .agg(min(col("duration")).alias("min_dur"),
              max(col("duration")).alias("max_dur"),
              mean(col("duration")).alias("mean_dur"),
              count(col("duration")).alias("num_tasks"))
            .collect().map { row =>
          val stageId = row.getInt(0)
          val minDur = row.getLong(1)
          val maxDur = row.getLong(2)
          val meanDur = row.getDouble(3)
          val numTasks = row.getLong(4)
          (stageId, StageMetrics(numTasks.toInt,
            s"MIN: ${formatter.format(minDur)} ms " +
                s"MAX: ${formatter.format(maxDur)} ms " +
                s"AVG: ${formatter.format(meanDur)} ms"))
        }.toMap

        val accumSummary = accums
          .select(col("sqlId"), col("accumulatorId"), col("max_value"))
          .collect()
        val sqlIdToMaxMetric = new mutable.HashMap[Long, ArrayBuffer[(Long,Long)]]()
        for (row <- accumSummary) {
          val list = sqlIdToMaxMetric.getOrElseUpdate(row.getLong(0),
            new ArrayBuffer[(Long, Long)]())
          list += row.getLong(1) -> row.getLong(2)
        }

        val sqlPlansMap = app.sqlPlan.map { case (sqlId, sparkPlanInfo) =>
          sqlId -> ((sparkPlanInfo, app.physicalPlanDescription(sqlId)))
        }
        for ((sqlID,  (planInfo, physicalPlan)) <- sqlPlansMap) {
          val dotFileWriter = new ToolTextFileWriter(outputDirectory,
            s"${app.appId}-query-$sqlID.dot")
          try {
            val metrics = sqlIdToMaxMetric.getOrElse(sqlID, Seq.empty).toMap
            GenerateDot.writeDotGraph(QueryPlanWithMetrics(planInfo, metrics),
              physicalPlan, accumIdToStageId, stageIdToStageMetrics, dotFileWriter,
              sqlID, app.appId)
          } finally {
            dotFileWriter.close()
          }
        }

        val duration = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        fileWriter.foreach(_.write(s"Generated DOT graphs for app ${app.appId} " +
          s"to $outputDirectory in $duration second(s)\n"))
      } else {
        val missingDataFrames = requiredDataFrames.filterNot(app.allDataFrames.contains)
        fileWriter.foreach(_.write(s"Could not generate DOT graph for app ${app.appId} " +
          s"because of missing data frames: ${missingDataFrames.mkString(", ")}\n"))
      }
    }
  }

  // Print SQL Plan Metrics
  def printSQLPlanMetrics(shouldGenDot: Boolean, outputDir: String): Unit = {
    for (app <- apps){
      if (app.allDataFrames.contains(s"sqlMetricsDF_${app.index}") &&
        app.allDataFrames.contains(s"driverAccumDF_${app.index}") &&
        app.allDataFrames.contains(s"taskStageAccumDF_${app.index}") &&
        app.allDataFrames.contains(s"jobDF_${app.index}") &&
        app.allDataFrames.contains(s"sqlDF_${app.index}")) {
        val messageHeader = "\nSQL Plan Metrics for Application:\n"
        val accums = app.runQuery(app.generateSQLAccums, fileWriter = fileWriter,
          messageHeader=messageHeader)
        if (shouldGenDot) {
          generateDot(outputDir, Some(accums))
        }
      }
    }
  }
}
