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

import scala.collection.mutable.{ArrayBuffer, HashMap}

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

case class StageMetrics(numTasks: Int, duration: String)

/**
 * CollectInformation mainly print information based on this event log:
 * Such as executors, parameters, etc.
 */
class CollectInformation(apps: Seq[ApplicationInfo],
    fileWriter: Option[ToolTextFileWriter], numOutputRows: Int) extends Logging {

  require(apps.nonEmpty)

  // Print Application Information
  def printAppInfo(): Seq[AppInfoProfileResults] = {
    val messageHeader = "\nApplication Information:\n"
    fileWriter.foreach(_.write(messageHeader))

    val allRows = apps.map { app =>
      val a = app.appInfo
      AppInfoProfileResults(app.index, a.appName, a.appId,
        a.sparkUser,  a.startTime, a.endTime, a.duration,
        a.durationStr, a.sparkVersion, a.pluginEnabled)
    }.toList
    if (allRows.size > 0) {
      val sortedRows = allRows.sortBy(cols => (cols.appIndex))
      val headerNames = sortedRows.head.outputHeaders
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        headerNames, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
      sortedRows
    } else {
      fileWriter.foreach(_.write("No Application Information Found!\n"))
      Seq.empty
    }
  }

  // Print rapids-4-spark and cuDF jar if CPU Mode is on.
  def printRapidsJAR(): Unit = {
    val headers = Seq("appIndex", "Rapids4Spark jars")
    fileWriter.foreach(_.write("\nRapids Accelerator Jar and cuDF Jar:\n"))
    val allRows = apps.flatMap { app =>
      if (app.gpuMode) {
        // Look for rapids-4-spark and cuDF jar
        val rapidsJar = app.classpathEntries.filterKeys(_ matches ".*rapids-4-spark.*jar")
        val cuDFJar = app.classpathEntries.filterKeys(_ matches ".*cudf.*jar")

        val cols = (rapidsJar.keys ++ cuDFJar.keys).toSeq
        val rowsWithAppindex = cols.map(jar => Seq(app.index.toString, jar))
        rowsWithAppindex
      } else {
        Seq.empty
      }
    }
    if (allRows.size > 0) {
      val sortedRows = allRows.sortBy(cols => (cols(0).toLong, cols(1)))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        headers, sortedRows)
      fileWriter.foreach(_.write(outStr + "\n"))
    } else {
      fileWriter.foreach(_.write("No Rapids 4 Spark Jars Found!\n"))
    }
  }

  // Print read data schema information
  def printDataSourceInfo(): Unit = {
    val messageHeader = "\nData Source Information:\n"
    fileWriter.foreach(_.write(messageHeader))
    val outputHeaders =
      Seq("appIndex", "sqlID", "format", "location", "pushedFilters", "schema")
    val allRows = apps.flatMap { app =>
      if (app.dataSourceInfo.size > 0) {
        app.dataSourceInfo.map { ds =>
          Seq(app.index.toString, ds.sqlID.toString, ds.format, ds.location,
            ds.pushedFilters, ds.schema)
        }
      } else {
        Seq.empty
      }
    }
    if (allRows.size > 0) {
      val sortedRows = allRows.sortBy(cols => (cols(0).toLong, cols(1).toLong, cols(3), cols(5)))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No Data Source Information Found!\n"))
    }
  }

  def getDataSourceInfo(app: ApplicationInfo, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val df = app.dataSourceInfo.toDF.sort(asc("sqlID"), asc("location"), asc("schema"),
      asc("pushedFilters"))
    df.withColumn("appIndex", lit(app.index.toString))
      .select("appIndex", df.columns:_*)
  }

  // Print executor related information
  def printExecutorInfo(): Unit = {
    val messageHeader = "\nExecutor Information:\n"
    fileWriter.foreach(_.write(messageHeader))
    if (apps.size > 0) {
      val allRows = apps.flatMap { app =>
        if (app.executorIdToInfo.size > 0) {
          // first see if any executors have different resourceProfile ids
          val groupedExecs = app.executorIdToInfo.groupBy(_._2.resourceProfileId)

          groupedExecs.map { case (rpId, execs) =>
            val rp = app.resourceProfIdToInfo.get(rpId)
            val execMem = rp.map(_.executorResources.get(ResourceProfile.MEMORY)
              .map(_.amount).getOrElse(0L))
            val execCores = rp.map(_.executorResources.get(ResourceProfile.CORES)
              .map(_.amount).getOrElse(0L))
            val execGpus = rp.map(_.executorResources.get(ResourceProfile.CORES)
              .map(_.amount).getOrElse(0L))
            val taskCpus = rp.map(_.taskResources.get(ResourceProfile.CPUS)
              .map(_.amount).getOrElse(0L))
            val taskGpus = rp.map(_.taskResources.get("gpu").map(_.amount).getOrElse(0))
            val execOffHeap = rp.map(_.executorResources.get(ResourceProfile.OFFHEAP_MEM)
              .map(_.amount).getOrElse(0L))

            val numExecutors = execs.size
            val exec = execs.head._2
            // We could print a lot more information here if we decided, more like the Spark UI
            // per executor info.
            ExecutorInfoProfileResult(app.index, rpId, numExecutors,
              exec.totalCores, exec.maxMemory, exec.totalOnHeap,
              exec.totalOffHeap, execMem, execGpus, execOffHeap, taskCpus, taskGpus)
          }
        } else {
          Seq.empty
        }
      }
      if (allRows.size > 0) {
        val sortedRows = allRows.sortBy(cols => (cols.appIndex, cols.numExecutors))
        val outputHeaders = sortedRows.head.outputHeaders
        val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
          outputHeaders, sortedRows.map(_.convertToSeq))
        fileWriter.foreach(_.write(outStr))
      } else {
        fileWriter.foreach(_.write("No Executor Information Found!\n"))
      }
    } else {
      fileWriter.foreach(_.write("No Executor Information Found!\n"))
    }
  }

  // Print job related information
  def printJobInfo(): Seq[JobInfoProfileResult] = {
    val messageHeader = "\nJob Information:\n"
    fileWriter.foreach(_.write(messageHeader))

    val allRows = apps.flatMap { app =>
      if (app.jobIdToInfo.size > 0) {
        app.jobIdToInfo.map { case (jobId, j) =>
          JobInfoProfileResult(app.index, j.jobID,
            s"[${j.stageIds.mkString(",")}]",
            j.sqlID)
        }
      } else {
        Seq.empty
      }
    }
    if (allRows.size > 0) {
      val sortedRows = allRows.sortBy(cols => (cols.appIndex, cols.jobID))
      val outputHeaders = sortedRows.head.outputHeaders
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
      sortedRows
    } else {
      fileWriter.foreach(_.write("No Job Information Found!\n"))
      Seq.empty
    }
  }

  // Print Rapids related Spark Properties
  def printRapidsProperties(): Unit = {
    val messageHeader = "\nSpark Rapids parameters set explicitly:\n"
    fileWriter.foreach(_.write(messageHeader))
    val outputHeaders = ArrayBuffer("propertyName")
    val props = HashMap[String, ArrayBuffer[String]]()
    var numApps = 0
    val filtered = apps.filter(_.sparkProperties.size > 0)
    filtered.foreach { app =>
      numApps += 1
      outputHeaders += s"appIndex_${app.index}"
      val rapidsRelated = app.sparkProperties.filterKeys { key =>
        key.startsWith("spark.rapids") || key.startsWith("spark.executorEnv.UCX") ||
          key.startsWith("spark.shuffle.manager") || key.equals("spark.shuffle.server.enabled")
      }
      val inter = props.keys.toSeq.intersect(rapidsRelated.keys.toSeq)
      val existDiff = props.keys.toSeq.diff(inter)
      val newDiff = rapidsRelated.keys.toSeq.diff(inter)

      // first update intersecting
      inter.foreach { k =>
        val appVals = props.getOrElse(k, ArrayBuffer[String]())
        appVals += rapidsRelated.getOrElse(k, "null")
      }

      // this app doesn't contain a key that was in another app
      existDiff.foreach { k =>
        val appVals = props.getOrElse(k, ArrayBuffer[String]())
        appVals += "null"
      }

      // this app contains a key not in other apps
      newDiff.foreach { k =>
        // we need to fill if some apps didn't have it
        val appVals = ArrayBuffer[String]()
        appVals ++= Seq.fill(numApps - 1)("null")
        appVals += rapidsRelated.getOrElse(k, "null")

        props.put(k, appVals)
      }
    }
    if (props.size > 0) {
      val allRows = props.map { case(k, v) => Seq(k) ++ v }.toSeq
      val sortedRows = allRows.sortBy(cols => (cols(0)))
      if (sortedRows.size > 0) {
        val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
          outputHeaders, sortedRows)
        fileWriter.foreach(_.write(outStr))
      } else {
        fileWriter.foreach(_.write("No Spark Rapids parameters Found!\n"))
      }
    } else {
      fileWriter.foreach(_.write("No Spark Rapids parameters Found!\n"))
    }
  }

  def printSQLPlans(outputDirectory: String): Unit = {
    for (app <- apps) {
      val planFileWriter = new ToolTextFileWriter(outputDirectory,
        s"planDescriptions-${app.appId}", "SQL Plan")
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

  // Print SQL Plan Metrics
  def printSQLPlanMetrics(): Seq[SQLAccumProfileResults] = {
    val messageHeader = "\nSQL Plan Metrics for Application:\n"
    fileWriter.foreach(_.write(messageHeader))
    val filtered = CollectInformation.generateSQLAccums(apps)
    if (filtered.size > 0) {
      val sortedRows = filtered.sortBy(cols => (cols.appIndex, cols.sqlID,
        cols.nodeID, cols.nodeName, cols.accumulatorId, cols.metricType))
      val outputHeaders = sortedRows.head.outputHeaders
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
      sortedRows
    } else {
      fileWriter.foreach(_.write("No SQL Plan Metrics Found!\n"))
      Seq.empty
    }
  }
}

object CollectInformation {
  def generateSQLAccums(apps: Seq[ApplicationInfo]): Seq[SQLAccumProfileResults] = {
    val filtered = apps.filter(a => ((a.taskStageAccumMap.size > 0 || a.driverAccumMap.size > 0) &&
      a.allSQLMetrics.size > 0))
    val allRows = filtered.flatMap { app =>
      app.allSQLMetrics.map { metric =>
        val accums = app.taskStageAccumMap.get(metric.accumulatorId)
        val driverAccums = app.driverAccumMap.get(metric.accumulatorId)
        val driverMax = driverAccums match {
          case Some(acc) =>
            Some(acc.map(_.value).max)
          case None =>
            None
        }
        val taskMax = accums match {
          case Some(acc) =>
            Some(acc.map(_.value.getOrElse(0L)).max)
          case None =>
            None
        }

        if ((taskMax.isDefined) || (driverMax.isDefined)) {
          val max = Math.max(driverMax.getOrElse(0L), taskMax.getOrElse(0L))
          /*
          Some(SQLAccumProfileResults(app.index.toString, metric.sqlID.toString,
                     metric.nodeID.toString, metric.nodeName, metric.accumulatorId.toString,
                     metric.name, max.toString, metric.metricType))
                     */
          Some(SQLAccumProfileResults(app.index, metric.sqlID,
            metric.nodeID, metric.nodeName, metric.accumulatorId,
            metric.name, max, metric.metricType))
        } else {
          None
        }
      }
    }
    allRows.filter(_.isDefined).map(_.get)
  }
}