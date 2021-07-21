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

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rapids.tool.ToolUtils
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
  def printAppInfo(): Unit = {
    val messageHeader = "\nApplication Information:\n"
    fileWriter.foreach(_.write(messageHeader))
    val allRows = apps.map(a => a.appInfo.fieldsToPrint(a.index)).toList
    if (apps.size > 0) {
      val headerNames = apps.head.appInfo.outputHeaders
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        headerNames, allRows)
      fileWriter.foreach(_.write(outStr + "\n"))
    } else {
      fileWriter.foreach(_.write("No Application Information Found!\n"))
    }
  }

  // Print rapids-4-spark and cuDF jar if CPU Mode is on.
  def printRapidsJAR(): Unit = {
    val headers = Seq("appIndex", "Rapids4Spark jars")
    val allRows = apps.flatMap { app =>
      if (app.gpuMode) {
        fileWriter.foreach(_.write("\nRapids Accelerator Jar and cuDF Jar:\n"))
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
    if (apps.size > 0) {
      val sortedRows = allRows.sortBy(cols => (cols(0).toLong, cols(1).toLong, cols(3), cols(5)))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr + "\n"))
    } else {
      fileWriter.foreach(_.write("No Data Source Information Found!\n"))
    }
  }

  def getDataSourceInfo(app: ApplicationInfo, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val df = app.dataSourceInfo.toDF.sort(asc("sqlID"), asc("location"))
    df.withColumn("appIndex", lit(app.index.toString))
      .select("appIndex", df.columns:_*)
  }

  // Print executor related information
  def printExecutorInfo(): Unit = {
    val messageHeader = "\nExecutor Information:\n"
    fileWriter.foreach(_.write(messageHeader))
    if (apps.size > 0) {

      val allRows = apps.flatMap { app =>
        if (app.executors.size > 0) {
          // first see if any executors have different resourceProfile ids
          val groupedExecs = app.executors.groupBy(_._2.resourceProfileId)

          groupedExecs.map { case (rpId, execs) =>
            val rp = app.resourceProfiles.get(rpId)
            val execMem = rp.map(_.executorResources.get(ResourceProfile.MEMORY)
              .map(_.amount).getOrElse(0))
            val execCores = rp.map(_.executorResources.get(ResourceProfile.CORES)
              .map(_.amount).getOrElse(0))
            val execGpus = rp.map(_.executorResources.get(ResourceProfile.CORES)
              .map(_.amount).getOrElse(0))
            val taskCpus = rp.map(_.taskResources.get(ResourceProfile.CPUS)
              .map(_.amount).getOrElse(0))
            val taskGpus = rp.map(_.taskResources.get("gpu").map(_.amount).getOrElse(0))
            val execOffHeap = rp.map(_.executorResources.get(ResourceProfile.OFFHEAP_MEM)
              .map(_.amount).getOrElse(0))

            val numExecutors = execs.size
            val exec = execs.head._2
            // We could print a lot more information here if we decided, more like the Spark UI
            // per executor info.
            Seq(app.index.toString, rpId.toString, numExecutors.toString,
              exec.totalCores.toString, exec.maxMemory.toString, exec.totalOnHeap.toString,
              exec.totalOffHeap.toString,
              execMem.map(_.toString).getOrElse(null), execGpus.map(_.toString).getOrElse(null),
              execOffHeap.map(_.toString).getOrElse(null), taskCpus.map(_.toString).getOrElse(null),
              taskGpus.map(_.toString).getOrElse(null))
          }
        } else {
          Seq.empty
        }
      }
      val outputHeaders =
        Seq("appIndex", "resourceProfileId", "numExecutors", "executorCores",
          "maxMem", "maxOnHeapMem", "maxOffHeapMem", "executorMemory", "numGpusPerExecutor",
          "executorOffHeap", "taskCpu", "taskGpu")

      val sortedRows = allRows.sortBy(cols => (cols(0).toLong, cols(2).toLong))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr + "\n"))
    } else {
      fileWriter.foreach(_.write("No Executor Information Found!\n"))
    }
  }

  // Print job related information
  def printJobInfo(): Unit = {
    val messageHeader = "\nJob Information:\n"
    fileWriter.foreach(_.write(messageHeader))
    val outputHeaders =
      Seq("appIndex", "jobID", "stageIds", "sqlID")
    val allRows = apps.flatMap { app =>
      if (app.liveJobs.size > 0) {
        app.liveJobs.map { case (jobId, j) =>
          Seq(app.index.toString, j.jobID.toString,
            s"[${j.stageIds.mkString(",")}]",
            j.sqlID.map(_.toString).getOrElse(null))
        }
      } else {
        Seq.empty
      }
    }
    if (apps.size > 0) {
      val sortedRows = allRows.sortBy(cols => (cols(0).toLong, cols(1).toLong))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr + "\n"))
    } else {
      fileWriter.foreach(_.write("No Job Information Found!\n"))
    }
  }
  /*
    // Print Rapids related Spark Properties
    def printRapidsProperties(): Unit = {
      val messageHeader = "\nSpark Rapids parameters set explicitly:\n"
      for (app <- apps) {
        if (app.allDataFrames.contains(s"propertiesDF_${app.index}")) {
          app.runQuery(query = app.generateNvidiaProperties + " order by propertyName",
            fileWriter = fileWriter, messageHeader = messageHeader)
        } else {
          fileWriter.foreach(_.write("No Spark Rapids parameters Found!\n"))
        }
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
    def printSQLPlanMetrics(): Unit = {
      for (app <- apps){
        if (app.allDataFrames.contains(s"sqlMetricsDF_${app.index}") &&
          app.allDataFrames.contains(s"driverAccumDF_${app.index}") &&
          app.allDataFrames.contains(s"taskStageAccumDF_${app.index}") &&
          app.allDataFrames.contains(s"jobDF_${app.index}") &&
          app.allDataFrames.contains(s"sqlDF_${app.index}")) {
          val messageHeader = "\nSQL Plan Metrics for Application:\n"
          app.runQuery(app.generateSQLAccums, fileWriter = fileWriter, messageHeader=messageHeader)
        }
      }
    }
    */
}
