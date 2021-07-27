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

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * Does analysis on the DataFrames
 * from object of ApplicationInfo
 */
class Analysis(apps: Seq[ApplicationInfo], fileWriter: Option[ToolTextFileWriter],
    numOutputRows: Int) extends Logging {

  def genTaskMetricsColumnHeaders: Seq[String] = {
    val cols = taskMetricsColumns.flatMap { case (col, aggType) =>
      // If aggType=all, it means all 4 aggregation: sum, max, min, avg.
      if (aggType == "all") {
        Seq(s"${col}_sum", s"${col}_max", s"${col}_min", s"${col}_avg")

      }
      else {
        Seq(s"${col}_${aggType}")
      }
    }
    cols.toSeq
  }

  // All the metrics column names in Task Metrics with the aggregation type
  val taskMetricsColumns: scala.collection.mutable.SortedMap[String, String]
  = scala.collection.mutable.SortedMap(
    "duration" -> "all",
    "gettingResultTime" -> "sum",
    "executorDeserializeTime" -> "sum",
    "executorDeserializeCPUTime" -> "sum",
    "executorRunTime" -> "sum",
    "executorCPUTime" -> "sum",
    "peakExecutionMemory" -> "max",
    "resultSize" -> "max",
    "jvmGCTime" -> "sum",
    "resultSerializationTime" -> "sum",
    "memoryBytesSpilled" -> "sum",
    "diskBytesSpilled" -> "sum",
    "sr_remoteBlocksFetched" -> "sum",
    "sr_localBlocksFetched" -> "sum",
    "sr_fetchWaitTime" -> "sum",
    "sr_remoteBytesRead" -> "sum",
    "sr_remoteBytesReadToDisk" -> "sum",
    "sr_localBytesRead" -> "sum",
    "sr_totalBytesRead" -> "sum",
    "sw_bytesWritten" -> "sum",
    "sw_writeTime" -> "sum",
    "sw_recordsWritten" -> "sum",
    "input_bytesRead" -> "sum",
    "input_recordsRead" -> "sum",
    "output_bytesWritten" -> "sum",
    "output_recordsWritten" -> "sum"
  )

  def getDurations(tcs: ArrayBuffer[TaskCase]): Seq[String] = {
    val durations = tcs.map(_.duration)
    Seq(durations.sum.toString, durations.max.toString,
      durations.min.toString, (durations.sum/durations.size).toString)
  }

  // Job + Stage Level TaskMetrics Aggregation
  def jobAndStageMetricsAggregation(): Unit = {
    val messageHeader = "\nJob + Stage level aggregated task metrics:\n"

    fileWriter.foreach(_.write(messageHeader))
    val outputHeaders = Seq("appIndex", "ID", "numTasks") ++ genTaskMetricsColumnHeaders
    val allJobRows = apps.flatMap { app =>
      if ((app.taskEnd.size > 0) && (app.liveJobs.size > 0) && (app.liveStages.size > 0)) {
        app.liveJobs.map { case (id, jc) =>
          val stageIdsInJob = jc.stageIds
          val stagesInJob = app.liveStages.filterKeys { case (sid, _) =>
            stageIdsInJob.contains(sid)
          }.keys.map(_._1).toSeq
          val tasksInJob = app.taskEnd.filter { tc =>
            stagesInJob.contains(tc.stageId)
          }
          // don't count duplicate task attempts
          val uniqueTasks = tasksInJob.groupBy(tc => tc.taskId)
          uniqueTasks.foreach { case (id, groups) =>
            logWarning(s"task $id num attempts is: ${groups.size}")
          }
          // TODO - how to deal with attempts?

          val jobInfo = Seq(app.index.toString, s"job_$id", uniqueTasks.size.toString)
          val durs = getDurations(tasksInJob)
          val metrics = Seq(tasksInJob.map(_.gettingResultTime).sum.toString,
            tasksInJob.map(_.executorDeserializeTime).sum.toString,
            tasksInJob.map(_.executorDeserializeCPUTime).sum.toString,
            tasksInJob.map(_.executorRunTime).sum.toString,
            tasksInJob.map(_.executorCPUTime).sum.toString,
            tasksInJob.map(_.peakExecutionMemory).sum.toString,
            tasksInJob.map(_.resultSize).sum.toString,
            tasksInJob.map(_.jvmGCTime).sum.toString,
            tasksInJob.map(_.resultSerializationTime).sum.toString,
            tasksInJob.map(_.memoryBytesSpilled).sum.toString,
            tasksInJob.map(_.diskBytesSpilled).sum.toString,
            tasksInJob.map(_.sr_remoteBlocksFetched).sum.toString,
            tasksInJob.map(_.sr_localBlocksFetched).sum.toString,
            tasksInJob.map(_.sr_fetchWaitTime).sum.toString,
            tasksInJob.map(_.sr_remoteBytesRead).sum.toString,
            tasksInJob.map(_.sr_remoteBytesReadToDisk).sum.toString,
            tasksInJob.map(_.sr_localBytesRead).sum.toString,
            tasksInJob.map(_.sr_totalBytesRead).sum.toString,
            tasksInJob.map(_.sw_bytesWritten).sum.toString,
            tasksInJob.map(_.input_bytesRead).sum.toString,
            tasksInJob.map(_.input_recordsRead).sum.toString,
            tasksInJob.map(_.output_bytesWritten).sum.toString,
            tasksInJob.map(_.output_recordsWritten).sum.toString
          )
          jobInfo ++ durs ++ metrics
        }
      } else {
        Seq.empty
      }
    }
    val allStageRows = apps.flatMap { app =>
      if ((app.taskEnd.size > 0) && (app.liveJobs.size > 0) && (app.liveStages.size > 0)) {
        app.liveJobs.map { case (id, jc) =>
          val stageIdsInJob = jc.stageIds
          val stagesInJob = app.liveStages.filterKeys { case (sid, _) =>
            stageIdsInJob.contains(sid)
          }.keys.map(_._1).toSeq
          stagesInJob.flatMap { id =>
            val tasksInStage = app.taskEnd.filter { tc =>
              tc.stageId == id
            }
            // don't count duplicate task attempts
            val uniqueTasks = tasksInStage.groupBy(tc => tc.taskId)
            uniqueTasks.foreach { case (id, groups) =>
              logWarning(s"task $id num attempts is: ${groups.size}")
            }
            // TODO - how to deal with attempts?

            val stageInfo = Seq(app.index.toString, s"stage_$id", uniqueTasks.size.toString)
            val durs = getDurations(tasksInStage)
            val metrics = Seq(tasksInStage.map(_.gettingResultTime).sum.toString,
              tasksInStage.map(_.executorDeserializeTime).sum.toString,
              tasksInStage.map(_.executorDeserializeCPUTime).sum.toString,
              tasksInStage.map(_.executorRunTime).sum.toString,
              tasksInStage.map(_.executorCPUTime).sum.toString,
              tasksInStage.map(_.peakExecutionMemory).sum.toString,
              tasksInStage.map(_.resultSize).sum.toString,
              tasksInStage.map(_.jvmGCTime).sum.toString,
              tasksInStage.map(_.resultSerializationTime).sum.toString,
              tasksInStage.map(_.memoryBytesSpilled).sum.toString,
              tasksInStage.map(_.diskBytesSpilled).sum.toString,
              tasksInStage.map(_.sr_remoteBlocksFetched).sum.toString,
              tasksInStage.map(_.sr_localBlocksFetched).sum.toString,
              tasksInStage.map(_.sr_fetchWaitTime).sum.toString,
              tasksInStage.map(_.sr_remoteBytesRead).sum.toString,
              tasksInStage.map(_.sr_remoteBytesReadToDisk).sum.toString,
              tasksInStage.map(_.sr_localBytesRead).sum.toString,
              tasksInStage.map(_.sr_totalBytesRead).sum.toString,
              tasksInStage.map(_.sw_bytesWritten).sum.toString,
              tasksInStage.map(_.input_bytesRead).sum.toString,
              tasksInStage.map(_.input_recordsRead).sum.toString,
              tasksInStage.map(_.output_bytesWritten).sum.toString,
              tasksInStage.map(_.output_recordsWritten).sum.toString
            )
            stageInfo ++ durs ++ metrics
          }
        }
      } else {
        Seq.empty
      }
    }

    val allRows = allJobRows ++ allStageRows
    if (allRows.size > 0) {
      val sortedRows = allRows.sortBy(cols => (cols(0).toLong, cols(3).toLong, cols(1).toLong))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No Job/Stage Metrics Found!\n"))
    }
  }

  /*
  // SQL Level TaskMetrics Aggregation(Only when SQL exists)
  def sqlMetricsAggregation(): DataFrame = {
    val messageHeader = "\nSQL level aggregated task metrics:\n"
    if (apps.size == 1) {
      val app = apps.head
      if (app.allDataFrames.contains(s"taskDF_${app.index}") &&
        app.allDataFrames.contains(s"stageDF_${app.index}") &&
        app.allDataFrames.contains(s"jobDF_${app.index}") &&
        app.allDataFrames.contains(s"sqlDF_${app.index}")) {
        apps.head.runQuery(apps.head.sqlMetricsAggregationSQL + " order by Duration desc",
          false, fileWriter, messageHeader)
      } else {
        apps.head.sparkSession.emptyDataFrame
      }
    } else {
      var query = ""
      val appsWithSQL = apps.filter(p => p.allDataFrames.contains(s"sqlDF_${p.index}"))
      for (app <- appsWithSQL) {
        if (app.allDataFrames.contains(s"taskDF_${app.index}") &&
          app.allDataFrames.contains(s"stageDF_${app.index}") &&
          app.allDataFrames.contains(s"jobDF_${app.index}") &&
          app.allDataFrames.contains(s"sqlDF_${app.index}")) {
          if (query.isEmpty) {
            query += app.sqlMetricsAggregationSQL
          } else {
            query += " union " + app.sqlMetricsAggregationSQL
          }
        }
      }
      if (query.nonEmpty) {
        apps.head.runQuery(query + " order by appIndex, Duration desc", false,
          fileWriter, messageHeader)
      } else {
        fileWriter.foreach(_.write("Unable to aggregate SQL task Metrics\n"))
        apps.head.sparkSession.emptyDataFrame
      }
    }
  }

  // sql metrics aggregation specific for qualification because
  // it aggregates executor time metrics differently
  def sqlMetricsAggregationQual(): DataFrame = {
    val query = apps
      .filter { p =>
        p.allDataFrames.contains(s"sqlDF_${p.index}") &&
          p.allDataFrames.contains(s"stageDF_${p.index}") &&
          p.allDataFrames.contains(s"jobDF_${p.index}")
      }.map( app => "(" + app.sqlMetricsAggregationSQLQual + ")")
      .mkString(" union ")
    if (query.nonEmpty) {
      apps.head.runQuery(query)
    } else {
      apps.head.sparkSession.emptyDataFrame
    }
  }

  def sqlMetricsAggregationDurationAndCpuTime(): DataFrame = {
    val messageHeader = "\nSQL Duration and Executor CPU Time Percent\n"
    val query = apps
      .filter { p =>
        p.allDataFrames.contains(s"sqlDF_${p.index}") &&
        p.allDataFrames.contains(s"appDF_${p.index}")
      }.map( app => "(" + app.profilingDurationSQL + ")")
      .mkString(" union ")
    if (query.nonEmpty) {
      apps.head.runQuery(query + "order by appIndex, sqlID, `SQL Duration`",
        false, fileWriter, messageHeader)
    } else {
      apps.head.sparkSession.emptyDataFrame
    }
  }

  // custom query execution. Normally for debugging use.
  def customQueryExecution(app: ApplicationInfo): Unit = {
    fileWriter.foreach(_.write("Custom query execution:"))
    val customQuery =
      s"""select stageId from stageDF_${app.index} limit 1
         |""".stripMargin
    app.runQuery(customQuery)
  }

  // Function to find out shuffle read skew(For Joins or Aggregation)
  def shuffleSkewCheck(): Unit ={
    for (app <- apps){
      shuffleSkewCheckSingleApp(app)
    }
  }

  def shuffleSkewCheckSingleApp(app: ApplicationInfo): DataFrame = {
    if (app.allDataFrames.contains(s"taskDF_${app.index}")) {
      val customQuery =
        s"""with tmp as
           |(select stageId, stageAttemptId,
           |avg(sr_totalBytesRead) avgShuffleReadBytes,
           |avg(duration) avgDuration
           |from taskDF_${app.index}
           |group by stageId,stageAttemptId)
           |select ${app.index} as appIndex, t.stageId,t.stageAttemptId,
           |t.taskId, t.attempt,
           |round(t.duration/1000,2) as taskDurationSec,
           |round(tmp.avgDuration/1000,2) as avgDurationSec,
           |round(t.sr_totalBytesRead/1024/1024,2) as taskShuffleReadMB,
           |round(tmp.avgShuffleReadBytes/1024/1024,2) as avgShuffleReadMB,
           |round(t.peakExecutionMemory/1024/1024,2) as taskPeakMemoryMB,
           |t.successful,
           |substr(t.endReason,0,100) reason
           |from tmp, taskDF_${app.index} t
           |where tmp.stageId=t.StageId
           |and tmp.stageAttemptId=t.stageAttemptId
           |and t.sr_totalBytesRead > 3 * tmp.avgShuffleReadBytes
           |and t.sr_totalBytesRead > 100*1024*1024
           |order by t.stageId, t.stageAttemptId, t.taskId,t.attempt
           |""".stripMargin
      val messageHeader = s"\nShuffle Skew Check:" +
        " (When task's Shuffle Read Size > 3 * Avg Stage-level size)\n"
      app.runQuery(customQuery, false, fileWriter, messageHeader)
    } else {
      apps.head.sparkSession.emptyDataFrame
    }
  }
  */
}
