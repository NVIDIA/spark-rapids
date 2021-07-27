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
import org.apache.spark.sql.rapids.tool.ToolUtils
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

  // TODO - match with columsn below?
  // All the metrics column names in Task Metrics with the aggregation type
  val taskMetricsColumns: scala.collection.mutable.SortedMap[String, String]
  = scala.collection.mutable.SortedMap(
    "diskBytesSpilled" -> "sum",
    "duration" -> "all",
    "executorCPUTime" -> "sum",
    "executorDeserializeCPUTime" -> "sum",
    "executorDeserializeTime" -> "sum",
    "executorRunTime" -> "sum",
    "gettingResultTime" -> "sum",
    "input_bytesRead" -> "sum",
    "input_recordsRead" -> "sum",
    "jvmGCTime" -> "sum",
    "memoryBytesSpilled" -> "sum",
    "output_bytesWritten" -> "sum",
    "output_recordsWritten" -> "sum",
    "peakExecutionMemory" -> "max",
    "resultSerializationTime" -> "sum",
    "resultSize" -> "max",
    "sr_fetchWaitTime" -> "sum",
    "sr_localBlocksFetched" -> "sum",
    "sr_localBytesRead" -> "sum",
    "sr_remoteBlocksFetched" -> "sum",
    "sr_remoteBytesRead" -> "sum",
    "sr_remoteBytesReadToDisk" -> "sum",
    "sr_totalBytesRead" -> "sum",
    "sw_bytesWritten" -> "sum",
    "sw_recordsWritten" -> "sum",
    "sw_writeTime" -> "sum"
  )

  def getDurations(tcs: ArrayBuffer[TaskCase]): Seq[String] = {
    val durations = tcs.map(_.duration)
    Seq(durations.sum.toString, durations.max.toString,
      durations.min.toString, ToolUtils.calculateAverage(durations.sum, durations.size, 1).toString)
  }

  // Job + Stage Level TaskMetrics Aggregation
  def jobAndStageMetricsAggregation(): Unit = {
    val messageHeader = "\nJob + Stage level aggregated task metrics:\n"
    fileWriter.foreach(_.write(messageHeader))
    val outputHeaders = Seq("appIndex", "ID", "numTasks", "Duration") ++ genTaskMetricsColumnHeaders
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

          val jobDuration = jc.duration match {
            case Some(dur) => dur.toString
            case None => ""
          }
          val jobInfo = Seq(app.index.toString, s"job_$id", uniqueTasks.size.toString,
            jobDuration)
          val diskBytes = Seq(tasksInJob.map(_.diskBytesSpilled).sum.toString)
          val durs = getDurations(tasksInJob)
          val metrics = Seq(
            tasksInJob.map(_.executorCPUTime).sum.toString,
            tasksInJob.map(_.executorDeserializeCPUTime).sum.toString,
            tasksInJob.map(_.executorDeserializeTime).sum.toString,
            tasksInJob.map(_.executorRunTime).sum.toString,
            tasksInJob.map(_.gettingResultTime).sum.toString,
            tasksInJob.map(_.input_bytesRead).sum.toString,
            tasksInJob.map(_.input_recordsRead).sum.toString,
            tasksInJob.map(_.jvmGCTime).sum.toString,
            tasksInJob.map(_.memoryBytesSpilled).sum.toString,
            tasksInJob.map(_.output_bytesWritten).sum.toString,
            tasksInJob.map(_.output_recordsWritten).sum.toString,
            tasksInJob.map(_.peakExecutionMemory).max.toString,
            tasksInJob.map(_.resultSerializationTime).sum.toString,
            tasksInJob.map(_.resultSize).max.toString,
            tasksInJob.map(_.sr_fetchWaitTime).sum.toString,
            tasksInJob.map(_.sr_localBlocksFetched).sum.toString,
            tasksInJob.map(_.sr_localBytesRead).sum.toString,
            tasksInJob.map(_.sr_remoteBlocksFetched).sum.toString,
            tasksInJob.map(_.sr_remoteBytesRead).sum.toString,
            tasksInJob.map(_.sr_remoteBytesReadToDisk).sum.toString,
            tasksInJob.map(_.sr_totalBytesRead).sum.toString,
            tasksInJob.map(_.sw_bytesWritten).sum.toString,
            tasksInJob.map(_.sw_recordsWritten).sum.toString,
            tasksInJob.map(_.sw_writeTime).sum.toString
          )
          jobInfo ++ diskBytes ++ durs ++ metrics
        }
      } else {
        Seq.empty
      }
    }
    val allStageRows = apps.flatMap { app =>
      if ((app.taskEnd.size > 0) && (app.liveJobs.size > 0) && (app.liveStages.size > 0)) {
        app.liveJobs.flatMap { case (id, jc) =>
          val stageIdsInJob = jc.stageIds
          val stagesInJob = app.liveStages.filterKeys { case (sid, _) =>
            stageIdsInJob.contains(sid)
          }
          stagesInJob.map { case ((id, said), sc) =>
            val tasksInStage = app.taskEnd.filter { tc =>
              tc.stageId == id
            }
            // don't count duplicate task attempts
            val uniqueTasks = tasksInStage.groupBy(tc => tc.taskId)
            uniqueTasks.foreach { case (id, groups) =>
              logWarning(s"task $id num attempts is: ${groups.size}")
            }
            // TODO - how to deal with attempts?

            val scDuration = sc.duration match {
              case Some(dur) => dur.toString
              case None => ""
            }
            val stageInfo = Seq(app.index.toString, s"stage_$id", uniqueTasks.size.toString,
              scDuration)
            val diskBytes = Seq(tasksInStage.map(_.diskBytesSpilled).sum.toString)
            val durs = getDurations(tasksInStage)
            val metrics = Seq(
              tasksInStage.map(_.executorCPUTime).sum.toString,
              tasksInStage.map(_.executorDeserializeCPUTime).sum.toString,
              tasksInStage.map(_.executorDeserializeTime).sum.toString,
              tasksInStage.map(_.executorRunTime).sum.toString,
              tasksInStage.map(_.gettingResultTime).sum.toString,
              tasksInStage.map(_.input_bytesRead).sum.toString,
              tasksInStage.map(_.input_recordsRead).sum.toString,
              tasksInStage.map(_.jvmGCTime).sum.toString,
              tasksInStage.map(_.memoryBytesSpilled).sum.toString,
              tasksInStage.map(_.output_bytesWritten).sum.toString,
              tasksInStage.map(_.output_recordsWritten).sum.toString,
              tasksInStage.map(_.peakExecutionMemory).max.toString,
              tasksInStage.map(_.resultSerializationTime).sum.toString,
              tasksInStage.map(_.resultSize).max.toString,
              tasksInStage.map(_.sr_fetchWaitTime).sum.toString,
              tasksInStage.map(_.sr_localBlocksFetched).sum.toString,
              tasksInStage.map(_.sr_localBytesRead).sum.toString,
              tasksInStage.map(_.sr_remoteBlocksFetched).sum.toString,
              tasksInStage.map(_.sr_remoteBytesRead).sum.toString,
              tasksInStage.map(_.sr_remoteBytesReadToDisk).sum.toString,
              tasksInStage.map(_.sr_totalBytesRead).sum.toString,
              tasksInStage.map(_.sw_bytesWritten).sum.toString,
              tasksInStage.map(_.sw_recordsWritten).sum.toString,
              tasksInStage.map(_.sw_writeTime).sum.toString
            )
            stageInfo ++ diskBytes ++ durs ++ metrics
          }
        }
      } else {
        Seq.empty
      }
    }

    val allRows = allJobRows ++ allStageRows
    if (allRows.size > 0) {
      val sortedRows = allRows.sortBy(cols => (cols(0).toLong, -(cols(3).toLong), cols(1)))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No Job/Stage Metrics Found!\n"))
    }
  }

  // SQL Level TaskMetrics Aggregation(Only when SQL exists)
  def sqlMetricsAggregation(): Unit = {
    val messageHeader = "\nSQL level aggregated task metrics:\n"
    fileWriter.foreach(_.write(messageHeader))
    val outputHeaders = Seq("appIndex", "appID", "sqlID", "description", "numTasks", "Duration",
      "executorCPUTime", "executorRunTime", "executorCPURatio") ++ genTaskMetricsColumnHeaders
    val allRows = apps.flatMap { app =>
      if ((app.taskEnd.size > 0) && (app.liveJobs.size > 0) && (app.liveStages.size > 0) &&
         (app.liveSQL.size > 0)) {

        // TODO - how to deal with attempts?
        app.liveSQL.map { case (sqlId, sqlCase) =>
          logWarning("sqlid for live is: " + sqlId)
          val jcs = app.liveJobs.filter { case (_, jc) =>
            val jcid = jc.sqlID.getOrElse(-1)
            logWarning("jc sqlid is: " + jcid)
            jc.sqlID.getOrElse(-1) == sqlId
          }
          if (jcs.isEmpty) {
            Seq.empty
          } else {
            logWarning("jobs for sql are: " + jcs.mkString(","))

            val stageIdsForSQL = jcs.flatMap(_._2.stageIds).toSeq
            logWarning("stage ids for sql is: " + stageIdsForSQL.mkString(","))

            val tasksInSQL = app.taskEnd.filter { tc =>
              stageIdsForSQL.contains(tc.stageId)
            }
            if (tasksInSQL.isEmpty) {
              Seq.empty
            } else {
              logWarning("tasks in sql size: " + tasksInSQL.size)

              // don't count duplicate task attempts ???
              val uniqueTasks = tasksInSQL.groupBy(tc => tc.taskId)
              uniqueTasks.foreach { case (id, groups) =>
                logWarning(s"task $id num attempts is: ${groups.size}")
              }

              val duration = sqlCase.duration match {
                case Some(dur) => dur.toString
                case None => ""
              }

              val sqlStats = Seq(app.index.toString, app.appId, s"$sqlId", sqlCase.description,
                uniqueTasks.size.toString, duration)

              val diskBytes = Seq(tasksInSQL.map(_.diskBytesSpilled).sum.toString)
              val execCpuTime = tasksInSQL.map(_.executorCPUTime).sum
              val execRunTime = tasksInSQL.map(_.executorRunTime).sum
              val execCPURatio = ToolUtils.calculateDurationPercent(execCpuTime, execRunTime)
              // TODO - set this here make sure we don't get it again until later
              sqlCase.sqlCpuTimePercent = execCPURatio
              val execStats = Seq(
                execCpuTime.toString,
                execRunTime.toString,
                execCPURatio.toString
              )
              val durs = getDurations(tasksInSQL)
              val metrics = Seq(
                execCpuTime.toString,
                tasksInSQL.map(_.executorDeserializeCPUTime).sum.toString,
                tasksInSQL.map(_.executorDeserializeTime).sum.toString,
                execRunTime.toString,
                tasksInSQL.map(_.gettingResultTime).sum.toString,
                tasksInSQL.map(_.input_bytesRead).sum.toString,
                tasksInSQL.map(_.input_recordsRead).sum.toString,
                tasksInSQL.map(_.jvmGCTime).sum.toString,
                tasksInSQL.map(_.memoryBytesSpilled).sum.toString,
                tasksInSQL.map(_.output_bytesWritten).sum.toString,
                tasksInSQL.map(_.output_recordsWritten).sum.toString,
                tasksInSQL.map(_.peakExecutionMemory).max.toString,
                tasksInSQL.map(_.resultSerializationTime).sum.toString,
                tasksInSQL.map(_.resultSize).max.toString,
                tasksInSQL.map(_.sr_fetchWaitTime).sum.toString,
                tasksInSQL.map(_.sr_localBlocksFetched).sum.toString,
                tasksInSQL.map(_.sr_localBytesRead).sum.toString,
                tasksInSQL.map(_.sr_remoteBlocksFetched).sum.toString,
                tasksInSQL.map(_.sr_remoteBytesRead).sum.toString,
                tasksInSQL.map(_.sr_remoteBytesReadToDisk).sum.toString,
                tasksInSQL.map(_.sr_totalBytesRead).sum.toString,
                tasksInSQL.map(_.sw_bytesWritten).sum.toString,
                tasksInSQL.map(_.sw_recordsWritten).sum.toString,
                tasksInSQL.map(_.sw_writeTime).sum.toString
              )
              sqlStats ++ execStats ++ diskBytes ++ durs ++ metrics
            }
          }
        }
      } else {
        logWarning("various empty")
        Seq.empty
      }
    }
    val allNonEmptyRows = allRows.filter(!_.isEmpty)
    if (allNonEmptyRows.size > 0) {
      val sortedRows = allNonEmptyRows.sortBy(cols => (cols(0).toLong, -(cols(5).toLong), cols(2)))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No SQL Metrics Found!\n"))
    }

  }

  def sqlMetricsAggregationDurationAndCpuTime(): Unit = {
    val messageHeader = "\nSQL Duration and Executor CPU Time Percent\n"
    fileWriter.foreach(_.write(messageHeader))
    val outputHeaders = Seq("appIndex", "App ID", "sqlID", "SQL Duration", "Contains Dataset Op",
      "App Duration", "Potential Problsm", "Executor CPU Time Percent")

    val allRows = apps.flatMap { app =>
      if ((app.taskEnd.size > 0) && (app.liveJobs.size > 0) && (app.liveStages.size > 0) &&
        (app.liveSQL.size > 0)) {

        val appDuration = app.appInfo.duration match {
          case Some(dur) => dur.toString()
          case None => ""
        }

        app.liveSQL.map { case (sqlId, sqlCase) =>
          val sqlDuration = sqlCase.duration match {
            case Some(dur) => dur.toString()
            case None => ""
          }
          val execCpuTimePercent = if (sqlCase.sqlCpuTimePercent == -1) {
            "null"
          } else {
            sqlCase.sqlCpuTimePercent.toString
          }
          Seq(app.index.toString, app.appId, s"$sqlId", sqlDuration,
            sqlCase.hasDataset.toString, appDuration, sqlCase.problematic,
            execCpuTimePercent)
        }
      } else {
        Seq.empty
      }
    }

    val allNonEmptyRows = allRows.filter(!_.isEmpty)
    if (allNonEmptyRows.size > 0) {
      val sortedRows = allNonEmptyRows.sortBy(cols => (cols(0).toLong, cols(2).toLong,
        (cols(3).toLong)))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No SQL Duration and Executor CPU Time Percent Found!\n"))
    }
  }


  /*

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
