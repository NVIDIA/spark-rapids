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

  def getDurations(tcs: ArrayBuffer[TaskCase]): (Long, Long, Long, Double) = {
    val durations = tcs.map(_.duration)
    if (durations.size > 0 ) {
      (durations.sum, durations.max, durations.min,
        ToolUtils.calculateAverage(durations.sum, durations.size, 1))
    } else {
      (0L, 0L, 0L, 0.toDouble)
    }
  }

  // Job + Stage Level TaskMetrics Aggregation
  def jobAndStageMetricsAggregation(): Unit = {
    val messageHeader = "\nJob + Stage level aggregated task metrics:\n"
    fileWriter.foreach(_.write(messageHeader))
    val filtered = apps.filter(app =>
      (app.taskEnd.size > 0) && (app.jobIdToInfo.size > 0) && (app.stageIdToInfo.size > 0))
    val allJobRows = filtered.flatMap { app =>
      app.jobIdToInfo.map { case (id, jc) =>
        val stageIdsInJob = jc.stageIds
        val stagesInJob = app.stageIdToInfo.filterKeys { case (sid, _) =>
          stageIdsInJob.contains(sid)
        }.keys.map(_._1).toSeq
        if (stagesInJob.isEmpty) {
          None
        } else {
          val tasksInJob = app.taskEnd.filter { tc =>
            stagesInJob.contains(tc.stageId)
          }
          // count duplicate task attempts
          val numTaskAttempt = tasksInJob.size
          // Above is a bit out of ordinary fro spark perhaps print both
          // val uniqueTasks = tasksInJob.groupBy(tc => tc.taskId)
          logWarning("count duplicates " + tasksInJob.size)

          // TODO - how to deal with attempts?
          val (durSum, durMax, durMin, durAvg) = getDurations(tasksInJob)
          Some(JobStageAggTaskMetrics(app.index,
            s"job_$id",
            numTaskAttempt,
            jc.duration,
            tasksInJob.map(_.diskBytesSpilled).sum,
            durSum,
            durMax,
            durMin,
            durAvg,
            tasksInJob.map(_.executorCPUTime).sum,
            tasksInJob.map(_.executorDeserializeCPUTime).sum,
            tasksInJob.map(_.executorDeserializeTime).sum,
            tasksInJob.map(_.executorRunTime).sum,
            tasksInJob.map(_.gettingResultTime).sum,
            tasksInJob.map(_.input_bytesRead).sum,
            tasksInJob.map(_.input_recordsRead).sum,
            tasksInJob.map(_.jvmGCTime).sum,
            tasksInJob.map(_.memoryBytesSpilled).sum,
            tasksInJob.map(_.output_bytesWritten).sum,
            tasksInJob.map(_.output_recordsWritten).sum,
            tasksInJob.map(_.peakExecutionMemory).max,
            tasksInJob.map(_.resultSerializationTime).sum,
            tasksInJob.map(_.resultSize).max,
            tasksInJob.map(_.sr_fetchWaitTime).sum,
            tasksInJob.map(_.sr_localBlocksFetched).sum,
            tasksInJob.map(_.sr_localBytesRead).sum,
            tasksInJob.map(_.sr_remoteBlocksFetched).sum,
            tasksInJob.map(_.sr_remoteBytesRead).sum,
            tasksInJob.map(_.sr_remoteBytesReadToDisk).sum,
            tasksInJob.map(_.sr_totalBytesRead).sum,
            tasksInJob.map(_.sw_bytesWritten).sum,
            tasksInJob.map(_.sw_recordsWritten).sum,
            tasksInJob.map(_.sw_writeTime).sum
          ))
        }
      }
    }

    filtered.foreach { app =>
      val res = app.jobIdToInfo.flatMap { case (id, jc) =>
        val stageIdsInJob = jc.stageIds
        app.stageIdToInfo.filterKeys { case (sid, _) =>
          stageIdsInJob.contains(sid)
        }
      }
      if (res.size != app.stageIdToInfo.size) {
        logWarning("number of stages not included: " + (app.stageIdToInfo.size - res.size))
      }
    }
    val allStageRows = filtered.flatMap { app =>
      // TODO need to get stages not in a job
      app.jobIdToInfo.flatMap { case (id, jc) =>
        val stageIdsInJob = jc.stageIds
        val stagesInJob = app.stageIdToInfo.filterKeys { case (sid, _) =>
          stageIdsInJob.contains(sid)
        }
        logWarning(s"stages in $id job: " + stagesInJob.keys.map(_._1).mkString(","))

        if (stagesInJob.isEmpty) {
          None
        } else {
          stagesInJob.map { case ((id, said), sc) =>
            val tasksInStage = app.taskEnd.filter { tc =>
              tc.stageId == id
            }
            // count duplicate task attempts
            val numAttempts = tasksInStage.size
            // Above is a bit out of ordinary fro spark perhaps print both
            // val uniqueTasks = tasksInStage.groupBy(tc => tc.taskId)
            // TODO - how to deal with attempts?

            val (durSum, durMax, durMin, durAvg) = getDurations(tasksInStage)
            Some(JobStageAggTaskMetrics(app.index,
              s"stage_$id",
              numAttempts,
              sc.duration,
              tasksInStage.reduceOption(_.diskBytesSpilled + _.diskBytesSpilled).getOrElse(0L),
              durSum,
              durMax,
              durMin,
              durAvg,
              tasksInStage.reduceOption(_.executorCPUTime + _.executorCPUTime).getOrElse(0L),
              tasksInStage.reduceOption(_.executorDeserializeCPUTime + _.executorDeserializeCPUTime)
                .getOrElse(0L),
              tasksInStage.reduceOption(_.executorDeserializeTime + _.executorDeserializeTime)
                .getOrElse(0L),
              tasksInStage.reduceOption(_.executorRunTime + _.executorRunTime).getOrElse(0L),
              tasksInStage.reduceOption(_.gettingResultTime + _.gettingResultTime).getOrElse(0L,
              tasksInStage.reduceOption(_.input_bytesRead + _.input_bytesRead).getOrElse(0L),
              tasksInStage.reduceOption(_.input_recordsRead + _.input_recordsRead).getOrElse(0L),
              tasksInStage.reduceOption(_.jvmGCTime + _.jvmGCTime).getOrElse(0L),
              tasksInStage.reduceOption(_.memoryBytesSpilled + _.memoryBytesSpilled).getOrElse(0L),
              tasksInStage.reduceOption(_.output_bytesWritten + _.output_bytesWritten).getOrElse(0L),
              tasksInStage.reduceOption(_.output_recordsWritten + _.output_recordsWritten).getOrElse(0L),
              tasksInStage.reduceOption(_.peakExecutionMemory max _.peakExecutionMemory).getOrElse(0L),
              tasksInStage.reduceOption(_.resultSerializationTime + _.resultSerializationTime)
                .getOrElse(0L),
              tasksInStage.reduceOption(_.resultSize max _.resultSize).getOrElse(0L),
              tasksInStage.reduceOption(_.sr_fetchWaitTime + _.sr_fetchWaitTime).getOrElse(0L),
              tasksInStage.reduceOption(_.sr_localBlocksFetched + _.sr_localBlocksFetched).getOrElse(0L),
              tasksInStage.reduceOption(_.sr_localBytesRead + _.sr_localBytesRead).getOrElse(0L),
              tasksInStage.reduceOption(_.sr_remoteBlocksFetched + _.sr_remoteBlocksFetched).getOrElse(0L),
              tasksInStage.reduceOption(_.sr_remoteBytesRead + _.sr_remoteBytesRead).getOrElse(0L),
              tasksInStage.reduceOption(_.sr_remoteBytesReadToDisk + _.sr_remoteBytesReadToDisk)
                .getOrElse(0L),
              tasksInStage.reduceOption(_.sr_totalBytesRead + _.sr_totalBytesRead).getOrElse(0L),
              tasksInStage.reduceOption(_.sw_bytesWritten + _.sw_bytesWritten).getOrElse(0L),
              tasksInStage.reduceOption(_.sw_recordsWritten + _.sw_recordsWritten).getOrElse(0L),
              tasksInStage.reduceOption(_.sw_writeTime + _.sw_writeTime).getOrElse(0L)
            ))
          }
        }
      }
    }

    val allRows = allJobRows ++ allStageRows
    val filteredRows = allRows.filter(_.isDefined).map(_.get)
    if (filteredRows.size > 0) {
      val sortedRows = filteredRows.sortBy { cols =>
        val sortDur = cols.duration.getOrElse(0L)
        (cols.appIndex, -(sortDur), cols.id)
      }
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No Job/Stage Metrics Found!\n"))
    }
  }

  // SQL Level TaskMetrics Aggregation(Only when SQL exists)
  def sqlMetricsAggregation(): Unit = {
    val messageHeader = "\nSQL level aggregated task metrics:\n"
    fileWriter.foreach(_.write(messageHeader))
    val filtered = apps.filter(app =>
      (app.taskEnd.size > 0) && (app.jobIdToInfo.size > 0) &&
        (app.stageIdToInfo.size > 0) && (app.sqlIdToInfo.size > 0))
    logWarning("here 3 num: " + filtered.size)

    val allRows = filtered.flatMap { app =>
      // TODO - how to deal with attempts?
      app.sqlIdToInfo.map { case (sqlId, sqlCase) =>
        val jcs = app.jobIdToInfo.filter { case (_, jc) =>
          val jcid = jc.sqlID.getOrElse(-1)
          jc.sqlID.getOrElse(-1) == sqlId
        }
        if (jcs.isEmpty) {
          None
        } else {
          val stageIdsForSQL = jcs.flatMap(_._2.stageIds).toSeq
          val tasksInSQL = app.taskEnd.filter { tc =>
            stageIdsForSQL.contains(tc.stageId)
          }
          if (tasksInSQL.isEmpty) {
            None
          } else {
            // don't count duplicate task attempts ???
            // val uniqueTasks = tasksInSQL.groupBy(tc => tc.taskId)
            // count all attempts
            val numAttempts = tasksInSQL.size

            val diskBytes = tasksInSQL.map(_.diskBytesSpilled).sum
            val execCpuTime = tasksInSQL.map(_.executorCPUTime).sum
            val execRunTime = tasksInSQL.map(_.executorRunTime).sum
            val execCPURatio = ToolUtils.calculateDurationPercent(execCpuTime, execRunTime)
            // TODO - set this here make sure we don't get it again until later
            sqlCase.sqlCpuTimePercent = execCPURatio
           
            val (durSum, durMax, durMin, durAvg) = getDurations(tasksInSQL)
            Some(SQLTaskAggMetrics(app.index,
              app.appId,
              sqlId,
              sqlCase.description,
              numAttempts,
              sqlCase.duration,
              execCpuTime,
              execRunTime,
              execCPURatio,
              diskBytes,
              durSum,
              durMax,
              durMin,
              durAvg,
              execCpuTime,
              tasksInSQL.map(_.executorDeserializeCPUTime).sum,
              tasksInSQL.map(_.executorDeserializeTime).sum,
              execRunTime,
              tasksInSQL.map(_.gettingResultTime).sum,
              tasksInSQL.map(_.input_bytesRead).sum,
              tasksInSQL.map(_.input_recordsRead).sum,
              tasksInSQL.map(_.jvmGCTime).sum,
              tasksInSQL.map(_.memoryBytesSpilled).sum,
              tasksInSQL.map(_.output_bytesWritten).sum,
              tasksInSQL.map(_.output_recordsWritten).sum,
              tasksInSQL.map(_.peakExecutionMemory).max,
              tasksInSQL.map(_.resultSerializationTime).sum,
              tasksInSQL.map(_.resultSize).max,
              tasksInSQL.map(_.sr_fetchWaitTime).sum,
              tasksInSQL.map(_.sr_localBlocksFetched).sum,
              tasksInSQL.map(_.sr_localBytesRead).sum,
              tasksInSQL.map(_.sr_remoteBlocksFetched).sum,
              tasksInSQL.map(_.sr_remoteBytesRead).sum,
              tasksInSQL.map(_.sr_remoteBytesReadToDisk).sum,
              tasksInSQL.map(_.sr_totalBytesRead).sum,
              tasksInSQL.map(_.sw_bytesWritten).sum,
              tasksInSQL.map(_.sw_recordsWritten).sum,
              tasksInSQL.map(_.sw_writeTime).sum
            ))
          }
        }
      }
    }
    logWarning("here 1")
    val allFiltered = allRows.filter(_.isDefined).map(_.get)
    if (allFiltered.size > 0) {
      val sortedRows = allFiltered.sortBy { cols =>
        val sortDur = cols.duration.getOrElse(0L)
        (cols.appIndex, -(sortDur), cols.sqlId, cols.executorCpuTime)
      }
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No SQL Metrics Found!\n"))
    }
    logWarning("here 2")

  }

  def sqlMetricsAggregationDurationAndCpuTime(): Unit = {
    val messageHeader = "\nSQL Duration and Executor CPU Time Percent\n"
    fileWriter.foreach(_.write(messageHeader))

    val filtered = apps.filter(app => (app.sqlIdToInfo.size > 0))
    val allRows = filtered.flatMap { app =>
      app.sqlIdToInfo.map { case (sqlId, sqlCase) =>
        // Potential problems not properly track, add it later
        SQLDurationExecutorTime(app.index, app.appId, sqlId, sqlCase.duration,
          sqlCase.hasDataset, app.appInfo.duration, sqlCase.problematic,
          sqlCase.sqlCpuTimePercent)
      }
    }

    if (allRows.size > 0) {
      val sortedRows = allRows.sortBy { cols =>
        val sortDur = cols.duration.getOrElse(0L)
        (cols.appIndex, cols.sqlID, sortDur)
      }
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No SQL Duration and Executor CPU Time Percent Found!\n"))
    }
  }

  private case class AverageStageInfo(avgDuration: Double, avgShuffleReadBytes: Double)

  def shuffleSkewCheck(): Unit = {
    val messageHeader = s"\nShuffle Skew Check:" +
      " (When task's Shuffle Read Size > 3 * Avg Stage-level size)\n"
    fileWriter.foreach(_.write(messageHeader))

    // TODO - how expensive on large number tasks?
    val filtered = apps.filter(app => (app.taskEnd.size > 0) && (app.stageIdToInfo.size > 0))
    val allRows = filtered.flatMap { app =>
      val tasksPerStageAttempt = app.taskEnd.groupBy { tc =>
        (tc.stageId, tc.stageAttemptId)
      }
      val avgsStageInfos = tasksPerStageAttempt.map { case ((sId, saId), tcArr) =>
        val sumDuration = tcArr.map(_.duration).sum
        val avgDuration = ToolUtils.calculateAverage(sumDuration, tcArr.size, 2)
        val sumShuffleReadBytes = tcArr.map(_.sr_totalBytesRead).sum
        val avgShuffleReadBytes = ToolUtils.calculateAverage(sumShuffleReadBytes, tcArr.size, 2)
        ((sId, saId), AverageStageInfo(avgDuration, avgShuffleReadBytes))
      }

      val tasksWithSkew = app.taskEnd.filter { tc =>
        val avgShuffleDur = avgsStageInfos.get((tc.stageId, tc.stageAttemptId))
        avgShuffleDur match {
          case Some(avg) =>
            (tc.sr_totalBytesRead > 3 * avg.avgShuffleReadBytes) &&
              (tc.sr_totalBytesRead > 100 * 1024 * 1024)
          case None => false
        }
      }

      val groupedTasks = tasksWithSkew.groupBy { tc =>
        (tc.stageId, tc.stageAttemptId)
      }

      tasksWithSkew.map { tc =>
        val avgShuffleDur = avgsStageInfos.get((tc.stageId, tc.stageAttemptId))
        avgShuffleDur match {
          case Some(avg) =>
            Some(ShuffleSkewInfo(app.index, tc.stageId, tc.stageAttemptId,
              tc.taskId, tc.attempt, tc.duration, avg.avgDuration, tc.sr_totalBytesRead,
              avg.avgShuffleReadBytes, tc.peakExecutionMemory, tc.successful, tc.endReason))
          case None =>
            None
        }
      }
    }

    val allNonEmptyRows = allRows.filter(_.isDefined).map(_.get)
    if (allNonEmptyRows.size > 0) {
      val sortedRows = allNonEmptyRows.sortBy { cols =>
        (cols.appIndex, cols.stageId, cols.stageAttemptId, cols.taskId, cols.taskAttemptId)
      }
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No SQL Duration and Executor CPU Time Percent Found!\n"))
    }
  }
}
