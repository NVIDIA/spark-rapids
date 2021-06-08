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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * Does analysis on the DataFrames
 * from object of ApplicationInfo
 */
class Analysis(apps: ArrayBuffer[ApplicationInfo], fileWriter: Option[ToolTextFileWriter]) {

  require(apps.nonEmpty)

  // Job Level TaskMetrics Aggregation
  def jobMetricsAggregation(): Unit = {
    val messageHeader = "\nJob level aggregated task metrics:\n"
    if (apps.size == 1) {
      apps.head.runQuery(apps.head.jobMetricsAggregationSQL + " order by Duration desc",
        false, fileWriter, messageHeader)
    } else {
      var query = ""
      for (app <- apps) {
        if (query.isEmpty) {
          query += app.jobMetricsAggregationSQL
        } else {
          query += " union " + app.jobMetricsAggregationSQL
        }
      }
      apps.head.runQuery(query + " order by appIndex, Duration desc",
        false, fileWriter, messageHeader)
    }
  }

  // Stage Level TaskMetrics Aggregation
  def stageMetricsAggregation(): Unit = {
    val messageHeader = "\nStage level aggregated task metrics:\n"
    if (apps.size == 1) {
      apps.head.runQuery(apps.head.stageMetricsAggregationSQL + " order by Duration desc",
        false, fileWriter, messageHeader)
    } else {
      var query = ""
      for (app <- apps) {
        if (query.isEmpty) {
          query += app.stageMetricsAggregationSQL
        } else {
          query += " union " + app.stageMetricsAggregationSQL
        }
      }
      apps.head.runQuery(query + " order by appIndex, Duration desc",
        false, fileWriter, messageHeader)
    }
  }

  // Job + Stage Level TaskMetrics Aggregation
  def jobAndStageMetricsAggregation(): DataFrame = {
    val messageHeader = "\nJob + Stage level aggregated task metrics:\n"
    if (apps.size == 1) {
      apps.head.runQuery(apps.head.jobAndStageMetricsAggregationSQL + " order by Duration desc",
        false, fileWriter, messageHeader)
    } else {
      var query = ""
      for (app <- apps) {
        if (query.isEmpty) {
          query += app.jobAndStageMetricsAggregationSQL
        } else {
          query += " union " + app.jobAndStageMetricsAggregationSQL
        }
      }
      apps.head.runQuery(query + " order by appIndex, Duration desc",
        false, fileWriter, messageHeader)
    }
  }

  // SQL Level TaskMetrics Aggregation(Only when SQL exists)
  def sqlMetricsAggregation(): DataFrame = {
    val messageHeader = "\nSQL level aggregated task metrics:\n"
    if (apps.size == 1) {
      if (apps.head.allDataFrames.contains(s"sqlDF_${apps.head.index}")) {
        apps.head.runQuery(apps.head.sqlMetricsAggregationSQL + " order by Duration desc",
          false, fileWriter, messageHeader)
      } else {
        apps.head.sparkSession.emptyDataFrame
      }
    } else {
      var query = ""
      val appsWithSQL = apps.filter(p => p.allDataFrames.contains(s"sqlDF_${p.index}"))
      for (app <- appsWithSQL) {
        if (query.isEmpty) {
          query += app.sqlMetricsAggregationSQL
        } else {
          query += " union " + app.sqlMetricsAggregationSQL
        }
      }
      apps.head.runQuery(query + " order by appIndex, Duration desc", false,
        fileWriter, messageHeader)
    }
  }

  // sql metrics aggregation specific for qualification because
  // it aggregates executor time metrics differently
  def sqlMetricsAggregationQual(): DataFrame = {
    val query = apps
      .filter(p => p.allDataFrames.contains(s"sqlDF_${p.index}"))
      .map( app => "(" + app.sqlMetricsAggregationSQLQual + ")")
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
      .filter(p => p.allDataFrames.contains(s"sqlDF_${p.index}"))
      .map( app => "(" + app.profilingDurationSQL+ ")")
      .mkString(" union ")
    if (query.nonEmpty) {
      apps.head.runQuery(query, false, fileWriter, messageHeader)
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

  def shuffleSkewCheckSingleApp(app: ApplicationInfo): DataFrame ={
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
         |substr(t.endReason,0,100) endReason_first100char
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
  }
}
