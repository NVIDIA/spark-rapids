

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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.rapids.tool.profiling._


/**
 * Analysis does analysis on the DataFrames
 * from object of ApplicationInfo
 */
class Analysis(apps: ArrayBuffer[ApplicationInfo]) {

  require(apps.nonEmpty)
  private val fileWriter = apps.head.fileWriter

  // Job Level TaskMetrics Aggregation
  def jobMetricsAggregation(): Unit = {
    if (apps.size == 1) {
      fileWriter.write("Job level aggregated task metrics:")
      apps.head.runQuery(apps.head.jobMetricsAggregationSQL + " order by Duration desc")
    }
    else {
      var query = ""
      var i = 1
      for (app <- apps) {
        if (i == 1) {
          query += app.jobMetricsAggregationSQL
        }
        else {
          query += " union " + app.jobMetricsAggregationSQL
        }
        i += 1
      }
      fileWriter.write("Job level aggregated task metrics:")
      apps.head.runQuery(query + " order by appIndex, Duration desc")
    }
  }

  // Stage Level TaskMetrics Aggregation
  def stageMetricsAggregation(): Unit = {
    if (apps.size == 1) {
      fileWriter.write("Stage level aggregated task metrics:")
      apps.head.runQuery(apps.head.stageMetricsAggregationSQL + " order by Duration desc")
    }
    else {
      var query = ""
      var i = 1
      for (app <- apps) {
        if (i == 1) {
          query += app.stageMetricsAggregationSQL
        }
        else {
          query += " union " + app.stageMetricsAggregationSQL
        }
        i += 1
      }
      fileWriter.write("Stage level aggregated task metrics:")
      apps.head.runQuery(query + " order by appIndex, Duration desc")
    }
  }

  // Job + Stage Level TaskMetrics Aggregation
  def jobAndStageMetricsAggregation(): Unit = {
    if (apps.size == 1) {
      val messageHeader = "Job + Stage level aggregated task metrics:"
      apps.head.runQuery(apps.head.jobAndStageMetricsAggregationSQL + " order by Duration desc")
    }
    else {
      var query = ""
      var i = 1
      for (app <- apps) {
        if (i == 1) {
          query += app.jobAndStageMetricsAggregationSQL
        }
        else {
          query += " union " + app.jobAndStageMetricsAggregationSQL
        }
        i += 1
      }
      fileWriter.write("Job + Stage level aggregated task metrics:")
      apps.head.runQuery(query + " order by appIndex, Duration desc")
    }
  }

  // SQL Level TaskMetrics Aggregation(Only when SQL exists)
  def sqlMetricsAggregation(): DataFrame = {
    if (apps.size == 1) {
      if (apps.head.allDataFrames.contains(s"sqlDF_${apps.head.index}")) {
        val messageHeader = "SQL level aggregated task metrics:"
        apps.head.runQuery(apps.head.sqlMetricsAggregationSQL + " order by Duration desc")
      }
      else {
        apps.head.sparkSession.emptyDataFrame
      }
    }
    else {
      var query = ""
      var i = 1
      val appsWithSQL = apps.filter(p => p.allDataFrames.contains(s"sqlDF_${p.index}"))
      for (app <- appsWithSQL) {
        if (i == 1) {
          query += app.sqlMetricsAggregationSQL
        }
        else {
          query += " union " + app.sqlMetricsAggregationSQL
        }
        i += 1
      }
      val messageHeader = "SQL level aggregated task metrics:"
      apps.head.runQuery(query + " order by appIndex, Duration desc")
    }
  }

  // custom query execution. Normally for debugging use.
  def customQueryExecution(app: ApplicationInfo): Unit = {
    fileWriter.write("Custom query execution:")
    val customQuery =
      s"""select stageId from stageDF_${app.index} limit 1
         |""".stripMargin
    app.runQuery(customQuery)
  }
}