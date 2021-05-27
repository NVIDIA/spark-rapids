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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * Qualifies or disqualifies an application for GPU acceleration.
 */
class Qualification(
    apps: ArrayBuffer[ApplicationInfo],
    sqlAggMetricsDF: DataFrame) extends Logging {

  require(apps.nonEmpty)
  require(!sqlAggMetricsDF.isEmpty)
  private val fileWriter = apps.head.fileWriter

  // Register sqlAggMetricsDF as a temp view
  sqlAggMetricsDF.createOrReplaceTempView("sqlAggMetricsDF")

  // Qualify each App
  for (app <- apps) {
    qualifyApp(app)
  }

  // Function to qualify an application. Below criteria is used to decide if the application can
  // be qualified.
  // 1. If the application doesn't contain SQL, then it is disqualified.
  // 2. If the application has SQL, below 2 conditions have to be met to mark it as qualified:
  //    a. SQL duration is greater than 30 seconds.
  //    b. executorCPUTime_sum/executorRunTime_sum > 30 ( atleast 30%)
  def qualifyApp(app: ApplicationInfo): Boolean = {

    // If this application does not have SQL
    if (!app.allDataFrames.contains(s"sqlDF_${app.index}")) {
      logInfo(s"${app.appId} (index=${app.index}) is disqualified because no SQL is inside.")
      fileWriter.write(s"${app.appId} (index=${app.index}) is " +
        s"disqualified because no SQL is inside.\n")
      return false
    }

    val dfProb = app.queryToDF(app.qualificationSQLDataSet)
    if (!dfProb.isEmpty) {
      logInfo(s"${app.appId} (index=${app.index}) is disqualified because it is problematic " +
        "(UDF, Dataset, etc).")
      fileWriter.write(s"${app.appId} (index=${app.index}) is " +
        s"disqualified because problematic (UDF, Dataset, etc.)\n")
      fileWriter.write("Reason disqualified:\n")
      fileWriter.write(ToolUtils.showString(dfProb, app.args.numOutputRows.getOrElse(1000)))
    }
    val df = app.queryToDF(app.qualificationSQL)
    if (df.isEmpty) {
      logInfo(s"${app.appId} (index=${app.index}) is disqualified because no SQL is qualified.")
      fileWriter.write(s"${app.appId} (index=${app.index}) is " +
        s"disqualified because no SQL is qualified\n")
      false
    } else {
      fileWriter.write(s"${app.appId} (index=${app.index}) " +
        s"is qualified with below qualified SQL(s):\n")
      fileWriter.write("\n" + ToolUtils.showString(df, app.args.numOutputRows.getOrElse(1000)))
      true
    }
  }
}
