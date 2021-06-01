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
 * Ranks the applications for GPU acceleration.
 */
object Qualification extends Logging {

  def qualifyApps(apps: ArrayBuffer[ApplicationInfo]): DataFrame = {
    var query = ""
    val appsWithSQL = apps.filter(p => p.allDataFrames.contains(s"sqlDF_${p.index}"))
    for (app <- appsWithSQL) {
      if (query.isEmpty) {
        query += app.qualificationDurationSumSQL
      } else {
        query += " union (" + app.qualificationDurationSumSQL + ")"
      }
    }
    val messageHeader = "SQL qualify app union:"
    val df = apps.head.runQuery(query + " order by dfRankTotal desc, appDuration desc")

    df
  }

  def getPercentIO(): DataFrame = {
    val df = app.queryToDF(app.qualificationSQL)

  }

  def writeQualification(apps: ArrayBuffer[ApplicationInfo], df: DataFrame): Unit = {
    val fileWriter = apps.head.fileWriter
    val dfRenamed = apps.head.renameQualificationColumns(df)
    fileWriter.write("\n" + ToolUtils.showString(dfRenamed,
      apps(0).args.numOutputRows.getOrElse(1000)))
  }
}
