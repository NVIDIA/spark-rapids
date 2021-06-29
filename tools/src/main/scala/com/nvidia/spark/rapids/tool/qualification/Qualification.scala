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

package com.nvidia.spark.rapids.tool.qualification

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.EventLogInfo
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification._

/**
 * Scores the applications for GPU acceleration and outputs the
 * reports.
 */
object Qualification extends Logging {

  def qualifyApps(
      allPaths: Seq[EventLogInfo],
      numRows: Int,
      outputDir: String,
      hadoopConf: Configuration): ArrayBuffer[QualificationSummaryInfo] = {
    val allAppsSum: ArrayBuffer[QualificationSummaryInfo] = ArrayBuffer[QualificationSummaryInfo]()
    allPaths.foreach { path =>
      val app = QualAppInfo.createApp(path, numRows, hadoopConf)
      if (!app.isDefined) {
        logWarning("No Applications found that contain SQL!")
      } else {
        val qualSumInfo = app.get.aggregateStats()
        if (qualSumInfo.isDefined) {
          allAppsSum += qualSumInfo.get
        } else {
          logWarning(s"No aggregated stats for event log at: $path")
        }
      }
    }
    val sorted = allAppsSum.sortBy(sum => (-sum.score, -sum.sqlDataFrameDuration, -sum.appDuration))
    val qWriter = new QualOutputWriter(outputDir, numRows)
    qWriter.writeCSV(sorted)
    qWriter.writeReport(sorted)
    sorted
  }
}
