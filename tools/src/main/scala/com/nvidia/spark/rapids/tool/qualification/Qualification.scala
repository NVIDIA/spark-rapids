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

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import com.nvidia.spark.rapids.tool.profiling.Analysis
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * Ranks the applications for GPU acceleration.
 */
object Qualification extends Logging {

  def logApplicationInfo(app: ApplicationInfo) = {
    logInfo(s"==============  ${app.appId} (index=${app.index})  ==============")
  }

  def qualifyApps(
      allPaths: ArrayBuffer[Path],
      numRows: Int,
      sparkSession: SparkSession,
      includeCpuPercent: Boolean,
      dropTempViews: Boolean): Option[DataFrame] = {
    var index: Int = 1
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    for (path <- allPaths.filterNot(_.getName.contains("."))) {
      try {
        // This apps only contains 1 app in each loop.
        val app = new ApplicationInfo(numRows, sparkSession, path, index, true)
        apps += app
        logApplicationInfo(app)
        index += 1
      } catch {
        case e: com.fasterxml.jackson.core.JsonParseException =>
          logWarning(s"Error parsing JSON, skipping $path")
      }
    }
    if (apps.isEmpty) return None
    val analysis = new Analysis(apps, None)
    if (includeCpuPercent) {
      val sqlAggMetricsDF = analysis.sqlMetricsAggregationQual()
      sqlAggMetricsDF.cache().createOrReplaceTempView("sqlAggMetricsDF")
      // materialize table to cache
      sqlAggMetricsDF.count()
    }

    val df = constructQueryQualifyApps(apps, includeCpuPercent)
    if (dropTempViews) {
      sparkSession.catalog.dropTempView("sqlAggMetricsDF")
      apps.foreach( _.dropAllTempViews())
    }
    Some(df)
  }

  def constructQueryQualifyApps(apps: ArrayBuffer[ApplicationInfo],
      includeCpuPercent: Boolean): DataFrame = {
    val query = apps
      .filter(p => p.allDataFrames.contains(s"sqlDF_${p.index}"))
      .map { app =>
        includeCpuPercent match {
          case true => "(" + app.qualificationDurationSumSQL + ")"
          case false => "(" + app.qualificationDurationNoMetricsSQL + ")"
        }
      }.mkString(" union ")
    if (query.nonEmpty) {
      apps.head.runQuery(query + " order by Score desc, `App Duration` desc")
    } else {
      apps.head.sparkSession.emptyDataFrame
    }
  }

  def writeQualification(df: DataFrame, outputDir: String,
      format: String, includeCpuPercent:Boolean, numOutputRows: Int): Unit = {
    val finalOutputDir = s"$outputDir/rapids_4_spark_qualification_output"
    format match {
      case "csv" =>
        df.repartition(1).write.option("header", "true").
          mode("overwrite").csv(finalOutputDir)
        logInfo(s"Output log location:  $finalOutputDir")
      case "text" =>
        val logFileName = "rapids_4_spark_qualification_output.log"
        val textFileWriter = new ToolTextFileWriter(finalOutputDir, logFileName)
        textFileWriter.write(df, numOutputRows)
        textFileWriter.close()
      case _ => logError("Invalid format")
    }
  }
}
