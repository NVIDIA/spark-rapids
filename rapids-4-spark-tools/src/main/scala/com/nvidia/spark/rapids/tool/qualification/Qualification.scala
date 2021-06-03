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

import com.nvidia.spark.rapids.tool.profiling.Analysis
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.rapids.tool.profiling._
import org.apache.spark.sql.types.StringType

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
      dropTempViews: Boolean): DataFrame = {
    var index: Int = 1
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    for (path <- allPaths.filterNot(_.getName.contains("."))) {
      // This apps only contains 1 app in each loop.
      val app = new ApplicationInfo(numRows, sparkSession,
        path, index, true)
      apps += app
      logApplicationInfo(app)
      index += 1
    }
    val analysis = new Analysis(apps, None)
    if (includeCpuPercent) {
      val sqlAggMetricsDF = analysis.sqlMetricsAggregation()
      sqlAggMetricsDF.cache().createOrReplaceTempView("sqlAggMetricsDF")
      // materialize table to cache
      sqlAggMetricsDF.count()
    }

    val df = constructQueryQualifyApps(apps, includeCpuPercent)
    if (dropTempViews) {
      sparkSession.catalog.dropTempView("sqlAggMetricsDF")
      apps.foreach( _.dropAllTempViews())
    }
    df
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
    val df = apps.head.runQuery(query + " order by Rank desc, `App Duration` desc")
    df
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
        // This tool's output log file name
        val logFileName = "rapids_4_spark_qualification_output.log"
        val outputFilePath = new Path(s"$finalOutputDir/$logFileName")
        val fs = FileSystem.get(outputFilePath.toUri, new Configuration())
        // this overwrites existing path
        val outFile = fs.create(outputFilePath)
        outFile.writeBytes(ToolUtils.showString(df, numOutputRows))
        outFile.flush()
        outFile.close()
        logInfo(s"Output log location: $outputFilePath")
      case _ => logError("Invalid format")
    }
  }
}
