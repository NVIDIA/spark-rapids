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
import com.nvidia.spark.rapids.tool.qualification.QualificationMain.{logApplicationInfo, logInfo}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * Ranks the applications for GPU acceleration.
 */
object Qualification extends Logging {

  def qualifyApps(allPaths: ArrayBuffer[Path],
      numRows: Int, sparkSession: SparkSession, includeCpuPercent: Boolean): DataFrame = {
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
      logWarning("before count sql agg")
      // materialize table to cache
      sqlAggMetricsDF.count()
      logWarning("after count sql agg")
    }

    val df = constructQueryQualifyApps(apps, includeCpuPercent)
    // sparkSession.catalog.dropTempView("sqlAggMetricsDF")
    // apps.foreach( _.dropAllTempViews())
    // apps.head.renameQualificationColumns(df)
    df
  }

  def constructQueryQualifyApps(apps: ArrayBuffer[ApplicationInfo],
      includeCpuPercent: Boolean): DataFrame = {
    val query = apps
      .filter(p => p.allDataFrames.contains(s"sqlDF_${p.index}"))
      .map {
        if (includeCpuPercent) {
          "(" + _.qualificationDurationSumSQL + ")"
        } else {
          "(" + _.qualificationDurationNoMetricsSQL + ")"
        }
      }.mkString(" union ")
    val df = apps.head.runQuery(query + " order by Rank desc, `App Duration` desc")
    df
  }

  def qualifyApps2(apps: ArrayBuffer[ApplicationInfo]): DataFrame = {

    // The query generated for a lot of apps is to big and Spark analyzer
    // takes forever to handle. Break it up and do a few apps at a time and then
    // cache and union at the end. The results of each query per app is tiny so
    // caching should not use much memory.
    val groupedApps = apps.
      filter(p => p.allDataFrames.contains(s"sqlDF_${p.index}")).grouped(4)
    val queries = groupedApps.map { group =>
      group.map("(" + _.qualificationDurationSumSQL + ")")
        .mkString(" union ")
    }
    val subqueryResults = queries.map { query =>
      apps.head.runQuery(query)
    }
    val cached = subqueryResults.toArray
    //val cached = subqueryResults.map(_.cache()).toArray
    // materialize the cached datasets
    // cached.foreach(_.count())
    val finalDf = if (cached.size > 1) {
      cached.reduce(_ union _).orderBy(col("dfRankTotal").desc, col("appDuration").desc)
    } else {
      cached.head.orderBy(col("dfRankTotal").desc, col("appDuration").desc)
    }
    finalDf
  }

  def writeQualification(df: DataFrame,
      outputDir: String, format: String): Unit = {
    // val fileWriter = apps.head.fileWriter
    // val dfRenamed = apps.head.renameQualificationColumns(df)
    if (format.equals("csv")) {
      df.repartition(1).write.option("header", "true").
        mode("overwrite").csv(s"$outputDir/rapids_4_spark_qualification_output")
      logInfo(s"Output log location:  $outputDir")
    } else {
      // This tool's output log file name
      val logFileName = "rapids_4_spark_qualification_output.log"
      val outputFilePath = new Path(s"$outputDir/$logFileName")
      val fs = FileSystem.get(outputFilePath.toUri, new Configuration())
      val outFile = fs.create(outputFilePath)
      outFile.writeUTF(ToolUtils.showString(df, 1000))
      outFile.flush()
      outFile.close()
      logInfo(s"Output log location: $outputFilePath")
    }

    // fileWriter.write("\n" + ToolUtils.showString(dfRenamed,
    //   apps(0).numOutputRows))
  }
}
