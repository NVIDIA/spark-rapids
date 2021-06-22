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

import com.nvidia.spark.rapids.tool.{EventLogInfo, ToolTextFileWriter}
import com.nvidia.spark.rapids.tool.profiling.Analysis

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * Ranks the applications for GPU acceleration.
 */
object Qualification extends Logging {

  def qualifyApps(
      allPaths: Seq[EventLogInfo],
      numRows: Int,
      sparkSession: SparkSession,
      includeCpuPercent: Boolean,
      dropTempViews: Boolean): Option[DataFrame] = {

    val (apps, _) = ApplicationInfo.createApps(allPaths, numRows, sparkSession,
      forQualification = true)
    if (apps.isEmpty) {
      logWarning("No Applications found that contain SQL!")
      return None
    }

    // Add tables for the data source information
    apps.foreach { app =>
      import sparkSession.implicits._
      val df = app.dataSourceInfo.toDF.sort(asc("sqlID"), asc("location"))
      val dfWithApp = df.withColumn("appIndex", lit(app.index.toString))
        .select("appIndex", df.columns:_*)
      // here we want to check the schema of what we are reading so perhaps combine these
      // and just look at data types
      val groupedTypes = app.dataSourceInfo.groupBy(ds => ds.format.toLowerCase)
      val res = groupedTypes.map { case (format, dsArr) =>
        (format -> (dsArr.flatMap(_.schema.split(",")).toSet, dsArr.exists(_.schemaIncomplete)))
      }

      res.foreach { case (format, info) =>
        logWarning(s" format $format  types are (incomplete: ${info._2}: " + info._1.mkString(", "))
      }
      // debug
      // val supportedDf = sparkSession.read.csv("tools/src/main/resources/supportedDataSource.csv")
      // supportedDf.filter(supportedDf("Format") === )
      df.createOrReplaceTempView(s"datasource_${app.index}")
    }

    val analysis = new Analysis(apps, None)
    if (includeCpuPercent) {
      val sqlAggMetricsDF = analysis.sqlMetricsAggregationQual()
      sqlAggMetricsDF.cache().createOrReplaceTempView("sqlAggMetricsDF")
      // materialize table to cache
      sqlAggMetricsDF.count()
    }

    val dfOpt = constructQueryQualifyApps(apps, includeCpuPercent)
    if (dropTempViews) {
      sparkSession.catalog.dropTempView("sqlAggMetricsDF")
      apps.foreach( _.dropAllTempViews())
    }
    dfOpt
  }

  def constructQueryQualifyApps(apps: Seq[ApplicationInfo],
      includeCpuPercent: Boolean): Option[DataFrame] = {
    val (qualApps, nonQualApps) = apps
      .partition { p =>
        p.allDataFrames.contains(s"sqlDF_${p.index}") &&
          p.allDataFrames.contains(s"appDF_${p.index}") &&
          p.allDataFrames.contains(s"jobDF_${p.index}")
      }
    val query = qualApps.map { app =>
        includeCpuPercent match {
          case true => "(" + app.qualificationDurationSumSQL + ")"
          case false => "(" + app.qualificationDurationNoMetricsSQL + ")"
        }
      }.mkString(" union ")
    if (nonQualApps.nonEmpty) {
      logWarning("The following event logs were skipped because the event logs don't " +
        "contain enough information to run qualification on: " +
        s"${nonQualApps.map(_.eventLogInfo.eventLog).mkString(", ")}")
    }
    if (query.nonEmpty) {
      Some(apps.head.runQuery(query + " order by Score desc, `App Duration` desc"))
    } else {
      None
    }
  }

  def writeQualification(df: DataFrame, outputDir: String,
      format: String, includeCpuPercent: Boolean, numOutputRows: Int): Unit = {
    val finalOutputDir = s"$outputDir/rapids_4_spark_qualification_output"
    format match {
      case "csv" =>
        df.repartition(1)
          .sortWithinPartitions(desc("Score"), desc("SQL Dataframe Duration"))
          .write.option("header", "true")
          .mode("overwrite").csv(finalOutputDir)
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
