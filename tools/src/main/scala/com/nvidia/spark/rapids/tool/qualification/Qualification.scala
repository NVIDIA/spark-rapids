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

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids.tool.EventLogInfo
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification._

/**
 * Scores the applications for GPU acceleration and outputs the
 * reports.
 */
class Qualification(outputDir: String, numRows: Int, hadoopConf: Configuration,
    timeout: Option[Long], nThreads: Int) extends Logging {

  private val allApps = new ConcurrentLinkedQueue[QualificationSummaryInfo]()
  // default is 24 hours
  private val waitTimeInSec = timeout.getOrElse(60 * 60 * 24L)

/*
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
        (format, (dsArr.flatMap(_.schema.split(",")).toSet, dsArr.exists(_.schemaIncomplete)))
      }

      res.foreach { case (format, info) =>
        logWarning(s" format $format  types are (incomplete: ${info._2}): " + info._1.mkString(", "))
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

*/

  private val threadFactory = new ThreadFactoryBuilder()
    .setDaemon(true).setNameFormat("qualTool" + "-%d").build()
  logInfo(s"Threadpool size is $nThreads")
  private val threadPool = Executors.newFixedThreadPool(nThreads, threadFactory)
    .asInstanceOf[ThreadPoolExecutor]

  private class QualifyThread(path: EventLogInfo) extends Runnable {
    def run: Unit = qualifyApp(path, numRows, hadoopConf)
  }

  def qualifyApps(allPaths: Seq[EventLogInfo]): Seq[QualificationSummaryInfo] = {
    allPaths.foreach { path =>
      try {
        threadPool.submit(new QualifyThread(path))
      } catch {
        case e: Exception =>
          logError(s"Unexpected exception submitting log ${path.eventLog.toString}, skipping!", e)
      }
    }
    // wait for the threads to finish processing the files
    threadPool.shutdown()
    if (!threadPool.awaitTermination(waitTimeInSec, TimeUnit.SECONDS)) {
      logError(s"Processing log files took longer then $waitTimeInSec seconds," +
        " stopping processing any more event logs")
      threadPool.shutdownNow()
    }

    val allAppsSum = allApps.asScala.toSeq
    val sorted = allAppsSum.sortBy(sum => (-sum.score, -sum.sqlDataFrameDuration, -sum.appDuration))
    val qWriter = new QualOutputWriter(outputDir, numRows)
    qWriter.writeCSV(sorted)
    qWriter.writeReport(sorted)
    sorted
  }

<<<<<<< HEAD
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
=======
  private def qualifyApp(
      path: EventLogInfo,
      numRows: Int,
      hadoopConf: Configuration): Unit = {
    try {
      val startTime = System.currentTimeMillis()
      val app = QualAppInfo.createApp(path, numRows, hadoopConf)
      if (!app.isDefined) {
        logWarning(s"No Application found that contain SQL for ${path.eventLog.toString}!")
        None
      } else {
        val qualSumInfo = app.get.aggregateStats()
        if (qualSumInfo.isDefined) {
          allApps.add(qualSumInfo.get)
          val endTime = System.currentTimeMillis()
          logInfo(s"Took ${endTime - startTime}ms to process ${path.eventLog.toString}")
        } else {
          logWarning(s"No aggregated stats for event log at: ${path.eventLog.toString}")
        }
      }
    } catch {
      case e: Exception =>
        logError(s"Unexpected exception processing log ${path.eventLog.toString}, skipping!", e)
>>>>>>> origin/branch-21.08
    }
  }
}
