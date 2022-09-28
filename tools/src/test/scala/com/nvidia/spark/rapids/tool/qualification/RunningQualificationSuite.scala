/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import java.io.File
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, ToolTestUtils}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession, TrampolineUtil}
import org.apache.spark.sql.functions.{desc, hex, udf}
import org.apache.spark.sql.rapids.tool.{AppBase, AppFilterImpl, ToolUtils}
import org.apache.spark.sql.rapids.tool.qualification.{QualificationAppInfo, QualificationSummaryInfo}
import org.apache.spark.sql.types._

// drop the fields that won't go to DataFrame without encoders
case class TestQualificationSummary(
    appName: String,
    appId: String,
    recommendation: String,
    estimatedGpuSpeedup: Double,
    estimatedGpuDur: Double,
    estimatedGpuTimeSaved: Double,
    sqlDataframeDuration: Long,
    sqlDataframeTaskDuration: Long,
    appDuration: Long,
    gpuOpportunity: Long,
    executorCpuTimePercent: Double,
    failedSQLIds: String,
    readFileFormatAndTypesNotSupported: String,
    writeDataFormat: String,
    complexTypes: String,
    nestedComplexTypes: String,
    potentialProblems: String,
    longestSqlDuration: Long,
    nonSqlTaskDurationAndOverhead: Long,
    unsupportedSQLTaskDuration: Long,
    supportedSQLTaskDuration: Long,
    taskSpeedupFactor: Double,
    endDurationEstimated: Boolean)

class RunningQualificationSuite extends FunSuite with BeforeAndAfterEach with Logging {

  private var sparkSession: SparkSession = _

  private def createSparkSession(): Unit = {
    sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Rapids Spark Profiling Tool Unit Tests")
      .getOrCreate()
  }

  override protected def beforeEach(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    createSparkSession()
  }


  test("running qualification app files with per sql") {
    TrampolineUtil.withTempPath { outParquetFile =>
      TrampolineUtil.withTempPath { outJsonFile =>

        val qualApp = new RunningQualificationApp()
        ToolTestUtils.runAndCollect("streaming") { spark =>
          val listener = qualApp.getEventListener
          spark.sparkContext.addSparkListener(listener)
          import spark.implicits._
          val testData = Seq((1, 2), (3, 4)).toDF("a", "b")
          testData.write.json(outJsonFile.getCanonicalPath)
          testData.write.parquet(outParquetFile.getCanonicalPath)
          val df = spark.read.parquet(outParquetFile.getCanonicalPath)
          val df2 = spark.read.json(outJsonFile.getCanonicalPath)
          df.join(df2.select($"a" as "a2"), $"a" === $"a2")
        }
        // just basic testing that line exists and has right separator
        val csvHeader = qualApp.getPerSqlCSVHeader
        assert(csvHeader.contains("App Name,App ID,SQL ID,SQL Description,SQL DF Duration," +
          "GPU Opportunity,Estimated GPU Duration,Estimated GPU Speedup," +
          "Estimated GPU Time Saved,Recommendation"))
        val txtHeader = qualApp.getPerSqlTextHeader
        assert(txtHeader.contains("|                              App Name|             App ID|" +
          "SQL ID" +
          "|                                                                                     " +
          "SQL Description|" +
          "SQL DF Duration|GPU Opportunity|Estimated GPU Duration|" +
          "Estimated GPU Speedup|Estimated GPU Time Saved|      Recommendation|"))
        val randHeader = qualApp.getPerSqlHeader(";", true, 20)
        assert(randHeader.contains(";                              App Name;             App ID" +
          ";SQL ID;     SQL Description;SQL DF Duration;GPU Opportunity;Estimated GPU Duration;" +
          "Estimated GPU Speedup;Estimated GPU Time Saved;      Recommendation;"))
        val allSQLIds = qualApp.getAvailableSqlIDs
        val numSQLIds = allSQLIds.size
        assert(numSQLIds > 0)
        val sqlIdToLookup = allSQLIds.head
        val (csvOut, txtOut) = qualApp.getPerSqlTextAndCSVSummary(sqlIdToLookup)
        assert(txtOut.contains("QualificationSuite.scala") && txtOut.contains("|"),
          s"TXT output was: $txtOut")
        assert(csvOut.nonEmpty)
        assert(csvOut.contains("QualificationSuite.scala") && csvOut.contains(","),
          s"CSV output was: $csvOut")
        val sqlOut = qualApp.getPerSQLSummary(sqlIdToLookup, ":", true, 5)
        assert(sqlOut.contains(":json :"), s"SQL output was: $sqlOut")
        qualApp.cleanupSQL(sqlIdToLookup)
        assert(qualApp.getAvailableSqlIDs.size == numSQLIds - 1)

        // test different delimiter
        val sumOut = qualApp.getSummary(":", false)
        val rowsSumOut = sumOut.split("\n")
        assert(rowsSumOut.size == 2)
        val headers = rowsSumOut(0).split(":")
        val values = rowsSumOut(1).split(":")
        val appInfo = qualApp.aggregateStats()
        assert(appInfo.nonEmpty)
        assert(headers.size ==
          QualOutputWriter.getSummaryHeaderStringsAndSizes(30, 30).keys.size)
        assert(values.size == headers.size - 1) // UnsupportedExpr is empty
        // 3 should be the SQL DF Duration
        assert(headers(3).contains("SQL DF"))
        assert(values(3).toInt > 0)
        val detailedOut = qualApp.getDetailed(":", prettyPrint = false, reportReadSchema = true)
        val rowsDetailedOut = detailedOut.split("\n")
        assert(rowsDetailedOut.size == 2)
        val headersDetailed = rowsDetailedOut(0).split(":")
        val valuesDetailed = rowsDetailedOut(1).split(":")
        // Check Read Schema contains json and parquet
        val readSchemaIndex = headersDetailed.length - 1
        assert(headersDetailed(readSchemaIndex).contains("Read Schema"))
        assert(
          valuesDetailed(readSchemaIndex).contains("json") &&
            valuesDetailed(readSchemaIndex).contains("parquet"))
      }
    }
  }
}

class ToolTestListener extends SparkListener {
  val completedStages = new ListBuffer[SparkListenerStageCompleted]()
  var executorCpuTime = 0L

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    executorCpuTime += NANOSECONDS.toMillis(taskEnd.taskMetrics.executorCpuTime)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    completedStages.append(stageCompleted)
  }
}
