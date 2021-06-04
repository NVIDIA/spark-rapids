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

import java.io.File

import com.nvidia.spark.rapids.tool.ToolTestUtils
import org.scalatest.FunSuite
import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.{SparkSession, TrampolineUtil}

class QualificationSuite extends FunSuite with Logging {

  lazy val sparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Rapids Spark Profiling Tool Unit Tests")
      .getOrCreate()
  }

  private val expRoot = ToolTestUtils.getTestResourceFile("QualificationExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")

  private def runQualificationTest(eventLogs: Array[String], expectFileName: String) = {
    Seq(true, false).foreach { hasExecCpu =>
      TrampolineUtil.withTempDir { outpath =>
        val resultExpectation = new File(expRoot, expectFileName)
        val outputArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath())
        val allArgs = hasExecCpu match  {
          case true => outputArgs ++ Array("--include-exec-cpu-percent")
          case false => outputArgs
        }
        val appArgs = new QualificationArgs(allArgs ++ eventLogs)

        val (exit, dfQualOpt) =
          QualificationMain.mainInternal(sparkSession, appArgs, writeOutput=false,
            dropTempViews=true)
        assert(exit == 0)
        // make sure to change null value so empty strings don't show up as nulls
        val dfExpectOrig = sparkSession.read.option("header", "true").
          option("nullValue", "-").csv(resultExpectation.getPath)
        val dfExpect = if (hasExecCpu) dfExpectOrig else dfExpectOrig.drop("executorCPURatio")
        val diffCount = dfQualOpt.map { dfQual =>
          dfQual.except(dfExpect).union(dfExpect.except(dfExpect)).count
        }.getOrElse(-1)

        // print for easier debugging
        if (diffCount != 0) {
          logWarning("Diff:")
          dfExpect.show()
          dfQualOpt.foreach(_.show())
        }
        assert(diffCount == 0)
      }
    }
  }

  test("test udf event logs") {
    val logFiles = Array(
      s"$logDir/dataset_eventlog",
      s"$logDir/dsAndDf_eventlog",
      s"$logDir/udf_dataset_eventlog",
      s"$logDir/udf_func_eventlog"
    )
    runQualificationTest(logFiles, "qual_test_simple_expectation.csv")
  }

  test("test missing sql end") {
    val logFiles = Array(s"$logDir/join_missing_sql_end")
    runQualificationTest(logFiles, "qual_test_missing_sql_end_expectation.csv")
  }

  test("test truncated log file 1") {
    val logFiles = Array(s"$logDir/truncated_eventlog")
    runQualificationTest(logFiles, "truncated_1_end_expectation.csv")
  }

  test("test nds q86 test") {
    val logFiles = Array(s"$logDir/nds_q86_test")
    runQualificationTest(logFiles, "nds_q86_test_expectation.csv")
  }

  test("sql metric agg") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val listener = new ToolTestListener
      val eventLog = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        spark.sparkContext.addSparkListener(listener)
        import spark.implicits._
        val t1 = Seq((1, 2), (3, 4)).toDF("a", "b")
        t1.createOrReplaceTempView("t1")
        spark.sql("SELECT a, MAX(b) FROM t1 GROUP BY a ORDER BY a")
      }

      TrampolineUtil.withTempDir { outpath =>
        val appArgs = new QualificationArgs(Array(
          "--include-exec-cpu-percent",
          "--output-directory",
          outpath.getAbsolutePath,
          eventLog))

        val (exit, _) =
          QualificationMain.mainInternal(sparkSession, appArgs, writeOutput = false,
            dropTempViews = false)
        assert(exit == 0)

        val df = sparkSession.table("sqlAggMetricsDF")
        df.show()

        val stage = listener.completedStages.head

        val rows = df.collect()
        assert(rows.length === 1)
        val collect = rows.head
        assert(collect.getString(3).startsWith("collect"))

        // compare metrics from event log with metrics from listener
        assert(collect.getLong(6) === stage.stageInfo.taskMetrics.executorCpuTime)
      }
    }
  }
}

class ToolTestListener extends SparkListener {
  val completedStages = new ListBuffer[SparkListenerStageCompleted]()

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =
    completedStages.append(stageCompleted)
}
