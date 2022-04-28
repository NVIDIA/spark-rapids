/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, ToolTestUtils}
import com.nvidia.spark.rapids.tool.qualification._
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.qualification.QualificationAppInfo

class SQLPlanParserSuite extends FunSuite with BeforeAndAfterEach with Logging {

  private var sparkSession: SparkSession = _

  override protected def beforeEach(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Rapids Spark Profiling Tool Unit Tests")
      .getOrCreate()
  }

  test("WholeStage with Filter and Project") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val listener = new ToolTestListener
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        spark.sparkContext.addSparkListener(listener)
        import spark.implicits._
        val df = spark.sparkContext.makeRDD(1 to 100000, 6).toDF
        val df2 = spark.sparkContext.makeRDD(1 to 100000, 6).toDF
        df.select( $"value" as "a")
          .join(df2.select($"value" as "b"), $"a" === $"b")
          .filter($"b" < 100)
      }

      TrampolineUtil.withTempDir { outpath =>
        val hadoopConf = new Configuration()
        val (_, allEventLogs) = EventLogPathProcessor.processAllPaths(
          None, None, List(eventLog), hadoopConf)
        val pluginTypeChecker = new PluginTypeChecker()
        assert(allEventLogs.size == 1)
        val appOption = QualificationAppInfo.createApp(allEventLogs.head, hadoopConf,
          pluginTypeChecker, 20)
        assert(appOption.nonEmpty)
        val app = appOption.get
        assert(app.sqlPlans.size == 1)
        app.sqlPlans.foreach { case(sqlID, plan) =>
          val planInfo = SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
          assert(planInfo.execInfo == 10)
          val wholeStages = planInfo.execInfo.filter(_.exec == "WholeStageCodegen")
          assert(wholeStages == 5)
          assert(wholeStages.forall(_.isSupported == true))
          assert(wholeStages.forall(_.duration.nonEmpty))
          val allChildren = wholeStages.flatMap(_.children).flatten
          assert(allChildren == 10)

          val filters = allChildren.filter(_.exec == "FilterExec")
          assert(filters.forall(_.speedupFactor == 2))
          assert(filters.forall(_.isSupported == true))
          assert(filters.forall(_.children.isEmpty))
          assert(filters.forall(_.duration.isEmpty))

          val projects = allChildren.filter(_.exec == "ProjectExec")
          assert(projects.forall(_.speedupFactor == 2))
          assert(projects.forall(_.isSupported == true))
          assert(projects.forall(_.children.isEmpty))
          assert(projects.forall(_.duration.isEmpty))
        }
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
