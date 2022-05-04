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

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, ToolTestUtils}
import com.nvidia.spark.rapids.tool.qualification._
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, TrampolineUtil}
import org.apache.spark.sql.functions.col
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
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
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
          assert(planInfo.execInfo.size == 9)
          val wholeStages = planInfo.execInfo.filter(_.exec.contains("WholeStageCodegen"))
          assert(wholeStages.size == 5)
          // only 2 in the above example have projects and filters
          val numSupported = wholeStages.filter(_.isSupported).size
          assert(numSupported == 2)
          assert(wholeStages.forall(_.duration.nonEmpty))
          val allChildren = wholeStages.flatMap(_.children).flatten
          assert(allChildren.size == 9)
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

  test("Parsing various Execs - Coalesce, CollectLimit, Expand, Range, Sample" +
      "TakeOrderedAndProject and Union") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        val df1 = spark.sparkContext.makeRDD(1 to 1000, 6).toDF
        df1.coalesce(1).collect // Coalesce
        spark.range(10).where(col("id") === 2).collect // Range
        df1.orderBy("value").limit(10).collect // TakeOrderedAndProject
        df1.limit(10).collect // CollectLimit
        df1.union(df1).collect // Union
        df1.rollup(col("value")).agg(col("value")).collect // Expand
        df1.sample(0.1) // Sample
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
        assert(app.sqlPlans.size == 7)
        val supportedExecs = Array("Coalesce", "CollectLimit", "Expand", "Range", "Sample",
          "TakeOrderedAndProject", "Union")
        app.sqlPlans.foreach { case (sqlID, plan) =>
          val planInfo = SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
          for (execName <- supportedExecs) {
            val supportedExec = planInfo.execInfo.filter(_.exec == execName)
            if (supportedExec.nonEmpty) {
              assert(supportedExec.size == 1)
              assert(supportedExec.forall(_.children.isEmpty))
              assert(supportedExec.forall(_.duration.isEmpty))
              execName match {
                // Execs not supported by default
                case "CollectLimit" => assert(supportedExec.forall(_.isSupported == false))
                  assert(supportedExec.forall(_.speedupFactor == 1), execName)
                case _ => assert(supportedExec.forall(_.isSupported == true))
                  assert(supportedExec.forall(_.speedupFactor == 2), execName)
              }
            }
          }
        }
      }
    }
  }
}
