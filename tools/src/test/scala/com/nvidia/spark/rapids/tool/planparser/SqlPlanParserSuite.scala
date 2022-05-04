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
      .master("local[1]")
      .appName("Rapids Spark Profiling Tool Unit Tests")
      .getOrCreate()
  }

  private def assertSizeAndNotSupported(size: Int, execs: Seq[ExecInfo]): Unit = {
    for (t <- Seq(execs)) {
      assert(t.size == size, t)
      assert(t.forall(_.speedupFactor == 1), t)
      assert(t.forall(_.isSupported == false), t)
      assert(t.forall(_.children.isEmpty), t)
      assert(t.forall(_.duration.isEmpty), t)
    }
  }

  private def assertSizeAndSupported(size: Int, execs: Seq[ExecInfo],
      expectedDur: Seq[Option[Long]] = Seq.empty): Unit = {
    for (t <- Seq(execs)) {
      assert(t.size == size, t)
      assert(t.forall(_.speedupFactor == 2), t)
      assert(t.forall(_.isSupported == true), t)
      assert(t.forall(_.children.isEmpty), t)
      if (expectedDur.nonEmpty) {
        val durations = t.map(_.duration)
        val foo = durations.diff(expectedDur)
        assert(durations.diff(expectedDur).isEmpty,
          s"durations differ expected ${expectedDur.mkString(",")} " +
            s"but got ${durations.mkString(",")}")
      } else {
        assert(t.forall(_.duration.isEmpty), t)
      }
    }
  }

  private def createAppFromEventlog(eventLog: String): QualificationAppInfo = {
    val hadoopConf = new Configuration()
    val (_, allEventLogs) = EventLogPathProcessor.processAllPaths(
      None, None, List(eventLog), hadoopConf)
    val pluginTypeChecker = new PluginTypeChecker()
    assert(allEventLogs.size == 1)
    val appOption = QualificationAppInfo.createApp(allEventLogs.head, hadoopConf,
      pluginTypeChecker, 20)
    assert(appOption.nonEmpty)
    appOption.get
  }

  test("WholeStage with Filter, Project and Exchange") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "WholeStageFilterProject") { spark =>
        import spark.implicits._
        val df = spark.sparkContext.makeRDD(1 to 100000, 6).toDF
        val df2 = spark.sparkContext.makeRDD(1 to 100000, 6).toDF
        df.select( $"value" as "a")
          .join(df2.select($"value" as "b"), $"a" === $"b")
          .filter($"b" < 100)
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      app.sqlPlans.foreach { case (sqlID, plan) =>
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
        val filters = allChildren.filter(_.exec == "Filter")
        assertSizeAndSupported(2, filters)
        val projects = allChildren.filter(_.exec == "Project")
        assertSizeAndSupported(2, projects)
        val exchanges = planInfo.execInfo.filter(_.exec == "Exchange")
        assertSizeAndSupported(2, exchanges)
      }
    }
  }

  test("FileSourceScan") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val eventLog = s"$profileLogDir/eventlog_dsv1.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size == 7)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
    }
    val allExecInfo = parsedPlans.flatMap(_.execInfo)
    val json = allExecInfo.filter(_.exec.contains("Scan json"))
    val orc = allExecInfo.filter(_.exec.contains("Scan orc"))
    val parquet = allExecInfo.filter(_.exec.contains("Scan parquet"))
    val text = allExecInfo.filter(_.exec.contains("Scan text"))
    val csv = allExecInfo.filter(_.exec.contains("Scan csv"))
    assertSizeAndNotSupported(2, json.toSeq)
    assertSizeAndNotSupported(1, text.toSeq)
    for (t <- Seq(parquet, csv)) {
      assertSizeAndSupported(1, t.toSeq)
    }
    assertSizeAndSupported(2, orc.toSeq)
  }

  test("BatchScan") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val eventLog = s"$profileLogDir/eventlog_dsv2.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size == 9)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
    }
    val allExecInfo = parsedPlans.flatMap(_.execInfo)
    // Note that the text scan from this file is v1 so ignore it
    val json = allExecInfo.filter(_.exec.contains("BatchScan json"))
    val orc = allExecInfo.filter(_.exec.contains("BatchScan orc"))
    val parquet = allExecInfo.filter(_.exec.contains("BatchScan parquet"))
    val csv = allExecInfo.filter(_.exec.contains("BatchScan csv"))
    assertSizeAndNotSupported(3, json.toSeq)
    assertSizeAndSupported(1, csv.toSeq)
    for (t <- Seq(orc, parquet)) {
      assertSizeAndSupported(2, t.toSeq)
    }
  }

  test("InsertIntoHadoopFsRelationCommand") {
    TrampolineUtil.withTempDir { outputLoc =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
          "InsertIntoHadoopFsRelationCommand") { spark =>
          import spark.implicits._
          val df = spark.sparkContext.makeRDD(1 to 10000, 6).toDF
          val dfWithStrings = df.select(col("value").cast("string"))
          dfWithStrings.write.text(s"$outputLoc/testtext")
          df.write.parquet(s"$outputLoc/testparquet")
          df.write.orc(s"$outputLoc/testorc")
          df.write.json(s"$outputLoc/testjson")
          df.write.csv(s"$outputLoc/testcsv")
          df
        }
        val pluginTypeChecker = new PluginTypeChecker()
        val app = createAppFromEventlog(eventLog)
        assert(app.sqlPlans.size == 6)
        val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
        }
        val allExecInfo = parsedPlans.flatMap(_.execInfo)
        val text = allExecInfo.filter(_.exec.contains("InsertIntoHadoopFsRelationCommand text"))
        val json = allExecInfo.filter(_.exec.contains("InsertIntoHadoopFsRelationCommand json"))
        val orc = allExecInfo.filter(_.exec.contains("InsertIntoHadoopFsRelationCommand orc"))
        val parquet =
          allExecInfo.filter(_.exec.contains("InsertIntoHadoopFsRelationCommand parquet"))
        val csv = allExecInfo.filter(_.exec.contains("InsertIntoHadoopFsRelationCommand csv"))
        for (t <- Seq(json, csv, text)) {
          assertSizeAndNotSupported(1, t.toSeq)
        }
        for (t <- Seq(orc, parquet)) {
          assertSizeAndSupported(1, t.toSeq)
        }
      }
    }
  }

  test("CreateDataSourceTableAsSelectCommand") {
    val qualLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")
    // using event log to not deal with enabling hive support
    val eventLog = s"$qualLogDir/createdatasourcetable_eventlog.zstd"
    val app = createAppFromEventlog(eventLog)
    val pluginTypeChecker = new PluginTypeChecker()
    assert(app.sqlPlans.size == 1)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
    }
    val allExecInfo = parsedPlans.flatMap(_.execInfo)
    val parquet = {
      allExecInfo.filter(_.exec.contains("CreateDataSourceTableAsSelectCommand"))
    }
    assertSizeAndNotSupported(1, parquet.toSeq)
  }

  test("InMemoryTableScan") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "InMemoryTableScan") { spark =>
        import spark.implicits._
        val df = spark.sparkContext.makeRDD(1 to 10000, 6).toDF
        val dfc = df.cache()
        dfc
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
      }
      val allExecInfo = parsedPlans.flatMap(_.execInfo)
      val tableScan = allExecInfo.filter(_.exec == ("InMemoryTableScan"))
      assertSizeAndSupported(1, tableScan.toSeq)
    }
  }

  test("BroadcastExchangeExec and SubqueryBroadcastExec") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")
    val eventLog = s"$profileLogDir/nds_q86_test"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size > 0)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
    }
    val allExecInfo = parsedPlans.flatMap(_.execInfo)
    val broadcasts = allExecInfo.filter(_.exec == "BroadcastExchange")
    val subqueryBroadcast = allExecInfo.filter(_.exec == "SubqueryBroadcast")
    assertSizeAndSupported(3, broadcasts.toSeq, Seq(Some(1154), Some(1154), Some(1855)))
    assertSizeAndSupported(1, subqueryBroadcast.toSeq, Seq(Some(1175)))
  }

  test("CustomShuffleReaderExec") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")
    // this is eventlog because CustomShuffleReaderExec only available before 3.2.0
    val eventLog = s"$profileLogDir/customshuffle_eventlog.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size > 0)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
    }
    val allExecInfo = parsedPlans.flatMap(_.execInfo)
    val reader = allExecInfo.filter(_.exec == "CustomShuffleReader")
    assertSizeAndSupported(2, reader.toSeq)
  }

  test("AQEShuffleReadExec") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")
    // this is eventlog because AQEShuffleReadExec only available after 3.2.0
    val eventLog = s"$profileLogDir/aqeshuffle_eventlog.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size > 0)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
    }
    val allExecInfo = parsedPlans.flatMap(_.execInfo)
    val reader = allExecInfo.filter(_.exec == "AQEShuffleRead")
    assertSizeAndSupported(2, reader.toSeq)
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
