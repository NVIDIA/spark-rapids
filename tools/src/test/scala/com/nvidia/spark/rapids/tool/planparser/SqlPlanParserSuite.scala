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
import org.apache.spark.sql.functions.{col, explode}
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
      expectedDur: Seq[Option[Long]] = Seq.empty, extraText: String = ""): Unit = {
    for (t <- Seq(execs)) {
      assert(t.size == size, s"$extraText $t")
      assert(t.forall(_.speedupFactor == 2), s"$extraText $t")
      assert(t.forall(_.isSupported == true), s"$extraText $t")
      assert(t.forall(_.children.isEmpty), s"$extraText $t")
      if (expectedDur.nonEmpty) {
        val durations = t.map(_.duration)
        val foo = durations.diff(expectedDur)
        assert(durations.diff(expectedDur).isEmpty,
          s"$extraText durations differ expected ${expectedDur.mkString(",")} " +
            s"but got ${durations.mkString(",")}")
      } else {
        assert(t.forall(_.duration.isEmpty), s"$extraText $t")
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

  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")

  test("WholeStage with Filter, Project and Sort") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "WholeStageFilterProject") { spark =>
        import spark.implicits._
        val df = spark.sparkContext.makeRDD(1 to 100000, 6).toDF
        val df2 = spark.sparkContext.makeRDD(1 to 100000, 6).toDF
        df.select( $"value" as "a")
          .join(df2.select($"value" as "b"), $"a" === $"b")
          .filter($"b" < 100)
          .sort($"b")
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      app.sqlPlans.foreach { case (sqlID, plan) =>
        val planInfo = SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
        assert(planInfo.execInfo.size == 11)
        val wholeStages = planInfo.execInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.size == 6)
        // only 2 in the above example have projects and filters
        val numSupported = wholeStages.filter(_.isSupported).size
        assert(numSupported == 5)
        assert(wholeStages.forall(_.duration.nonEmpty))
        val allChildren = wholeStages.flatMap(_.children).flatten
        assert(allChildren.size == 10)
        val filters = allChildren.filter(_.exec == "Filter")
        assertSizeAndSupported(2, filters)
        val projects = allChildren.filter(_.exec == "Project")
        assertSizeAndSupported(2, projects)
        val sorts = allChildren.filter(_.exec == "Sort")
        assertSizeAndSupported(3, sorts)
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

  test("BroadcastExchangeExec, SubqueryBroadcastExec and Exchange") {
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
    assertSizeAndSupported(3, broadcasts.toSeq, Seq(Some(1154), Some(1154), Some(1855)))
    val subqueryBroadcast = allExecInfo.filter(_.exec == "SubqueryBroadcast")
    assertSizeAndSupported(1, subqueryBroadcast.toSeq, Seq(Some(1175)))
    val exchanges = allExecInfo.filter(_.exec == "Exchange")
    assertSizeAndSupported(2, exchanges.toSeq, Seq(Some(15688), Some(8)))
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
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 7)
      val supportedExecs = Array("Coalesce", "Expand", "Range", "Sample",
        "TakeOrderedAndProject", "Union")
      val unsupportedExecs = Array("CollectLimit")
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
      }
      val allExecInfo = parsedPlans.flatMap(_.execInfo)
      for (execName <- supportedExecs) {
        val execs = allExecInfo.filter(_.exec == execName)
        assertSizeAndSupported(1, execs.toSeq, Seq.empty, execName)
      }
      for (execName <- unsupportedExecs) {
        val execs = allExecInfo.filter(_.exec == execName)
        assertSizeAndNotSupported(1, execs.toSeq)
      }
    }
  }

  test("Parse Execs - CartesianProduct and Generate") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        val genDf = spark.sparkContext.parallelize(List(List(1, 2, 3), List(4, 5, 6)), 4).toDF
        val joinDf1 = spark.sparkContext.makeRDD(1 to 10, 4).toDF
        val joinDf2 = spark.sparkContext.makeRDD(1 to 10, 4).toDF
        genDf.select(explode($"value")).collect
        joinDf1.crossJoin(joinDf2)
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 2)
      val supportedExecs = Array("CartesianProduct", "Generate")
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
      }
      val allExecInfo = parsedPlans.flatMap(_.execInfo)
      for (execName <- supportedExecs) {
        val supportedExec = allExecInfo.filter(_.exec == execName)
        assertSizeAndSupported(1, supportedExec.toSeq)
      }
    }
  }

  // GlobalLimit and LocalLimit is not in physical plan when collect is called on the dataframe.
  // We are reading from static eventlogs to test these execs.
  test("Parse execs - LocalLimit and GlobalLimit") {
    val eventLog = s"$logDir/global_local_limit_eventlog.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size == 1)
    val supportedExecs = Array("GlobalLimit", "LocalLimit")
    app.sqlPlans.foreach { case (sqlID, plan) =>
      val planInfo = SQLPlanParser.parseSQLPlan(plan, sqlID, pluginTypeChecker, app)
      // GlobalLimit and LocalLimit are inside WholeStageCodegen. So getting the children of
      // WholeStageCodegenExec
      val wholeStages = planInfo.execInfo.filter(_.exec.contains("WholeStageCodegen"))
      val allChildren = wholeStages.flatMap(_.children).flatten
      for (execName <- supportedExecs) {
        val supportedExec = allChildren.filter(_.exec == execName)
        assertSizeAndSupported(1, supportedExec)
      }
    }
  }
}
