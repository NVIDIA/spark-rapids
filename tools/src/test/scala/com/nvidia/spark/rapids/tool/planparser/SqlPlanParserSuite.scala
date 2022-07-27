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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, ceil, col, collect_list, count, explode, hex, round, row_number, sum}
import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.qualification.QualificationAppInfo
import org.apache.spark.sql.types.StringType


class SQLPlanParserSuite extends FunSuite with BeforeAndAfterEach with Logging {

  private var sparkSession: SparkSession = _

  private val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
  private val qualLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")

  override protected def beforeEach(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    sparkSession = SparkSession
      .builder()
      .master("local[*]")
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

  private def assertSizeAndSupported(size: Int, execs: Seq[ExecInfo], speedUpFactor: Double = 3.0,
      expectedDur: Seq[Option[Long]] = Seq.empty, extraText: String = ""): Unit = {
    for (t <- Seq(execs)) {
      assert(t.size == size, s"$extraText $t")
      assert(t.forall(_.speedupFactor == speedUpFactor), s"$extraText $t")
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
      pluginTypeChecker, reportSqlLevel = false)
    assert(appOption.nonEmpty)
    appOption.get
  }

  private def getAllExecsFromPlan(plans: Seq[PlanInfo]): Seq[ExecInfo] = {
    val topExecInfo = plans.flatMap(_.execInfo)
    topExecInfo.flatMap { e =>
      e.children.getOrElse(Seq.empty) :+ e
    }
  }

  test("WholeStage with Filter, Project, Sort and SortMergeJoin") {
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
        val planInfo = SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "",
          pluginTypeChecker, app)
        assert(planInfo.execInfo.size == 11)
        val wholeStages = planInfo.execInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.size == 6)
        // only 2 in the above example have projects and filters, 3 have sort and 1 has SMJ
        val numSupported = wholeStages.filter(_.isSupported).size
        assert(numSupported == 6)
        assert(wholeStages.forall(_.duration.nonEmpty))
        val allChildren = wholeStages.flatMap(_.children).flatten
        assert(allChildren.size == 10)
        val filters = allChildren.filter(_.exec == "Filter")
        assertSizeAndSupported(2, filters, 2.4)
        val projects = allChildren.filter(_.exec == "Project")
        assertSizeAndSupported(2, projects)
        val sorts = allChildren.filter(_.exec == "Sort")
        assertSizeAndSupported(3, sorts, 5.2)
        val smj = allChildren.filter(_.exec == "SortMergeJoin")
        assertSizeAndSupported(1, smj, 14.1)
      }
    }
  }

  test("HashAggregate") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "sqlMetric") { spark =>
        import spark.implicits._
        spark.range(10).
            groupBy('id % 3 as "group").agg(sum("id") as "sum")
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      app.sqlPlans.foreach { case (sqlID, plan) =>
        val planInfo = SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "",
          pluginTypeChecker, app)
        val wholeStages = planInfo.execInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.size == 2)
        val numSupported = wholeStages.filter(_.isSupported).size
        assert(numSupported == 2)
        assert(wholeStages.forall(_.duration.nonEmpty))
        val allChildren = wholeStages.flatMap(_.children).flatten
        val hashAggregate = allChildren.filter(_.exec == "HashAggregate")
        assertSizeAndSupported(2, hashAggregate, 3.4)
      }
    }
  }

  test("FileSourceScan") {
    val eventLog = s"$profileLogDir/eventlog_dsv1.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size == 7)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
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
    val eventLog = s"$profileLogDir/eventlog_dsv2.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size == 9)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
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
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
        }
        val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
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
    // using event log to not deal with enabling hive support
    val eventLog = s"$qualLogDir/createdatasourcetable_eventlog.zstd"
    val app = createAppFromEventlog(eventLog)
    val pluginTypeChecker = new PluginTypeChecker()
    assert(app.sqlPlans.size == 1)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "",
        pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
    val parquet = {
      allExecInfo.filter(_.exec.contains("CreateDataSourceTableAsSelectCommand"))
    }
    assertSizeAndNotSupported(1, parquet.toSeq)
  }

  test("Stages and jobs failure") {
    val eventLog = s"$profileLogDir/tasks_executors_fail_compressed_eventlog.zstd"
    val app = createAppFromEventlog(eventLog)
    val stats = app.aggregateStats()
    assert(stats.nonEmpty)
    val estimatedGpuSpeed = stats.get.estimatedInfo.estimatedGpuSpeedup
    val recommendation = stats.get.estimatedInfo.recommendation
    assert (ToolUtils.truncateDoubleToTwoDecimal(estimatedGpuSpeed) == 1.11)
    assert(recommendation.equals("Not Applicable"))
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
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val tableScan = allExecInfo.filter(_.exec == ("InMemoryTableScan"))
      assertSizeAndSupported(1, tableScan.toSeq)
    }
  }

  test("BroadcastExchangeExec, SubqueryBroadcastExec and Exchange") {
    val eventLog = s"$qualLogDir/nds_q86_test"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size > 0)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
    val broadcasts = allExecInfo.filter(_.exec == "BroadcastExchange")
    assertSizeAndSupported(3, broadcasts.toSeq,
      expectedDur = Seq(Some(1154), Some(1154), Some(1855)))
    val subqueryBroadcast = allExecInfo.filter(_.exec == "SubqueryBroadcast")
    assertSizeAndSupported(1, subqueryBroadcast.toSeq, expectedDur = Seq(Some(1175)))
    val exchanges = allExecInfo.filter(_.exec == "Exchange")
    assertSizeAndSupported(2, exchanges.toSeq, 3.1, expectedDur = Seq(Some(15688), Some(8)))
  }

  test("CustomShuffleReaderExec") {
    // this is eventlog because CustomShuffleReaderExec only available before 3.2.0
    val eventLog = s"$qualLogDir/customshuffle_eventlog.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size > 0)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
    val reader = allExecInfo.filter(_.exec == "CustomShuffleReader")
    assertSizeAndSupported(2, reader.toSeq)
  }

  test("AQEShuffleReadExec") {
    // this is eventlog because AQEShuffleReadExec only available after 3.2.0
    val eventLog = s"$qualLogDir/aqeshuffle_eventlog.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size > 0)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
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
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      for (execName <- supportedExecs) {
        val execs = allExecInfo.filter(_.exec == execName)
        assertSizeAndSupported(1, execs.toSeq, expectedDur = Seq.empty, extraText = execName)
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
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      for (execName <- supportedExecs) {
        val supportedExec = allExecInfo.filter(_.exec == execName)
        assertSizeAndSupported(1, supportedExec.toSeq)
      }
    }
  }

  test("Parse Execs - BroadcastHashJoin, BroadcastNestedLoopJoin and ShuffledHashJoin") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        val df1 = spark.sparkContext.parallelize(List(1, 2, 3, 4)).toDF
        val df2 = spark.sparkContext.parallelize(List(4, 5, 6, 2)).toDF
        // BroadcastHashJoin
        df1.join(broadcast(df2), "value").collect
        // ShuffledHashJoin
        df1.createOrReplaceTempView("t1")
        df2.createOrReplaceTempView("t2")
        spark.sql("SELECT /*+ SHUFFLE_HASH(t1) */ * FROM t1 INNER JOIN t2 ON " +
            "t1.value = t2.value").collect
        // BroadcastNestedLoopJoin
        val nums = spark.range(2)
        val letters = ('a' to 'c').map(_.toString).toDF("letter")
        nums.crossJoin(letters)
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 5)
      val supportedExecs = Array("BroadcastHashJoin", "BroadcastNestedLoopJoin", "ShuffledHashJoin")
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val bhj = allExecInfo.filter(_.exec == "BroadcastHashJoin")
      assertSizeAndSupported(1, bhj, 3.0)
      val broadcastNestedJoin = allExecInfo.filter(_.exec == "BroadcastNestedLoopJoin")
      assertSizeAndSupported(1, broadcastNestedJoin)
      val shj = allExecInfo.filter(_.exec == "ShuffledHashJoin")
      assertSizeAndSupported(1, shj)
    }
  }

  test("Parse Exec - SortAggregate") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        spark.conf.set("spark.sql.execution.useObjectHashAggregateExec", "false")
        val df1 = Seq((1, "a"), (1, "aa"), (1, "a"), (2, "b"),
                         (2, "b"), (3, "c"), (3, "c")).toDF("num", "letter")
        df1.groupBy("num").agg(collect_list("letter").as("collected_letters"))
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val execInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val sortAggregate = execInfo.filter(_.exec == "SortAggregate")
      assertSizeAndSupported(2, sortAggregate)
    }
  }

  test("Parse Exec - ObjectHashAggregate") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        val df1 = Seq((1, "a"), (1, "aa"), (1, "a"), (2, "b"),
          (2, "b"), (3, "c"), (3, "c")).toDF("num", "letter")
        df1.groupBy("num").agg(collect_list("letter").as("collected_letters"))
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val execInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val objectHashAggregate = execInfo.filter(_.exec == "ObjectHashAggregate")
      assertSizeAndSupported(2, objectHashAggregate)
    }
  }

  test("WindowExec and expressions within WIndowExec") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        val metrics = Seq(
          (0, 0, 0), (1, 0, 1), (2, 5, 2), (3, 0, 3), (4, 0, 1), (5, 5, 3), (6, 5, 0)
        ).toDF("id", "device", "level")
        val rangeWithTwoDevicesById = Window.partitionBy('device).orderBy('id)
        metrics.withColumn("sum", sum('level) over rangeWithTwoDevicesById)
            .withColumn("row_number", row_number.over(rangeWithTwoDevicesById))
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val execInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val windowExecs = execInfo.filter(_.exec == "Window")
      assertSizeAndSupported(1, windowExecs)
    }
  }

  test("Parse Pandas execs - AggregateInPandas, ArrowEvalPython, " +
      "FlatMapGroupsInPandas, MapInPandas, WindowInPandas") {
    val eventLog = s"$qualLogDir/pandas_execs_eventlog.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size > 0)
    val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
      SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
    }
    val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
    val flatMapGroups = allExecInfo.filter(_.exec == "FlatMapGroupsInPandas")
    assertSizeAndSupported(1, flatMapGroups, speedUpFactor = 1.2)
    val aggregateInPandas = allExecInfo.filter(_.exec == "AggregateInPandas")
    assertSizeAndSupported(1, aggregateInPandas, speedUpFactor = 1.2)
    // this event log had UDF for ArrowEvalPath so shows up as not supported
    val arrowEvalPython = allExecInfo.filter(_.exec == "ArrowEvalPython")
    assertSizeAndSupported(1, arrowEvalPython, speedUpFactor = 1.2)
    val mapInPandas = allExecInfo.filter(_.exec == "MapInPandas")
    assertSizeAndSupported(1, mapInPandas, speedUpFactor = 1.2)
    // WindowInPandas configured off by default
    val windowInPandas = allExecInfo.filter(_.exec == "WindowInPandas")
    assertSizeAndNotSupported(1, windowInPandas)
  }

  // GlobalLimit and LocalLimit is not in physical plan when collect is called on the dataframe.
  // We are reading from static eventlogs to test these execs.
  test("Parse execs - LocalLimit and GlobalLimit") {
    val eventLog = s"$qualLogDir/global_local_limit_eventlog.zstd"
    val pluginTypeChecker = new PluginTypeChecker()
    val app = createAppFromEventlog(eventLog)
    assert(app.sqlPlans.size == 1)
    val supportedExecs = Array("GlobalLimit", "LocalLimit")
    app.sqlPlans.foreach { case (sqlID, plan) =>
      val planInfo = SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
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

  test("Expression not supported in FilterExec") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
        "Expressions in FilterExec") { spark =>
        import spark.implicits._
        val df1 = spark.sparkContext.parallelize(List(10, 20, 30, 40)).toDF
        df1.filter(hex($"value") === "A") // hex is not supported in GPU yet.
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      app.sqlPlans.foreach { case (sqlID, plan) =>
        val planInfo = SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "",
          pluginTypeChecker, app)
        val wholeStages = planInfo.execInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.size == 1)
        val allChildren = wholeStages.flatMap(_.children).flatten
        val filters = allChildren.filter(_.exec == "Filter")
        assertSizeAndNotSupported(1, filters)
      }
    }
  }

  test("Expressions supported in SortAggregateExec") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        import spark.implicits._
        spark.conf.set("spark.sql.execution.useObjectHashAggregateExec", "false")
        val df1 = Seq((1, "a"), (1, "aa"), (1, "a"), (2, "b"),
          (2, "b"), (3, "c"), (3, "c")).toDF("num", "letter")
        df1.groupBy("num").agg(collect_list("letter").as("collected_letters"),
          count("letter").as("letter_count"))
      }
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog)
      assert(app.sqlPlans.size == 1)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val execInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val sortAggregate = execInfo.filter(_.exec == "SortAggregate")
      assertSizeAndSupported(2, sortAggregate)
    }
  }

  test("Expressions supported in SortExec") {
    TrampolineUtil.withTempDir { parquetoutputLoc =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
          "ProjectExprsSupported") { spark =>
          import spark.implicits._
          val df1 = Seq((1.7, "a"), (1.6, "aa"), (1.1, "b"), (2.5, "a"), (2.2, "b"),
            (3.2, "a"), (10.6, "c")).toDF("num", "letter")
          df1.write.parquet(s"$parquetoutputLoc/testsortExec")
          val df2 = spark.read.parquet(s"$parquetoutputLoc/testsortExec")
          df2.sort("num").collect
          df2.orderBy("num").collect
          df2.select(round(col("num")), col("letter")).sort(round(col("num")), col("letter").desc)
        }
        val pluginTypeChecker = new PluginTypeChecker()
        val app = createAppFromEventlog(eventLog)
        assert(app.sqlPlans.size == 4)
        val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
        }
        val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
        val sortExec = allExecInfo.filter(_.exec.contains("Sort"))
        assert(sortExec.size == 3)
        assertSizeAndSupported(3, sortExec, 5.2)
      }
    }
  }

  test("Expressions supported in ProjectExec") {
    TrampolineUtil.withTempDir { parquetoutputLoc =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
          "ProjectExprsSupported") { spark =>
          import spark.implicits._
          val df1 = Seq(9.9, 10.2, 11.6, 12.5).toDF("value")
          df1.write.parquet(s"$parquetoutputLoc/testtext")
          val df2 = spark.read.parquet(s"$parquetoutputLoc/testtext")
          df2.select(df2("value").cast(StringType), ceil(df2("value")), df2("value"))
        }
        val pluginTypeChecker = new PluginTypeChecker()
        val app = createAppFromEventlog(eventLog)
        assert(app.sqlPlans.size == 2)
        val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
        }
        val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
        val wholeStages = allExecInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.size == 1)
        assert(wholeStages.forall(_.duration.nonEmpty))
        val allChildren = wholeStages.flatMap(_.children).flatten
        val projects = allChildren.filter(_.exec == "Project")
        assertSizeAndSupported(1, projects)
      }
    }
  }

  test("Expressions not supported in ProjectExec") {
    TrampolineUtil.withTempDir { parquetoutputLoc =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir,
          "ProjectExprsNotSupported") { spark =>
          import spark.implicits._
          val df1 = spark.sparkContext.parallelize(List(10, 20, 30, 40)).toDF
          df1.write.parquet(s"$parquetoutputLoc/testtext")
          val df2 = spark.read.parquet(s"$parquetoutputLoc/testtext")
          df2.select(hex($"value") === "A")
        }
        val pluginTypeChecker = new PluginTypeChecker()
        val app = createAppFromEventlog(eventLog)
        assert(app.sqlPlans.size == 2)
        val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
          SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "test desc", pluginTypeChecker, app)
        }
        parsedPlans.foreach { pInfo =>
          assert(pInfo.sqlDesc == "test desc")
        }
        val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
        val wholeStages = allExecInfo.filter(_.exec.contains("WholeStageCodegen"))
        assert(wholeStages.forall(_.duration.nonEmpty))
        val allChildren = wholeStages.flatMap(_.children).flatten
        val projects = allChildren.filter(_.exec == "Project")
        assertSizeAndNotSupported(1, projects)
      }
    }
  }
}
