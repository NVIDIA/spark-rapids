/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.{GpuBroadcastHashJoinExec,
  GpuBroadcastNestedLoopJoinExec}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._

/**
 * Test suite for issue #14283: join condition with "cast to bigint" should be pushed into
 * the join operator rather than being placed in a post-join GpuFilter.
 *
 * The root cause is that GpuCast is not AST-compatible, so equi-join Meta classes
 * (BroadcastHashJoin, ShuffledHashJoin, SortMergeJoin) use an all-or-nothing check:
 * if any sub-expression in the join condition is not AST-able, the ENTIRE condition
 * becomes a post-join GpuFilterExec. For inner joins this causes catastrophic row explosion.
 *
 * BNLJ already handles this correctly via AstUtil.extractNonAstFromJoinCond which
 * pre-computes non-AST sub-expressions via GpuProjectExec.
 *
 * This suite:
 * 1. Proves BNLJ handles cast in join conditions correctly (no post-filter)
 * 2. Verifies BHJ/SHJ/SMJ use the same extraction pattern for equi-joins
 * 3. Covers fallback, schema, and no-extra-project regression cases
 */
class CastInJoinConditionSuite extends SparkQueryCompareTestSuite {

  private val BroadcastHint = "/*+ BROADCAST(b) */"
  private val ShuffleHashHint = "/*+ SHUFFLE_HASH(b) */"

  private val DefaultSelectColumns =
    "a.category, a.range_start, a.range_end, b.b_start, b.b_end"

  private val CastRangePredicate =
    """a.range_start < CAST(b.b_end AS BIGINT)
      |  AND a.range_end > CAST(b.b_start AS BIGINT)""".stripMargin

  private val AstRangePredicate =
    """a.range_start < b.b_end
      |  AND a.range_end > b.b_start""".stripMargin

  private def rangeJoinConf: SparkConf = new SparkConf()
    .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")

  private def rangeJoinSql(
      hint: String = BroadcastHint,
      includeEquality: Boolean = true,
      predicate: String = CastRangePredicate,
      joinType: String = "INNER JOIN",
      leftTable: String = "table_a",
      rightTable: String = "table_b",
      selectColumns: String = DefaultSelectColumns): String = {
    val selectHint = if (hint.isEmpty) "" else s" $hint"
    val joinCondition = if (includeEquality) {
      s"b.b_category = a.category\n  AND $predicate"
    } else {
      predicate
    }

    s"""SELECT$selectHint
      |  $selectColumns
      |FROM $leftTable a
      |$joinType $rightTable b
      |  ON $joinCondition
    """.stripMargin
  }

  /**
   * Creates table_a with BIGINT range columns (range_start, range_end) and a category column.
   */
  private def tableADf(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      ("cat1", 100L, 200L),
      ("cat1", 150L, 250L),
      ("cat2", 300L, 400L),
      ("cat2", 350L, 450L),
      ("cat3", 500L, 600L)
    ).toDF("category", "range_start", "range_end")
  }

  /**
   * Creates table_b with INT range columns (b_start, b_end) and a category column.
   * The INT type triggers CAST(b_end AS BIGINT) when compared with BIGINT columns.
   */
  private def tableBDf(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      ("cat1", 120, 180),
      ("cat1", 210, 260),
      ("cat2", 310, 390),
      ("cat2", 410, 460),
      ("cat3", 510, 590)
    ).toDF("b_category", "b_start", "b_end")
  }

  private def withDefaultTables[T](spark: SparkSession)(body: => T): T = {
    tableADf(spark).createOrReplaceTempView("table_a")
    tableBDf(spark).createOrReplaceTempView("table_b")
    body
  }

  private def executedPlan(df: DataFrame): SparkPlan = {
    df.collect()
    df.queryExecution.executedPlan
  }

  private def withDefaultGpuQuery(
      conf: SparkConf,
      sqlText: String)(body: DataFrame => Unit): Unit = {
    withGpuSparkSession(spark => {
      withDefaultTables(spark) {
        body(spark.sql(sqlText))
      }
    }, conf)
  }

  private def withDefaultGpuPlan(
      conf: SparkConf,
      sqlText: String)(body: SparkPlan => Unit): Unit = {
    withDefaultGpuQuery(conf, sqlText) { df =>
      body(executedPlan(df))
    }
  }

  private def collectDefaultGpuResult(conf: SparkConf, sqlText: String) = {
    withGpuSparkSession(spark => {
      withDefaultTables(spark) {
        spark.sql(sqlText).collect().groupBy(identity).map {
          case (row, matchingRows) => row -> matchingRows.length
        }
      }
    }, conf)
  }

  private def collectDefaultCpuResult(sqlText: String) = {
    withCpuSparkSession(spark => {
      withDefaultTables(spark) {
        spark.sql(sqlText).collect().groupBy(identity).map {
          case (row, matchingRows) => row -> matchingRows.length
        }
      }
    })
  }

  private def assertDefaultGpuCpuResultsMatch(
      conf: SparkConf,
      sqlText: String,
      message: String): Unit = {
    val gpuResult = collectDefaultGpuResult(conf, sqlText)
    val cpuResult = collectDefaultCpuResult(sqlText)

    assert(gpuResult == cpuResult, s"$message\nGPU: $gpuResult\nCPU: $cpuResult")
  }

  private def assertSparkPlanHasJoin[T <: SparkPlan : ClassTag](
      df: DataFrame,
      joinName: String): Unit = {
    val sparkPlan = df.queryExecution.sparkPlan
    val joinClass = implicitly[ClassTag[T]].runtimeClass
    val joins = PlanUtils.findOperators(sparkPlan, joinClass.isInstance)
    assert(joins.nonEmpty, s"Expected $joinName in Spark physical plan but got:\n$sparkPlan")
  }

  private def isHashJoin(plan: SparkPlan): Boolean = plan match {
    case _: GpuBroadcastHashJoinExec => true
    case _: GpuShuffledSymmetricHashJoinExec => true
    case _: GpuShuffledHashJoinExec => true
    case _ => false
  }

  private def broadcastHashJoin(plan: SparkPlan): GpuBroadcastHashJoinExec = {
    val bhjOps = PlanUtils.findOperators(plan, _.isInstanceOf[GpuBroadcastHashJoinExec])
    assert(bhjOps.nonEmpty, s"Expected GpuBroadcastHashJoinExec in plan but got:\n$plan")
    bhjOps.head.asInstanceOf[GpuBroadcastHashJoinExec]
  }

  private def broadcastNestedLoopJoin(plan: SparkPlan): GpuBroadcastNestedLoopJoinExec = {
    val bnljOps = PlanUtils.findOperators(plan, _.isInstanceOf[GpuBroadcastNestedLoopJoinExec])
    assert(bnljOps.nonEmpty,
      s"Expected GpuBroadcastNestedLoopJoinExec in plan but got:\n$plan")
    bnljOps.head.asInstanceOf[GpuBroadcastNestedLoopJoinExec]
  }

  /**
   * Helper: checks whether a GpuFilterExec exists as a direct parent of a join operator
   * in the executed plan. If so, the join condition was NOT pushed into the join.
   */
  private def hasPostJoinFilter(plan: SparkPlan): Boolean = {
    PlanUtils.findOperators(plan, _.isInstanceOf[GpuFilterExec]).exists { filter =>
      isHashJoin(filter.asInstanceOf[GpuFilterExec].child)
    }
  }

  /**
   * Helper: checks whether a GpuProjectExec wraps a child of the join (extraction pattern).
   * This indicates that non-AST expressions were successfully extracted.
   */
  private def hasPreJoinProject(plan: SparkPlan): Boolean = {
    PlanUtils.findOperators(plan, isHashJoin).exists { join =>
      join.children.exists(_.isInstanceOf[GpuProjectExec])
    }
  }

  // ============================================================================
  // SECTION 1: Prove BNLJ handles CAST in join conditions correctly
  // These tests should PASS — BNLJ already implements the extraction pattern
  // ============================================================================

  test("BNLJ handles cast-to-bigint in join condition without post-filter") {
    val query = rangeJoinSql(includeEquality = false)
    withDefaultGpuPlan(rangeJoinConf, query) { plan =>
      // Verify the BNLJ has its condition inside (not in a separate filter)
      val bnlj = broadcastNestedLoopJoin(plan)
      assert(bnlj.condition.isDefined,
        "BNLJ should have the join condition pushed in (not as post-filter)")
    }
  }

  test("BNLJ with cast produces correct results") {
    assertDefaultGpuCpuResultsMatch(
      rangeJoinConf,
      rangeJoinSql(includeEquality = false),
      "GPU and CPU results should match.")
  }

  // ============================================================================
  // SECTION 2: BroadcastHashJoin extraction behavior
  // ============================================================================

  test("BHJ with cast-to-bigint produces correct results") {
    assertDefaultGpuCpuResultsMatch(
      rangeJoinConf,
      rangeJoinSql(),
      "GPU and CPU results should match.")
  }

  test("BHJ with cast should keep condition in join (not post-filter)") {
    withDefaultGpuPlan(rangeJoinConf, rangeJoinSql()) { plan =>
      assert(!hasPostJoinFilter(plan),
        "BroadcastHashJoin should NOT use a post-filter for CAST in join condition. " +
          "The extraction pattern should pre-compute CAST via GpuProjectExec on the child.")
    }
  }

  test("BHJ with cast should have condition defined in join operator") {
    withDefaultGpuPlan(rangeJoinConf, rangeJoinSql()) { plan =>
      val bhj = broadcastHashJoin(plan)
      assert(bhj.condition.isDefined,
        "BroadcastHashJoin should have the join condition defined " +
          "(rewritten to use pre-computed CAST columns from GpuProjectExec)")
    }
  }

  test("BHJ with cast should use extraction pattern (GpuProjectExec on child)") {
    withDefaultGpuPlan(rangeJoinConf, rangeJoinSql()) { plan =>
      assert(hasPreJoinProject(plan),
        "BroadcastHashJoin should have GpuProjectExec wrapping a child " +
          "to pre-compute CAST (extraction pattern from BNLJ)")
    }
  }

  test("BHJ with cast keeps output schema clean (no leaked attributes)") {
    withGpuSparkSession(spark => {
      withDefaultTables(spark) {
        val df = spark.sql(rangeJoinSql())
        df.collect()
        val schema = df.schema

        // The output schema should only contain the expected columns
        // No intermediate _agpu_non_ast_* columns should leak through
        val columnNames = schema.fieldNames.toSet
        assert(!columnNames.exists(_.startsWith("_agpu_non_ast")),
          "Output schema should not contain intermediate extraction attributes. " +
            s"Got columns: ${columnNames.mkString(", ")}")

        val expectedColumns = Set("category", "range_start", "range_end", "b_start", "b_end")
        assert(columnNames == expectedColumns,
          s"Output columns should be $expectedColumns, got $columnNames")
      }
    }, rangeJoinConf)
  }

  test("BHJ with cast on both sides should use extraction pattern") {
    withGpuSparkSession(spark => {
      import spark.implicits._
      // Both sides have INT columns — casting both to BIGINT
      val dfA = Seq(
        ("cat1", 100, 200),
        ("cat1", 150, 250),
        ("cat2", 300, 400)
      ).toDF("category", "a_start", "a_end")
      dfA.createOrReplaceTempView("int_table_a")

      val dfB = Seq(
        ("cat1", 120, 180),
        ("cat1", 210, 260),
        ("cat2", 310, 390)
      ).toDF("b_category", "b_start", "b_end")
      dfB.createOrReplaceTempView("int_table_b")

      val query = rangeJoinSql(
        hint = "/*+ BROADCAST(int_table_b) */",
        predicate =
          """CAST(a.a_start AS BIGINT) < CAST(b.b_end AS BIGINT)
            |  AND CAST(a.a_end AS BIGINT) > CAST(b.b_start AS BIGINT)""".stripMargin,
        leftTable = "int_table_a",
        rightTable = "int_table_b",
        selectColumns = "a.category, a.a_start, a.a_end, b.b_start, b.b_end")
      val plan = executedPlan(spark.sql(query))

      assert(!hasPostJoinFilter(plan),
        "BroadcastHashJoin with CAST on both sides should use extraction pattern")
    }, rangeJoinConf)
  }

  test("BHJ with non-extractable condition should still fall back to post-filter") {
    val query = rangeJoinSql(
      predicate =
        // CASE WHEN itself is non-AST and this subtree references both sides of the join,
        // so it cannot be pre-computed on either child.
        """CASE WHEN a.range_start > CAST(b.b_end AS BIGINT)
          |    THEN a.range_start
          |    ELSE CAST(b.b_end AS BIGINT)
          |  END > 100""".stripMargin)
    withDefaultGpuPlan(rangeJoinConf, query) { plan =>
      // Non-extractable conditions (referencing both sides in non-AST expr) should
      // still gracefully fall back to post-filter
      assert(hasPostJoinFilter(plan),
        "Non-extractable conditions should still fall back to post-filter gracefully")
    }
  }

  test("BHJ without cast should have condition in join (no regression)") {
    withGpuSparkSession(spark => {
      import spark.implicits._
      // Same types on both sides — no cast needed — already AST-able
      val dfA = Seq(
        ("cat1", 100L, 200L),
        ("cat1", 150L, 250L),
        ("cat2", 300L, 400L)
      ).toDF("category", "range_start", "range_end")
      dfA.createOrReplaceTempView("long_table_a")

      val dfB = Seq(
        ("cat1", 120L, 180L),
        ("cat1", 210L, 260L),
        ("cat2", 310L, 390L)
      ).toDF("b_category", "b_start", "b_end")
      dfB.createOrReplaceTempView("long_table_b")

      val query = rangeJoinSql(
        hint = "/*+ BROADCAST(long_table_b) */",
        predicate = AstRangePredicate,
        leftTable = "long_table_a",
        rightTable = "long_table_b")
      val plan = executedPlan(spark.sql(query))

      // No CAST means the condition is already fully AST-able
      // This should already work today — regression test
      assert(!hasPostJoinFilter(plan),
        "BroadcastHashJoin without CAST should have condition in join (already AST-able)")

      val bhj = broadcastHashJoin(plan)
      assert(bhj.condition.isDefined,
        "BroadcastHashJoin without CAST should have condition defined")
    }, rangeJoinConf)
  }

  test("BHJ with cast-to-bigint matches CPU results after extraction") {
    assertDefaultGpuCpuResultsMatch(
      rangeJoinConf,
      rangeJoinSql(),
      "GPU and CPU results should match after fix.")
  }

  test("Left outer BHJ with extractable cast condition plans on GPU") {
    val query = rangeJoinSql(joinType = "LEFT OUTER JOIN")
    withDefaultGpuPlan(rangeJoinConf, query) { plan =>
      val bhj = broadcastHashJoin(plan)
      assert(bhj.condition.isDefined,
        "Left outer BroadcastHashJoin should keep extractable CAST condition in the join")
      assert(!hasPostJoinFilter(plan),
        "Left outer BroadcastHashJoin cannot use a post-filter for the join condition")
    }
    assertDefaultGpuCpuResultsMatch(
      rangeJoinConf,
      query,
      "Left outer GPU and CPU results should match with extracted CAST condition.")
  }

  // ============================================================================
  // SECTION 3: ShuffledHashJoin extraction behavior
  // ============================================================================

  private def shuffledJoinConf: SparkConf = new SparkConf()
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.sql.join.preferSortMergeJoin", "false")
    .set("spark.sql.shuffle.partitions", "2")
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")

  test("SHJ with cast should keep condition in join (not post-filter)") {
    withDefaultGpuQuery(shuffledJoinConf, rangeJoinSql(hint = ShuffleHashHint)) { df =>
      assertSparkPlanHasJoin[ShuffledHashJoinExec](df, "ShuffledHashJoinExec")
      val plan = executedPlan(df)
      assert(!hasPostJoinFilter(plan),
        "ShuffledHashJoin should NOT use a post-filter for CAST in join condition")
    }
  }

  test("SHJ with cast-to-bigint produces correct results") {
    assertDefaultGpuCpuResultsMatch(
      shuffledJoinConf,
      rangeJoinSql(hint = ShuffleHashHint),
      "ShuffledHashJoin GPU and CPU results should match.")
  }

  // ============================================================================
  // SECTION 4: Edge cases
  // ============================================================================

  test("Multiple cast types (SHORT -> INT) in BHJ condition") {
    withGpuSparkSession(spark => {
      import spark.implicits._
      val dfA = Seq(
        ("cat1", 100, 200),
        ("cat2", 300, 400)
      ).toDF("category", "range_start", "range_end")
      dfA.createOrReplaceTempView("int_range_a")

      val dfB = Seq(
        ("cat1", 120.toShort, 180.toShort),
        ("cat2", 310.toShort, 390.toShort)
      ).toDF("b_category", "b_start", "b_end")
      dfB.createOrReplaceTempView("short_range_b")

      val query = rangeJoinSql(
        hint = "/*+ BROADCAST(short_range_b) */",
        predicate =
          """a.range_start < CAST(b.b_end AS INT)
            |  AND a.range_end > CAST(b.b_start AS INT)""".stripMargin,
        leftTable = "int_range_a",
        rightTable = "short_range_b")
      val plan = executedPlan(spark.sql(query))

      assert(!hasPostJoinFilter(plan),
        "BHJ with SHORT->INT cast should use extraction pattern")
    }, rangeJoinConf)
  }

  test("Multiple non-AST conditions in single join") {
    withGpuSparkSession(spark => {
      import spark.implicits._
      val dfA = Seq(
        ("cat1", 100L, 200L, "active"),
        ("cat2", 300L, 400L, "inactive")
      ).toDF("category", "range_start", "range_end", "status")
      dfA.createOrReplaceTempView("multi_a")

      val dfB = Seq(
        ("cat1", 120, 180, "active"),
        ("cat2", 310, 390, "inactive")
      ).toDF("b_category", "b_start", "b_end", "b_status")
      dfB.createOrReplaceTempView("multi_b")

      // Multiple CAST operations in the join condition
      val query = rangeJoinSql(
        hint = "/*+ BROADCAST(multi_b) */",
        leftTable = "multi_a",
        rightTable = "multi_b")
      val plan = executedPlan(spark.sql(query))

      assert(!hasPostJoinFilter(plan),
        "Multiple CAST operations should all be extracted via GpuProjectExec")
    }, rangeJoinConf)
  }

  // ============================================================================
  // SECTION 5: SortMergeJoin extraction behavior
  // ============================================================================

  private def sortMergeJoinConf: SparkConf = new SparkConf()
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.sql.join.preferSortMergeJoin", "true")
    .set("spark.sql.shuffle.partitions", "2")
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")

  test("SMJ with cast should keep condition in join (not post-filter)") {
    withDefaultGpuQuery(sortMergeJoinConf, rangeJoinSql(hint = "")) { df =>
      assertSparkPlanHasJoin[SortMergeJoinExec](df, "SortMergeJoinExec")
      val plan = executedPlan(df)
      assert(!hasPostJoinFilter(plan),
        "SortMergeJoin should NOT use a post-filter for CAST in join condition")
    }
  }

  test("SMJ with cast-to-bigint produces correct results") {
    assertDefaultGpuCpuResultsMatch(
      sortMergeJoinConf,
      rangeJoinSql(hint = ""),
      "SortMergeJoin GPU and CPU results should match.")
  }

  // ============================================================================
  // SECTION 6: No unnecessary projections when condition is fully AST-able
  // ============================================================================

  test("Fully AST-able condition should NOT add GpuProjectExec (no unnecessary work)") {
    withGpuSparkSession(spark => {
      import spark.implicits._
      // Same types on both sides — no cast needed — fully AST-able
      val dfA = Seq(
        ("cat1", 100L, 200L),
        ("cat2", 300L, 400L)
      ).toDF("category", "range_start", "range_end")
      dfA.createOrReplaceTempView("long_a")

      val dfB = Seq(
        ("cat1", 120L, 180L),
        ("cat2", 310L, 390L)
      ).toDF("b_category", "b_start", "b_end")
      dfB.createOrReplaceTempView("long_b")

      val query = rangeJoinSql(
        hint = "/*+ BROADCAST(long_b) */",
        predicate = AstRangePredicate,
        leftTable = "long_a",
        rightTable = "long_b")
      val plan = executedPlan(spark.sql(query))

      // No CAST → fully AST-able → should NOT introduce unnecessary GpuProjectExec
      // on join children (the extraction pattern should only activate when needed)
      assert(!hasPreJoinProject(plan),
        "Fully AST-able condition should NOT add GpuProjectExec on join children. " +
          "The extraction pattern should only activate when non-AST expressions exist.")

      // Also verify condition is in the join
      assert(!hasPostJoinFilter(plan),
        "Fully AST-able condition should be in the join (no post-filter needed)")
    }, rangeJoinConf)
  }
}
