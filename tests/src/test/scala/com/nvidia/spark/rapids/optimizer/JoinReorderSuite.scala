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

package com.nvidia.spark.rapids.optimizer

import java.io.File

import scala.collection.mutable

import com.nvidia.spark.rapids.{FunSuiteWithTempDir, RapidsConf, SparkQueryCompareTestSuite}
import com.nvidia.spark.rapids.FuzzerUtils.{createSchema, generateDataFrame}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.{DataTypes, StructType}

class JoinReorderSuite extends SparkQueryCompareTestSuite with FunSuiteWithTempDir {

  // TODO write tests for the following
  // - build left-deep, right-deep, and bushy trees from fact and dim relations
  // - write tests for each config option to prove that they are used
  // - test for all conditions in the code where we fall back to the original plan
  // - test these methods in FactDimensionJoinReorder:
  //   - isSupportedJoin
  //   - reorder
  //   - extractInnerJoins
  //   - containsJoin

  private val defaultConf = new SparkConf()

  private val confJoinOrderingEnabled = new SparkConf()
    .set(RapidsConf.JOIN_REORDERING.key, "true")
    .set(RapidsConf.JOIN_REORDERING_FILTER_SELECTIVITY.key, "0.2")
    .set(RapidsConf.JOIN_REORDERING_RATIO.key, "0.3")
    .set(RapidsConf.JOIN_REORDERING_MAX_FACT.key, "2")
    .set(RapidsConf.JOIN_REORDERING_PRESERVE_ORDER.key, "true")

  test("findFactDimRels") {
    withGpuSparkSession(spark => {
      val fact = createTestRelation(spark, "fact", 1000)
      val dim1 = createTestRelation(spark, "dim1", 200)
      val dim2 = createTestRelation(spark, "dim2", 250)
      val (facts, dims) = FactDimensionJoinReorder.findFactDimRels(Seq(fact, dim1, dim2), 0.3)
      assert(Seq(fact) === facts)
      assert(Seq(dim1, dim2) === dims)
      // test with a different ratio
      val (facts2, dims2) = FactDimensionJoinReorder.findFactDimRels(Seq(fact, dim1, dim2), 0.22)
      assert(Seq(fact, dim2) === facts2)
      assert(Seq(dim1) === dims2)
    })
  }

  test("relationsOrdered: default behavior") {
    withGpuSparkSession(spark => {
      val dim1 = createTestRelation(spark, "dim1", 250)
      val dim2 = createTestRelation(spark, "dim2", 200)
      val _dim3 = createLogicalPlan(spark, "dim3", 1000)
      val dim3 = Relation.apply(Filter(EqualTo(_dim3.output.head, Literal(123)), _dim3)).get
      val rels = FactDimensionJoinReorder.relationsOrdered(Seq(dim1, dim2, dim3),
        joinReorderingPreserveOrder = true, joinReorderingFilterSelectivity = 0.1)
      val actual: Seq[String] = rels.map(x => buildPlanString(x.plan))
      val expected: Seq[String] = Seq(
        """Filter: (dim3_c0 = 123)
          |    LogicalRelation: dim3.parquet""".stripMargin,
        "LogicalRelation: dim1.parquet",
        "LogicalRelation: dim2.parquet")
      assert(expected === actual)
    })
  }

  test("relationsOrdered: non-default behavior") {
    withGpuSparkSession(spark => {
      val dim1 = createTestRelation(spark, "dim1", 250)
      val dim2 = createTestRelation(spark, "dim2", 210)
      val _dim3 = createLogicalPlan(spark, "dim3", 1000)
      val dim3 = Relation.apply(Filter(EqualTo(_dim3.output.head, Literal(123)), _dim3)).get
      val rels = FactDimensionJoinReorder.relationsOrdered(Seq(dim1, dim2, dim3),
        joinReorderingPreserveOrder = false, joinReorderingFilterSelectivity = 0.24)
      val actual: Seq[String] = rels.map(x => buildPlanString(x.plan))
      val expected: Seq[String] = Seq(
        "LogicalRelation: dim2.parquet",
        """Filter: (dim3_c0 = 123)
          |    LogicalRelation: dim3.parquet""".stripMargin,
        "LogicalRelation: dim1.parquet",
        )
      assert(expected === actual)
    })
  }

  test("buildJoinTree: left-deep") {
    withGpuSparkSession(spark => {
      val fact = createLogicalPlan(spark, "fact", 1000)
      val dim1 = createLogicalPlan(spark, "dim1", 200)
      val dim2 = createLogicalPlan(spark, "dim2", 250)
      val conds = new mutable.HashSet[Expression]()
      conds += EqualTo(fact.output.head, dim1.output.head)
      conds += EqualTo(fact.output.head, dim2.output.head)
      val (n, join) = FactDimensionJoinReorder.buildJoinTree(fact, Seq(dim1, dim2), conds, LeftDeep)
      assert(n == 2)
      assert(conds.isEmpty)
      val expected = """Join: (fact_c0 = dim2_c0)
                       |  Join: (fact_c0 = dim1_c0)
                       |    LogicalRelation: fact.parquet
                       |    LogicalRelation: dim1.parquet
                       |  LogicalRelation: dim2.parquet""".stripMargin
      val actual = buildPlanString(join)
      assert(expected === actual)
    })
  }

  test("buildJoinTree: right-deep") {
    withGpuSparkSession(spark => {
      val fact = createLogicalPlan(spark, "fact", 1000)
      val dim1 = createLogicalPlan(spark, "dim1", 200)
      val dim2 = createLogicalPlan(spark, "dim2", 250)
      val conds = new mutable.HashSet[Expression]()
      conds += EqualTo(fact.output.head, dim1.output.head)
      conds += EqualTo(fact.output.head, dim2.output.head)
      val (n, join) = FactDimensionJoinReorder.buildJoinTree(fact, Seq(dim1, dim2), conds,
        RightDeep)
      assert(n == 2)
      assert(conds.isEmpty)
      val expected =
        """Join: (fact_c0 = dim2_c0)
          |  LogicalRelation: dim2.parquet
          |  Join: (fact_c0 = dim1_c0)
          |    LogicalRelation: dim1.parquet
          |    LogicalRelation: fact.parquet""".stripMargin
      val actual = buildPlanString(join)
      assert(expected === actual)
    })
  }

  test("join filtered dimension earlier") {
    val tables = Map("fact" -> 1000, "dim1" -> 100, "dim2" -> 200)
    val sql =
      """SELECT * FROM fact
        |JOIN dim1 ON fact_c0 = dim1_c0
        |JOIN dim2 ON fact_c0 = dim2_c0
        |WHERE dim2_c1 LIKE 'test%'
        |""".stripMargin

    // run with join reordering disabled
    {
      val df = execute(sql, defaultConf, tables)
      val plan = df.queryExecution.optimizedPlan
      // scalastyle:off line.size.limit
      val expected =
        """Join: (fact.fact_c0 = dim2.dim2_c0)
          |  Join: (fact.fact_c0 = dim1.dim1_c0)
          |    Filter: (fact.fact_c0 IS NOT NULL)
          |      LogicalRelation: fact.parquet
          |    Filter: (dim1.dim1_c0 IS NOT NULL)
          |      LogicalRelation: dim1.parquet
          |  Filter: (((dim2.dim2_c1 IS NOT NULL) AND startswith(dim2.dim2_c1, 'test')) AND (dim2.dim2_c0 IS NOT NULL))
          |    LogicalRelation: dim2.parquet""".stripMargin
      // scalastyle:on maxLineLength
      val actual = buildPlanString(plan)
      assert(expected === actual)
    }

    // run with join reordering enabled
    val df = execute(sql, confJoinOrderingEnabled, tables)
    val plan = df.queryExecution.optimizedPlan
    // scalastyle:off line.size.limit
    val expected =
      """Join: (fact.fact_c0 = dim1.dim1_c0)
        |  Join: (fact.fact_c0 = dim2.dim2_c0)
        |    Filter: (fact.fact_c0 IS NOT NULL)
        |      LogicalRelation: fact.parquet
        |    Filter: (((dim2.dim2_c1 IS NOT NULL) AND startswith(dim2.dim2_c1, 'test')) AND (dim2.dim2_c0 IS NOT NULL))
        |      LogicalRelation: dim2.parquet
        |  Filter: (dim1.dim1_c0 IS NOT NULL)
        |    LogicalRelation: dim1.parquet""".stripMargin
    // scalastyle:on line.size.limit
    val actual = buildPlanString(plan)
    assert(expected === actual)
  }

  test("multiple fact tables") {
    val tables = Map(
      "fact1" -> 1000,
      "fact2" -> 2000,
      "dim1" -> 100,
      "dim2" -> 200,
      "dim3" -> 300)
    val sql =
      """SELECT * FROM fact1
        |JOIN fact2 ON fact1_c0 = fact2_c0
        |JOIN dim1 ON fact2_c0 = dim1_c0
        |JOIN dim2 ON fact2_c0 = dim2_c0
        |JOIN dim3 ON fact1_c0 = dim3_c0
        |WHERE dim2_c1 LIKE 'test%'
        |""".stripMargin

    // run with join reordering disabled
    {
      val df = execute(sql, defaultConf, tables)
      val plan = df.queryExecution.optimizedPlan
      // scalastyle:off line.size.limit
      val expected =
        """Join: (fact1.fact1_c0 = dim3.dim3_c0)
          |  Join: (fact2.fact2_c0 = dim2.dim2_c0)
          |    Join: (fact2.fact2_c0 = dim1.dim1_c0)
          |      Join: (fact1.fact1_c0 = fact2.fact2_c0)
          |        Filter: (fact1.fact1_c0 IS NOT NULL)
          |          LogicalRelation: fact1.parquet
          |        Filter: (fact2.fact2_c0 IS NOT NULL)
          |          LogicalRelation: fact2.parquet
          |      Filter: (dim1.dim1_c0 IS NOT NULL)
          |        LogicalRelation: dim1.parquet
          |    Filter: (((dim2.dim2_c1 IS NOT NULL) AND startswith(dim2.dim2_c1, 'test')) AND (dim2.dim2_c0 IS NOT NULL))
          |      LogicalRelation: dim2.parquet
          |  Filter: (dim3.dim3_c0 IS NOT NULL)
          |    LogicalRelation: dim3.parquet""".stripMargin
      // scalastyle:on line.size.limit
      val actual = buildPlanString(plan)
      assert(expected === actual)
    }

    // run with join reordering enabled
    val df = execute(sql, confJoinOrderingEnabled, tables)
    val plan = df.queryExecution.optimizedPlan
    // scalastyle:off line.size.limit
    val expected =
      """Join: (fact1.fact1_c0 = fact2.fact2_c0)
        |  Join: (fact2.fact2_c0 = dim1.dim1_c0)
        |    Join: (fact2.fact2_c0 = dim2.dim2_c0)
        |      Filter: (fact2.fact2_c0 IS NOT NULL)
        |        LogicalRelation: fact2.parquet
        |      Filter: (((dim2.dim2_c1 IS NOT NULL) AND startswith(dim2.dim2_c1, 'test')) AND (dim2.dim2_c0 IS NOT NULL))
        |        LogicalRelation: dim2.parquet
        |    Filter: (dim1.dim1_c0 IS NOT NULL)
        |      LogicalRelation: dim1.parquet
        |  Join: (fact1.fact1_c0 = dim3.dim3_c0)
        |    Filter: (fact1.fact1_c0 IS NOT NULL)
        |      LogicalRelation: fact1.parquet
        |    Filter: (dim3.dim3_c0 IS NOT NULL)
        |      LogicalRelation: dim3.parquet""".stripMargin
    // scalastyle:on line.size.limit
    val actual = buildPlanString(plan)
    assert(expected === actual)
  }
  test("preserve order") {
    val tables = Map("fact" -> 1000, "dim1" -> 200, "dim2" -> 150, "dim3" -> 100)
    val sql =
      """SELECT * FROM fact
        |JOIN dim1 ON fact_c0 = dim1_c0
        |JOIN dim2 ON fact_c0 = dim2_c0
        |JOIN dim3 ON fact_c0 = dim3_c0
        |""".stripMargin

    // run with join reordering enabled, preserveOrder enabled
    {
      val conf = confJoinOrderingEnabled.clone()
        .set(RapidsConf.JOIN_REORDERING_PRESERVE_ORDER.key, "true")
      val df = execute(sql, conf, tables)
      val plan = df.queryExecution.optimizedPlan
      val expected =
        """Join: (fact.fact_c0 = dim3.dim3_c0)
          |  Join: (fact.fact_c0 = dim2.dim2_c0)
          |    Join: (fact.fact_c0 = dim1.dim1_c0)
          |      Filter: (fact.fact_c0 IS NOT NULL)
          |        LogicalRelation: fact.parquet
          |      Filter: (dim1.dim1_c0 IS NOT NULL)
          |        LogicalRelation: dim1.parquet
          |    Filter: (dim2.dim2_c0 IS NOT NULL)
          |      LogicalRelation: dim2.parquet
          |  Filter: (dim3.dim3_c0 IS NOT NULL)
          |    LogicalRelation: dim3.parquet""".stripMargin
      val actual = buildPlanString(plan)
      assert(expected === actual)
    }

    // run with join reordering enabled, preserveOrder disabled
    {
      val conf = confJoinOrderingEnabled.clone()
        .set(RapidsConf.JOIN_REORDERING_PRESERVE_ORDER.key, "false")
      val df = execute(sql, conf, tables)
      val plan = df.queryExecution.optimizedPlan
      val expected =
        """Join: (fact.fact_c0 = dim1.dim1_c0)
          |  Join: (fact.fact_c0 = dim2.dim2_c0)
          |    Join: (fact.fact_c0 = dim3.dim3_c0)
          |      Filter: (fact.fact_c0 IS NOT NULL)
          |        LogicalRelation: fact.parquet
          |      Filter: (dim3.dim3_c0 IS NOT NULL)
          |        LogicalRelation: dim3.parquet
          |    Filter: (dim2.dim2_c0 IS NOT NULL)
          |      LogicalRelation: dim2.parquet
          |  Filter: (dim1.dim1_c0 IS NOT NULL)
          |    LogicalRelation: dim1.parquet""".stripMargin
      val actual = buildPlanString(plan)
      assert(expected === actual)
    }
  }
  private def execute(sql: String, conf: SparkConf, tables: Map[String,Int]): DataFrame = {
    withGpuSparkSession(spark => {
      tables.foreach(spec => createTestRelation(spark, spec._1, spec._2))
      val df = spark.sql(sql)
      df.collect()
      df
    }, conf)
  }

  private def createTestRelation(spark: SparkSession, relName: String, rowCount: Int): Relation = {
    val schema = createSchema(Seq(DataTypes.IntegerType, DataTypes.StringType))
    val schema2 = StructType(schema.fields.map(f => f.copy(name = s"${relName}_${f.name}")))
    val df = generateDataFrame(spark, schema2, rowCount)
    val path = new File(TEST_FILES_ROOT, s"$relName.parquet").getAbsolutePath
    df.write.mode(SaveMode.Overwrite).parquet(path)
    val rel = spark.read.parquet(path)
    rel.createOrReplaceTempView(relName)
    Relation.apply(rel.queryExecution.logical).get
  }

  private def createLogicalPlan(spark: SparkSession, name: String, size: Int): LogicalPlan = {
    createTestRelation(spark, name, size).plan
  }

  /** Format a plan consistently regardless of Spark version */
  private def buildPlanString(plan: LogicalPlan): String = {
    def exprToString(expr: Expression): String = {
      expr.sql.replaceAll("`", "")
    }

    def buildPlanString(indent: String, plan: LogicalPlan): String = {
      val nodeString: String = plan match {
        case Filter(cond, _) =>
          s"Filter: ${exprToString(cond)}"
        case l: LogicalRelation =>
          val relation = l.relation.asInstanceOf[HadoopFsRelation]
          "LogicalRelation: " + relation.location.rootPaths.head.getName
        case j: Join =>
          s"Join: ${exprToString(j.condition.get)}"
        case p: Project =>
          s"Project: ${p.projectList.map(_.name).mkString(", ")}"
        case _ =>
          throw new IllegalStateException()
      }
      var str = indent + nodeString + "\n"
      for (child <- plan.children) {
        str += buildPlanString(indent + "  ", child)
      }
      str
    }

    buildPlanString("", plan).trim
  }


}
