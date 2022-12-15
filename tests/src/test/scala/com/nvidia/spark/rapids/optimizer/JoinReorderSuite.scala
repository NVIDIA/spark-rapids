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

import com.nvidia.spark.rapids.{FunSuiteWithTempDir, RapidsConf, SparkQueryCompareTestSuite}
import com.nvidia.spark.rapids.FuzzerUtils.{createSchema, generateDataFrame}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.{DataTypes, StructType}

class JoinReorderSuite extends SparkQueryCompareTestSuite with FunSuiteWithTempDir {

  private val defaultConf = new SparkConf()

  private val confJoinOrderingEnabled = new SparkConf()
    .set(RapidsConf.JOIN_REORDERING.key, "true")
    .set(RapidsConf.JOIN_REORDERING_RATIO.key, "0.3")
    .set(RapidsConf.JOIN_REORDERING_MAX_FACT.key, "2")
    .set(RapidsConf.JOIN_REORDERING_PRESERVE_ORDER.key, "true")

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

  private def createTestRelation(spark: SparkSession, relName: String, rowCount: Int): Unit = {
    val schema = createSchema(Seq(DataTypes.IntegerType, DataTypes.StringType))
    val schema2 = StructType(schema.fields.map(f => f.copy(name = s"${relName}_${f.name}")))
    val df = generateDataFrame(spark, schema2, rowCount)
    val path = new File(TEST_FILES_ROOT, s"$relName.parquet").getAbsolutePath
    df.write.mode(SaveMode.Overwrite).parquet(path)
    spark.read.parquet(path).createOrReplaceTempView(relName)
  }

  /** Format a plan consistently regardless of Spark version */
  def buildPlanString(plan: LogicalPlan): String = {
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
