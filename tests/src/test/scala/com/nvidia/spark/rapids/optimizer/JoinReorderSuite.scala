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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.DataTypes

class JoinReorderSuite extends SparkQueryCompareTestSuite with FunSuiteWithTempDir {

  private val defaultConf = new SparkConf()

  private val confJoinOrderingEnabled = new SparkConf()
    .set(RapidsConf.JOIN_REORDERING.key, "true")
    .set(RapidsConf.JOIN_REORDERING_RATIO.key, "0.3")
    .set(RapidsConf.JOIN_REORDERING_MAX_FACT.key, "2")
    .set(RapidsConf.JOIN_REORDERING_PRESERVE_ORDER.key, "true")

  test("join filtered dimension earlier") {
    val sql =
      """SELECT * FROM fact
        |JOIN dim1 ON fact.c0 = dim1.c0
        |JOIN dim2 ON fact.c0 = dim2.c0
        |WHERE dim2.c1 LIKE 'test%'
        |""".stripMargin

    // run with join reordering disabled
    {
      val df = execute(sql, defaultConf)
      val plan = df.queryExecution.optimizedPlan
      val expected =
        """Join: (fact.c0 = dim2.c0)
          |  Join: (fact.c0 = dim1.c0)
          |    Filter: (fact.c0 IS NOT NULL)
          |      LogicalRelation: fact.parquet
          |    Filter: (dim1.c0 IS NOT NULL)
          |      LogicalRelation: dim1.parquet
          |  Filter: (((dim2.c1 IS NOT NULL) AND startswith(dim2.c1, 'test')) AND (dim2.c0 IS NOT NULL))
          |    LogicalRelation: dim2.parquet""".stripMargin
      val actual = buildPlanString(plan)
      println(actual)
      assert(expected === actual)
    }

    // run with join reordering enabled
    val df = execute(sql, confJoinOrderingEnabled)
    val plan = df.queryExecution.optimizedPlan
    val expected =
      """Join: (fact.c0 = dim1.c0)
        |  Join: (fact.c0 = dim2.c0)
        |    Filter: (fact.c0 IS NOT NULL)
        |      LogicalRelation: fact.parquet
        |    Filter: (((dim2.c1 IS NOT NULL) AND startswith(dim2.c1, 'test')) AND (dim2.c0 IS NOT NULL))
        |      LogicalRelation: dim2.parquet
        |  Filter: (dim1.c0 IS NOT NULL)
        |    LogicalRelation: dim1.parquet""".stripMargin
    val actual = buildPlanString(plan)
    println(actual)
    assert(expected === actual)
  }

  private def execute(sql: String, conf: SparkConf): DataFrame = {
    withGpuSparkSession(spark => {
      createTestRelation(spark, "fact", 1000)
      createTestRelation(spark, "dim1", 100)
      createTestRelation(spark, "dim2", 200)
      val df = spark.sql(sql)
      df.collect()
      df
    }, conf)
  }

  private def createTestRelation(spark: SparkSession, name: String, rowCount: Int): Unit = {
    val schema = createSchema(Seq(DataTypes.IntegerType, DataTypes.StringType))
    val df = generateDataFrame(spark, schema, rowCount)
    val path = new File(TEST_FILES_ROOT, s"$name.parquet").getAbsolutePath
    df.write.mode(SaveMode.Overwrite).parquet(path)
    spark.read.parquet(path).createOrReplaceTempView(name)
  }

  /** Format the plan consistently, regardless of Spark version */
  private def buildPlanString(plan: LogicalPlan): String = {
    def exprToString(expr: Expression): String = {
      expr.sql
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
          plan.simpleString(5)
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
