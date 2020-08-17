/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.TestUtils.{findOperator, getFinalPlan}
import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.{SortExec, SparkPlan}

/** Test plan modifications to add optimizing sorts after hash joins in the plan */
class HashSortOptimizeSuite extends FunSuite {
  private def buildDataFrame1(spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("a", "b", "c")
  }

  private def buildDataFrame2(spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    Seq(
      (1, 12),
      (5, 14),
      (7, 17)
    ).toDF("x", "y")
  }

  private val sparkConf = new SparkConf()
      .set(RapidsConf.SQL_ENABLED.key, "true")
      .set(RapidsConf.ENABLE_HASH_OPTIMIZE_SORT.key, "true")

  /**
   * Find the first GPU optimize sort in the plan and verify it has been inserted after the
   * specified join node.
   **/
  private def validateOptimizeSort(queryPlan: SparkPlan, joinNode: SparkPlan): Unit = {
    val executedPlan = ExecutionPlanCaptureCallback.extractExecutedPlan(Some(queryPlan))
    val sortNode = executedPlan.find(_.isInstanceOf[GpuSortExec])
    assert(sortNode.isDefined, "No sort node found")
    val gse = sortNode.get.asInstanceOf[GpuSortExec]
    assert(gse.children.length == 1)
    assert(gse.global == false)
    assert(gse.coalesceGoal.isInstanceOf[TargetSize])
    val sortChild = gse.children.head
    assert(sortChild.isInstanceOf[GpuCoalesceBatches])
    assertResult(joinNode) { sortChild.children.head }
  }

  test("sort inserted after broadcast hash join") {
    SparkSessionHolder.withSparkSession(sparkConf, { spark =>
      val df1 = buildDataFrame1(spark)
      val df2 = buildDataFrame2(spark)
      val rdf = df1.join(df2, df1("a") === df2("x"))
      val plan = rdf.queryExecution.executedPlan
      // execute the plan so that the final adaptive plan is available when AQE is on
      rdf.collect()

      val joinNode = findOperator(plan, ShimLoader.getSparkShims.isGpuBroadcastHashJoin(_))
      assert(joinNode.isDefined, "No broadcast join node found")
      validateOptimizeSort(plan, joinNode.get)
    })
  }

  test("sort inserted after shuffled hash join") {
    val conf = sparkConf.clone().set("spark.sql.autoBroadcastJoinThreshold", "0")
    SparkSessionHolder.withSparkSession(conf, { spark =>
      val df1 = buildDataFrame1(spark)
      val df2 = buildDataFrame2(spark)
      val rdf = df1.join(df2, df1("a") === df2("x"))
      val plan = rdf.queryExecution.executedPlan
      // execute the plan so that the final adaptive plan is available when AQE is on
      rdf.collect()
      val joinNode = findOperator(plan, ShimLoader.getSparkShims.isGpuShuffledHashJoin(_))
      assert(joinNode.isDefined, "No broadcast join node found")
      validateOptimizeSort(plan, joinNode.get)
    })
  }

  test("config to disable") {
    val conf = sparkConf.clone().set(RapidsConf.ENABLE_HASH_OPTIMIZE_SORT.key, "false")
    SparkSessionHolder.withSparkSession(conf, { spark =>
      val df1 = buildDataFrame1(spark)
      val df2 = buildDataFrame2(spark)
      val rdf = df1.join(df2, df1("a") === df2("x"))
      val plan = rdf.queryExecution.executedPlan
      val sortNode = plan.find(_.isInstanceOf[GpuSortExec])
      assert(sortNode.isEmpty)
    })
  }

  test("sort not inserted if there is already ordering") {
    SparkSessionHolder.withSparkSession(sparkConf, { spark =>
      val df1 = buildDataFrame1(spark)
      val df2 = buildDataFrame2(spark)
      val rdf = df1.join(df2, df1("a") === df2("x")).orderBy(df1("a"))
      val plan = rdf.queryExecution.executedPlan
      // Get the final executed plan when AQE is either enabled or disabled.
      val finalPlan = getFinalPlan(plan)

      val numSorts = finalPlan.map {
        case _: SortExec | _: GpuSortExec => 1
        case _ => 0
      }.sum
      assertResult(1) {
        numSorts
      }
      val sort = plan.find(_.isInstanceOf[GpuSortExec])
      if (sort.isDefined) {
        assertResult(true) {
          sort.get.asInstanceOf[GpuSortExec].global
        }
      }
    })
  }
}
