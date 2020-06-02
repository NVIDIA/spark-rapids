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

package ai.rapids.spark

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.rapids.execution.GpuBroadcastHashJoinExec

/** Test plan modifications to add optimizing sorts after hash joins in the plan */
class HashSortOptimizeSuite extends FunSuite with BeforeAndAfterAll {
  import SparkSessionHolder.spark
  import spark.sqlContext.implicits._

  private val df1 = Seq(
    (1, 2, 3),
    (4, 5, 6),
    (7, 8, 9)
  ).toDF("a", "b", "c")

  private val df2 = Seq(
    (1, 12),
    (5, 14),
    (7, 17)
  ).toDF("x", "y")

  override def beforeAll(): Unit = {
    // Setup the conf for the spark session
    SparkSessionHolder.resetSparkSessionConf()
    // Turn on the GPU
    spark.conf.set(RapidsConf.SQL_ENABLED.key, "true")
    // Turn on hash optimized sort
    spark.conf.set(RapidsConf.ENABLE_HASH_OPTIMIZE_SORT.key, "true")
  }

  /**
   * Find the first GPU optimize sort in the plan and verify it has been inserted after the
   * specified join node.
   **/
  private def validateOptimizeSort(queryPlan: SparkPlan, joinNode: SparkPlan): Unit = {
    val sortNode = queryPlan.find(_.isInstanceOf[GpuSortExec])
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
    val rdf = df1.join(df2, df1("a") === df2("x"))
    val plan = rdf.queryExecution.executedPlan
    val joinNode = plan.find(_.isInstanceOf[GpuBroadcastHashJoinExec])
    assert(joinNode.isDefined, "No broadcast join node found")
    validateOptimizeSort(plan, joinNode.get)
  }

  test("sort inserted after shuffled hash join") {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 0)
    val rdf = df1.join(df2, df1("a") === df2("x"))
    val plan = rdf.queryExecution.executedPlan
    val joinNode = plan.find(_.isInstanceOf[GpuShuffledHashJoinExec])
    assert(joinNode.isDefined, "No broadcast join node found")
    validateOptimizeSort(plan, joinNode.get)
  }

  test("config to disable") {
    spark.conf.set(RapidsConf.ENABLE_HASH_OPTIMIZE_SORT.key, "false")
    try {
      val rdf = df1.join(df2, df1("a") === df2("x"))
      val plan = rdf.queryExecution.executedPlan
      val sortNode = plan.find(_.isInstanceOf[GpuSortExec])
      assert(sortNode.isEmpty)
    } finally {
      spark.conf.set(RapidsConf.ENABLE_HASH_OPTIMIZE_SORT.key, "true")
    }
  }

  test("sort not inserted if there is already ordering") {
    val rdf = df1.join(df2, df1("a") === df2("x")).orderBy(df1("a"))
    val plan = rdf.queryExecution.executedPlan
    val numSorts = plan.map {
      case _: SortExec | _: GpuSortExec => 1
      case _ => 0
    }.sum
    assertResult(1) { numSorts }
    val sort = plan.find(_.isInstanceOf[GpuSortExec])
    if (sort.isDefined) {
      assertResult(true) { sort.get.asInstanceOf[GpuSortExec].global }
    }
  }
}
