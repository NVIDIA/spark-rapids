/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{PartialReducerPartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.{GpuCustomShuffleReaderExec, GpuShuffledHashJoinBase}

class AdaptiveQueryExecSuite
    extends SparkQueryCompareTestSuite
        with AdaptiveSparkPlanHelper {

  private def runAdaptiveAndVerifyResult(
      spark: SparkSession, query: String): (SparkPlan, SparkPlan) = {

    val dfAdaptive = spark.sql(query)
    val planBefore = dfAdaptive.queryExecution.executedPlan
    // isFinalPlan is a private field so we have to use toString to access it
    assert(planBefore.toString.startsWith("AdaptiveSparkPlan isFinalPlan=false"))

    dfAdaptive.collect()
    val planAfter = dfAdaptive.queryExecution.executedPlan
    // isFinalPlan is a private field so we have to use toString to access it
    assert(planAfter.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
    val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan

    // With AQE, the query is broken down into query stages based on exchange boundaries, so the
    // final query that is executed depends on the results from its child query stages. There
    // cannot be any exchange nodes left when the final query is executed because they will
    // have already been replaced with QueryStageExecs.
    val exchanges = adaptivePlan.collect {
      case e: Exchange => e
    }
    assert(exchanges.isEmpty, "The final plan should not contain any Exchange node.")
    (dfAdaptive.queryExecution.sparkPlan, adaptivePlan)
  }

  private def findTopLevelSortMergeJoin(plan: SparkPlan): Seq[GpuShuffledHashJoinBase] = {
    collect(plan) {
      case j: GpuShuffledHashJoinBase => j
    }
  }

  test("skewed inner join optimization") {
    skewJoinTest { spark =>
      val (_, innerAdaptivePlan) = runAdaptiveAndVerifyResult(
        spark,
        "SELECT * FROM skewData1 join skewData2 ON key1 = key2")
      val innerSmj = findTopLevelSortMergeJoin(innerAdaptivePlan)
      checkSkewJoin(innerSmj, 2, 1)
    }
  }

  test("skewed left outer join optimization") {
    skewJoinTest { spark =>
      val (_, leftAdaptivePlan) = runAdaptiveAndVerifyResult(
        spark,
        "SELECT * FROM skewData1 left outer join skewData2 ON key1 = key2")
      val leftSmj = findTopLevelSortMergeJoin(leftAdaptivePlan)
      checkSkewJoin(leftSmj, 2, 0)
    }
  }

  test("skewed right outer join optimization") {
    skewJoinTest { spark =>
      val (_, rightAdaptivePlan) = runAdaptiveAndVerifyResult(
        spark,
        "SELECT * FROM skewData1 right outer join skewData2 ON key1 = key2")
      val rightSmj = findTopLevelSortMergeJoin(rightAdaptivePlan)
      checkSkewJoin(rightSmj, 0, 1)
    }
  }

  def skewJoinTest(fun: SparkSession => Unit) {

    // this test requires Spark 3.0.1 or later
    val isValidTestForSparkVersion = ShimLoader.getSparkShims.getSparkShimVersion match {
      case SparkShimVersion(3, 0, 0) => false
      case DatabricksShimVersion(3, 0, 0) => false
      case _ => true
    }
    assume(isValidTestForSparkVersion)

    val conf = new SparkConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key, "1")
      .set(SQLConf.SHUFFLE_PARTITIONS.key, "100")
      .set(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key, "800")
      .set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "800")

    withGpuSparkSession(spark => {
      import spark.implicits._

      spark
          .range(0, 1000, 1, 10)
          .select(
            when('id < 250, 249)
                .when('id >= 750, 1000)
                .otherwise('id).as("key1"),
            'id as "value1")
          .createOrReplaceTempView("skewData1")

      // note that the skew amount here has been modified compared to the original Spark test to
      // compensate for the effects of compression when running on GPU which can change the
      // partition sizes substantially
      spark
          .range(0, 1000, 1, 10)
          .select(
            when('id < 500, 249)
                .otherwise('id).as("key2"),
            'id as "value2")
          .createOrReplaceTempView("skewData2")

      // invoke the test function
      fun(spark)

    }, conf)
  }

  def checkSkewJoin(
      joins: Seq[GpuShuffledHashJoinBase],
      leftSkewNum: Int,
      rightSkewNum: Int): Unit = {
    assert(joins.size == 1 && joins.head.isSkewJoin)

    val leftSkew = joins.head.left.collect {
      case r: GpuCustomShuffleReaderExec => r
    }.head.partitionSpecs.collect {
      case p: PartialReducerPartitionSpec => p.reducerIndex
    }.distinct
    assert(leftSkew.length == leftSkewNum)

    val rightSkew = joins.head.right.collect {
      case r: GpuCustomShuffleReaderExec => r
    }.head.partitionSpecs.collect {
      case p: PartialReducerPartitionSpec => p.reducerIndex
    }.distinct
    assert(rightSkew.length == rightSkewNum)
  }

}
