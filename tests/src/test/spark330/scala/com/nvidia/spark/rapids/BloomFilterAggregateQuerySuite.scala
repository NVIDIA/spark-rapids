/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback

class BloomFilterAggregateQuerySuite extends SparkQueryCompareTestSuite {
  val bloomFilterEnabledConf = new SparkConf()
  val funcId_bloom_filter_agg = new FunctionIdentifier("bloom_filter_agg")
  val funcId_might_contain = new FunctionIdentifier("might_contain")

  private def installSqlFuncs(spark: SparkSession): Unit = {
    // Register 'bloom_filter_agg' to builtin.
    spark.sessionState.functionRegistry.registerFunction(funcId_bloom_filter_agg,
      new ExpressionInfo(classOf[BloomFilterAggregate].getName, "bloom_filter_agg"),
      (children: Seq[Expression]) => children.size match {
        case 1 => new BloomFilterAggregate(children.head)
        case 2 => new BloomFilterAggregate(children.head, children(1))
        case 3 => new BloomFilterAggregate(children.head, children(1), children(2))
      })

    // Register 'might_contain' to builtin.
    spark.sessionState.functionRegistry.registerFunction(funcId_might_contain,
      new ExpressionInfo(classOf[BloomFilterMightContain].getName, "might_contain"),
      (children: Seq[Expression]) => BloomFilterMightContain(children.head, children(1)))
  }

  private def uninstallSqlFuncs(spark: SparkSession): Unit = {
    spark.sessionState.functionRegistry.dropFunction(funcId_bloom_filter_agg)
    spark.sessionState.functionRegistry.dropFunction(funcId_might_contain)
  }

  private def buildData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    (Seq(Some(Long.MinValue), Some(0L), Some(Long.MaxValue), None) ++
        (1L to 10000L).map(x => Some(x)) ++
        (1L to 100L).map(_ => None)).toDF("col")
  }

  private def withExposedSqlFuncs[T](spark: SparkSession)(func: SparkSession => T): T = {
    try {
      installSqlFuncs(spark)
      func(spark)
    } finally {
      uninstallSqlFuncs(spark)
    }
  }

  private def doBloomFilterTest(numEstimated: Long, numBits: Long): DataFrame => DataFrame = {
    df =>
      val table = "bloom_filter_test"
      val sqlString =
        s"""
           |SELECT might_contain(
           |            (SELECT bloom_filter_agg(col,
           |              cast($numEstimated as long),
           |              cast($numBits as long))
           |             FROM $table),
           |            col) positive_membership_test,
           |       might_contain(
           |            (SELECT bloom_filter_agg(col,
           |              cast($numEstimated as long),
           |              cast($numBits as long))
           |             FROM values (-1L), (100001L), (20000L) as t(col)),
           |            col) negative_membership_test
           |FROM $table
          """.stripMargin
      df.createOrReplaceTempView(table)
      withExposedSqlFuncs(df.sparkSession) { spark =>
        spark.sql(sqlString)
      }
  }

  private def getPlanValidator(exec: String): (SparkPlan, SparkPlan) => Unit = {
    def searchPlan(p: SparkPlan): Boolean = {
      ExecutionPlanCaptureCallback.didFallBack(p, exec) ||
        p.children.exists(searchPlan) ||
        p.subqueries.exists(searchPlan)
    }
    (_, gpuPlan) => {
      val executedPlan = ExecutionPlanCaptureCallback.extractExecutedPlan(gpuPlan)
      assert(searchPlan(executedPlan), s"Could not find $exec in the GPU plan:\n$executedPlan")
    }
  }

  // test with GPU bloom build, GPU bloom probe
  for (numEstimated <- Seq(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS.defaultValue.get)) {
    for (numBits <- Seq(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS.defaultValue.get)) {
      testSparkResultsAreEqual(
        s"might_contain GPU build GPU probe estimated=$numEstimated numBits=$numBits",
        buildData,
        conf = bloomFilterEnabledConf.clone()
      )(doBloomFilterTest(numEstimated, numBits))
    }
  }

  // test with CPU bloom build, GPU bloom probe
  for (numEstimated <- Seq(4096L, 4194304L, Long.MaxValue,
    SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS.defaultValue.get)) {
    for (numBits <- Seq(4096L, 4194304L,
      SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS.defaultValue.get)) {
      ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
        s"might_contain CPU build GPU probe estimated=$numEstimated numBits=$numBits",
        buildData,
        Seq("ObjectHashAggregateExec", "ShuffleExchangeExec"),
        conf = bloomFilterEnabledConf.clone()
          .set("spark.rapids.sql.expression.BloomFilterAggregate", "false")
      )(doBloomFilterTest(numEstimated, numBits))(getPlanValidator("ObjectHashAggregateExec"))
    }
  }

  // test with GPU bloom build, CPU bloom probe
  for (numEstimated <- Seq(4096L, 4194304L, Long.MaxValue,
    SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS.defaultValue.get)) {
    for (numBits <- Seq(4096L, 4194304L,
      SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS.defaultValue.get)) {
      ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
        s"might_contain GPU build CPU probe estimated=$numEstimated numBits=$numBits",
        buildData,
        Seq("LocalTableScanExec", "ProjectExec", "ShuffleExchangeExec"),
        conf = bloomFilterEnabledConf.clone()
          .set("spark.rapids.sql.expression.BloomFilterMightContain", "false")
      )(doBloomFilterTest(numEstimated, numBits))(getPlanValidator("ProjectExec"))
    }
  }

  // test with partial/final-only GPU bloom build, CPU bloom probe
  for (mode <- Seq("partial", "final")) {
    for (numEstimated <- Seq(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS.defaultValue.get)) {
      for (numBits <- Seq(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS.defaultValue.get)) {
        ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
          s"might_contain GPU $mode build CPU probe estimated=$numEstimated numBits=$numBits",
          buildData,
          Seq("ObjectHashAggregateExec", "ProjectExec", "ShuffleExchangeExec"),
          conf = bloomFilterEnabledConf.clone()
            .set("spark.rapids.sql.expression.BloomFilterMightContain", "false")
            .set("spark.rapids.sql.hashAgg.replaceMode", mode)
        )(doBloomFilterTest(numEstimated, numBits))(getPlanValidator("ObjectHashAggregateExec"))
      }
    }
  }

  testSparkResultsAreEqual(
    "might_contain with literal bloom filter buffer",
    spark => spark.range(1, 1).asInstanceOf[DataFrame],
    conf=bloomFilterEnabledConf.clone()) {
    df =>
      withExposedSqlFuncs(df.sparkSession) { spark =>
        spark.sql(
          """SELECT might_contain(
            |X'00000001000000050000000343A2EC6EA8C117E2D3CDB767296B144FC5BFBCED9737F267',
            |cast(201 as long))""".stripMargin)
      }
  }

  testSparkResultsAreEqual(
    "might_contain with all NULL inputs",
    spark => spark.range(1, 1).asInstanceOf[DataFrame],
    conf=bloomFilterEnabledConf.clone()) {
    df =>
      withExposedSqlFuncs(df.sparkSession) { spark =>
        spark.sql(
          """
            |SELECT might_contain(null, null) both_null,
            |       might_contain(null, 1L) null_bf,
            |       might_contain((SELECT bloom_filter_agg(cast(id as long)) from range(1, 10000)),
            |            null) null_value
          """.stripMargin)
      }
  }

  testSparkResultsAreEqual(
    "bloom_filter_agg with empty input",
    spark => spark.range(1, 1).asInstanceOf[DataFrame],
    conf=bloomFilterEnabledConf.clone()) {
    df =>
      withExposedSqlFuncs(df.sparkSession) { spark =>
        spark.sql("""SELECT bloom_filter_agg(cast(id as long)) from range(1, 1)""")
      }
  }
}
