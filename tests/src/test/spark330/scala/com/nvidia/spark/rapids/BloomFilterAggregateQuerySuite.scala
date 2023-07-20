/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
{"spark": "332db"}
{"spark": "333"}
{"spark": "340"}
{"spark": "341"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.internal.SQLConf

class BloomFilterAggregateQuerySuite extends SparkQueryCompareTestSuite {
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
    (Seq(Long.MinValue, 0, Long.MaxValue) ++ (1L to 10000L)).toDF("col")
  }

  for (numEstimated <- Seq(4096L, 4194304L, Long.MaxValue,
    SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS.defaultValue.get)) {
    for (numBits <- Seq(4096L, 4194304L, Long.MaxValue,
      SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS.defaultValue.get)) {
      ALLOW_NON_GPU_testSparkResultsAreEqual(
        s"might_contain estimated=$numEstimated numBits=$numBits",
        buildData,
        Seq("ObjectHashAggregateExec", "ShuffleExchangeExec"))(df =>
        {
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
          try {
            installSqlFuncs(df.sparkSession)
            df.sparkSession.sql(sqlString)
          } finally {
            uninstallSqlFuncs(df.sparkSession)
          }
        })
    }
  }
}
