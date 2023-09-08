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
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "340"}
{"spark": "341"}
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{GpuOverrides, GpuTransitionOverrides, SparkQueryCompareTestSuite}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, ExprId}
import org.apache.spark.sql.execution.{FilterExec, InSubqueryExec, LocalTableScanExec, ProjectExec, RowToColumnarExec, SparkPlan, SubqueryExec}

/**
 * Testing for GpuInSubqeryExec. It is difficult to build a series of DataFrame
 * operations, even at the logical plan level, that will reliably translate
 * to an InSubqueryExec in the final physical CPU plan. Most Spark implementations
 * turn this into some sort of join instead. Therefore these tests build low-level
 * physical plans with InSubqueryExec and manually invoke the GPU optimization rules
 * to make sure we're exercising GpuInSubqueryExec.
 */
class GpuInSubqueryExecSuite extends SparkQueryCompareTestSuite {
  private def readToPhysicalPlan(df: DataFrame): SparkPlan = {
    // Since we're building up the low-level plan manually, Spark won't
    // automatically inject the columnar transitions for us. This adds
    // a columnar transition to the local table scan which will
    // remain on the CPU. When the GPU optimization rules later run,
    // this will be turned into a GPU columnar transition.
    df.queryExecution.executedPlan.transformUp {
      case s: LocalTableScanExec => RowToColumnarExec(s)
    }
  }

  private def subqueryTable(spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    Seq("400.0", "123.4").toDF("strings")
  }

  private def buildCpuInSubqueryPlan(
      spark: SparkSession,
      shouldBroadcast: Boolean): SparkPlan = {
    val df1ReadExec = readToPhysicalPlan(nullableStringsIntsDf(spark))
    val df2ReadExec = readToPhysicalPlan(subqueryTable(spark))
    val inSubquery = InSubqueryExec(
      df1ReadExec.output.head,
      SubqueryExec("sbe",
        ProjectExec(Seq(df2ReadExec.output.head), df2ReadExec)),
      ExprId(7),
      shouldBroadcast=shouldBroadcast)
    FilterExec(DynamicPruningExpression(inSubquery), df1ReadExec)
  }

  for (shouldBroadcast <- Seq(false, true)) {
    test(s"InSubqueryExec shouldBroadcast=$shouldBroadcast") {
      val gpuResults = withGpuSparkSession({ spark =>
        val overrides = new GpuOverrides()
        val transitionOverrides = new GpuTransitionOverrides()
        val cpuPlan = buildCpuInSubqueryPlan(spark, shouldBroadcast)
        val gpuPlan = transitionOverrides(overrides(cpuPlan))
        gpuPlan.execute().collect()
      })
      assertResult(1)(gpuResults.length)
      val row = gpuResults.head
      assertResult(2)(row.numFields)
      assertResult("400.0")(row.getString(0))
      assert(row.isNullAt(1))
    }
  }
}
