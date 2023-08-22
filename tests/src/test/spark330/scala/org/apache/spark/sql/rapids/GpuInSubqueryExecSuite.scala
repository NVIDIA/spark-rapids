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
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{GpuOverrides, GpuTransitionOverrides, SparkQueryCompareTestSuite}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, ExprId}
import org.apache.spark.sql.execution.{FilterExec, InSubqueryExec, LocalTableScanExec, ProjectExec, RowToColumnarExec, SparkPlan, SubqueryExec}

class GpuInSubqueryExecSuite extends SparkQueryCompareTestSuite {
  private def readToPhysicalPlan(df: DataFrame): SparkPlan = {
    df.queryExecution.executedPlan.transformUp {
      case s: LocalTableScanExec => RowToColumnarExec(s)
    }
  }

  private def subqueryTable(spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    Seq("400.0", "123.4").toDF("strings")
  }

  private def buildCpuInSubqueryPlan(spark: SparkSession): SparkPlan = {
    val df1ReadExec = readToPhysicalPlan(nullableStringsIntsDf(spark))
    val df2ReadExec = readToPhysicalPlan(subqueryTable(spark))
    val inSubquery = InSubqueryExec(
      df1ReadExec.output.head,
      SubqueryExec("sbe",
        ProjectExec(Seq(df2ReadExec.output.head), df2ReadExec)),
      ExprId(7))
    FilterExec(DynamicPruningExpression(inSubquery), df1ReadExec)
  }

  test("InSubqueryExec") {
    val gpuResults = withGpuSparkSession({ spark =>
      val overrides = new GpuOverrides()
      val transitionOverrides = new GpuTransitionOverrides()
      val gpuPlan = transitionOverrides(overrides(buildCpuInSubqueryPlan(spark)))
      gpuPlan.execute().collect()
    })
    assertResult(1)(gpuResults.length)
    val row = gpuResults.head
    assertResult(2)(row.numFields)
    assertResult("400.0")(row.getString(0))
    assert(row.isNullAt(1))
  }
}
