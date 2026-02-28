/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import org.apache.spark.sql.{DataFrameSetOperationsSuite, Row}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait

class RapidsDataFrameSetOperationsSuite
    extends DataFrameSetOperationsSuite
    with RapidsSQLTestsBaseTrait {
  
  import testImplicits._

  testRapids(
      "SPARK-37371: GPU UnionExec should support columnar if all children support columnar") {
    def checkPlanExists(
        plan: SparkPlan,
        targetPlan: SparkPlan => Boolean,
        name: String): Seq[SparkPlan] = {
      val target = plan.collect {
        case p if targetPlan(p) => p
      }
      assert(target.nonEmpty, s"No matching $name nodes found in: ${plan.treeString}")
      target
    }

    def isGpuNode(plan: SparkPlan): Boolean = {
      plan.getClass.getName.contains(".rapids.")
    }

    def checkColumnarContract(
        target: Seq[SparkPlan],
        expectedCpuColumnar: Boolean,
        label: String): Unit = {
      target.foreach { p =>
        val expected = if (isGpuNode(p)) true else expectedCpuColumnar
        assert(p.supportsColumnar == expected,
          s"$label node ${p.nodeName} expected supportsColumnar=$expected, " +
            s"but got ${p.supportsColumnar}")
      }
    }

    Seq(true, false).foreach { supported =>
      withSQLConf(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> supported.toString) {
        val df1 = Seq(1, 2, 3).toDF("i").cache()
        val df2 = Seq(4, 5, 6).toDF("j").cache()

        // Materialize the cache
        df1.count()
        df2.count()

        val union = df1.union(df2)
        
        val scanNodes = checkPlanExists(
          union.queryExecution.executedPlan,
          _.nodeName.contains("InMemoryTableScan"),
          "InMemoryTableScan")
        checkColumnarContract(scanNodes, expectedCpuColumnar = supported, "InMemoryTableScan")
        
        val unionNodes = checkPlanExists(
          union.queryExecution.executedPlan,
          _.nodeName.contains("Union"),
          "Union")
        checkColumnarContract(unionNodes, expectedCpuColumnar = supported, "Union")
        
        // Verify query results
        checkAnswer(union, Row(1) :: Row(2) :: Row(3) :: Row(4) :: Row(5) :: Row(6) :: Nil)

        // Test union with cached and non-cached DataFrames.
        // GPU and CPU can differ here in plan details, but result semantics must match.
        val mixedUnion = df1.union(Seq(7, 8, 9).toDF("k"))
        checkPlanExists(
          mixedUnion.queryExecution.executedPlan,
          _.nodeName.contains("Union"),
          "Union")
        
        // Verify query results
        checkAnswer(mixedUnion,
          Row(1) :: Row(2) :: Row(3) :: Row(7) :: Row(8) :: Row(9) :: Nil)
      }
    }
  }
}

