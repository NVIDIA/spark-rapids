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

package com.nvidia.spark.rapids

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.execution._

class RowBasedExpressionSuite extends SparkQueryCompareTestSuite {

  def fallbackProject: SparkConf = {
    new SparkConf()
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "ProjectExec")
  }

  def enableRowBasedExpression: SparkConf = {
    new SparkConf()
      .set(RapidsConf.ENABLE_CPU_ROW_BASED_EXPRESSIONS.key, "true")
  }
  
  def simpleDF(spark: SparkSession): Dataset[Row] = {
    spark.range(0, 1000).selectExpr("cast(id AS STRING) as id")
  }

  test("project should fallback to CPU") {
     withGpuSparkSession(spark => {
      val df = simpleDF(spark).selectExpr("regexp_extract(id, \"(!\\\\w+)\\\\d+([0-5]{0,2})\", 1)")
      val gpuPlan = df.queryExecution.executedPlan
      val project = gpuPlan.find {
        case ProjectExec(projectList, child) =>
          val boundRefs = BindReferences.bindReferences(projectList, child.output)
          boundRefs.exists {
            case Alias(c, _) =>
              c.isInstanceOf[RegExpExtract]
            case _: RegExpExtract => true
            case _ => false
          }
        case _ =>
          false
      }
      assert(project.isDefined)
     }, fallbackProject)
  }

  test("gpuwrapped expression is in the final plan") {
     withGpuSparkSession(spark => {
      val df = simpleDF(spark).selectExpr("regexp_extract(id, \"(!\\\\w+)\\\\d+([0-5]{0,2})\", 1)")
      val gpuPlan = df.queryExecution.executedPlan
      
      val project = gpuPlan.find {
        case GpuProjectExec(projectList, child) => 
          val boundRefs = GpuBindReferences.bindGpuReferences(projectList, child.output)
          boundRefs.exists {
            case GpuAlias(c, _) =>
              c.isInstanceOf[GpuRegExpExtractWithFallback]
            case _: GpuRegExpExtractWithFallback => true
            case _ => 
              false
          }
        case _ => false
      }
      assert(project.isDefined)
     }, enableRowBasedExpression)
  }

  testSparkResultsAreEqual("rlike execution on CPU while holding the GPU semaphore", 
      simpleDF,
      conf = enableRowBasedExpression) { frame =>
    frame.selectExpr("id RLIKE \"(!\\\\w+)\\\\d+([0-5]{0,2})\"")
  }

  testSparkResultsAreEqual("regexp_extract execution on CPU while holding the GPU semaphore", 
      simpleDF,
      conf = enableRowBasedExpression) { frame =>
    frame.selectExpr("regexp_extract(id, \"(!\\\\w+)\\\\d+([0-5]{0,2})\", 1)")
  }

  testSparkResultsAreEqual("regexp_extract_all execution on CPU while holding the GPU semaphore", 
      simpleDF,
      conf = enableRowBasedExpression) { frame =>
    frame.selectExpr("regexp_extract_all(id, \"(!\\\\w+)\\\\d+([0-5]{0,2})\", 1)")
  }

  testSparkResultsAreEqual("regexp_replace execution on CPU while holding the GPU semaphore", 
      simpleDF,
      conf = enableRowBasedExpression) { frame =>
    frame.selectExpr("regexp_replace(id, \"(!\\\\w+)\\\\d+([0-5]{0,2})\", \"foo\", 2)")
  }

  testSparkResultsAreEqual("split execution on CPU while holding the GPU semaphore", 
      simpleDF,
      conf = enableRowBasedExpression) { frame =>
    frame.selectExpr("split(id, \"(!\\\\w+)\\\\d+([0-5]{0,2})\", 2)")
  }
  
}
