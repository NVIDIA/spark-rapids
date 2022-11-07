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
import org.apache.spark.sql.internal.SQLConf

class RowBasedExpressionSuite extends SparkQueryCompareTestSuite {

  def enableRowBasedExpression: SparkConf = {
    return new SparkConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .set(RapidsConf.ENABLE_CPU_ROW_BASED_EXPRESSIONS.key, "true")
  }
  
  def simpleDF(spark: SparkSession): Dataset[Row] = {
    spark.range(0, 1000).selectExpr("cast(id AS STRING) as id")
  }

  test("gpuwrapped expression is in the final plan") {
     withGpuSparkSession(spark => {
      val df = simpleDF(spark).selectExpr("regexp_extract(id, \"(!\\\\w+)\\\\d+([0-5]{0,2})\", 1)")
      val gpuPlan = df.queryExecution.executedPlan
      assert(gpuPlan.toString.contains("gpuwrapped[regexp_extract]"))
     }, enableRowBasedExpression)
  }
  
}
