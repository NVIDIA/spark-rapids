/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION. All rights reserved.
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

package com.nvidia.spark.rapids.tests.tpch

import com.nvidia.spark.rapids.tests.common.BenchmarkSuite

import org.apache.spark.sql.{DataFrame, SparkSession}

object TpchLikeBench extends BenchmarkSuite {
  override def name(): String = "TPC-H"

  override def shortName(): String = "tpch"

  override def setupAllParquet(spark: SparkSession, path: String): Unit = {
    TpchLikeSpark.setupAllParquet(spark, path)
  }

  override def setupAllCSV(spark: SparkSession, path: String): Unit = {
    TpchLikeSpark.setupAllCSV(spark, path)
  }

  override def setupAllOrc(spark: SparkSession, path: String): Unit = {
    TpchLikeSpark.setupAllOrc(spark, path)
  }

  override def createDataFrame(spark: SparkSession, query: String): DataFrame = {
    getQuery(query)(spark)
  }

  private def getQuery(query: String)(spark: SparkSession) = {
    query match {
      case "q1" => Q1Like(spark)
      case "q2" => Q2Like(spark)
      case "q3" => Q3Like(spark)
      case "q4" => Q4Like(spark)
      case "q5" => Q5Like(spark)
      case "q6" => Q6Like(spark)
      case "q7" => Q7Like(spark)
      case "q8" => Q8Like(spark)
      case "q9" => Q9Like(spark)
      case "q10" => Q10Like(spark)
      case "q11" => Q11Like(spark)
      case "q12" => Q12Like(spark)
      case "q13" => Q13Like(spark)
      case "q14" => Q14Like(spark)
      case "q15" => Q15Like(spark)
      case "q16" => Q16Like(spark)
      case "q17" => Q17Like(spark)
      case "q18" => Q18Like(spark)
      case "q19" => Q19Like(spark)
      case "q20" => Q20Like(spark)
      case "q21" => Q21Like(spark)
      case "q22" => Q22Like(spark)
    }
  }
}
