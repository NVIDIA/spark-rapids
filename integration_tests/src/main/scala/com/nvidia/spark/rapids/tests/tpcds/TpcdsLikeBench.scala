/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
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

package com.nvidia.spark.rapids.tests.tpcds

import com.nvidia.spark.rapids.tests.common.BenchmarkSuite

import org.apache.spark.sql.{DataFrame, SparkSession}

class TpcdsLikeBench(val appendDat: Boolean) extends BenchmarkSuite {
  override def name(): String = "TPC-DS"

  override def shortName(): String = "tpcds"

  override def setupAllParquet(spark: SparkSession, path: String): Unit = {
    TpcdsLikeSpark.setupAllParquet(spark, path, appendDat)
  }

  override def setupAllCSV(spark: SparkSession, path: String): Unit = {
    TpcdsLikeSpark.setupAllCSV(spark, path, appendDat)
  }

  override def setupAllOrc(spark: SparkSession, path: String): Unit = {
    TpcdsLikeSpark.setupAllOrc(spark, path, appendDat)
  }

  override def createDataFrame(spark: SparkSession, query: String): DataFrame = {
    TpcdsLikeSpark.run(spark, query)
  }
}

