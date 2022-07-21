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

import scala.util.Random

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

class StructBinaryOpSuite extends SparkQueryCompareTestSuite {

  private def generateStructComparisonDF(
        spark: SparkSession,
        rowCount: Int = 1024,
        seed: Long = 0): DataFrame = {
    val rnd = new Random(seed);
    val data = (0 to rowCount).map { i =>
      Row(
        Row(Row(rnd.nextLong % 4,
          if (rnd.nextBoolean()) null else 1),
          if (rnd.nextInt % 8 == 0) null else Row(rnd.nextLong % 4)),
        Row(Row(rnd.nextLong % 4,
          if (rnd.nextBoolean()) null else 1),
          if (rnd.nextInt % 8 == 0) null else Row(rnd.nextLong % 4)))
    }
    val schema = StructType(Seq(
      StructField("sc1", StructType(Seq(
        StructField("sf1", StructType(
          Seq(StructField("ssf1", LongType), StructField("ssf2", IntegerType)))),
        StructField("sf2", StructType(
          Seq(StructField("ssf3", LongType))))))),
      StructField("sc2", StructType(Seq(
        StructField("sf1", StructType(
          Seq(StructField("ssf1", LongType), StructField("ssf2", IntegerType)))),
        StructField("sf2", StructType(
          Seq(StructField("ssf3", LongType)))))))))
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  testSparkResultsAreEqual("StructEqualTo", spark => { generateStructComparisonDF(spark)}) {
    df => df.selectExpr("sc1 == sc2")
  }

  testSparkResultsAreEqual("StructNotEqual", spark => { generateStructComparisonDF(spark)}) {
    df => df.selectExpr("sc1 != sc2")
  }

  testSparkResultsAreEqual("StructLessThan", spark => { generateStructComparisonDF(spark)}) {
    df => df.selectExpr("sc1 < sc2")
  }

  testSparkResultsAreEqual("StructLessThanOrEqual", spark => generateStructComparisonDF(spark)) {
    df => df.selectExpr("sc1 <= sc2")
  }

  testSparkResultsAreEqual("StructGreaterThan", spark => { generateStructComparisonDF(spark)}) {
    df => df.selectExpr("sc1 > sc2")
  }

  testSparkResultsAreEqual("StructGreaterThanOrEqual",
    spark => generateStructComparisonDF(spark)) {
    df => df.selectExpr("sc1 >= sc2")
  }
}
