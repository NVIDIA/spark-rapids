/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date, unix_timestamp}
import org.apache.spark.sql.internal.SQLConf

class ParseDateTimeSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual("to_date",
      datesAsStrings,
      new SparkConf().set(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "CORRECTED")) {
    df => df.withColumn("c1", to_date(col("c0"), "yyyy-MM-dd"))
  }

  testSparkResultsAreEqual("to_date default pattern",
      datesAsStrings,
      new SparkConf().set(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "CORRECTED")) {
    df => df.withColumn("c1", to_date(col("c0")))
  }

  testSparkResultsAreEqual("unix_timestamp parse date",
      timestampsAsStrings,
      new SparkConf().set(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "CORRECTED")) {
    df => df.withColumn("c1", unix_timestamp(col("c0"), "yyyy-MM-dd"))
  }

  testSparkResultsAreEqual("unix_timestamp parse timestamp",
      timestampsAsStrings,
      new SparkConf().set(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "CORRECTED")) {
    df => df.withColumn("c1", unix_timestamp(col("c0"), "yyyy-MM-dd HH:mm:ss"))
  }

  testSparkResultsAreEqual("unix_timestamp parse timestamp millis (fall back to CPU)",
    timestampsAsStrings,
    new SparkConf().set(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "CORRECTED")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "ProjectExec,Alias,UnixTimestamp,Literal")) {
    df => df.withColumn("c1", unix_timestamp(col("c0"), "yyyy-MM-dd HH:mm:ss.SSS"))
  }

  testSparkResultsAreEqual("unix_timestamp parse timestamp default pattern",
      timestampsAsStrings,
      new SparkConf().set(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "CORRECTED")) {
    df => df.withColumn("c1", unix_timestamp(col("c0")))
  }

  test("fail to parse when policy is EXCEPTION") {
    val e = intercept[SparkException] {
      val df = withGpuSparkSession(spark => {
        timestampsAsStrings(spark)
            .repartition(2)
            .withColumn("c1", unix_timestamp(col("c0"), "yyyy-MM-dd HH:mm:ss"))
      }, new SparkConf().set(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "EXCEPTION"))
      df.collect()
    }
    assert(e.getMessage.contains("failed to parse one or more values"))
  }

  test("fall back to CPU when policy is LEGACY") {
    val e = intercept[IllegalArgumentException] {
      val df = withGpuSparkSession(spark => {
        timestampsAsStrings(spark)
            .repartition(2)
            .withColumn("c1", unix_timestamp(col("c0"), "yyyy-MM-dd HH:mm:ss"))
      }, new SparkConf().set(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "LEGACY"))
      df.collect()
    }
    assert(e.getMessage.contains(
      "Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec"))
  }

  private def timestampsAsStrings(spark: SparkSession) = {
    import spark.implicits._
    timestampValues.toDF("c0")
  }

  private def datesAsStrings(spark: SparkSession) = {
    import spark.implicits._
    val values = Seq(
      GpuCast.EPOCH,
      GpuCast.NOW,
      GpuCast.TODAY,
      GpuCast.YESTERDAY,
      GpuCast.TOMORROW,
    ) ++ timestampValues
    values.toDF("c0")
  }

  private val timestampValues = Seq(
    "",
    "null",
    null,
    "\n",
    "1999-12-31 ",
    "1999-12-31 11",
    "1999-12-31 11:",
    "1999-12-31 11:5",
    "1999-12-31 11:59",
    "1999-12-31 11:59:",
    "1999-12-31 11:59:5",
    "1999-12-31 11:59:59",
    "1999-12-31 11:59:59.",
    "1999-12-31 11:59:59.9",
    "1999-12-31 11:59:59.99",
    "1999-12-31 11:59:59.999",
    "1999-12-31",
    "1999-12-31\n",
    "\t1999-12-31",
    "\n1999-12-31",
    "1999/12/31"
  )
}

