/*
 * Copyright (c) 2019-2022, NVIDIA CORPORATION.
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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, StructField, StructType}

class OrcScanSuite extends SparkQueryCompareTestSuite {

  private val fileSplitsOrc = frameFromOrc("file-splits.orc")

  testSparkResultsAreEqual("Test ORC chunks", fileSplitsOrc,
    new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "2048")) {
    frame => frame.select(col("loan_id"), col("orig_interest_rate"), col("zip"))
  }

  testSparkResultsAreEqual("Test ORC count chunked by rows", fileSplitsOrc,
    new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "2048"))(frameCount)

  testSparkResultsAreEqual("Test ORC count chunked by bytes", fileSplitsOrc,
    new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "100"))(frameCount)

  testSparkResultsAreEqual("schema-can-prune dis-order read schema",
    frameFromOrcWithSchema("schema-can-prune.orc", StructType(Seq(
      StructField("c2_string", StringType),
      StructField("c3_long", LongType),
      StructField("c1_int", IntegerType))))) { frame => frame }

  testSparkResultsAreEqual("schema-can-prune dis-order read schema 1",
    frameFromOrcWithSchema("schema-can-prune.orc", StructType(Seq(
      StructField("c2_string", StringType),
      StructField("c1_int", IntegerType),
      StructField("c3_long", LongType))))) { frame => frame }

  testSparkResultsAreEqual("schema-can-prune dis-order read schema 2",
    frameFromOrcWithSchema("schema-can-prune.orc", StructType(Seq(
      StructField("c3_long", LongType),
      StructField("c2_string", StringType),
      StructField("c1_int", IntegerType))))) { frame => frame }

  testSparkResultsAreEqual("schema-can-prune dis-order read schema 3",
    frameFromOrcWithSchema("schema-can-prune.orc", StructType(Seq(
      StructField("c3_long", LongType),
      StructField("c2_string", StringType))))) { frame => frame }

  testSparkResultsAreEqual("schema-can-prune dis-order read schema 4",
    frameFromOrcWithSchema("schema-can-prune.orc", StructType(Seq(
      StructField("c2_string", StringType),
      StructField("c1_int", IntegerType))))) { frame => frame }

  testSparkResultsAreEqual("schema-can-prune dis-order read schema 5",
    frameFromOrcWithSchema("schema-can-prune.orc", StructType(Seq(
      StructField("c3_long", LongType),
      StructField("c1_int", IntegerType))))) { frame => frame }

  /**
   * We can't compare the results from CPU and GPU, since CPU will get in-correct result
   * see https://github.com/NVIDIA/spark-rapids/issues/3060
   */
  test("schema can't be pruned") {
    withGpuSparkSession( spark => {
      val df = frameFromOrcWithSchema("schema-cant-prune.orc",
        StructType(Seq(
          StructField("_col2", StringType),
          StructField("_col3", LongType),
          StructField("_col1", IntegerType))))(spark)
      val ret = df.collect()
      assert(ret(0).getString(0) === "hello")
      assert(ret(0).getLong(1) === 2021)
      assert(ret(0).getInt(2) === 1)

      val df1 = frameFromOrcWithSchema("schema-cant-prune.orc",
        StructType(Seq(
          StructField("_col3", LongType),
          StructField("_col1", IntegerType),
          StructField("_col2", StringType))))(spark)
      val ret1 = df1.collect()
      assert(ret1(0).getLong(0) === 2021)
      assert(ret1(0).getInt(1) === 1)
      assert(ret1(0).getString(2) === "hello")
    })
  }

  /**
   *
   * The calendar of hybrid-Julian-calendar.orc file is hybrid Julian Gregorian
   * This file has one date column one row, value is 1582-10-03
   * When specify "orc.proleptic.gregorian" calendar to filter the file with c1 >= 1582-10-03,
   * then no result returned. Because of 1582-10-03 in hybrid calender
   * is actually 1582-09-23 in proleptic Gregorian calendar.
   */
  ignore("test hybrid Julian Gregorian calendar vs proleptic Gregorian calendar") {
    // After Spark 3.1.1, Orc failed to prune when converting Hybrid calendar to Proleptic calendar
    // Orc bug: https://issues.apache.org/jira/browse/ORC-1083

    withCpuSparkSession(spark => {
      val df = frameFromOrcWithSchema("hybrid-Julian-calendar.orc",
        StructType(Seq(StructField("c1", DateType))))(spark)
      val ret = df.collect()
      ret.length == 1
      assert(ret(0).toString() == "[1582-10-03]")
    })

    def check(spark: SparkSession) = {
      val df = frameFromOrcWithSchema("hybrid-Julian-calendar.orc",
        StructType(Seq(StructField("c1", DateType))))(spark)
      df.createOrReplaceTempView("df1")
      // should not have data, if orc.proleptic.gregorian is specified
      val ret = spark.sql("select c1 from df1 where c1 >= to_date('1582-10-03', 'yyyy-MM-dd') ")
          .collect()
      assert(ret.isEmpty)
    }

    val conf: SparkConf = new SparkConf()
    // indicate convert to proleptic Gregorian calendar
    conf.set("orc.proleptic.gregorian", "true")

    // both cpu and gpu should return empty
    withGpuSparkSession(check, conf)
    withCpuSparkSession(check, conf)
  }

}
