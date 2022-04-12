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
import org.apache.spark.sql.functions.{col, date_add, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}

class CsvScanSuite extends SparkQueryCompareTestSuite {
  testSparkResultsAreEqual("Test CSV projection with whitespace delimiter between date and time",
      mixedTypesFromCsvWithHeader) {
    frame => frame.select(col("c_string"), col("c_int"), col("c_timestamp"))
  }

  testSparkResultsAreEqual("Test CSV splits with chunks", floatCsvDf,
    conf = new SparkConf()
        .set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "1")) {
    frame => frame.select(col("floats"))
  }

  testSparkResultsAreEqual(
      "Test CSV count chunked by rows",
      intsFromCsv,
      conf = new SparkConf()
          .set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "1")) {
    frameCount
  }

  testSparkResultsAreEqual(
      "Test CSV count chunked by bytes",
      intsFromCsv,
      conf = new SparkConf()
          .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "0")) {
    frameCount
  }

  /**
   * Running with an inferred schema results in running things that are not columnar optimized.
   */
  ALLOW_NON_GPU_testSparkResultsAreEqual("Test CSV inferred schema",
    intsFromCsvInferredSchema, Seq("FileSourceScanExec", "FilterExec", "CollectLimitExec",
      "GreaterThan", "Length", "StringTrim", "LocalTableScanExec", "DeserializeToObjectExec",
      "Invoke", "AttributeReference", "Literal"),
    conf = new SparkConf()) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual(
    "Test CSV parse dates",
    datesCsvDf,
    conf=new SparkConf()) {
    df => df.withColumn("next_day", date_add(col("dates"), lit(1)))
  }

  testSparkResultsAreEqual(
    "Test CSV parse timestamps as dates",
    timestampsAsDatesCsvDf,
    conf=new SparkConf()) {
    df => df.withColumn("next_day", date_add(col("dates"), lit(1)))
  }

  // Fails with Spark 3.2.0 and later - see https://github.com/NVIDIA/spark-rapids/issues/4940
  testSparkResultsAreEqual(
    "Test CSV parse ints as timestamps ansiEnabled=false",
    intsAsTimestampsFromCsv,
    assumeCondition = _ => (!VersionUtils.isSpark320OrLater,
      "https://github.com/NVIDIA/spark-rapids/issues/4940"),
    conf=new SparkConf().set(SQLConf.ANSI_ENABLED.key, "false")) {
    df => df
  }

  // Fails with Spark 3.2.0 and later - see https://github.com/NVIDIA/spark-rapids/issues/4940
  testSparkResultsAreEqual(
    "Test CSV parse ints as timestamps ansiEnabled=true",
    intsAsTimestampsFromCsv,
    assumeCondition = _ => (!VersionUtils.isSpark320OrLater,
      "https://github.com/NVIDIA/spark-rapids/issues/4940"),
    conf=new SparkConf().set(SQLConf.ANSI_ENABLED.key, "true")) {
    df => df
  }

  private def intsAsTimestampsFromCsv = {
    fromCsvDf("ints.csv", StructType(Array(
      StructField("ints_1", TimestampType),
      StructField("ints_2", TimestampType),
      StructField("ints_3", TimestampType),
      StructField("ints_4", TimestampType)
    )))(_)
  }


}
