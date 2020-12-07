/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

import java.io.File
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

class ParquetScanSuite extends SparkQueryCompareTestSuite {
  private val fileSplitsParquet = frameFromParquet("file-splits.parquet")

  testSparkResultsAreEqual("Test Parquet with row chunks", fileSplitsParquet,
    conf = new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "100")) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test Parquet with byte chunks", fileSplitsParquet,
    conf = new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "100")) {
    frame => frame.select(col("*"))
  }

  // Eventually it would be nice to move this to the integration tests,
  // but the file it depends on is used in other tests too.
  testSparkResultsAreEqual("Test Parquet timestamps and dates",
    frameFromParquet("timestamp-date-test.parquet")) {
    frame => frame.select(col("*"))
  }

  // Column schema of decimal-test.parquet is: [_c0: decimal(18, 0), _c1: decimal(10, 10),
  //  _c2: decimal(15, 12), _c3: int64, _c4: float]
  // LIMIT: Because we only support DECIMAL64, we only support reading decimal columns whose
  // physical storage type are INT64 in current.
  testSparkResultsAreEqual("Test Parquet decimal",
    frameFromParquet("decimal-test.parquet"),
    new SparkConf().set("spark.sql.sources.useV1SourceList", "")) {
    frame => frame.select(col("*"))
  }
}
