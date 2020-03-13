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

package ai.rapids.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col

class OrcScanSuite extends SparkQueryCompareTestSuite {

  private val fileSplitsOrc = frameFromOrc("file-splits.orc")

  private val orcSplitsConf = new SparkConf().set("spark.sql.files.maxPartitionBytes", "30000")

  testSparkResultsAreEqual("Test ORC", frameFromOrc("test.snappy.orc")) {
    // dropping the timestamp column since timestamp expressions are not GPU supported yet
    frame => frame.select(col("*")).drop("timestamp")
  }

  testSparkResultsAreEqual("Test ORC file splitting", fileSplitsOrc, conf=orcSplitsConf) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test ORC count", fileSplitsOrc,
    conf=orcSplitsConf)(frameCount)

  testSparkResultsAreEqual("Test ORC predicate push-down", fileSplitsOrc) {
    frame => frame.select(col("loan_id"), col("orig_interest_rate"), col("zip"))
      .where(col("orig_interest_rate") > 10)
  }

  testSparkResultsAreEqual("Test ORC splits predicate push-down", fileSplitsOrc,
    conf=orcSplitsConf) {
    frame => frame.select(col("loan_id"), col("orig_interest_rate"), col("zip"))
      .where(col("orig_interest_rate") > 10)
  }

  testSparkResultsAreEqual("Test partitioned ORC", frameFromOrc("partitioned-orc")) {
    frame => frame.select(col("partKey"), col("ints_5"), col("ints_3"), col("ints_1"))
  }

  testSparkResultsAreEqual("Test ORC msec timestamps and dates",
      frameFromOrc("timestamp-date-test-msec.orc")) {
    frame => frame.select(col("*"))
  }

  // This test is commented out because cudf doesn't support loading anything more than
  // millsecond resolution timestamps yet.  See https://github.com/rapidsai/cudf/issues/2497
  // NOTE: When this is fixed, the timestamp msec test and data file should be deleted
  //       in preference of this test.
  //testSparkResultsAreEqual("Test ORC timestamps and dates",
  //  frameFromOrc("timestamp-date-test.orc")) {
  //  frame => frame.select(col("*"))
  //}
  testSparkResultsAreEqual("Test ORC chunks", fileSplitsOrc,
    new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "2048")) {
    frame => frame.select(col("loan_id"), col("orig_interest_rate"), col("zip"))
  }

  testSparkResultsAreEqual("Test ORC count chunked by rows", fileSplitsOrc,
    new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "2048"))(frameCount)

  testSparkResultsAreEqual("Test ORC count chunked by bytes", fileSplitsOrc,
    new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "100"))(frameCount)
}
