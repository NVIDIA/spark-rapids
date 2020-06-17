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

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col

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
}
