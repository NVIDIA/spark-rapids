/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

class CsvScanSuite extends SparkQueryCompareTestSuite {
  testExpectedException[IllegalArgumentException]("Test CSV projection including unsupported types",
      _.getMessage.startsWith("Part of the plan is not columnar"),
      mixedTypesFromCsvWithHeader) {
    frame => frame.select(col("c_string"), col("c_int"), col("c_timestamp"))
  }

  testSparkResultsAreEqual("Test CSV splits with chunks", floatCsvDf, conf= new SparkConf().set(
    RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "1")) {
    frame => frame.select(col("floats"))
  }

  testSparkResultsAreEqual(
      "Test CSV count chunked by rows",
      intsFromCsv,
      conf=new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "1")) {
    frameCount
  }

  testSparkResultsAreEqual(
      "Test CSV count chunked by bytes",
      intsFromCsv,
      conf=new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "0")) {
    frameCount
  }

  /**
   * Running with an inferred schema results in running things that are not columnar optimized.
   */
  ALLOW_NON_GPU_testSparkResultsAreEqual("Test CSV inferred schema",
    intsFromCsvInferredSchema, Seq("FileSourceScanExec", "FilterExec", "CollectLimitExec",
      "GreaterThan", "Length", "StringTrim", "LocalTableScanExec", "DeserializeToObjectExec",
      "Invoke", "AttributeReference", "Literal")) {
    frame => frame.select(col("*"))
  }
}
