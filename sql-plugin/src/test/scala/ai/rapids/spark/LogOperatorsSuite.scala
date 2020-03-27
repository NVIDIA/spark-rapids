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

package ai.rapids.spark

import org.apache.spark.sql.functions._

class LogOperatorsSuite extends SparkQueryCompareTestSuite {

  INCOMPAT_testSparkResultsAreEqual("Test log doubles", nonZeroDoubleDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log(abs(col("doubles"))), log(abs(col("more_doubles"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log floats", nonZeroFloatDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative and we also need to skip 0
    frame => frame.select(log(abs(col("floats"))), log(abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log2 doubles", nonZeroDoubleDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log2(abs(col("doubles"))), log2(abs(col("more_doubles"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log1p floats with Nan", floatWithNansDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log1p(abs(col("floats"))), log1p(abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log1p floats with infinity", floatWithInfinityDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log1p(abs(col("floats"))), log1p(abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log1p floats", nonZeroFloatDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log1p(abs(col("floats"))), log1p(abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log1p longs", nonZeroLongsDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log1p(abs(col("longs"))), log1p(abs(col("more_longs"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log2 floats with Nan", floatWithNansDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log2(abs(col("floats"))), log2(abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log2 floats with infinity", floatWithInfinityDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log2(abs(col("floats"))), log2(abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log2 floats", nonZeroFloatDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log2(abs(col("floats"))), log2(abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log2 longs", nonZeroLongsDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log2(abs(col("longs"))), log2(abs(col("more_longs"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log10 doubles", nonZeroDoubleDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log10(abs(col("doubles"))), log10(abs(col("more_doubles"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log10 floats with Nan", floatWithNansDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log10(abs(col("floats"))), log10(abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log10 floats with infinity", floatWithInfinityDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log10(abs(col("floats"))), log10(abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log10 floats", nonZeroFloatDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log10(abs(col("floats"))), log10(abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log10 longs", nonZeroLongsDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log10(abs(col("longs"))), log10(abs(col("more_longs"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log variable base doubles", nonZeroDoubleDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log(1.23, abs(col("doubles"))), log(1.23, abs(col("more_doubles"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log variable base floats with Nan", floatWithNansDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log(1.23, abs(col("floats"))), log(1.23, abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log variable base floats with infinity", floatWithInfinityDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log(1.23, abs(col("floats"))), log(1.23, abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log variable base floats", nonZeroFloatDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log(1.23, abs(col("floats"))), log(1.23, abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log variable base longs", nonZeroLongsDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log(7, abs(col("longs"))), log(7, abs(col("more_longs"))))
  }
}
