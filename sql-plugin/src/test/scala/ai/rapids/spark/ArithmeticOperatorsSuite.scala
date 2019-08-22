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

import org.apache.spark.sql.functions._

class ArithmeticOperatorsSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual("Test scalar addition", longsDf) {
    frame => frame.select(col("longs") + 100)
  }

  testSparkResultsAreEqual("Test addition", longsDf) {
    frame => frame.select(col("longs") + col("more_longs"))
  }

  testSparkResultsAreEqual("Test unary minus", longsDf) {
    frame => frame.select( -col("longs"))
  }

  testSparkResultsAreEqual("Test unary plus", longsDf) {
    frame => frame.selectExpr( "+longs")
  }

  testSparkResultsAreEqual("Test abs", longsDf) {
    frame => frame.select( abs(col("longs")))
  }

  testSparkResultsAreEqual("Test scalar subtraction", longsDf) {
    frame => frame.select(col("longs") - 100)
  }

  testSparkResultsAreEqual("Test scalar subtraction 2", longsDf) {
    frame => frame.selectExpr("50 - longs")
  }

  testSparkResultsAreEqual("Test subtraction", longsDf) {
    frame => frame.select(col("longs") - col("more_longs"))
  }

  testSparkResultsAreEqual("Test scalar multiply", longsDf) {
    frame => frame.select(col("longs") * 100)
  }

  testSparkResultsAreEqual("Test multiply", longsDf) {
    frame => frame.select(col("longs") * col("more_longs"))
  }

  INCOMPAT_testSparkResultsAreEqual("Test scalar divide", doubleDf) {
    frame => frame.select(col("doubles") / 100.0)
  }

  // Divide by 0 results in null for spark, but -Infinity for cudf...
  INCOMPAT_testSparkResultsAreEqual("Test divide", nonZeroDoubleDf) {
    frame => frame.select(col("doubles") / col("more_doubles"))
  }

  INCOMPAT_testSparkResultsAreEqual("Test scalar int divide", longsDf) {
    frame => frame.selectExpr("longs DIV 100")
  }

  // Divide by 0 results in null for spark, but -1 for cudf...
  INCOMPAT_testSparkResultsAreEqual("Test int divide", nonZeroLongsDf) {
    frame => frame.selectExpr("longs DIV more_longs")
  }

  INCOMPAT_testSparkResultsAreEqual("Test scalar remainder", longsDf) {
    frame => frame.selectExpr("longs % 100")
  }

  // Divide by 0 results in null for spark, but -1 for cudf...
  INCOMPAT_testSparkResultsAreEqual("Test remainder", nonZeroLongsDf) {
    frame => frame.selectExpr("longs % more_longs")
  }

  INCOMPAT_testSparkResultsAreEqual("Test scalar pow", longsDf, 0.00001) {
    frame => frame.select(pow(col("longs"), 3))
  }

  INCOMPAT_testSparkResultsAreEqual("Test pow", longsDf, 0.00001) {
    frame => frame.select(pow(col("longs"), col("more_longs")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test exp doubles", smallDoubleDf, 0.00001) {
    frame => frame.select(exp(col("doubles")), exp(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test exp floats", smallFloatDf, 0.00001) {
    frame => frame.select(exp(col("floats")), exp(col("more_floats")))
  }

  testSparkResultsAreEqual("Test sqrt doubles", doubleDf) {
    frame => frame.select(sqrt(col("doubles")), sqrt(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test sqrt floats", floatDf) {
    frame => frame.select(sqrt(col("floats")), sqrt(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log doubles", nonZeroDoubleDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log(abs(col("doubles"))), log(abs(col("more_doubles"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log floats", nonZeroFloatDf, 0.00001) {
    // Use ABS to work around incompatibility when input is negative and we also need to skip 0
    frame => frame.select(log(abs(col("floats"))), log(abs(col("more_floats"))))
  }
}
