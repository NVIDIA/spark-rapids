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

class UnaryOperatorsSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual("Test acos doubles", doubleDf, maxFloatDiff=0.0001) {
    frame => frame.select(acos(col("doubles")), acos(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test acos floats", mixedFloatDf, maxFloatDiff=0.0001) {
    frame => frame.select(acos(col("floats")), acos(col("more_floats")))
  }

  testSparkResultsAreEqual("Test asin doubles", doubleDf, maxFloatDiff=0.0001) {
    frame => frame.select(asin(col("doubles")), asin(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test sinh floats", mixedFloatDf, maxFloatDiff=0.0001) {
    frame => frame.select(sinh(col("floats")), sinh(col("more_floats")))
  }

  testSparkResultsAreEqual("Test sinh doubles", doubleDf, maxFloatDiff=0.0001) {
    frame => frame.select(sinh(col("doubles")), sinh(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test cosh floats", mixedFloatDf, maxFloatDiff=0.0001) {
    frame => frame.select(cosh(col("floats")), cosh(col("more_floats")))
  }

  testSparkResultsAreEqual("Test cosh doubles", doubleDf, maxFloatDiff=0.0001) {
    frame => frame.select(cosh(col("doubles")), cosh(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test tanh floats", mixedFloatDf, maxFloatDiff=0.0001) {
    frame => frame.select(tanh(col("floats")), tanh(col("more_floats")))
  }

  testSparkResultsAreEqual("Test tanh doubles", doubleDf, maxFloatDiff=0.0001) {
    frame => frame.select(tanh(col("doubles")), tanh(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test asin floats", mixedFloatDf, maxFloatDiff=0.0001) {
    frame => frame.select(asin(col("floats")), asin(col("more_floats")))
  }

  testSparkResultsAreEqual("Test asinh floats", mixedFloatDf, maxFloatDiff=0.00001) {
    frame => frame.selectExpr("asinh(floats)")
  }

  testSparkResultsAreEqual("Test asinh doubles", doubleDf, maxFloatDiff=0.00001) {
    frame => frame.selectExpr("asinh(doubles)")
  }

  testSparkResultsAreEqual("Test acosh floats", mixedFloatDf, maxFloatDiff=0.00001) {
    frame => frame.selectExpr("acosh(floats)")
  }

  testSparkResultsAreEqual("Test acosh doubles", doubleDf, maxFloatDiff=0.00001) {
    frame => frame.selectExpr("acosh(doubles)")
  }

  testSparkResultsAreEqual("Test atanh floats", mixedFloatDf, maxFloatDiff=0.00001) {
    frame => frame.selectExpr("atanh(floats)")
  }

  testSparkResultsAreEqual("Test atanh doubles", doubleDf, maxFloatDiff=0.00001) {
    frame => frame.selectExpr("atanh(doubles)")
  }

  testSparkResultsAreEqual("Test atan doubles", doubleDf, maxFloatDiff=0.00001) {
    frame => frame.select(atan(col("doubles")), atan(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test atan floats", mixedFloatDf, maxFloatDiff=0.00001) {
    frame => frame.select(atan(col("floats")), atan(col("more_floats")))
  }

  testSparkResultsAreEqual("Test ceil longs", longsDf) {
    frame => frame.select(ceil(col("longs")), ceil(col("more_longs")))
  }

  testSparkResultsAreEqual("Test ceil doubles", doubleDf) {
    frame => frame.select(ceil(col("doubles")), ceil(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test ceil floats", mixedFloatDf) {
    frame => frame.select(col("floats"), ceil(col("floats")),
      col("more_floats"), ceil(col("more_floats")))
  }

  // TODO need a way to fill a column from a string
  //  testSparkResultsAreEqual("Test literal string values in select", mixedFloatDf) {
  //    frame => frame.select(col("floats"), lit("test"))
  //  }

  testSparkResultsAreEqual("Test cos doubles", doubleDf, maxFloatDiff=0.00001) {
    frame => frame.select(cos(col("doubles")), cos(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test cos floats", mixedFloatDf, maxFloatDiff=0.00001) {
    frame => frame.select(cos(col("floats")), cos(col("more_floats")))
  }

  testSparkResultsAreEqual("Test floor doubles", doubleDf) {
    frame => frame.select(floor(col("doubles")), floor(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test floor floats", mixedFloatDf) {
    frame => frame.select(floor(col("floats")), floor(col("more_floats")))
  }

  testSparkResultsAreEqual("Test sin doubles", doubleDf, maxFloatDiff=0.00001) {
    frame => frame.select(sin(col("doubles")), sin(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test sin floats", mixedFloatDf, maxFloatDiff=0.00001) {
    frame => frame.select(sin(col("floats")), sin(col("more_floats")))
  }

  testSparkResultsAreEqual("Test tan doubles", doubleDf, maxFloatDiff=0.00001) {
    frame => frame.select(tan(col("doubles")), tan(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test tan floats", mixedFloatDf, maxFloatDiff=0.00001) {
    frame => frame.select(tan(col("floats")), tan(col("more_floats")))
  }

  testSparkResultsAreEqual("Test year", datesDf) {
    frame => frame.select(year(col("dates")),
      year(col("more_dates")))
  }

  testSparkResultsAreEqual("Test month", datesDf) {
    frame => frame.select(month(col("dates")),
      month(col("more_dates")))
  }

  testSparkResultsAreEqual("Test day of month", datesDf) {
    frame => frame.select(dayofmonth(col("dates")),
      dayofmonth(col("more_dates")))
  }

  testSparkResultsAreEqual("Test cube root floats", mixedFloatDf, maxFloatDiff=0.0001) {
    frame => frame.select(cbrt(col("floats")), cbrt(col("more_floats")))
  }

  testSparkResultsAreEqual("Test cube root doubles", doubleDf, maxFloatDiff=0.0001) {
    frame => frame.select(cbrt(col("doubles")), cbrt(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test rint", doubleDf) {
    frame => frame.select(rint(col("doubles")), rint(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test EulerNumber", singularDoubleDf) {
    frame => frame.selectExpr("e()")
  }

  testSparkResultsAreEqual("Test pi", singularDoubleDf) {
    frame => frame.selectExpr("pi()")
  }

  testSparkResultsAreEqual("Test signum", mixedDf) {
    frame => frame.select(signum(col("ints")),
      signum(col("longs")),
      signum(col("doubles")))
  }

  testSparkResultsAreEqual("Test cot", floatWithNansDf, maxFloatDiff=0.0001) {
    frame => frame.selectExpr("cot(floats)")
  }

  testSparkResultsAreEqual(
      "Test ToDegrees doubles",
      mixedSingleColumnDoubleDf,
      maxFloatDiff=0.0001) {
    frame => frame.select(degrees(col("doubles")))
  }

  testSparkResultsAreEqual(
      "Test ToDegrees floats",
      mixedSingleColumnFloatDf,
      maxFloatDiff=0.0001) {
    frame => frame.select(degrees(col("floats")))
  }

  testSparkResultsAreEqual(
      "Test ToRadians doubles",
      mixedSingleColumnDoubleDf,
      maxFloatDiff=0.0001) {
    frame => frame.select(radians(col("doubles")))
  }

  testSparkResultsAreEqual(
      "Test ToRadians floats",
      mixedSingleColumnFloatDf,
      maxFloatDiff=0.0001) {
    frame => frame.select(radians(col("floats")))
  }
}
