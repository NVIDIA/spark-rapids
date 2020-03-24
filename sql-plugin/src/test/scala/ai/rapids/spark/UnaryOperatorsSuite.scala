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

class UnaryOperatorsSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual("Test acos doubles", doubleDf) {
    frame => frame.select(acos(col("doubles")), acos(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test acos floats", floatDf) {
    frame => frame.select(acos(col("floats")), acos(col("more_floats")))
  }

  testSparkResultsAreEqual("Test asin doubles", doubleDf) {
    frame => frame.select(asin(col("doubles")), asin(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test sinh floats", floatDf, 0.0001) {
    frame => frame.select(sinh(col("floats")), sinh(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test sinh doubles", doubleDf, 0.0001) {
    frame => frame.select(sinh(col("doubles")), sinh(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test cosh floats", floatDf, 0.0001) {
    frame => frame.select(cosh(col("floats")), cosh(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test cosh doubles", doubleDf, 0.0001) {
    frame => frame.select(cosh(col("doubles")), cosh(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test tanh floats", floatDf, 0.0001) {
    frame => frame.select(tanh(col("floats")), tanh(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test tanh doubles", doubleDf, 0.0001) {
    frame => frame.select(tanh(col("doubles")), tanh(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test asin floats", floatDf) {
    frame => frame.select(asin(col("floats")), asin(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test asinh floats", floatDf, 0.00001) {
    frame => frame.selectExpr("asinh(floats)")
  }

  INCOMPAT_testSparkResultsAreEqual("Test asinh doubles", doubleDf, 0.00001) {
    frame => frame.selectExpr("asinh(doubles)")
  }

  INCOMPAT_testSparkResultsAreEqual("Test acosh floats", floatDf, 0.00001) {
    frame => frame.selectExpr("acosh(floats)")
  }

  INCOMPAT_testSparkResultsAreEqual("Test acosh doubles", doubleDf, 0.00001) {
    frame => frame.selectExpr("acosh(doubles)")
  }

  INCOMPAT_testSparkResultsAreEqual("Test atanh floats", floatDf, 0.00001) {
    frame => frame.selectExpr("atanh(floats)")
  }

  INCOMPAT_testSparkResultsAreEqual("Test atanh doubles", doubleDf, 0.00001) {
    frame => frame.selectExpr("atanh(doubles)")
  }

  INCOMPAT_testSparkResultsAreEqual("Test atan doubles", doubleDf, 0.00001) {
    frame => frame.select(atan(col("doubles")), atan(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test atan floats", floatDf, 0.00001) {
    frame => frame.select(atan(col("floats")), atan(col("more_floats")))
  }

  testSparkResultsAreEqual("Test ceil doubles", doubleDf) {
    frame => frame.select(ceil(col("doubles")), ceil(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test ceil floats", floatDf) {
    frame => frame.select(col("floats"), ceil(col("floats")),
      col("more_floats"), ceil(col("more_floats")))
  }

  // TODO need a way to fill a column from a string
  //  testSparkResultsAreEqual("Test literal string values in select", floatDf) {
  //    frame => frame.select(col("floats"), lit("test"))
  //  }

  INCOMPAT_testSparkResultsAreEqual("Test cos doubles", doubleDf, 0.00001) {
    frame => frame.select(cos(col("doubles")), cos(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test cos floats", floatDf, 0.00001) {
    frame => frame.select(cos(col("floats")), cos(col("more_floats")))
  }

  testSparkResultsAreEqual("Test floor doubles", doubleDf) {
    frame => frame.select(floor(col("doubles")), floor(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test floor floats", floatDf) {
    frame => frame.select(floor(col("floats")), floor(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test sin doubles", doubleDf, 0.00001) {
    frame => frame.select(sin(col("doubles")), sin(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test sin floats", floatDf, 0.00001) {
    frame => frame.select(sin(col("floats")), sin(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test tan doubles", doubleDf, 0.00001) {
    frame => frame.select(tan(col("doubles")), tan(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test tan floats", floatDf, 0.00001) {
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

  INCOMPAT_testSparkResultsAreEqual("Test cube root floats", floatDf, 0.0001) {
    frame => frame.select(cbrt(col("floats")), cbrt(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test cube root doubles", doubleDf, 0.0001) {
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

}
