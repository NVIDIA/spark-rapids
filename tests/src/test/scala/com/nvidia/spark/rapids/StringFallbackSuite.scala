/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

class StringFallbackSuite extends SparkQueryCompareTestSuite {

  private val conf = new SparkConf().set("spark.rapids.sql.expression.RegExpReplace", "true")

  testGpuFallback(
    "String regexp_replace replace str columnar fall back",
    "RegExpReplace",
    nullableStringsFromCsv,
    execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal"), conf = conf) {
    frame => frame.selectExpr("regexp_replace(strings,'a',strings)")
  }

  testGpuFallback("String regexp_replace null cpu fall back",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal"), conf = conf) {
    frame => {
      // this test is only valid in Spark 3.0.x because the expression is NullIntolerant
      // since Spark 3.1.0 and gets replaced with a null literal instead
      val isValidTestForSparkVersion = ShimLoader.getSparkShims.getSparkShimVersion match {
        case SparkShimVersion(major, minor, _) => major == 3 && minor == 0
        case DatabricksShimVersion(major, minor, _, _) => major == 3 && minor == 0
        case ClouderaShimVersion(major, minor, _, _) => major == 3 && minor == 0
        case _ => true
      }
      assume(isValidTestForSparkVersion)
      frame.selectExpr("regexp_replace(strings,null,'D')")
    }
  }

  testGpuFallback("String regexp_replace input empty cpu fall back",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal"), conf = conf) {
    frame => frame.selectExpr("regexp_replace(strings,'','D')")
  }

  testGpuFallback("String regexp_replace regex 1 cpu fall back",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal"), conf = conf) {
    frame => frame.selectExpr("regexp_replace(strings,'.*','D')")
  }

  testSparkResultsAreEqual("String regexp_replace regex 2",
    nullableStringsFromCsv, conf = conf) {
    frame => frame.selectExpr("regexp_replace(strings,'[a-z]+','D')")
  }

  testSparkResultsAreEqual("String regexp_replace regex 3",
    nullableStringsFromCsv, conf = conf) {
    frame => frame.selectExpr("regexp_replace(strings,'foo$','D')")
  }

  testSparkResultsAreEqual("String regexp_replace regex 4",
    nullableStringsFromCsv, conf = conf) {
    frame => frame.selectExpr("regexp_replace(strings,'^foo','D')")
  }

  testSparkResultsAreEqual("String regexp_replace regex 5",
    nullableStringsFromCsv, conf = conf) {
    frame => frame.selectExpr("regexp_replace(strings,'(foo)','D')")
  }

  testSparkResultsAreEqual("String regexp_replace regex 6",
    nullableStringsFromCsv, conf = conf) {
    frame => frame.selectExpr("regexp_replace(strings,'\\(foo\\)','D')")
  }
}
