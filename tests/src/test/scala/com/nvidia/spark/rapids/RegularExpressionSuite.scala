/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.shims.SparkShimImpl

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

class RegularExpressionSuite extends SparkQueryCompareTestSuite {

  private val conf = new SparkConf()
    .set(RapidsConf.ENABLE_REGEXP.key, "true")

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
      val isValidTestForSparkVersion = SparkShimImpl.getSparkShimVersion match {
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

  testSparkResultsAreEqual("String regexp_replace regex 1",
    nullableStringsFromCsv, conf = conf) {
    frame => frame.selectExpr("regexp_replace(strings,'.*','D')")
  }

  testSparkResultsAreEqual("String regexp_replace regex 2",
    nullableStringsFromCsv, conf = conf) {
    frame => frame.selectExpr("regexp_replace(strings,'[a-z]+','D')")
  }

  testGpuFallback("String regexp_replace regex 3",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal"), conf = conf) {
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

  testSparkResultsAreEqual("String regexp_extract regex 1",
    extractStrings, conf = conf) {
    frame => frame.selectExpr("regexp_extract(strings, '^([a-z]*)([0-9]*)([a-z]*)$', 1)")
  }

  testSparkResultsAreEqual("String regexp_extract regex 2",
    extractStrings, conf = conf) {
    frame => frame.selectExpr("regexp_extract(strings, '^([a-z]*)([0-9]*)([a-z]*)$', 2)")
  }

  // note that regexp_extract with a literal string gets replaced with the literal result of
  // the regexp_extract call on CPU
  testSparkResultsAreEqual("String regexp_extract literal input",
    extractStrings, conf = conf) {
    frame => frame.selectExpr("regexp_extract('abc123def', '^([a-z]*)([0-9]*)([a-z]*)$', 2)")
  }

  private def extractStrings(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(String)](
      (""),
      (null),
      ("abc123def"),
      ("abc\r\n12\r3\ndef"),
      ("123abc456")
    ).toDF("strings")
  }

}
