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
package ai.rapids.spark

class StringFallbackSuite extends SparkQueryCompareTestSuite {
  testGpuFallback(
    "String regexp_replace replace str columnar fall back",
    "RegExpReplace",
    nullableStringsFromCsv,
    execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => frame.selectExpr("regexp_replace(strings,'a',strings)")
  }

  testGpuFallback("String regexp_replace null cpu fall back",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => frame.selectExpr("regexp_replace(strings,null,'D')")
  }

  testGpuFallback("String regexp_replace input empty cpu fall back",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => frame.selectExpr("regexp_replace(strings,'','D')")
  }

  testGpuFallback("String regexp_replace regex 1 cpu fall back",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => frame.selectExpr("regexp_replace(strings,'.*','D')")
  }

  testGpuFallback("String regexp_replace regex 2 cpu fall back",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => frame.selectExpr("regexp_replace(strings,'[a-z]+','D')")
  }

  testGpuFallback("String regexp_replace regex 3 cpu fall back",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => frame.selectExpr("regexp_replace(strings,'foo$','D')")
  }

  testGpuFallback("String regexp_replace regex 4 cpu fall back",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => frame.selectExpr("regexp_replace(strings,'^foo','D')")
  }

  testGpuFallback("String regexp_replace regex 5 cpu fall back",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => frame.selectExpr("regexp_replace(strings,'(foo)','D')")
  }

  testGpuFallback("String regexp_replace regex 6 cpu fall back",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal")) {
    frame => frame.selectExpr("regexp_replace(strings,'\\(foo\\)','D')")
  }
}
