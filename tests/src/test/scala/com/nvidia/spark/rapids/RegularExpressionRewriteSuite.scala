/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import org.scalatest.funsuite.AnyFunSuite

class RegularExpressionRewriteSuite extends AnyFunSuite {

  private def verifyRewritePattern(patterns: Seq[String], excepted: Seq[RegexOptimizationType]): 
      Unit = {
    val results = patterns.map { pattern =>
      val ast = new RegexParser(pattern).parse()
      RegexRewrite.matchSimplePattern(ast.children())
    }
    assert(results == excepted)
  }

  test("regex rewrite startsWith") {
    import RegexOptimizationType._
    val patterns = Seq("^abc.*", raw"\A(abc).*", "^(abc).*def", raw"\Aabc.*(.*).*", "^abc.*(.*).*", 
        raw"^(abc)\Z)")
    val excepted = Seq(StartsWith("abc"), StartsWith("abc"), NoOptimization, StartsWith("abc"), 
        StartsWith("abc"), NoOptimization)
    verifyRewritePattern(patterns, excepted)
  }

  test("regex rewrite contains") {
    import RegexOptimizationType._
    val patterns = Seq(".*abc.*", ".*(abc).*", "^.*(abc).*$", "^.*(.*)(abc).*.*", 
        raw".*\w.*\Z", raw".*..*\Z")
    val excepted = Seq(Contains("abc"), Contains("abc"), NoOptimization, Contains("abc"), 
        NoOptimization, NoOptimization)
    verifyRewritePattern(patterns, excepted)
  }

  test("regex rewrite prefix range") {
    import RegexOptimizationType._
    val patterns = Seq(
      "(.*)abc[0-9]{1,3}(.*)",
      "(.*)abc[0-9a-z]{1,3}(.*)",
      "(.*)abc[0-9]{2}.*",
      "^abc[0-9]{1,3}",
      "火花急流[\u4e00-\u9fa5]{1}",
      "^[0-9]{6}",
      "^[0-9]{3,10}",
      "^.*[0-9]{6}",
      "^(.*)[0-9]{3,10}"
    )
    val excepted = Seq(
      PrefixRange("abc", 1, 48, 57),
      NoOptimization, // prefix followed by a multi-range not supported
      PrefixRange("abc", 2, 48, 57),
      NoOptimization, // starts with PrefixRange not supported
      PrefixRange("火花急流", 1, 19968, 40869),
      NoOptimization, // starts with PrefixRange not supported
      NoOptimization, // starts with PrefixRange not supported
      PrefixRange("", 6, 48, 57),
      PrefixRange("", 3, 48, 57)
    )
    verifyRewritePattern(patterns, excepted)
  }
}
