/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

class RegexComplexityEstimatorSuite extends AnyFunSuite {
  private val conf = new RapidsConf(Map.empty[String, String])

  private def isValid(pattern: String): Boolean = {
    val ast = new RegexParser(pattern).parse()
    RegexComplexityEstimator.isValid(conf, ast)
  }

  test("reject regex patterns whose state memory estimate overflows Int arithmetic") {
    Seq(
      "a{65536}{65536}",
      "a{1073741824}",
      "(.){65536}{32768}",
      "a{2147483647}{2147483647}{2147483647}"
    ).foreach { pattern =>
      withClue(s"$pattern should exceed the default regex state memory limit") {
        assert(!isValid(pattern))
      }
    }
  }

  test("accept regex patterns below the default state memory limit") {
    assert(isValid("a{2}"))
  }
}
