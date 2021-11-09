/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import java.util.regex.Pattern

import scala.collection.mutable.ListBuffer
import scala.util.{Random, Try}

import ai.rapids.cudf.{ColumnVector, CudfException}
import org.scalatest.FunSuite

class RegularExpressionTranspilerSuite extends FunSuite with Arm {

  test("cuDF does not support choice with nothing to repeat") {
    val patterns = Seq("b+|^\t")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, "nothing to repeat")
    )
  }

  test("cuDF unsupported choice cases") {
    val input = Seq("cat", "dog")
    val patterns = Seq("c*|d*", "c*|dog", "[cat]{3}|dog")
    patterns.foreach(pattern => {
      val e = intercept[CudfException] {
        gpuContains(pattern, input)
      }
      assert(e.getMessage.contains("invalid regex pattern: nothing to repeat"))
    })
  }

  test("sanity check: choice edge case 2") {
    assertThrows[CudfException] {
      gpuContains("c+|d+", Seq("cat", "dog"))
    }
  }

  test("cuDF does not support possessive quantifier") {
    val patterns = Seq("a*+", "a|(a?|a*+)")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, "nothing to repeat")
    )
  }

  test("cuDF does not support empty sequence") {
    val patterns = Seq("", "a|", "()")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, "empty sequence not supported")
    )
  }

  test("cuDF does not support quantifier syntax when not quantifying anything") {
    // note that we could choose to transpile and escape the '{' and '}' characters
    val patterns = Seq("{1,2}", "{1,}", "{1}", "{2,1}")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, "nothing to repeat")
    )
  }

  test("cuDF does not support OR at BOL / EOL") {
    val patterns = Seq("$|a", "^|a")
    patterns.foreach(pattern => {
      assertUnsupported(pattern, "nothing to repeat")
    })
  }

  test("cuDF does not support null in pattern") {
    val patterns = Seq("\u0000", "a\u0000b", "a(\u0000)b", "a[a-b][\u0000]")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, "cuDF does not support null characters in regular expressions"))
  }

  test("nothing to repeat") {
    val patterns = Seq("$*", "^+")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, "nothing to repeat"))
  }

  ignore("known issue - multiline difference between CPU and GPU") {
    // see https://github.com/rapidsai/cudf/issues/9620
    val pattern = "2$"
    // this matches "2" but not "2\n" on the GPU
    assertCpuGpuContainsMatches(Seq(pattern), Seq("2", "2\n", "2\r", "\2\r\n"))
  }

  ignore("known issue - dot matches CR on GPU but not on CPU") {
    // see https://github.com/rapidsai/cudf/issues/9619
    val pattern = "1."
    // '.' matches '\r' on GPU but not on CPU
    assertCpuGpuContainsMatches(Seq(pattern), Seq("1\r2", "1\n2", "1\r\n2"))
  }

  ignore("known issue - octal digit") {
    val pattern = "a\\141|.$" // using hex works fine e.g. "a\\x61|.$"
    assertCpuGpuContainsMatches(Seq(pattern), Seq("] b["))
  }

  test("character class with ranges") {
    val patterns = Seq("[a-b]", "[a-zA-Z]")
    patterns.foreach(parse)
  }

  test("character class mixed") {
    val patterns = Seq("[a-b]", "[a+b]", "ab[cFef-g][^cat]")
    patterns.foreach(parse)
  }

  test("transpile character class unescaped range symbol") {
    val patterns = Seq("a[-b]", "a[+-]", "a[-+]", "a[-]", "a[^-]")
    val expected = Seq(raw"a[\-b]", raw"a[+\-]", raw"a[\-+]", raw"a[\-]", raw"a[^\-]")
    val transpiler = new CudfRegexTranspiler()
    val transpiled = patterns.map(transpiler.transpile)
    assert(transpiled === expected)
  }

  test("transpile complex regex 1") {
    val VALID_FLOAT_REGEX =
      "^" +                       // start of line
        "[+\\-]?" +               // optional + or - at start of string
        "(" +
        "(" +
        "(" +
        "([0-9]+)|" +             // digits, OR
        "([0-9]*\\.[0-9]+)|" +    // decimal with optional leading and mandatory trailing, OR
        "([0-9]+\\.[0-9]*)" +     // decimal with mandatory leading and optional trailing
        ")" +
        "([eE][+\\-]?[0-9]+)?" +  // exponent
        "[fFdD]?" +               // floating-point designator
        ")" +
        "|Inf" +                  // Infinity
        "|[nN][aA][nN]" +         // NaN
        ")" +
        "$"                       // end of line

    // input and output should be identical
    doTranspileTest(VALID_FLOAT_REGEX, VALID_FLOAT_REGEX)
  }

  test("transpile complex regex 2") {
    val TIMESTAMP_TRUNCATE_REGEX = "^([0-9]{4}-[0-9]{2}-[0-9]{2} " +
      "[0-9]{2}:[0-9]{2}:[0-9]{2})" +
      "(.[1-9]*(?:0)?[1-9]+)?(.0*[1-9]+)?(?:.0*)?$"

    // input and output should be identical
    doTranspileTest(TIMESTAMP_TRUNCATE_REGEX, TIMESTAMP_TRUNCATE_REGEX)

  }

  test("compare CPU and GPU: character range including unescaped + and -") {
    val patterns = Seq("a[-]+", "a[a-b-]+", "a[-a-b]", "a[-+]", "a[+-]")
    val inputs = Seq("a+", "a-", "a", "a-+", "a[a-b-]")
    assertCpuGpuContainsMatches(patterns, inputs)
  }

  test("compare CPU and GPU: character range including escaped + and -") {
    val patterns = Seq(raw"a[\-\+]", raw"a[\+\-]", raw"a[a-b\-]")
    val inputs = Seq("a+", "a-", "a", "a-+", "a[a-b-]")
    assertCpuGpuContainsMatches(patterns, inputs)
  }

  test("compare CPU and GPU: hex") {
    val patterns = Seq(raw"\x61")
    val inputs = Seq("a", "b")
    assertCpuGpuContainsMatches(patterns, inputs)
  }

  test("compare CPU and GPU: octal") {
    val patterns = Seq("\\\\141")
    val inputs = Seq("a", "b")
    assertCpuGpuContainsMatches(patterns, inputs)
  }

  test("compare CPU and GPU: fuzz test with limited chars") {
    // testing with this limited set of characters finds issues much
    // faster than using the full ASCII set
    // CR and LF has been excluded due to known issues
    doFuzzTest(Some("|()[]{},.^$*+?abc123x\\ \tB"))
  }

  test("compare CPU and GPU: fuzz test printable ASCII chars plus TAB") {
    // CR and LF has been excluded due to known issues
    doFuzzTest(Some((0x20 to 0x7F).map(_.toChar) + "\t"))
  }

  test("compare CPU and GPU: fuzz test ASCII chars") {
    // CR and LF has been excluded due to known issues
    val chars = (0x00 to 0x7F)
      .map(_.toChar)
      .filterNot(_ == '\n')
      .filterNot(_ == '\r')
    doFuzzTest(Some(chars.mkString))
  }

  ignore("compare CPU and GPU: fuzz test all chars") {
    // this test cannot be enabled until we support CR and LF
    doFuzzTest(None)
  }

  private def doFuzzTest(validChars: Option[String]) {

    val r = new EnhancedRandom(new Random(seed = 0L),
      options = FuzzerOptions(validChars, maxStringLen = 12))

    val data = Range(0, 1000).map(_ => r.nextString())

    // generate patterns that are valid on both CPU and GPU
    val patterns = ListBuffer[String]()
    while (patterns.length < 5000) {
      val pattern = r.nextString()
      if (Try(Pattern.compile(pattern)).isSuccess && Try(transpile(pattern)).isSuccess) {
        patterns += pattern
      }
    }

    assertCpuGpuContainsMatches(patterns, data)
  }

  private def assertCpuGpuContainsMatches(javaPatterns: Seq[String], input: Seq[String]) = {
    for (javaPattern <- javaPatterns) {
      val cpu = cpuContains(javaPattern, input)
      val cudfPattern = new CudfRegexTranspiler().transpile(javaPattern)
      val gpu = gpuContains(cudfPattern, input)
      for (i <- input.indices) {
        if (cpu(i) != gpu(i)) {
          fail(s"javaPattern=${toReadableString(javaPattern)}, " +
            s"cudfPattern=${toReadableString(cudfPattern)}, " +
            s"input='${toReadableString(input(i))}', " +
            s"cpu=${cpu(i)}, gpu=${gpu(i)}")
        }
      }
    }
  }

  /** cuDF containsRe helper */
  private def gpuContains(cudfPattern: String, input: Seq[String]): Array[Boolean] = {
    val result = new Array[Boolean](input.length)
    withResource(ColumnVector.fromStrings(input: _*)) { cv =>
      withResource(cv.containsRe(cudfPattern)) { c =>
        withResource(c.copyToHost()) { hv =>
          result.indices.foreach(i => result(i) = hv.getBoolean(i))
        }
      }
    }
    result
  }

  private def toReadableString(x: String): String = {
    x.map {
      case '\r' => "\\r"
      case '\n' => "\\n"
      case '\t' => "\\t"
      case other => other
    }.mkString
  }

  private def cpuContains(pattern: String, input: Seq[String]): Array[Boolean] = {
    val p = Pattern.compile(pattern)
    input.map(s => p.matcher(s).find(0)).toArray
  }

  private def doTranspileTest(pattern: String, expected: String) {
    val transpiled: String = transpile(pattern)
    assert(transpiled === expected)
  }

  private def transpile(pattern: String): String = {
    new CudfRegexTranspiler().transpile(pattern)
  }

  private def assertUnsupported(pattern: String, message: String): Unit = {
    val e = intercept[RegexUnsupportedException] {
      transpile(pattern)
    }
    assert(e.getMessage.startsWith(message))
  }

  private def parse(pattern: String): RegexAST = new RegexParser(pattern).parse()

}
