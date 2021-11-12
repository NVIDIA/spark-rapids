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

import org.apache.spark.sql.types.DataTypes

class RegularExpressionTranspilerSuite extends FunSuite with Arm {

  test("transpiler detects invalid cuDF patterns") {
    // The purpose of this test is to document some examples of valid Java regular expressions
    // that fail to compile in cuDF and to check that the transpiler detects these correctly.
    // Many (but not all) of the patterns here are odd edge cases found during testing with
    // random inputs.
    val cudfInvalidPatterns = Seq(
      "a*+",
      "\t+|a",
      "(\t+|a)Dc$1",
      "(?d)"
    )
    // data is not relevant because we are checking for compilation errors
    val inputs = Seq("a")
    for (pattern <- cudfInvalidPatterns) {
      // check that this is valid in Java
      Pattern.compile(pattern)
      Seq(true, false).foreach { replace =>
        try {
          if (replace) {
            gpuReplace(pattern, inputs)
          } else {
            gpuContains(pattern, inputs)
          }
          fail(s"cuDF unexpectedly compiled expression: $pattern")
        } catch {
          case e: CudfException =>
            // expected, now make sure that the transpiler can detect this
            try {
              transpile(pattern, replace)
              fail(
                s"transpiler failed to detect invalid cuDF pattern (replace=$replace): $pattern", e)
            } catch {
              case _: RegexUnsupportedException =>
                // expected
            }
        }
      }
    }
  }

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
    assertCpuGpuMatchesRegexpFind(Seq(pattern), Seq("2", "2\n", "2\r", "2\r\n"))
  }

  test("dot matches CR on GPU but not on CPU") {
    // see https://github.com/rapidsai/cudf/issues/9619
    val pattern = "1."
    assertCpuGpuMatchesRegexpFind(Seq(pattern), Seq("1\r2", "1\n2", "1\r\n2"))
  }

  ignore("known issue - octal digit") {
    val pattern = "a\\141|.$" // using hex works fine e.g. "a\\x61|.$"
    assertCpuGpuMatchesRegexpFind(Seq(pattern), Seq("] b["))
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
    val transpiler = new CudfRegexTranspiler(replace=false)
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

    // input and output should be identical except for `.` being replaced with `[^\r\n]`
    doTranspileTest(TIMESTAMP_TRUNCATE_REGEX,
      TIMESTAMP_TRUNCATE_REGEX.replaceAll("\\.", "[^\r\n]"))

  }

  test("compare CPU and GPU: character range including unescaped + and -") {
    val patterns = Seq("a[-]+", "a[a-b-]+", "a[-a-b]", "a[-+]", "a[+-]")
    val inputs = Seq("a+", "a-", "a", "a-+", "a[a-b-]")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  test("compare CPU and GPU: character range including escaped + and -") {
    val patterns = Seq(raw"a[\-\+]", raw"a[\+\-]", raw"a[a-b\-]")
    val inputs = Seq("a+", "a-", "a", "a-+", "a[a-b-]")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  test("compare CPU and GPU: hex") {
    val patterns = Seq(raw"\x61")
    val inputs = Seq("a", "b")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  test("compare CPU and GPU: octal") {
    val patterns = Seq("\\\\141")
    val inputs = Seq("a", "b")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  private val REGEXP_LIMITED_CHARS = "|()[]{},.^$*+?abc123x\\ \tBsdwSDW"

  test("compare CPU and GPU: regexp find fuzz test with limited chars") {
    // testing with this limited set of characters finds issues much
    // faster than using the full ASCII set
    // CR and LF has been excluded due to known issues
    doFuzzTest(Some(REGEXP_LIMITED_CHARS), replace = false)
  }

  test("compare CPU and GPU: regexp replace simple regular expressions") {
    val inputs = Seq("a", "b", "c")
    val patterns = Seq("a|b")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("compare CPU and GPU: regexp replace fuzz test with limited chars") {
    // testing with this limited set of characters finds issues much
    // faster than using the full ASCII set
    // LF has been excluded due to known issues
    doFuzzTest(Some(REGEXP_LIMITED_CHARS), replace = true)
  }

  test("compare CPU and GPU: regexp find fuzz test printable ASCII chars plus CR and TAB") {
    // CR and LF has been excluded due to known issues
    doFuzzTest(Some((0x20 to 0x7F).map(_.toChar) + "\r\t"), replace = false)
  }

  test("compare CPU and GPU: fuzz test ASCII chars") {
    // LF has been excluded due to known issues
    val chars = (0x00 to 0x7F)
      .map(_.toChar)
      .filterNot(_ == '\n')
    doFuzzTest(Some(chars.mkString), replace = true)
  }

  ignore("compare CPU and GPU: regexp find fuzz test all chars") {
    // this test cannot be enabled until we support CR and LF
    doFuzzTest(None, replace = false)
  }

  private def doFuzzTest(validChars: Option[String], replace: Boolean) {

    val r = new EnhancedRandom(new Random(seed = 0L),
      options = FuzzerOptions(validChars, maxStringLen = 12))

    val data = Range(0, 1000)
      // remove trailing newlines as workaround for https://github.com/rapidsai/cudf/issues/9620
      .map(_ => removeTrailingNewlines(r.nextString()))

    // generate patterns that are valid on both CPU and GPU
    val patterns = ListBuffer[String]()
    while (patterns.length < 5000) {
      val pattern = r.nextString()
      if (Try(Pattern.compile(pattern)).isSuccess && Try(transpile(pattern, replace)).isSuccess) {
        patterns += pattern
      }
    }

    if (replace) {
      assertCpuGpuMatchesRegexpReplace(patterns, data)
    } else {
      assertCpuGpuMatchesRegexpFind(patterns, data)
    }
  }

  private def removeTrailingNewlines(input: String): String = {
    var s = input
    while (s.endsWith("\r") || s.endsWith("\n")) {
      s = s.substring(0, s.length - 1)
    }
    s
  }

  private def assertCpuGpuMatchesRegexpFind(javaPatterns: Seq[String], input: Seq[String]) = {
    for (javaPattern <- javaPatterns) {
      val cpu = cpuContains(javaPattern, input)
      val cudfPattern = new CudfRegexTranspiler(replace = false).transpile(javaPattern)
      val gpu = try {
        gpuContains(cudfPattern, input)
      } catch {
        case e: CudfException =>
          fail(s"cuDF failed to compile pattern: $cudfPattern", e)
      }
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

  private def assertCpuGpuMatchesRegexpReplace(
      javaPatterns: Seq[String],
      input: Seq[String]) = {
    for (javaPattern <- javaPatterns) {
      val cpu = cpuReplace(javaPattern, input)
      val cudfPattern = new CudfRegexTranspiler(replace = true).transpile(javaPattern)
      val gpu = try {
        gpuReplace(cudfPattern, input)
      } catch {
        case e: CudfException =>
          fail(s"cuDF failed to compile pattern: $cudfPattern", e)
      }
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

  private val REPLACE_STRING = "_REPLACE_"

  /** cuDF replaceRe helper */
  private def gpuReplace(cudfPattern: String, input: Seq[String]): Array[String] = {
    val result = new Array[String](input.length)
    withResource(ColumnVector.fromStrings(input: _*)) { cv =>
      withResource(GpuScalar.from(REPLACE_STRING, DataTypes.StringType)) { replace =>
        withResource(cv.replaceRegex(cudfPattern, replace)) { c =>
          withResource(c.copyToHost()) { hv =>
            result.indices.foreach(i => result(i) = new String(hv.getUTF8(i)))
          }
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

  private def cpuReplace(pattern: String, input: Seq[String]): Array[String] = {
    val p = Pattern.compile(pattern)
    input.map(s => p.matcher(s).replaceAll(REPLACE_STRING)).toArray
  }

  private def doTranspileTest(pattern: String, expected: String) {
    val transpiled: String = transpile(pattern, replace = false)
    assert(transpiled === expected)
  }

  private def transpile(pattern: String, replace: Boolean): String = {
    new CudfRegexTranspiler(replace).transpile(pattern)
  }

  private def assertUnsupported(pattern: String, message: String): Unit = {
    val e = intercept[RegexUnsupportedException] {
      transpile(pattern, replace = false)
    }
    assert(e.getMessage.startsWith(message))
  }

  private def parse(pattern: String): RegexAST = new RegexParser(pattern).parse()

}
