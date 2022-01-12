/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import java.util.regex.{Matcher, Pattern}

import scala.collection.mutable.{HashSet, ListBuffer}
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
      "(?d)",
      "$|$[^\n]2]}|B",
      "a^|b",
      "w$|b"
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
      assertUnsupported(pattern, replace = false, "nothing to repeat")
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
      assertUnsupported(pattern, replace = false, "nothing to repeat")
    )
  }

  test("cuDF does not support empty sequence") {
    val patterns = Seq("", "a|", "()")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, replace = false, "empty sequence not supported")
    )
  }

  test("cuDF does not support quantifier syntax when not quantifying anything") {
    // note that we could choose to transpile and escape the '{' and '}' characters
    val patterns = Seq("{1,2}", "{1,}", "{1}", "{2,1}")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, replace = false, "nothing to repeat")
    )
  }

  test("cuDF does not support OR at BOL / EOL") {
    val patterns = Seq("$|a", "^|a")
    patterns.foreach(pattern => {
      assertUnsupported(pattern, replace = false,
        "nothing to repeat")
    })
  }

  test("cuDF does not support null in pattern") {
    val patterns = Seq("\u0000", "a\u0000b", "a(\u0000)b", "a[a-b][\u0000]")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, replace = false,
        "cuDF does not support null characters in regular expressions"))
  }

  test("cuDF does not support hex digits consistently with Spark") {
    // see https://github.com/NVIDIA/spark-rapids/issues/4486
    val patterns = Seq(raw"\xA9", raw"\x00A9", raw"\x10FFFF")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, replace = false,
        "cuDF does not support hex digits consistently with Spark"))
  }

  test("cuDF does not support octal digits consistently with Spark") {
    // see https://github.com/NVIDIA/spark-rapids/issues/4288
    val patterns = Seq(raw"\07", raw"\077", raw"\0377")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, replace = false,
        "cuDF does not support octal digits consistently with Spark"))
  }
  
  test("string anchors - find") {
    val patterns = Seq("\\Atest", "test\\z", "test\\Z")
    assertCpuGpuMatchesRegexpFind(patterns, Seq("", "test", "atest", "testa",
      "\ntest", "test\n", "\ntest\n"))
  }

  test("string anchors - replace") {
    val patterns = Seq("\\Atest")
    assertCpuGpuMatchesRegexpReplace(patterns, Seq("", "test", "atest", "testa",
      "\ntest", "test\n", "\ntest\n", "\ntest\r\ntest\n"))
  }

  test("end of line anchor with strings ending in valid newline") {
    val pattern = "2$"
    assertCpuGpuMatchesRegexpFind(Seq(pattern), Seq("2", "2\n", "2\r", "2\r\n"))
  }

  test("end of line anchor with strings ending in invalid newline") {
    val pattern = "2$"
    assertCpuGpuMatchesRegexpFind(Seq(pattern), Seq("2\n\r"))
  }

  test("dot matches CR on GPU but not on CPU") {
    // see https://github.com/rapidsai/cudf/issues/9619
    val pattern = "1."
    assertCpuGpuMatchesRegexpFind(Seq(pattern), Seq("1\r2", "1\n2", "1\r\n2"))
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
    val expected = Seq(raw"a[\-b]", raw"a[+\-]", raw"a[\-+]", raw"a[\-]", "a(?:[\r\n]|[^\\-])")
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

    // input and output should be identical except for '$' being replaced with '[\r]?[\n]?$'
    doTranspileTest(VALID_FLOAT_REGEX,
      VALID_FLOAT_REGEX.replaceAll("\\$",
        Matcher.quoteReplacement("[\r]?[\n]?$")))
  }

  test("transpile complex regex 2") {
    val TIMESTAMP_TRUNCATE_REGEX = "^([0-9]{4}-[0-9]{2}-[0-9]{2} " +
      "[0-9]{2}:[0-9]{2}:[0-9]{2})" +
      "(.[1-9]*(?:0)?[1-9]+)?(.0*[1-9]+)?(?:.0*)?$"

    // input and output should be identical except for `.` being replaced with `[^\r\n]`
    // and '$' being replaced with '[\r]?[\n]?$'
    doTranspileTest(TIMESTAMP_TRUNCATE_REGEX,
      TIMESTAMP_TRUNCATE_REGEX
        .replaceAll("\\.", "[^\r\n]")
        .replaceAll("\\$", Matcher.quoteReplacement("[\r]?[\n]?$")))
  }

  test("transpile \\z") {
    doTranspileTest("\\z", "$")
  }

  test("transpile \\Z") {
    doTranspileTest("\\Z", "(?:[\r\n]?$)")
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

  test("compare CPU and GPU: octal") {
    val patterns = Seq("\\\\141")
    val inputs = Seq("a", "b")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  private val REGEXP_LIMITED_CHARS_COMMON = "|()[]{},.^$*+?abc123x\\ \tBsdwSDW"

  // we currently only support \\z and \\Z in find mode
  // see https://github.com/NVIDIA/spark-rapids/issues/4425
  private val REGEXP_LIMITED_CHARS_FIND = REGEXP_LIMITED_CHARS_COMMON + "zZ"

  private val REGEXP_LIMITED_CHARS_REPLACE = REGEXP_LIMITED_CHARS_COMMON

  test("compare CPU and GPU: find digits") {
    val patterns = Seq("\\d", "\\d+", "\\d*", "\\d?",
      "\\D", "\\D+", "\\D*", "\\D?")
    val inputs = Seq("a", "1", "12", "a12z", "1az2")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  test("compare CPU and GPU: replace digits") {
    // note that we do not test with quantifiers `?` or `*` due
    // to https://github.com/NVIDIA/spark-rapids/issues/4468
    val patterns = Seq("\\d", "\\d+", "\\D", "\\D+")
    val inputs = Seq("a", "1", "12", "a12z", "1az2")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("compare CPU and GPU: regexp find fuzz test with limited chars") {
    // testing with this limited set of characters finds issues much
    // faster than using the full ASCII set
    // CR and LF has been excluded due to known issues
    doFuzzTest(Some(REGEXP_LIMITED_CHARS_FIND), replace = false)
  }

  test("compare CPU and GPU: regexp replace simple regular expressions") {
    val inputs = Seq("a", "b", "c")
    val patterns = Seq("a|b")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("compare CPU and GPU: regexp replace BOL / EOL supported use cases") {
    val inputs = Seq("a", "b", "c", "cat", "", "^", "$", "^a", "t$")
    val patterns = Seq("^a", "a$", "^a$", "(^a|t$)", "(^a)|(t$)", "^[ac]$", "^^^a$$$",
        "[\\^\\$]")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("cuDF does not support some uses of BOL/EOL in regexp_replace") {
    Seq("^$", "^", "$", "(^)($)", "(((^^^)))$", "^*", "$*", "^+", "$+").foreach(pattern =>
      assertUnsupported(pattern, replace = true,
        "sequences that only contain '^' or '$' are not supported")
    )
    Seq("^|$", "^^|$$").foreach(pattern =>
      assertUnsupported(pattern, replace = true,
        "nothing to repeat")
    )
  }

  test("compare CPU and GPU: regexp replace negated character class") {
    val inputs = Seq("a", "b", "a\nb", "a\r\nb\n\rc\rd")
    val patterns = Seq("[^z]", "[^\r]", "[^\n]", "[^\r]", "[^\r\n]",
      "[^a\n]", "[^b\r]", "[^bc\r\n]", "[^\\r\\n]")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("compare CPU and GPU: regexp replace fuzz test with limited chars") {
    // testing with this limited set of characters finds issues much
    // faster than using the full ASCII set
    // LF has been excluded due to known issues
    doFuzzTest(Some(REGEXP_LIMITED_CHARS_REPLACE), replace = true)
  }

  test("compare CPU and GPU: regexp find fuzz test printable ASCII chars plus CR and TAB") {
    // CR and LF has been excluded due to known issues
    doFuzzTest(Some((0x20 to 0x7F).map(_.toChar) + "\r\t"), replace = false)
  }

  test("compare CPU and GPU: fuzz test ASCII chars") {
    // LF has been excluded due to known issues
    val chars = (0x00 to 0x7F)
      .map(_.toChar)
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
      .map(_ => r.nextString())

    // generate patterns that are valid on both CPU and GPU
    val patterns = HashSet[String]()
    while (patterns.size < 5000) {
      val pattern = r.nextString()
      if (!patterns.contains(pattern)) {
        if (Try(Pattern.compile(pattern)).isSuccess && Try(transpile(pattern, replace)).isSuccess) {
          patterns += pattern
        }
      }
    }

    if (replace) {
      assertCpuGpuMatchesRegexpReplace(patterns.toSeq, data)
    } else {
      assertCpuGpuMatchesRegexpFind(patterns.toSeq, data)
    }
  }

  test("AST fuzz test - regexp_find") {
    doAstFuzzTest(Some(REGEXP_LIMITED_CHARS_FIND), replace = false)
  }

  test("AST fuzz test - regexp_replace") {
    doAstFuzzTest(Some(REGEXP_LIMITED_CHARS_REPLACE), replace = true)
  }

  private def doAstFuzzTest(validChars: Option[String], replace: Boolean) {

    val r = new EnhancedRandom(new Random(seed = 0L),
      FuzzerOptions(validChars, maxStringLen = 12))

    val fuzzer = new FuzzRegExp(REGEXP_LIMITED_CHARS_FIND)

    val data = Range(0, 1000)
      .map(_ => r.nextString())

    // generate patterns that are valid on both CPU and GPU
    val patterns = HashSet[String]()
    while (patterns.size < 5000) {
      val pattern = fuzzer.generate(0).toRegexString
      if (!patterns.contains(pattern)) {
        if (Try(Pattern.compile(pattern)).isSuccess && Try(transpile(pattern, replace)).isSuccess) {
          patterns += pattern
        }
      }
    }

    if (replace) {
      assertCpuGpuMatchesRegexpReplace(patterns.toSeq, data)
    } else {
      assertCpuGpuMatchesRegexpFind(patterns.toSeq, data)
    }
  }

  private def assertCpuGpuMatchesRegexpFind(javaPatterns: Seq[String], input: Seq[String]) = {
    for (javaPattern <- javaPatterns) {
      val cpu = cpuContains(javaPattern, input)
      val cudfPattern = new CudfRegexTranspiler(replace = false).transpile(javaPattern)
      val gpu = try {
        gpuContains(cudfPattern, input)
      } catch {
        case e: CudfException =>
          fail(s"cuDF failed to compile pattern: ${toReadableString(cudfPattern)}", e)
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
          fail(s"cuDF failed to compile pattern: ${toReadableString(cudfPattern)}", e)
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
    assert(toReadableString(transpiled) === toReadableString(expected))
  }

  private def transpile(pattern: String, replace: Boolean): String = {
    new CudfRegexTranspiler(replace).transpile(pattern)
  }

  private def assertUnsupported(pattern: String, replace: Boolean, message: String): Unit = {
    val e = intercept[RegexUnsupportedException] {
      transpile(pattern, replace)
    }
    assert(e.getMessage.startsWith(message), pattern)
  }

  private def parse(pattern: String): RegexAST = new RegexParser(pattern).parse()

}

/**
 * Generates random regular expression patterns.
 *
 * See https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html
 *
 * TODO still:
 *
 * - unicode escaped hex \\uhhhh
 * - newlines and special control characters  \t \n \r \f \a \e \cx
 * - nested character classes
 * - POSIX character classes
 * - Java Character classes
 * - Classes for Unicode scripts, blocks, categories and binary properties
 * - Back references
 * - Quotation
 * - Special constructs (named-capturing and non-capturing)
 */
class FuzzRegExp(suggestedChars: String, skipKnownIssues: Boolean = true) {
  val maxDepth = 5
  private val rr = new Random(0)
  val r = new EnhancedRandom(rr, FuzzerOptions())

  val chars = if (skipKnownIssues) {
    // skip '\\' and '-' due to https://github.com/NVIDIA/spark-rapids/issues/4505
    suggestedChars.filterNot(ch => "\\-".contains(ch))
  } else {
    suggestedChars
  }

  def generate(depth: Int): RegexAST = {
    if (depth == maxDepth) {
      // when we reach maximum depth we generate a non-nested type
      nonNestedTerm
    } else {
      val baseGenerators: Seq[() => RegexAST] = Seq(
        () => char,
        () => hexDigit,
        () => octalDigit,
        () => characterClass,
        () => choice(depth),
        () => group(depth),
        () => sequence(depth))
      val generators = if (skipKnownIssues) {
        baseGenerators
      } else {
        baseGenerators ++ Seq(
          () => escapedChar,
          () => lineTerminator,
          () => repetition(depth), // https://github.com/NVIDIA/spark-rapids/issues/4487
          () => boundaryMatch, // not implemented yet
          () => predefinedCharacterClass) // not implemented yet
      }
      generators(rr.nextInt(generators.length))()
    }
  }

  private def nonNestedTerm: RegexAST = {
    val baseGenerators: Seq[() => RegexAST] = Seq(
      () => char,
      () => hexDigit,
      () => octalDigit,
      () => charRange)
    //TODO link to GitHub issues
    val generators: Seq[() => RegexAST] = if (skipKnownIssues) {
      baseGenerators
    } else {
      baseGenerators ++ Seq(
        () => escapedChar,
        () => lineTerminator,
        () => boundaryMatch, // not implemented yet
        () => predefinedCharacterClass) // not implemented yet
    }
    generators(rr.nextInt(generators.length))()
  }

  private def characterClassComponent = {
    val generators: Seq[() => RegexCharacterClassComponent] = if (skipKnownIssues) {
      //TODO link to GitHub issues
      Seq(
        () => char,
        () => charRange)
    } else {
      Seq(() => escapedChar,
        () => char,
        () => hexDigit,
        () => octalDigit,
        () => charRange)
    }
    generators(rr.nextInt(generators.length))()
  }

  private def charRange: RegexCharacterClassComponent = {
    // TODO included specific tests for escaped chars
    // once https://github.com/NVIDIA/spark-rapids/issues/4505 is
    // implemented
    val baseGenerators = Seq[() => RegexCharacterClassComponent](
      () => RegexCharacterRange('a', 'z'),
      () => RegexCharacterRange('A', 'Z'),
      () => RegexCharacterRange('z', 'a'),
      () => RegexCharacterRange('Z', 'A'),
      () => RegexCharacterRange('0', '9'),
      () => RegexCharacterRange('9', '0')
    )
    val generators = if (skipKnownIssues) {
      baseGenerators
    } else {
      // we do not support escaped characters in character ranges yet
      // see https://github.com/NVIDIA/spark-rapids/issues/4505
      baseGenerators ++ Seq(() => RegexCharacterRange(char.ch, char.ch))
    }
    generators(rr.nextInt(generators.length))()
  }

  private def sequence(depth: Int) = {
    val b = new ListBuffer[RegexAST]()
    b.appendAll(Range(0, 3).map(_ => generate(depth + 1)))
    RegexSequence(b)
  }

  private def characterClass = {
    val characters = new ListBuffer[RegexCharacterClassComponent]()
    characters.appendAll(Range(0, 3).map(_ => characterClassComponent))
    RegexCharacterClass(negated = rr.nextBoolean(), characters = characters)
  }

  private def char: RegexChar = {
    RegexChar(chars(rr.nextInt(chars.length)))
  }

  /** Any escaped character */
  private def escapedChar: RegexEscaped = {
    RegexEscaped(char.ch)
  }

  private def lineTerminator: RegexAST = {
    val generators = Seq[() => RegexAST](
      () => RegexChar('\r'),
      () => RegexChar('\n'),
      () => RegexSequence(ListBuffer(RegexChar('\r'), RegexChar('\n'))),
      () => RegexChar('\u0085'),
      () => RegexChar('\u2028'),
      () => RegexChar('\u2029')
    )
    generators(rr.nextInt(generators.length))()
  }

  private def boundaryMatch: RegexAST = {
    val generators = Seq[() => RegexAST](
      () => RegexChar('^'),
      () => RegexChar('$'),
      () => RegexEscaped('b'),
      () => RegexEscaped('B'),
      () => RegexEscaped('A'),
      () => RegexEscaped('G'),
      () => RegexEscaped('Z'),
      () => RegexEscaped('z'),
    )
    generators(rr.nextInt(generators.length))()
  }

  private def predefinedCharacterClass: RegexAST = {
    val generators = Seq[() => RegexAST](
      () => RegexChar('.'),
      () => RegexEscaped('d'),
      () => RegexEscaped('D'),
      () => RegexEscaped('s'),
      () => RegexEscaped('S'),
      () => RegexEscaped('w'),
      () => RegexEscaped('W'),
    )
    generators(rr.nextInt(generators.length))()
  }

  private def hexDigit: RegexHexDigit = {
    //TODO randomize and cover all formats
    RegexHexDigit("61")
  }

  private def octalDigit: RegexOctalChar = {
    //TODO randomize and cover all formats
    RegexOctalChar("0101")
  }

  private def choice(depth: Int) = {
    RegexChoice(generate(depth + 1), generate(depth + 1))
  }

  private def group(depth: Int) = {
    RegexGroup(capture = rr.nextBoolean(), generate(depth + 1))
  }

  private def repetition(depth: Int) = {
    val generators = Seq(
      () =>
        // greedy quantifier
        RegexRepetition(generate(depth + 1), quantifier),
      () =>
        // reluctant quantifier
        RegexRepetition(RegexRepetition(generate(depth + 1), quantifier),
          SimpleQuantifier('?')),
      () =>
        // possessive quantifier
        RegexRepetition(RegexRepetition(generate(depth + 1), quantifier),
          SimpleQuantifier('+'))
    )
    generators(rr.nextInt(generators.length))()
  }

  private def quantifier: RegexQuantifier = {
    val generators = Seq[() => RegexQuantifier](
      () => SimpleQuantifier('+'),
      () => SimpleQuantifier('*'),
      () => SimpleQuantifier('?'),
      () => QuantifierFixedLength(rr.nextInt(3)),
      () => QuantifierVariableLength(rr.nextInt(3), None),
      () => {
        // this intentionally generates some invalid quantifiers where the maxLength
        // is less than the minLength, such as "{2,1}" which should be handled as a
        // literal string match on "{2,1}" rather than as a valid quantifier.
        QuantifierVariableLength(rr.nextInt(3), Some(rr.nextInt(3)))
      }
    )
    generators(rr.nextInt(generators.length))()
  }
}
