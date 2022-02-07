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

import java.util.regex.Pattern

import scala.collection.mutable.{HashSet, ListBuffer}
import scala.util.{Random, Try}

import ai.rapids.cudf.{ColumnVector, CudfException}
import org.scalatest.FunSuite

import org.apache.spark.sql.rapids.GpuRegExpUtils
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
      "w$|b",
      "\n[^\r\n]x*|^3x",
      "]*\\wWW$|zb"
    )
    // data is not relevant because we are checking for compilation errors
    val inputs = Seq("a")
    for (pattern <- cudfInvalidPatterns) {
      // check that this is valid in Java
      Pattern.compile(pattern)
      Seq(RegexFindMode, RegexReplaceMode).foreach { mode =>
        try {
          if (mode == RegexReplaceMode) {
            gpuReplace(pattern, inputs)
          } else {
            gpuContains(pattern, inputs)
          }
          fail(s"cuDF unexpectedly compiled expression: $pattern")
        } catch {
          case e: CudfException =>
            // expected, now make sure that the transpiler can detect this
            try {
              transpile(pattern, mode)
              fail(
                s"transpiler failed to detect invalid cuDF pattern (mode=$mode): $pattern", e)
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
      assertUnsupported(pattern, RegexFindMode, "nothing to repeat")
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
      assertUnsupported(pattern, RegexFindMode, "nothing to repeat")
    )
  }

  test("cuDF does not support empty sequence") {
    val patterns = Seq("", "a|", "()")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode, "empty sequence not supported")
    )
  }

  test("cuDF does not support quantifier syntax when not quantifying anything") {
    // note that we could choose to transpile and escape the '{' and '}' characters
    val patterns = Seq("{1,2}", "{1,}", "{1}", "{2,1}")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode, "nothing to repeat")
    )
  }

  test("cuDF does not support OR at BOL / EOL") {
    val patterns = Seq("$|a", "^|a")
    patterns.foreach(pattern => {
      assertUnsupported(pattern, RegexFindMode,
        "sequences that only contain '^' or '$' are not supported")
    })
  }

  test("cuDF does not support null in pattern") {
    val patterns = Seq("\u0000", "a\u0000b", "a(\u0000)b", "a[a-b][\u0000]")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "cuDF does not support null characters in regular expressions"))
  }

  test("cuDF does not support hex digits consistently with Spark") {
    // see https://github.com/NVIDIA/spark-rapids/issues/4486
    val patterns = Seq(raw"\xA9", raw"\x00A9", raw"\x10FFFF")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "cuDF does not support hex digits consistently with Spark"))
  }

  test("cuDF does not support octal digits consistently with Spark") {
    // see https://github.com/NVIDIA/spark-rapids/issues/4288
    val patterns = Seq(raw"\07", raw"\077", raw"\0377")
    patterns.foreach(pattern =>
      assertUnsupported(pattern, RegexFindMode,
        "cuDF does not support octal digits consistently with Spark"))
  }
  
  test("string anchors - find") {
    val patterns = Seq("\\Atest", "test\\z")
    assertCpuGpuMatchesRegexpFind(patterns, Seq("", "test", "atest", "testa",
      "\ntest", "test\n", "\ntest\n"))
  }

  test("string anchor \\Z fall back to CPU") {
    for (mode <- Seq(RegexFindMode, RegexReplaceMode)) {
      assertUnsupported("\\Z", mode, "string anchor \\Z is not supported")
    }
  }

  test("string anchors - replace") {
    val patterns = Seq("\\Atest")
    assertCpuGpuMatchesRegexpReplace(patterns, Seq("", "test", "atest", "testa",
      "\ntest", "test\n", "\ntest\n", "\ntest\r\ntest\n"))
  }

  test("line anchor $ fall back to CPU") {
    for (mode <- Seq(RegexFindMode, RegexReplaceMode)) {
      assertUnsupported("a$b", mode, "line anchor $ is not supported")
    }
  }

  test("match literal $ - find") {
    assertCpuGpuMatchesRegexpFind(
      Seq("\\$", "\\$[0-9]"),
      Seq("", "$", "$9.99"))
  }

  test("match literal $ - replace") {
    assertCpuGpuMatchesRegexpReplace(
      Seq("\\$", "\\$[0-9]"),
      Seq("", "$", "$9.99"))
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
    val transpiler = new CudfRegexTranspiler(RegexFindMode)
    val transpiled = patterns.map(transpiler.transpile)
    assert(transpiled === expected)
  }

  test("transpile complex regex 2") {
    val TIMESTAMP_TRUNCATE_REGEX = "^([0-9]{4}-[0-9]{2}-[0-9]{2} " +
      "[0-9]{2}:[0-9]{2}:[0-9]{2})" +
      "(.[1-9]*(?:0)?[1-9]+)?(.0*[1-9]+)?(?:.0*)?\\z"

    // input and output should be identical except for `.` being replaced with `[^\r\n]` and
    // `\z` being replaced with `$`
    doTranspileTest(TIMESTAMP_TRUNCATE_REGEX,
      TIMESTAMP_TRUNCATE_REGEX
        .replaceAll("\\.", "[^\r\n]")
        .replaceAll("\\\\z", "\\$"))
  }

  test("transpile \\z") {
    doTranspileTest("abc\\z", "abc$")
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

  private val REGEXP_LIMITED_CHARS_COMMON = "|()[]{},.^$*+?abc123x\\ \t\r\nBsdwSDWzZ"

  private val REGEXP_LIMITED_CHARS_FIND = REGEXP_LIMITED_CHARS_COMMON

  private val REGEXP_LIMITED_CHARS_REPLACE = REGEXP_LIMITED_CHARS_COMMON

  test("compare CPU and GPU: find digits") {
    val patterns = Seq("\\d", "\\d+", "\\d*", "\\d?")
    val inputs = Seq("a", "1", "12", "a12z", "1az2")
    assertCpuGpuMatchesRegexpFind(patterns, inputs)
  }

  test("fall back to CPU for \\D") {
    // see https://github.com/NVIDIA/spark-rapids/issues/4475
    for (mode <- Seq(RegexFindMode, RegexReplaceMode)) {
      assertUnsupported("\\D", mode, "non-digit class \\D is not supported")
    }
  }

  test("fall back to CPU for \\W") {
    // see https://github.com/NVIDIA/spark-rapids/issues/4475
    for (mode <- Seq(RegexFindMode, RegexReplaceMode)) {
      assertUnsupported("\\W", mode, "non-word class \\W is not supported")
    }
  }

  test("compare CPU and GPU: replace digits") {
    // note that we do not test with quantifiers `?` or `*` due
    // to https://github.com/NVIDIA/spark-rapids/issues/4468
    val patterns = Seq("\\d", "\\d+")
    val inputs = Seq("a", "1", "12", "a12z", "1az2")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("compare CPU and GPU: regexp find fuzz test with limited chars") {
    // testing with this limited set of characters finds issues much
    // faster than using the full ASCII set
    // CR and LF has been excluded due to known issues
    doFuzzTest(Some(REGEXP_LIMITED_CHARS_FIND), RegexFindMode)
  }

  test("compare CPU and GPU: regexp replace simple regular expressions") {
    val inputs = Seq("a", "b", "c")
    val patterns = Seq("a|b")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("compare CPU and GPU: regexp replace line anchor supported use cases") {
    val inputs = Seq("a", "b", "c", "cat", "", "^", "$", "^a", "t$")
    val patterns = Seq("^a", "^a", "(^a|^t)", "^[ac]", "^^^a", "[\\^^]")
    assertCpuGpuMatchesRegexpReplace(patterns, inputs)
  }

  test("cuDF does not support some uses of line anchors in regexp_replace") {
    Seq("^$", "^", "$", "(^)($)", "(((^^^)))$", "^*", "$*", "^+", "$+", "^|$", "^^|$$").foreach(
        pattern =>
      assertUnsupported(pattern, RegexReplaceMode,
        "sequences that only contain '^' or '$' are not supported")
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
    doFuzzTest(Some(REGEXP_LIMITED_CHARS_REPLACE), RegexReplaceMode)
  }

  test("compare CPU and GPU: regexp find fuzz test printable ASCII chars plus CR, LF, and TAB") {
    // CR and LF has been excluded due to known issues
    doFuzzTest(Some((0x20 to 0x7F).map(_.toChar) + "\r\n\t"), RegexFindMode)
  }

  test("compare CPU and GPU: fuzz test ASCII chars") {
    // LF has been excluded due to known issues
    val chars = (0x00 to 0x7F)
      .map(_.toChar)
    doFuzzTest(Some(chars.mkString), RegexReplaceMode)
  }

  test("compare CPU and GPU: regexp find fuzz test all chars") {
    // this test cannot be enabled until we support CR and LF
    doFuzzTest(None, RegexFindMode)
  }

  private def doFuzzTest(validChars: Option[String], mode: RegexMode) {

    val r = new EnhancedRandom(new Random(seed = 0L),
      options = FuzzerOptions(validChars, maxStringLen = 12))

    val data = Range(0, 1000)
      .map(_ => r.nextString())

    // generate patterns that are valid on both CPU and GPU
    val patterns = HashSet[String]()
    while (patterns.size < 5000) {
      val pattern = r.nextString()
      if (!patterns.contains(pattern)) {
        if (Try(Pattern.compile(pattern)).isSuccess && Try(transpile(pattern, mode)).isSuccess) {
          patterns += pattern
        }
      }
    }

    if (mode == RegexReplaceMode) {
      assertCpuGpuMatchesRegexpReplace(patterns.toSeq, data)
    } else {
      assertCpuGpuMatchesRegexpFind(patterns.toSeq, data)
    }
  }

  test("AST fuzz test - regexp_find") {
    doAstFuzzTest(Some(REGEXP_LIMITED_CHARS_FIND), RegexFindMode)
  }

  test("AST fuzz test - regexp_replace") {
    doAstFuzzTest(Some(REGEXP_LIMITED_CHARS_REPLACE), RegexReplaceMode)
  }

  test("string split - limit < 0") {
    val patterns = Set("[^A-Z]+", "[0-9]+", ":", "o", "[:o]")
    val data = Seq("abc", "123", "1\n2\n3\n", "boo:and:foo")
    for (limit <- Seq(Integer.MIN_VALUE, -2, -1)) {
      doStringSplitTest(patterns, data, limit)
    }
  }

  test("string split - limit > 1") {
    val patterns = Set("[^A-Z]+", "[0-9]+", ":", "o", "[:o]")
    val data = Seq("abc", "123", "1\n2\n3\n", "boo:and:foo")
    for (limit <- Seq(2, 5, Integer.MAX_VALUE)) {
      doStringSplitTest(patterns, data, limit)
    }
  }

  test("string split fuzz") {
    val (data, patterns) = generateDataAndPatterns(Some(REGEXP_LIMITED_CHARS_REPLACE),
      RegexSplitMode)
    for (limit <- Seq(-2, -1, 2, 5)) {
      doStringSplitTest(patterns, data, limit)
    }
  }

  def doStringSplitTest(patterns: Set[String], data: Seq[String], limit: Int) {
    for (pattern <- patterns) {
      val cpu = cpuSplit(pattern, data, limit)
      val cudfPattern = new CudfRegexTranspiler(RegexSplitMode).transpile(pattern)
      val gpu = gpuSplit(cudfPattern, data, limit)
      assert(cpu.length == gpu.length)
      for (i <- cpu.indices) {
        val cpuArray = cpu(i)
        val gpuArray = gpu(i)
        if (!cpuArray.sameElements(gpuArray)) {
          fail(s"string_split pattern=${toReadableString(pattern)} " +
            s"data=${toReadableString(data(i))} limit=$limit " +
            s"\nCPU [${cpuArray.length}]: ${toReadableString(cpuArray.mkString(", "))} " +
            s"\nGPU [${gpuArray.length}]: ${toReadableString(gpuArray.mkString(", "))}")
        }
      }
    }
  }

  private def doAstFuzzTest(validChars: Option[String], mode: RegexMode) {
    val (data, patterns) = generateDataAndPatterns(validChars, mode)
    if (mode == RegexReplaceMode) {
      assertCpuGpuMatchesRegexpReplace(patterns.toSeq, data)
    } else {
      assertCpuGpuMatchesRegexpFind(patterns.toSeq, data)
    }
  }

  private def generateDataAndPatterns(validChars: Option[String], mode: RegexMode)
      : (Seq[String], Set[String]) = {
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
        if (Try(Pattern.compile(pattern)).isSuccess && Try(transpile(pattern, mode)).isSuccess) {
          patterns += pattern
        }
      }
    }
    (data, patterns.toSet)
  }

  private def assertCpuGpuMatchesRegexpFind(javaPatterns: Seq[String], input: Seq[String]) = {
    for (javaPattern <- javaPatterns) {
      val cpu = cpuContains(javaPattern, input)
      val cudfPattern = new CudfRegexTranspiler(RegexFindMode).transpile(javaPattern)
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
      val cudfPattern = new CudfRegexTranspiler(RegexReplaceMode).transpile(javaPattern)
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
            s"cpu=${toReadableString(cpu(i))}, " +
            s"gpu=${toReadableString(gpu(i))}")
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

  private val REPLACE_STRING = "\\_\\RE\\\\P\\L\\A\\C\\E\\_"

  /** cuDF replaceRe helper */
  private def gpuReplace(cudfPattern: String, input: Seq[String]): Array[String] = {
    val result = new Array[String](input.length)
    val replace = GpuRegExpUtils.unescapeReplaceString(REPLACE_STRING)
    withResource(ColumnVector.fromStrings(input: _*)) { cv =>
      withResource(GpuScalar.from(replace, DataTypes.StringType)) { replace =>
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

  private def cpuSplit(pattern: String, input: Seq[String], limit: Int): Seq[Array[String]] = {
    input.map(s => s.split(pattern, limit))
  }

  private def gpuSplit(pattern: String, input: Seq[String], limit: Int): Seq[Array[String]] = {
    val isRegex = RegexParser.isRegExpString(pattern)
    withResource(ColumnVector.fromStrings(input: _*)) { cv =>
      withResource(cv.stringSplitRecord(pattern, limit, isRegex)) { x =>
        withResource(x.copyToHost()) { hcv =>
          (0 until hcv.getRowCount.toInt).map(i => {
            val list = hcv.getList(i)
            list.toArray(new Array[String](list.size()))
          })
        }
      }
    }
  }

  private def doTranspileTest(pattern: String, expected: String) {
    val transpiled: String = transpile(pattern, RegexFindMode)
    assert(toReadableString(transpiled) === toReadableString(expected))
  }

  private def transpile(pattern: String, mode: RegexMode): String = {
    new CudfRegexTranspiler(mode).transpile(pattern)
  }

  private def assertUnsupported(pattern: String, mode: RegexMode, message: String): Unit = {
    val e = intercept[RegexUnsupportedException] {
      transpile(pattern, mode)
    }
    assert(e.getMessage.startsWith(message), pattern)
  }

  private def parse(pattern: String): RegexAST = new RegexParser(pattern).parse()

}

/**
 * Generates random regular expression patterns by building an AST and then
 * converting to a string. This results in better coverage than randomly
 * generating strings directly.
 *
 * See https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html for
 * Java regular expression syntax.
 */
class FuzzRegExp(suggestedChars: String, skipKnownIssues: Boolean = true) {
  private val maxDepth = 5
  private val rr = new Random(0)

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
        () => lineTerminator,
        () => escapedChar,
        () => char,
        () => hexDigit,
        () => octalDigit,
        () => characterClass,
        () => predefinedCharacterClass,
        () => group(depth),
        () => boundaryMatch,
        () => sequence(depth))
      val generators = if (skipKnownIssues) {
        baseGenerators
      } else {
        baseGenerators ++ Seq(
          () => repetition(depth), // https://github.com/NVIDIA/spark-rapids/issues/4487
          () => choice(depth)) // https://github.com/NVIDIA/spark-rapids/issues/4603
      }
      generators(rr.nextInt(generators.length))()
    }
  }

  private def nonNestedTerm: RegexAST = {
    val generators: Seq[() => RegexAST] = Seq(
      () => lineTerminator,
      () => escapedChar,
      () => char,
      () => hexDigit,
      () => octalDigit,
      () => charRange,
      () => boundaryMatch,
      () => predefinedCharacterClass
    )
    generators(rr.nextInt(generators.length))()
  }

  private def characterClassComponent = {
    val baseGenerators = Seq[() => RegexCharacterClassComponent](
        () => char,
        () => charRange)
    val generators = if (skipKnownIssues) {
      baseGenerators
    } else {
      baseGenerators ++ Seq(
        () => escapedChar, // https://github.com/NVIDIA/spark-rapids/issues/4505
        () => hexDigit, // https://github.com/NVIDIA/spark-rapids/issues/4486
        () => octalDigit) // https://github.com/NVIDIA/spark-rapids/issues/4409
    }
    generators(rr.nextInt(generators.length))()
  }

  private def charRange: RegexCharacterClassComponent = {
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
      () => RegexEscaped('z')
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
      () => RegexEscaped('W')
    )
    generators(rr.nextInt(generators.length))()
  }

  private def hexDigit: RegexHexDigit = {
    // \\xhh      The character with hexadecimal value 0xhh
    // \\uhhhh    The character with hexadecimal value 0xhhhh
    // \\x{h...h} The character with hexadecimal value 0xh...h
    //            (Character.MIN_CODE_POINT  <= 0xh...h <=  Character.MAX_CODE_POINT)
    val generators: Seq[() => String] = Seq(
      () => rr.nextInt(0xFF).toHexString,
      () => rr.nextInt(0xFFFF).toHexString,
      () => Character.MIN_CODE_POINT.toHexString,
      () => Character.MAX_CODE_POINT.toHexString
    )
    RegexHexDigit(generators(rr.nextInt(generators.length))())
  }

  private def octalDigit: RegexOctalChar = {
    // \\0n   The character with octal value 0n (0 <= n <= 7)
    // \\0nn  The character with octal value 0nn (0 <= n <= 7)
    // \\0mnn The character with octal value 0mnn (0 <= m <= 3, 0 <= n <= 7)
    val chars = "01234567"
    val generators: Seq[() => String] = Seq(
      () => Range(0,1).map(_ => chars(rr.nextInt(chars.length))).mkString,
      () => Range(0,2).map(_ => chars(rr.nextInt(chars.length))).mkString,
      () =>
        // this will generate some invalid octal numbers were the first digit > 3
        Range(0,3).map(_ => chars(rr.nextInt(chars.length))).mkString
    )
    RegexOctalChar("0" + generators(rr.nextInt(generators.length))())
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
