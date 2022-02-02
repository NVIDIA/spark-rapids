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

import scala.collection.mutable.ListBuffer

import org.scalatest.FunSuite

class RegularExpressionParserSuite extends FunSuite {

  test("detect regexp strings") {
    // Based on https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
    val strings: Seq[String] = Seq("\\", "\u0000", "\\x00",
      "\f", "\\a", "\\e", "\\cx", "[abc]", "^", "[a-z&&[def]]", ".", "*", "\\d", "\\D",
      "\\h", "\\H", "\\s", "\\S", "\\v", "\\V", "\\w", "\\w", "\\p", "$", "\\b", "\\B",
      "\\A", "\\G", "\\Z", "\\z", "\\R", "?", "|", "(abc)", "a{1,}", "\\k", "\\Q", "\\E")
    for (string <- strings) {
      assert(!RegexParser.isNonRegExpString(string))
    }
  }

  test("detect non-regexp strings") {
    val strings = Seq("\\.", "A", ",", "\t")
    for (string <- strings) {
      assert(RegexParser.isNonRegExpString(string))
    }
  }

  test("empty pattern") {
    assert(parse("") === RegexSequence(ListBuffer()))
  }

  test("simple quantifier") {
    assert(parse("a{1}") ===
      RegexSequence(ListBuffer(
      RegexRepetition(RegexChar('a'), QuantifierFixedLength(1)))))
  }

  test("not a quantifier") {
    assert(parse("{1}") ===
      RegexSequence(ListBuffer(
        RegexChar('{'), RegexChar('1'),RegexChar('}'))))
  }

  test("nested repetition") {
    assert(parse("a*+") ===
      RegexSequence(ListBuffer(
        RegexRepetition(
          RegexRepetition(RegexChar('a'), SimpleQuantifier('*')),
            SimpleQuantifier('+')))))
  }

  test("choice") {
    assert(parse("a|b") ===
      RegexChoice(RegexSequence(ListBuffer(RegexChar('a'))),
        RegexSequence(ListBuffer(RegexChar('b')))))
  }

  test("group") {
      assert(parse("(a)(b)") ===
        RegexSequence(ListBuffer(
          RegexGroup(capture = true, RegexSequence(ListBuffer(RegexChar('a')))),
          RegexGroup(capture = true, RegexSequence(ListBuffer(RegexChar('b')))))))
  }

  test("character class") {
    assert(parse("[a-z+A-Z]") ===
      RegexSequence(ListBuffer(
        RegexCharacterClass(negated = false,
          ListBuffer(
            RegexCharacterRange('a', 'z'),
            RegexChar('+'),
            RegexCharacterRange('A', 'Z'))))))
  }

  test("character class complex example") {
    assert(parse("[^]+d]+") === RegexSequence(ListBuffer(
      RegexRepetition(
        RegexCharacterClass(negated = true,
          ListBuffer(RegexChar(']'), RegexChar('+'), RegexChar('d'))),
        SimpleQuantifier('+')))))
  }

  test("character classes containing ']'") {
    // "[]a]" is a valid character class containing ']' and 'a'
    assert(parse("[]a]") ===
      RegexSequence(ListBuffer(
        RegexCharacterClass(negated = false,
          ListBuffer(RegexChar(']'), RegexChar('a'))))))
    // "[^]a]" is a valid negated character class containing ']' and 'a'
    assert(parse("[^]a]") ===
      RegexSequence(ListBuffer(
        RegexCharacterClass(negated = true,
          ListBuffer(RegexChar(']'), RegexChar('a'))))))
    // "[a]]" is a valid character class "[a]" followed by character ']'
    assert(parse("[a]]") ===
      RegexSequence(ListBuffer(
        RegexCharacterClass(negated = false,
          ListBuffer(RegexChar('a'))), RegexChar(']'))))
  }

  test("unclosed character class") {
    val e = intercept[RegexUnsupportedException] {
      parse("[ab")
    }
    assert(e.getMessage === "Unclosed character class near index 3")
  }

  test("hex digit") {
    assert(parse(raw"\xFF") ===
      RegexSequence(ListBuffer(RegexHexDigit("FF"))))
  }

  test("octal digit") {
    val digits = Seq("1", "76", "123", "377")
    for (digit <- digits) {
      assert(parse(raw"\$digit") ===
        RegexSequence(ListBuffer(RegexOctalChar(digit))))
    }

    // parsing of the octal digit should terminate after parsing "\1"
    assert(parse(raw"\18") ===
      RegexSequence(ListBuffer(RegexOctalChar("1"), RegexChar('8'))))

    // parsing of the octal digit should terminate after parsing "\47"
    assert(parse(raw"\477") ===
      RegexSequence(ListBuffer(RegexOctalChar("47"), RegexChar('7'))))
  }

  test("group containing choice with repetition") {
    assert(parse("(\t+|a)") == RegexSequence(ListBuffer(
      RegexGroup(capture = true, RegexChoice(RegexSequence(ListBuffer(
        RegexRepetition(RegexChar('\t'),SimpleQuantifier('+')))),
        RegexSequence(ListBuffer(RegexChar('a'))))))))
  }

  test("group containing quantifier") {
    val e = intercept[RegexUnsupportedException] {
      parse("(?)")
    }
    assert(e.getMessage.startsWith("base expression cannot start with quantifier"))

    assert(parse("(?:a?)") === RegexSequence(ListBuffer(
      RegexGroup(capture = false, RegexSequence(ListBuffer(
        RegexRepetition(RegexChar('a'), SimpleQuantifier('?'))))))))
  }

  test("complex expression") {
    val ast = parse(
      "^" +            // start of line
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
    )
    assert(ast ===
      RegexSequence(ListBuffer(RegexChar('^'),
        RegexRepetition(RegexCharacterClass(negated = false, ListBuffer(
          RegexChar('+'), RegexEscaped('-'))), SimpleQuantifier('?')),
        RegexGroup(capture = true, RegexChoice(RegexSequence(ListBuffer(
          RegexGroup(capture = true, RegexSequence(ListBuffer(
            RegexGroup(capture = true, RegexChoice(RegexSequence(ListBuffer(
              RegexGroup(capture = true, RegexSequence(ListBuffer(
                RegexRepetition(RegexCharacterClass(negated = false, ListBuffer(
                  RegexCharacterRange('0', '9'))), SimpleQuantifier('+'))))))),
              RegexChoice(RegexSequence(ListBuffer(
                RegexGroup(capture = true, RegexSequence(ListBuffer(
                  RegexRepetition(
                    RegexCharacterClass(negated = false, ListBuffer(
                      RegexCharacterRange('0', '9'))), SimpleQuantifier('*')), RegexEscaped('.'),
                RegexRepetition(
                    RegexCharacterClass(negated = false, ListBuffer(RegexCharacterRange('0', '9'))),
                    SimpleQuantifier('+'))))))), RegexSequence(ListBuffer(
                RegexGroup(capture = true, RegexSequence(ListBuffer(
                RegexRepetition(
                    RegexCharacterClass(negated = false, ListBuffer(RegexCharacterRange('0', '9'))),
                    SimpleQuantifier('+')), RegexEscaped('.'),
                RegexRepetition(RegexCharacterClass(negated = false,
                    ListBuffer(RegexCharacterRange('0', '9'))),
                    SimpleQuantifier('*')))))))))),
                  RegexRepetition(
              RegexGroup(capture = true, RegexSequence(ListBuffer(
                RegexCharacterClass(negated = false, ListBuffer(RegexChar('e'), RegexChar('E'))),
                  RegexRepetition(RegexCharacterClass(negated = false,
                    ListBuffer(RegexChar('+'), RegexEscaped('-'))),SimpleQuantifier('?')),
                  RegexRepetition(RegexCharacterClass(negated = false,
                  ListBuffer(RegexCharacterRange('0', '9'))),
                  SimpleQuantifier('+'))))), SimpleQuantifier('?')),
            RegexRepetition(RegexCharacterClass(negated = false, ListBuffer(
              RegexChar('f'), RegexChar('F'), RegexChar('d'), RegexChar('D'))),
              SimpleQuantifier('?'))))))),
          RegexChoice(RegexSequence(ListBuffer(
            RegexChar('I'), RegexChar('n'), RegexChar('f'))),
            RegexSequence(ListBuffer(
              RegexCharacterClass(negated = false,
                ListBuffer(RegexChar('n'), RegexChar('N'))),
              RegexCharacterClass(negated = false,
                ListBuffer(RegexChar('a'), RegexChar('A'))),
              RegexCharacterClass(negated = false,
                ListBuffer(RegexChar('n'), RegexChar('N')))))))),
    RegexChar('$'))))
  }
  
  private def parse(pattern: String): RegexAST = {
    new RegexParser(pattern).parse()
  }

}
