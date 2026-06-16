/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import java.util.regex.PatternSyntaxException

import scala.collection.mutable.ListBuffer

import org.scalatest.funsuite.AnyFunSuite

class RegularExpressionParserSuite extends AnyFunSuite {

  test("detect regexp strings") {
    // Based on https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
    val strings: Seq[String] = Seq("\\", "\u0000", "\\x00", "\\.",
      "\f", "\\a", "\\e", "\\cx", "[abc]", "^", "[a-z&&[def]]", ".", "*", "\\d", "\\D",
      "\\h", "\\H", "\\s", "\\S", "\\v", "\\V", "\\w", "\\w", "\\p", "$", "\\b", "\\B",
      "\\A", "\\G", "\\Z", "\\z", "\\R", "?", "|", "(abc)", "a{1,}", "\\k", "\\Q", "\\E")
    for (string <- strings) {
      assert(RegexParser.isRegExpString(string))
    }
  }

  test("detect non-regexp strings") {
    val strings = Seq("A", ",", "\t", ":", "")
    for (string <- strings) {
      assert(!RegexParser.isRegExpString(string))
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
        RegexChar('{'), RegexChar('1'),RegexEscaped('}'))))
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
          RegexGroup(capture = true, RegexSequence(ListBuffer(RegexChar('a'))), None),
          RegexGroup(capture = true, RegexSequence(ListBuffer(RegexChar('b'))), None))))
  }

  test("character class") {
    assert(parse("[a-z+A-Z]") ===
      RegexSequence(ListBuffer(
        RegexCharacterClass(negated = false,
          ListBuffer(
            RegexCharacterRange(RegexChar('a'), RegexChar('z')),
            RegexChar('+'),
            RegexCharacterRange(RegexChar('A'), RegexChar('Z')))))))
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
          ListBuffer(RegexChar('a'))), RegexEscaped(']'))))
  }

  test("escaped brackets") {
    assert(parse("\\[([A-Z]+)\\]") ===
      RegexSequence(ListBuffer(
        RegexEscaped('['),
        RegexGroup(capture = true,
          RegexSequence(ListBuffer(
            RegexRepetition(
              RegexCharacterClass(negated = false, ListBuffer(
                RegexCharacterRange(RegexChar('A'), RegexChar('Z')))),
              SimpleQuantifier('+')
            )
          )), None
        ),
        RegexEscaped(']')
      ))
    )
  }

  test("unclosed character class") {
    val e = intercept[PatternSyntaxException] {
      parse("[ab")
    }
    assert(e.getMessage.contains("Unclosed"))
  }

  test("hex digit") {
    assert(parse(raw"\xFF") ===
      RegexSequence(ListBuffer(RegexHexDigit("FF"))))
  }

  test("variable length hex digit") {
    assert(parse(raw"\x{ABC}") ===
      RegexSequence(ListBuffer(RegexHexDigit("ABC"))))
  }

  test("octal digit") {
    val digits = Seq("00", "01", "076", "077", "0123", "0177", "0377")
    for (digit <- digits) {
      assert(parse(raw"\$digit") ===
        RegexSequence(ListBuffer(RegexOctalChar(digit))))
    }

    // parsing of the octal digit should terminate after parsing "\01"
    assert(parse(raw"\018") ===
      RegexSequence(ListBuffer(RegexOctalChar("01"), RegexChar('8'))))

    // parsing of the octal digit should terminate after parsing "\47"
    assert(parse(raw"\0477") ===
      RegexSequence(ListBuffer(RegexOctalChar("047"), RegexChar('7'))))
  }

  test("repetition with group containing simple repetition") {
    assert(parse("(3?)+") ===
      RegexSequence(ListBuffer(RegexRepetition(RegexGroup(capture = true, 
          RegexSequence(ListBuffer(RegexRepetition(RegexChar('3'), 
          SimpleQuantifier('?')))), None),SimpleQuantifier('+')))))
  }

  test("repetition with group containing escape character") {
    assert(parse(raw"(\A)+") ===
      RegexSequence(ListBuffer(RegexRepetition(RegexGroup(capture = true,
          RegexSequence(ListBuffer(RegexEscaped('A'))), None),
          SimpleQuantifier('+'))))
    )
  }

  test("group containing choice with repetition") {
    assert(parse("(\t+|a)") == RegexSequence(ListBuffer(
      RegexGroup(capture = true, RegexChoice(RegexSequence(ListBuffer(
        RegexRepetition(RegexChar('\t'),SimpleQuantifier('+')))),
        RegexSequence(ListBuffer(RegexChar('a')))), None))))
  }

  test("multiple choice (2)") {
    assert(parse("aa|bb") == RegexChoice(RegexSequence(ListBuffer(RegexChar('a'), RegexChar('a'))),
          RegexSequence(ListBuffer(RegexChar('b'), RegexChar('b')))
      ))
  }

  test("multiple choice (3)") {
    assert(parse("aa|bb|cc") ==
          RegexChoice(RegexSequence(ListBuffer(RegexChar('a'), RegexChar('a'))),
          RegexChoice(RegexSequence(ListBuffer(RegexChar('b'), RegexChar('b'))),
          RegexSequence(ListBuffer(RegexChar('c'), RegexChar('c')))
      )))
  }

  test("group containing quantifier") {
    val e = intercept[RegexUnsupportedException] {
      parse("(?)")
    }
    assert(e.getMessage.startsWith("Base expression cannot start with quantifier"))

    assert(parse("(?:a?)") === RegexSequence(ListBuffer(
      RegexGroup(capture = false, RegexSequence(ListBuffer(
        RegexRepetition(RegexChar('a'), SimpleQuantifier('?')))), None))))
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
                  RegexCharacterRange(RegexChar('0'), RegexChar('9')))), 
                SimpleQuantifier('+')))), None))),
              RegexChoice(RegexSequence(ListBuffer(
                RegexGroup(capture = true, RegexSequence(ListBuffer(
                  RegexRepetition(
                    RegexCharacterClass(negated = false, ListBuffer(
                      RegexCharacterRange(RegexChar('0'), RegexChar('9')))), 
                    SimpleQuantifier('*')), RegexEscaped('.'),
                RegexRepetition(
                    RegexCharacterClass(negated = false, ListBuffer(
                      RegexCharacterRange(RegexChar('0'), RegexChar('9')))),
                    SimpleQuantifier('+')))), None))), RegexSequence(ListBuffer(
                RegexGroup(capture = true, RegexSequence(ListBuffer(
                RegexRepetition(
                    RegexCharacterClass(negated = false, ListBuffer(
                      RegexCharacterRange(RegexChar('0'), RegexChar('9')))),
                    SimpleQuantifier('+')), RegexEscaped('.'),
                RegexRepetition(RegexCharacterClass(negated = false,
                    ListBuffer(RegexCharacterRange(RegexChar('0'), RegexChar('9')))),
                    SimpleQuantifier('*')))), None))))), None),
                  RegexRepetition(
              RegexGroup(capture = true, RegexSequence(ListBuffer(
                RegexCharacterClass(negated = false, ListBuffer(RegexChar('e'), RegexChar('E'))),
                  RegexRepetition(RegexCharacterClass(negated = false,
                    ListBuffer(RegexChar('+'), RegexEscaped('-'))),SimpleQuantifier('?')),
                  RegexRepetition(RegexCharacterClass(negated = false,
                  ListBuffer(RegexCharacterRange(RegexChar('0'), RegexChar('9')))),
                  SimpleQuantifier('+')))), None), SimpleQuantifier('?')),
            RegexRepetition(RegexCharacterClass(negated = false, ListBuffer(
              RegexChar('f'), RegexChar('F'), RegexChar('d'), RegexChar('D'))),
              SimpleQuantifier('?')))), None))),
          RegexChoice(RegexSequence(ListBuffer(
            RegexChar('I'), RegexChar('n'), RegexChar('f'))),
            RegexSequence(ListBuffer(
              RegexCharacterClass(negated = false,
                ListBuffer(RegexChar('n'), RegexChar('N'))),
              RegexCharacterClass(negated = false,
                ListBuffer(RegexChar('a'), RegexChar('A'))),
              RegexCharacterClass(negated = false,
                ListBuffer(RegexChar('n'), RegexChar('N'))))))), None),
    RegexChar('$'))))
  }
  
  test("issue-14742-subbug1: \\N in replacement is the literal character N, not a backref") {
    val repl = new RegexParser("\\1").parseReplacement(numCaptureGroups = 1)
    assert(repl.parts.toList === List(RegexChar('\\'), RegexChar('1')))
  }

  test("issue-14742-subbug1: \\a in replacement is the literal character a") {
    val repl = new RegexParser("\\a").parseReplacement(numCaptureGroups = 0)
    assert(repl.parts.toList === List(RegexChar('\\'), RegexChar('a')))
  }

  test("issue-14742-subbug2: trailing \\ in replacement throws") {
    val ex = intercept[RegexUnsupportedException] {
      new RegexParser("\\").parseReplacement(numCaptureGroups = 0)
    }
    assert(ex.getMessage.contains("character to be escaped is missing"))
  }

  test("issue-14742-subbug3: bare $X for non-digit X throws") {
    val ex = intercept[RegexUnsupportedException] {
      new RegexParser("$x").parseReplacement(numCaptureGroups = 0)
    }
    assert(ex.getMessage.contains("Illegal group reference"))
  }

  test("issue-14742-subbug3: trailing bare $ throws") {
    val ex = intercept[RegexUnsupportedException] {
      new RegexParser("$").parseReplacement(numCaptureGroups = 0)
    }
    assert(ex.getMessage.contains("Illegal group reference"))
  }

  test("issue-14742-subbug4: dollar-brace-digit-brace throws") {
    val ex = intercept[RegexUnsupportedException] {
      new RegexParser("$" + "{1}").parseReplacement(numCaptureGroups = 1)
    }
    assert(ex.getMessage.contains("Illegal group reference"))
    assert(ex.getMessage.contains("digit"))
  }

  test("issue-14742-subbug5: dollar-brace-name-brace for named group is not supported on GPU") {
    val ex = intercept[RegexUnsupportedException] {
      new RegexParser("$" + "{name}").parseReplacement(numCaptureGroups = 1)
    }
    assert(ex.getMessage.contains("named-group reference"))
  }

  test("issue-14742-subbug5: dollar-brace-name with missing closing brace throws") {
    val ex = intercept[RegexUnsupportedException] {
      new RegexParser("$" + "{name").parseReplacement(numCaptureGroups = 0)
    }
    assert(ex.getMessage.contains("Illegal group reference"))
  }

  test("issue-14742: dollar-brace with empty body throws") {
    val ex = intercept[RegexUnsupportedException] {
      new RegexParser("$" + "{}").parseReplacement(numCaptureGroups = 0)
    }
    assert(ex.getMessage.contains("Illegal group reference"))
  }

  test("issue-14742: numbered backref $0 still works") {
    val repl = new RegexParser("$0").parseReplacement(numCaptureGroups = 0)
    assert(repl.parts.toList === List(RegexBackref(0)))
  }

  test("issue-14742: numbered backref $1 still works") {
    val repl = new RegexParser("$1").parseReplacement(numCaptureGroups = 1)
    assert(repl.parts.toList === List(RegexBackref(1)))
  }

  test("issue-14742: numbered backref $12 still consumes max digits") {
    val repl = new RegexParser("$12").parseReplacement(numCaptureGroups = 12)
    assert(repl.parts.toList === List(RegexBackref(12)))
  }

  test("issue-14742: escaped metachar \\$ in replacement keeps the \\ pair") {
    val repl = new RegexParser("\\$").parseReplacement(numCaptureGroups = 0)
    assert(repl.parts.toList === List(RegexChar('\\'), RegexChar('$')))
  }

  test("issue-14742: escaped backslash \\\\ in replacement keeps the \\ pair") {
    val repl = new RegexParser("\\\\").parseReplacement(numCaptureGroups = 0)
    assert(repl.parts.toList === List(RegexChar('\\'), RegexChar('\\')))
  }

  test("issue-14742: non-ASCII Unicode digit after `$` triggers GPU fallback") {
    for (rep <- Seq("$٢", "$१", "$۱")) {
      val e = intercept[RegexUnsupportedException] {
        new RegexParser(rep).parseReplacement(numCaptureGroups = 4)
      }
      assert(e.getMessage.startsWith("Illegal group reference"),
        s"unexpected message for replacement '$rep': ${e.getMessage}")
    }
  }

  private def parse(pattern: String): RegexAST = {
    new RegexParser(pattern).parse()
  }

}
