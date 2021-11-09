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

import scala.collection.mutable.ListBuffer

import org.scalatest.FunSuite

class RegularExpressionParserSuite extends FunSuite {

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
          RegexGroup(RegexSequence(ListBuffer(RegexChar('a')))),
          RegexGroup(RegexSequence(ListBuffer(RegexChar('b')))))))
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
      RegexSequence(ListBuffer(
        RegexChar('^'),
        RegexRepetition(
          RegexCharacterClass(negated = false,
            ListBuffer(RegexChar('+'), RegexEscaped('-'))), SimpleQuantifier('?')),
        RegexGroup(RegexSequence(ListBuffer(
          RegexGroup(RegexSequence(ListBuffer(
            RegexGroup(RegexSequence(ListBuffer(
              RegexGroup(RegexSequence(ListBuffer(
                RegexRepetition(
                  RegexCharacterClass(negated = false,
                    ListBuffer(RegexCharacterRange('0', '9'))),SimpleQuantifier('+'))))),
                RegexChar('|'),
              RegexGroup(RegexSequence(ListBuffer(
                RegexRepetition(
                  RegexCharacterClass(negated = false,
                    ListBuffer(RegexCharacterRange('0', '9'))),SimpleQuantifier('*')),
                RegexEscaped('.'),
                RegexRepetition(RegexCharacterClass(negated = false,
                  ListBuffer(RegexCharacterRange('0', '9'))),SimpleQuantifier('+'))))),
                RegexChar('|'),
                RegexGroup(RegexSequence(ListBuffer(RegexRepetition(
                  RegexCharacterClass(negated = false,
                    ListBuffer(RegexCharacterRange('0', '9'))),SimpleQuantifier('+')),
                  RegexEscaped('.'),
                  RegexRepetition(
                    RegexCharacterClass(negated = false,
                      ListBuffer(RegexCharacterRange('0', '9'))),SimpleQuantifier('*')))))))),
                  RegexRepetition(RegexGroup(RegexSequence(ListBuffer(
                    RegexCharacterClass(negated = false,
                      ListBuffer(RegexChar('e'),
                  RegexChar('E'))),
                  RegexRepetition(RegexCharacterClass(negated = false,
                    ListBuffer(RegexChar('+'), RegexEscaped('-'))),SimpleQuantifier('?')),
                  RegexRepetition(RegexCharacterClass(negated = false,
                    ListBuffer(RegexCharacterRange('0', '9'))),SimpleQuantifier('+'))))),
                SimpleQuantifier('?')),
            RegexRepetition(RegexCharacterClass(negated = false,
              ListBuffer(RegexChar('f'), RegexChar('F'),
                RegexChar('d'), RegexChar('D'))),SimpleQuantifier('?'))))),
            RegexChar('|'), RegexChar('I'), RegexChar('n'), RegexChar('f'), RegexChar('|'),
          RegexCharacterClass(negated = false, ListBuffer(RegexChar('n'), RegexChar('N'))),
          RegexCharacterClass(negated = false, ListBuffer(RegexChar('a'), RegexChar('A'))),
          RegexCharacterClass(negated = false, ListBuffer(RegexChar('n'), RegexChar('N')))))
      ),
    RegexChar('$'))))
  }
  
  /*
Expected :RegexSequence(ListBuffer(RegexChar(^), RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexChar(+), RegexEscaped(-))),SimpleQuantifier(?)), RegexGroup(RegexSequence(ListBuffer(RegexGroup(RegexSequence(ListBuffer(RegexGroup(RegexSequence(ListBuffer(RegexGroup(RegexSequence(ListBuffer(RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexCharacterRange( ,	))),SimpleQuantifier(+))))), RegexChar(|), RegexGroup(RegexSequence(ListBuffer(RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexCharacterRange( ,	))),SimpleQuantifier(*)), RegexEscaped(.), RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexCharacterRange( ,	))),SimpleQuantifier(+))))), RegexChar(|), RegexGroup(RegexSequence(ListBuffer(RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexCharacterRange( ,	))),SimpleQuantifier(+)), RegexEscaped(.), RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexCharacterRange( ,	))),SimpleQuantifier(*)))))))), RegexRepetition(RegexGroup(RegexSequence(ListBuffer(RegexCharacterClass(false,ListBuffer(RegexChar(e), RegexChar(E))), RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexChar(+), RegexEscaped(-))),SimpleQuantifier(?)), RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexCharacterRange( ,	))),SimpleQuantifier(+))))),SimpleQuantifier(?)), RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexChar(f), RegexChar(F), RegexChar(d), RegexChar(D))),SimpleQuantifier(?))))), RegexChar(|), RegexChar(I), RegexChar(n), RegexChar(f), RegexChar(|), RegexCharacterClass(false,ListBuffer(RegexChar(n), RegexChar(N))), RegexCharacterClass(false,ListBuffer(RegexChar(a), RegexChar(A))), RegexCharacterClass(false,ListBuffer(RegexChar(n), RegexChar(N)))))), RegexChar($)))
Actual   :RegexSequence(ListBuffer(RegexChar(^), RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexChar(+), RegexEscaped(-))),SimpleQuantifier(?)), RegexGroup(RegexSequence(ListBuffer(RegexGroup(RegexSequence(ListBuffer(RegexGroup(RegexSequence(ListBuffer(RegexGroup(RegexSequence(ListBuffer(RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexCharacterRange(0,9))),SimpleQuantifier(+))))), RegexChar(|), RegexGroup(RegexSequence(ListBuffer(RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexCharacterRange(0,9))),SimpleQuantifier(*)), RegexEscaped(.), RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexCharacterRange(0,9))),SimpleQuantifier(+))))), RegexChar(|), RegexGroup(RegexSequence(ListBuffer(RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexCharacterRange(0,9))),SimpleQuantifier(+)), RegexEscaped(.), RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexCharacterRange(0,9))),SimpleQuantifier(*)))))))), RegexRepetition(RegexGroup(RegexSequence(ListBuffer(RegexCharacterClass(false,ListBuffer(RegexChar(e), RegexChar(E))), RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexChar(+), RegexEscaped(-))),SimpleQuantifier(?)), RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexCharacterRange(0,9))),SimpleQuantifier(+))))),SimpleQuantifier(?)), RegexRepetition(RegexCharacterClass(false,ListBuffer(RegexChar(f), RegexChar(F), RegexChar(d), RegexChar(D))),SimpleQuantifier(?))))), RegexChar(|), RegexChar(I), RegexChar(n), RegexChar(f), RegexChar(|), RegexCharacterClass(false,ListBuffer(RegexChar(n), RegexChar(N))), RegexCharacterClass(false,ListBuffer(RegexChar(a), RegexChar(A))), RegexCharacterClass(false,ListBuffer(RegexChar(n), RegexChar(N)))))), RegexChar($)))

   */

  private def parse(pattern: String): RegexAST = {
    new RegexParser(pattern).parse()
  }

}
