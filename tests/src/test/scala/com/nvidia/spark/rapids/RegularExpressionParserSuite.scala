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

import org.apache.spark.sql.rapids.{QuantifierFixedLength, RegexAST, RegexChar, RegexParser, RegexRepetition, RegexSequence, SimpleQuantifier}

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

  private def parse(pattern: String): RegexAST = {
    new RegexParser(pattern).parse()
  }

}
