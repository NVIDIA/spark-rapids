/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

object RegexComplexityEstimator {
  private def countStates(regex: RegexAST): Int = {
    regex match {
      case RegexSequence(parts) =>
        parts.map(countStates).sum
      case RegexGroup(true, term) =>
        1 + countStates(term)
      case RegexGroup(false, term) =>
        countStates(term)
      case RegexCharacterClass(_, _) =>
        1
      case RegexChoice(left, right) =>
        countStates(left) + countStates(right)
      case RegexRepetition(term, QuantifierFixedLength(length)) =>
        length * countStates(term)
      case RegexRepetition(term, SimpleQuantifier(ch)) =>
        ch match {
          case '*' =>
            countStates(term)
          case '+' =>
            1 + countStates(term)
          case '?' =>
            1 + countStates(term)
        }
      case RegexRepetition(term, QuantifierVariableLength(minLength, maxLengthOption)) =>
        maxLengthOption match {
          case Some(maxLength) =>
            maxLength * countStates(term)
          case None =>
            minLength * countStates(term)
        }
      case RegexChar(_) | RegexEscaped(_) | RegexHexDigit(_) | RegexOctalChar(_) =>
        1
      case _ =>
        0
    }
  }

  private def validate(regex: RegexAST, numRows: Int, meta: ExprMeta[_]) = {
    val numStates  = countStates(regex)
    if (numStates * numRows > meta.conf.maxRegExpStateMemory) {
      meta.willNotWorkOnGpu(s"Estimated memory needed for pattern exceeds the maximum." +
        s"Set ${RapidsConf.REGEXP_MAX_STATE_MEMORY} to change it.")
    }
  }
}
