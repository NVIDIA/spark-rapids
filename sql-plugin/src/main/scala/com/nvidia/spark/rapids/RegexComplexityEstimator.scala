/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

import org.apache.spark.sql.types.DataTypes

object RegexComplexityEstimator {
  private def requireNonNegative(name: String, value: Long): Unit = {
    require(value >= 0L, s"$name must be non-negative, got: $value")
  }

  // Saturating arithmetic for non-negative state and memory estimates.
  private def saturatedAdd(left: Long, right: Long): Long = {
    requireNonNegative("left", left)
    requireNonNegative("right", right)
    if (Long.MaxValue - left < right) {
      Long.MaxValue
    } else {
      left + right
    }
  }

  private def saturatedMultiply(left: Long, right: Long): Long = {
    requireNonNegative("left", left)
    requireNonNegative("right", right)
    if (left == 0L || right == 0L) {
      0L
    } else if (left > Long.MaxValue / right) {
      Long.MaxValue
    } else {
      left * right
    }
  }

  private def countStates(regex: RegexAST): Long = {
    regex match {
      case RegexSequence(parts) =>
        parts.foldLeft(0L) { (total, part) =>
          saturatedAdd(total, countStates(part))
        }
      case RegexGroup(true, term, _) =>
        saturatedAdd(1L, countStates(term))
      case RegexGroup(false, term, _) =>
        countStates(term)
      case RegexCharacterClass(_, _) =>
        1L
      case RegexChoice(left, right) =>
        saturatedAdd(countStates(left), countStates(right))
      case RegexRepetition(term, QuantifierFixedLength(length)) =>
        saturatedMultiply(length.toLong, countStates(term))
      case RegexRepetition(term, SimpleQuantifier(ch)) =>
        ch match {
          case '*' =>
            countStates(term)
          case '+' =>
            saturatedAdd(1L, countStates(term))
          case '?' =>
            saturatedAdd(1L, countStates(term))
        }
      case RegexRepetition(term, QuantifierVariableLength(minLength, maxLengthOption)) =>
        maxLengthOption match {
          case Some(maxLength) =>
            saturatedMultiply(maxLength.toLong, countStates(term))
          case None =>
            saturatedMultiply(minLength.max(1).toLong, countStates(term))
        }
      case RegexChar(_) | RegexEscaped(_) | RegexHexDigit(_) | RegexOctalChar(_) =>
        1L
      case _ =>
        0L
    }
  }

  private def estimateGpuMemory(numStates: Long, desiredBatchSizeBytes: Long): Long = {
    val numRows = GpuBatchUtils.estimateRowCount(
      desiredBatchSizeBytes, DataTypes.StringType.defaultSize, 1)

    // cuDF requests num_instructions * num_threads * 2 when allocating the memory on the device
    // (ignoring memory alignment). We are trying to reproduce that calculation here:
    saturatedMultiply(saturatedMultiply(numStates, numRows.toLong), 2L)
  }

  def isValid(conf: RapidsConf, regex: RegexAST): Boolean = {
    val numStates = countStates(regex)
    estimateGpuMemory(numStates, conf.gpuTargetBatchSizeBytes) <= conf.maxRegExpStateMemory
  }
}
