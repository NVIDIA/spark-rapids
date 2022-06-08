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

package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.rapids.TrampolineErrorUtils
import org.apache.spark.sql.types.DataType

object RapidsErrorUtils {
  def invalidArrayIndexError(index: Int, numElements: Int,
      isElementAtF: Boolean = false): ArrayIndexOutOfBoundsException = {
    if (isElementAtF) {
      TrampolineErrorUtils.invalidElementAtIndexError(index, numElements)
    } else {
      TrampolineErrorUtils.invalidArrayIndexError(index, numElements)
    }
  }

  def mapKeyNotExistError(
      key: String,
      keyType: DataType,
      origin: Origin): NoSuchElementException = {
    TrampolineErrorUtils.mapKeyNotExistError(key, keyType, origin)
  }

  def sqlArrayIndexNotStartAtOneError(): ArrayIndexOutOfBoundsException = {
    new ArrayIndexOutOfBoundsException("SQL array indices start at 1")
  }

  def divByZeroError(origin: Origin): ArithmeticException = {
    TrampolineErrorUtils.divByZeroError(origin)
  }

  def divOverflowError(origin: Origin): ArithmeticException = {
    TrampolineErrorUtils.divOverflowError(origin)
  }
}
