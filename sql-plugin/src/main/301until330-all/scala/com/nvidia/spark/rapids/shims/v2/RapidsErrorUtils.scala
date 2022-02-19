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

package com.nvidia.spark.rapids.shims.v2

import ai.rapids.cudf.{ColumnVector, ColumnView}

object RapidsErrorUtils {
  def throwArrayIndexOutOfBoundsException(index: Int, numElements: Int): ColumnVector = {
    throw new ArrayIndexOutOfBoundsException(s"index $index is beyond the max index allowed " +
        s"${numElements - 1}")
  }

  def throwInvalidElementAtIndexError(
      elementKey: String, isElementAtFunction: Boolean = false): ColumnVector = {
    // For now, the default argument is false. The caller sets the correct value accordingly.
    throw new NoSuchElementException(s"Key: ${elementKey} " +
      s"does not exist in any one of the rows in the map column")
  }

  def preprocessCastFloatToTimestamp(input: ColumnView): Unit = {
    // noop
  }
}
