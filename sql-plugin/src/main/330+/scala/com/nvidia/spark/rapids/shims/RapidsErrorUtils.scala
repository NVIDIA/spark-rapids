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

import org.apache.spark.sql.errors.QueryExecutionErrors

object RapidsErrorUtils {
  def invalidArrayIndexError(index: Int, numElements: Int,
      isElementAtF: Boolean = false): ArrayIndexOutOfBoundsException = {
    if (isElementAtF) {
      QueryExecutionErrors.invalidElementAtIndexError(index, numElements)
    } else {
      QueryExecutionErrors.invalidArrayIndexError(index, numElements)
    }
  }

  def mapKeyNotExistError(key: String, isElementAtF: Boolean = false): NoSuchElementException = {
    // For now, the default argument is false. The caller sets the correct value accordingly.
    // Pass "" to context parameter, context is a SQL string with line and position info, like:
    // == SQL(line 1, position 0) ==
    // element_at(col, 5)
    // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    // The context is generated from TreeNode.origin.context,
    // because of origin is thread local variable while CPU processing data,
    // we can't catch it.
    QueryExecutionErrors.mapKeyNotExistError(key, isElementAtF, "")
  }

  def sqlArrayIndexNotStartAtOneError(): ArrayIndexOutOfBoundsException = {
    new ArrayIndexOutOfBoundsException("SQL array indices start at 1")
  }
}
