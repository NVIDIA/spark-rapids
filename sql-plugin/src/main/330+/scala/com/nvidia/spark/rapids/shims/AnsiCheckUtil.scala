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

import java.time.DateTimeException

import ai.rapids.cudf.ColumnView
import com.nvidia.spark.rapids.{Arm, FloatUtils}

import org.apache.spark.sql.internal.SQLConf

object AnsiCheckUtil extends Arm {
  def checkAnsiCastFloatToTimestamp(input: ColumnView): Unit = {
    def throwDateTimeException: Unit = {
      throw new DateTimeException(s"The column contains at least a single value that is " +
          s"NaN or Infinity. To return NULL instead, use 'try_cast'. " +
          s"If necessary set ${SQLConf.ANSI_ENABLED.key} to false to bypass this error.")
    }
    val hasNaN = withResource(FloatUtils.getNanScalar(input.getType)) { nan =>
      input.contains(nan)
    }
    if (hasNaN) {
      throwDateTimeException
    } else {
      withResource(FloatUtils.getInfinityVector(input.getType)) { inf =>
        withResource(input.contains(inf)) { hasInf =>
          withResource(hasInf.any()) { isAny =>
            if (isAny.isValid && isAny.getBoolean) {
              throwDateTimeException
            }
          }
        }
      }
    }
  }
}
