/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

class RebaseHelperSuite extends AnyFunSuite {
  test("all null timestamp days column rebase check") {
    withResource(ColumnVector.timestampDaysFromBoxedInts(null, null, null)) { c =>
      assertResult(false)(DateTimeRebaseUtils.isDateRebaseNeededInWrite(c))
      assertResult(false)(DateTimeRebaseUtils.isDateRebaseNeededInRead(c))
    }
  }

  test("all null timestamp microseconds column rebase check") {
    withResource(ColumnVector.timestampMicroSecondsFromBoxedLongs(null, null, null)) { c =>
      assertResult(false)(DateTimeRebaseUtils.isTimeRebaseNeededInWrite(c))
      assertResult(false)(DateTimeRebaseUtils.isTimeRebaseNeededInRead(c))
    }
  }
}
