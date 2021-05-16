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

package com.nvidia.spark.rapids.shuffle

import com.nvidia.spark.rapids.shuffle.ucx.UCXConnection._
import org.scalatest.FunSuite

class UCXConnectionSuite extends FunSuite {
  test("generate active message id") {
    Seq(Int.MaxValue, -1 * Int.MaxValue, 
        -1, -1L, 0, 100000).foreach { intValueToPack: Long =>
      assertResult(intValueToPack)(
        extractExecutorId(composeRequestHeader(intValueToPack, intValueToPack)))
    }
  }
}
