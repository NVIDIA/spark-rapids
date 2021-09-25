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

import org.scalatest.FunSuite

class RapidsExecutorPluginSuite extends FunSuite {
  test("cudf version check") {
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7", "7"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7", "8"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7", "7.2"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7", "8.7"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7", "7.2.1"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0", "7.0"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0", "7.0.1"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0", "7.0.1.3"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0", "7"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0", "7.1"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0.1"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0.1.3"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0.2"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0.2.3"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0.0"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.1"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.1.1"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0.1-special"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1-special", "7.0.1"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1-special", "7.0.1-special"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.2-special", "7.0.1-special"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1-special", "7.0.2-special"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.2.2.2", "7.0.2.2"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.2.2.2", "7.0.2.2.2"))
  }
}
