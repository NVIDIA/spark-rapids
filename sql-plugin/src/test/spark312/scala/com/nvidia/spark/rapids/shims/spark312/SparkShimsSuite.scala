/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "312"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims.spark312;

import com.nvidia.spark.rapids._
import org.scalatest.FunSuite

class SparkShimsSuite extends FunSuite with FQSuiteName {
  test("spark shims version") {
    assert(ShimLoader.getShimVersion === SparkShimVersion(3, 1, 2))
  }

  test("shuffle manager class") {
    assert(ShimLoader.getRapidsShuffleManagerClass ===
      classOf[com.nvidia.spark.rapids.spark312.RapidsShuffleManager].getCanonicalName)
  }
}
