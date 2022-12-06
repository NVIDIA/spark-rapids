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

// spark-distros:312db:
package com.nvidia.spark.rapids.shims.spark312db

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.scalatest.FunSuite

class Spark312dbShimsSuite extends FunSuite {
  test("spark shims version") {
    assert(VersionUtils.cmpSparkVersion(3, 1, 2) === 0)
  }

  test("shuffle manager class") {
    assert(ShimLoader.getRapidsShuffleManagerClass ===
      classOf[com.nvidia.spark.rapids.spark312db.RapidsShuffleManager].getCanonicalName)
  }
}
