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

package com.nvidia.spark.rapids

import org.apache.spark.sql.functions.map_concat

class CollectionOpSuite extends SparkQueryCompareTestSuite {
  testGpuFallback(
    "MapConcat with Array keys fall back",
    "ProjectExec",
    ArrayKeyMapDF, 
    execsAllowedNonGpu = Seq("ProjectExec", "ShuffleExchangeExec")) {
    frame => {
      import frame.sparkSession.implicits._
      frame.select(map_concat($"col1", $"col2"))
    }
  }

  testGpuFallback(
    "MapConcat with Struct keys fall back",
    "ProjectExec",
    StructKeyMapDF, 
    execsAllowedNonGpu = Seq("ProjectExec", "ShuffleExchangeExec")) {
    frame => {
      import frame.sparkSession.implicits._
      frame.select(map_concat($"col1", $"col2"))
    }
  }
}
