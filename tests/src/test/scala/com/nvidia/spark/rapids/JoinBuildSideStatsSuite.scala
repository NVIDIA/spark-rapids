/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.rapids.execution.JoinBuildSideStats
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class JoinBuildSideStatsSuite extends RmmSparkRetrySuiteBase {

  private val key = GpuBoundReference(0, IntegerType, nullable = false)(ExprId(0), "a")

  private def buildBatch(ints: Int*): SpillableColumnarBatch = {
    SpillableColumnarBatch(
      new ColumnarBatch(
        Array(GpuColumnVector.from(ColumnVector.fromInts(ints: _*), IntegerType)),
        ints.length),
      SpillPriorities.ACTIVE_BATCHING_PRIORITY)
  }

  test("Stats from batches with 3 ones as input") {
    Arm.withResource(
      Seq(buildBatch(1, 2, 3, 3), buildBatch(2, 3, 4), buildBatch(3, 4, 5))
    ) { cbs =>
      val stats = JoinBuildSideStats.fromBatches(cbs, Seq(key))
      assertResult(false)(stats.isDistinct)
      assertResult(2.toDouble)(stats.streamMagnificationFactor)
    }
  }

  test("Stats from batches with one as input") {
    Arm.withResource(Seq(buildBatch(1, 2, 3, 3, 4, 5, 4, 5, 5, 2))) { cbs =>
      val stats = JoinBuildSideStats.fromBatches(cbs, Seq(key))
      assertResult(false)(stats.isDistinct)
      assertResult(2.toDouble)(stats.streamMagnificationFactor)
    }
  }

  test("Stats from batches with the empty input") {
    Arm.withResource(Seq.empty[SpillableColumnarBatch]) { cbs =>
      assertThrows[AssertionError] {
        JoinBuildSideStats.fromBatches(cbs, Seq(key))
      }
    }
  }
}
