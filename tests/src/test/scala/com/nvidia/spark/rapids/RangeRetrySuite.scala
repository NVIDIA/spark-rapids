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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark

class RangeRetrySuite extends RmmSparkRetrySuiteBase {

  test("Gpu range with split and retry OOM") {
    val start = BigInt(0)
    val end = BigInt(50)
    val step = 2
    val maxRows = 20
    val rangeIter = new GpuRangeIterator(start, end, step, maxRows, null, NoopMetric)
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId)
    var actualTotalRows = 0L
    var batchesNum = 0
    var curValue = start.toLong
    rangeIter.foreach { batch =>
      withResource(batch) { _ =>
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hCol =>
          (0 until hCol.getRowCount.toInt).foreach { pos =>
            assertResult(curValue)(hCol.getLong(pos))
            curValue += step
          }
        }
        actualTotalRows += batch.numRows()
        batchesNum += 1
      }
    }
    assertResult((end - start) / step)(actualTotalRows)
    // It should have (50/2/20 + 1) batches
    assertResult(2)(batchesNum)
  }

}
