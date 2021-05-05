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

import ai.rapids.cudf.ColumnVector
import org.scalatest.FunSuite

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuColumnarToRowSuite extends FunSuite with Arm {
  test("iterate past empty input batches") {
    val batchIter: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
      private[this] var batchCount = 0

      override def hasNext: Boolean = batchCount < 10

      override def next(): ColumnarBatch = {
        batchCount += 1
        if (batchCount % 2 == 0) {
          new ColumnarBatch(Array.empty, 0)
        } else {
          val gcv = GpuColumnVector.from(ColumnVector.fromStrings(batchCount.toString), StringType)
          new ColumnarBatch(Array(gcv), 1)
        }
      }
    }

    val ctriter = new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)
    assertResult(Seq("1", "3", "5", "7", "9"))(ctriter.map(_.getString(0)).toSeq)
  }
}
