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

package com.nvidia.spark.rapids.window

import ai.rapids.cudf.{ColumnVector, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, NoopMetric, RmmSparkRetrySuiteBase, SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.withResource
import java.util

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, ShortType}

class GpuUnboundedToUnboundedAggWindowSuite extends RmmSparkRetrySuiteBase {
  test("basic repeat test") {
    // First I need to setup the operations. I am trying to test repeat in isolation
    // so we are not going to build them up using the front door
    val aggOutput = Seq(AttributeReference("my_max", IntegerType, true)(),
      AttributeReference("_count", LongType, true)())

    val rideAlongOutput = Seq(AttributeReference("a", ShortType, true)())
    val (boundCount, repeatOutput, boundRepeat) =
      GpuUnboundedToUnboundedAggWindowIterator.repeatOps(aggOutput)

    val finalProject = GpuUnboundedToUnboundedAggWindowIterator.computeFinalProject(
      rideAlongOutput, repeatOutput, repeatOutput ++ rideAlongOutput)

    val conf = GpuUnboundedToUnboundedAggStages(Seq.empty, Seq.empty,
      boundCount, boundRepeat, finalProject)

    def makeRepeatCb(): SpillableColumnarBatch = {
      // very basic test to verify that the repeat stage works properly.
      val table = withResource(ColumnVector.fromInts(1, 2)) { data1 =>
        withResource(ColumnVector.fromLongs(2, 3)) { counts =>
          new Table(data1, counts)
        }
      }
      withResource(table) { _ =>
        SpillableColumnarBatch(
          GpuColumnVector.from(table, Array[DataType](IntegerType, LongType)),
          SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      }
    }

    def makeRideAlongCb(): SpillableColumnarBatch = {
      // very basic test to verify that the repeat stage works properly.
      val table = withResource(ColumnVector.fromShorts(1, 2, 3, 4, 5)) { data1 =>
        new Table(data1)
      }
      withResource(table) { _ =>
        SpillableColumnarBatch(
          GpuColumnVector.from(table, Array[DataType](ShortType)),
          SpillPriorities.ACTIVE_BATCHING_PRIORITY)

      }
    }
    val rideAlongList = new util.LinkedList[SpillableColumnarBatch]
    rideAlongList.add(makeRideAlongCb())
    val inputIter = Seq(SecondPassAggResult(rideAlongList, makeRepeatCb())).toIterator
    val repeatIter = new GpuUnboundedToUnboundedAggFinalIterator(inputIter, conf,
      NoopMetric, NoopMetric, NoopMetric)

    assert(repeatIter.hasNext)
    withResource(repeatIter.next()) { result =>
      assert(result.numCols() == 2)
    }
  }
}
