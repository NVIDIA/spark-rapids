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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{Rmm, RmmAllocationMode, RmmEventHandler, Table}
import com.nvidia.spark.rapids.jni.RmmSpark
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.sql.rapids.{CudfAggregate, CudfSum}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

class RapidsAggregateRetrySuite
    extends FunSuite
        with BeforeAndAfterEach with MockitoSugar with Arm {
  private def buildReductionBatch(): SpillableColumnarBatch = {
    val reductionTable = new Table.TestBuilder()
      .column(5L, null.asInstanceOf[java.lang.Long], 3L, 1L)
      .build()
    withResource(reductionTable) { tbl =>
      val cb = GpuColumnVector.from(tbl, Seq(LongType).toArray[DataType])
      spy(SpillableColumnarBatch(cb, -1, RapidsBuffer.defaultSpillCallback))
    }
  }

  private def buildGroupByBatch(): SpillableColumnarBatch = {
    val groupByTable = new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 1, 1)
        .column(1L.asInstanceOf[java.lang.Long], 2L, 3L, 4L)
        .build()
    withResource(groupByTable) { tbl =>
      val cb = GpuColumnVector.from(tbl, Seq(IntegerType, LongType).toArray[DataType])
      spy(SpillableColumnarBatch(cb, -1, RapidsBuffer.defaultSpillCallback))
    }
  }

  override def beforeEach(): Unit = {
    if (Rmm.isInitialized) {
      Rmm.shutdown()
    }

    Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
    val deviceStorage = new RapidsDeviceMemoryStore()
    val catalog = new RapidsBufferCatalog(deviceStorage)
    RapidsBufferCatalog.setCatalog(catalog)
    val baseEventHandler = new BaseRmmEventHandler()
    RmmSpark.setEventHandler(baseEventHandler)
    RmmSpark.associateThreadWithTask(RmmSpark.getCurrentThreadId, 1)

  }

  override def afterEach(): Unit = {
    RapidsBufferCatalog.close()
    if (Rmm.isInitialized) {
      Rmm.shutdown()
    }
  }

  def doReduction(input: SpillableColumnarBatch): Seq[SpillableColumnarBatch] = {
    val aggHelper = spy(new GpuHashAggregateIterator.AggHelper(
      Seq.empty, Seq.empty, Seq.empty,
      forceMerge = false, isSorted = false))

    // mock out a reduction on the first column
    val aggs = new ArrayBuffer[CudfAggregate]()
    val aggOrdinals = new ArrayBuffer[Int]()
    aggs.append(new CudfSum(LongType))
    aggOrdinals.append(0)
    when(aggHelper.cudfAggregates).thenReturn(aggs)
    when(aggHelper.aggOrdinals).thenReturn(aggOrdinals)

    // attempt a cuDF reduction
    withResource(input) { _ =>
      GpuHashAggregateIterator.aggregate(aggHelper, input)
    }
  }

  def makeGroupByAggHelper(forceMerge: Boolean): GpuHashAggregateIterator.AggHelper = {
    val aggHelper = spy(new GpuHashAggregateIterator.AggHelper(
      Seq.empty, Seq.empty, Seq.empty,
      forceMerge = forceMerge, isSorted = false))

    // mock out a group by with the first column as key, and second column
    // as a group by sum
    val groupingOrdinals = new Array[Int](1)
    groupingOrdinals(0) = 0 // groupby the 0th column
    val aggs = new ArrayBuffer[CudfAggregate]()
    aggs.append(new CudfSum(LongType))
    val aggOrdinals = new ArrayBuffer[Int]()
    aggOrdinals.append(1)
    val postStepDataTypes = new ArrayBuffer[DataType]()
    postStepDataTypes.append(IntegerType) // group by col
    postStepDataTypes.append(aggs(0).dataType) // sum

    when(aggHelper.cudfAggregates).thenReturn(aggs)
    when(aggHelper.aggOrdinals).thenReturn(aggOrdinals)
    when(aggHelper.groupingOrdinals).thenReturn(groupingOrdinals)
    when(aggHelper.postStepDataTypes).thenReturn(postStepDataTypes)
    aggHelper
  }

  def doGroupBy(
      input: SpillableColumnarBatch,
      isSorted: Boolean = false,
      forceMerge: Boolean = false): Seq[SpillableColumnarBatch] = {

    // attempt a cuDF group by
    val partiallyAgged =
      GpuHashAggregateIterator.aggregate(
        makeGroupByAggHelper(forceMerge = false), input)

    if (forceMerge) {
      // when we are merging in this case we want to create a new helper for
      // merge, and call aggregate again, like the regular aggregate does
      val mockMetrics = mock[GpuHashAggregateMetrics]
      when(mockMetrics.opTime).thenReturn(NoopMetric)
      when(mockMetrics.concatTime).thenReturn(NoopMetric)

      val singleBatch = GpuHashAggregateIterator.concatenateBatches(
        mockMetrics, partiallyAgged)
      GpuHashAggregateIterator.aggregate(
        makeGroupByAggHelper(forceMerge = true), singleBatch)
    } else {
      partiallyAgged
    }
  }

  test("computeAndAggregate reduction with retry") {
    val reductionBatch = buildReductionBatch()
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId)
    val result = doReduction(reductionBatch)
    assertResult(1)(result.length)
    withResource(result.head) { spillable =>
      withResource(spillable.getColumnarBatch) { cb =>
        assertResult(1)(cb.numRows)
        val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
        withResource(gcv.getBase.copyToHost()) { hcv =>
          assertResult(9)(hcv.getLong(0))
        }
      }
    }
    // we need to request a ColumnarBatch twice here for the retry
    verify(reductionBatch, times(2)).getColumnarBatch()
  }

  test("computeAndAggregate group by with retry") {
    val groupByBatch = buildGroupByBatch()
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId)
    val result = doGroupBy(groupByBatch)
    assertResult(1)(result.length)
    withResource(result.head) { spillable =>
      withResource(spillable.getColumnarBatch) { cb =>
        assertResult(3)(cb.numRows)
        val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
        val aggv = cb.column(1).asInstanceOf[GpuColumnVector]
        var rowsLeftToMatch = 3
        withResource(aggv.getBase.copyToHost()) { aggvh =>
          withResource(gcv.getBase.copyToHost()) { grph =>
            (0 until 3).foreach { row =>
              if (grph.isNull(row)) {
                assertResult(2L)(aggvh.getLong(row))
                rowsLeftToMatch -= 1
              } else if (grph.getInt(row) == 5) {
                assertResult(1L)(aggvh.getLong(row))
                rowsLeftToMatch -= 1
              } else if (grph.getInt(row) == 1) {
                assertResult(7L)(aggvh.getLong(row))
                rowsLeftToMatch -= 1
              }
            }
          }
        }
        assertResult(0)(rowsLeftToMatch)
      }
    }
    // we need to request a ColumnarBatch twice here for the retry
    verify(groupByBatch, times(2)).getColumnarBatch()
  }

  test("computeAndAggregate reduction with split and retry") {
    val reductionBatch = buildReductionBatch()
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId)
    val result = doReduction(reductionBatch)
    assertResult(2)(result.length)
    withResource(result.head) { spillable =>
      withResource(spillable.getColumnarBatch) { cb =>
        assertResult(1)(cb.numRows)
        val gcv = cb.column(0).asInstanceOf[GpuColumnVector]

        // I expect the 4 row batch to be split in half, the first two
        // rows add up to 5
        withResource(gcv.getBase.copyToHost()) { hcv =>
          assertResult(5)(hcv.getLong(0))
        }
      }
    }
    withResource(result.last) { spillable =>
      withResource(spillable.getColumnarBatch) { cb =>
        assertResult(1)(cb.numRows)
        val gcv = cb.column(0).asInstanceOf[GpuColumnVector]

        // The second two rows add up to 4
        withResource(gcv.getBase.copyToHost()) { hcv =>
          assertResult(4L)(hcv.getLong(0))
        }
      }
    }
    // the second time we access this batch is to split it
    verify(reductionBatch, times(2)).getColumnarBatch()
  }

  test("computeAndAggregate group by with split retry") {
    val groupByBatch = buildGroupByBatch()
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId)
    val result = doGroupBy(groupByBatch)
    assertResult(2)(result.length)
    withResource(result.head) { spillable =>
      withResource(spillable.getColumnarBatch) { cb =>
        assertResult(2)(cb.numRows)
        val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
        val aggv = cb.column(1).asInstanceOf[GpuColumnVector]
        var rowsLeftToMatch = 2
        withResource(aggv.getBase.copyToHost()) { aggvh =>
          withResource(gcv.getBase.copyToHost()) { grph =>
            (0 until 2).foreach { row =>
              if (grph.isNull(row)) {
                assertResult(2L)(aggvh.getLong(row))
                rowsLeftToMatch -= 1
              } else if (grph.getInt(row) == 5) {
                assertResult(1L)(aggvh.getLong(row))
                rowsLeftToMatch -= 1
              }
            }
          }
        }
        assertResult(0)(rowsLeftToMatch)
      }
    }
    withResource(result.last) { spillable =>
      withResource(spillable.getColumnarBatch) { cb =>
        assertResult(1)(cb.numRows)
        val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
        val aggv = cb.column(1).asInstanceOf[GpuColumnVector]
        var rowsLeftToMatch = 1
        withResource(aggv.getBase.copyToHost()) { aggvh =>
          withResource(gcv.getBase.copyToHost()) { grph =>
            (0 until 1).foreach { row =>
              if (grph.getInt(row) == 1) {
                assertResult(7L)(aggvh.getLong(row))
                rowsLeftToMatch -= 1
              }
            }
          }
        }
        assertResult(0)(rowsLeftToMatch)
      }
    }
    // the second time we access this batch is to split it
    verify(groupByBatch, times(2)).getColumnarBatch()
  }

  test("computeAndAggregate group by with retry and forceMerge") {
    // with forceMerge we expect 1 batch to be returned at all costs
    val groupByBatch = buildGroupByBatch()
    // we force a split because that would cause us to compute two aggs
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId)
    val result = doGroupBy(groupByBatch, forceMerge = true)
    assertResult(1)(result.length)
    withResource(result.head) { spillable =>
      withResource(spillable.getColumnarBatch) { cb =>
        assertResult(3)(cb.numRows)
        val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
        val aggv = cb.column(1).asInstanceOf[GpuColumnVector]
        var rowsLeftToMatch = 3
        withResource(aggv.getBase.copyToHost()) { aggvh =>
          withResource(gcv.getBase.copyToHost()) { grph =>
            (0 until 3).foreach { row =>
              if (grph.isNull(row)) {
                assertResult(2L)(aggvh.getLong(row))
                rowsLeftToMatch -= 1
              } else if (grph.getInt(row) == 5) {
                assertResult(1L)(aggvh.getLong(row))
                rowsLeftToMatch -= 1
              } else if (grph.getInt(row) == 1) {
                assertResult(7L)(aggvh.getLong(row))
                rowsLeftToMatch -= 1
              }
            }
          }
        }
        assertResult(0)(rowsLeftToMatch)
      }
    }
    // we need to request a ColumnarBatch twice here for the retry
    verify(groupByBatch, times(2)).getColumnarBatch()
  }

  class BaseRmmEventHandler extends RmmEventHandler {
    override def getAllocThresholds: Array[Long] = null
    override def getDeallocThresholds: Array[Long] = null
    override def onAllocThreshold(l: Long): Unit = {}
    override def onDeallocThreshold(l: Long): Unit = {}

    override def onAllocFailure(size: Long, retryCount: Int): Boolean = {
      false
    }
  }
}
