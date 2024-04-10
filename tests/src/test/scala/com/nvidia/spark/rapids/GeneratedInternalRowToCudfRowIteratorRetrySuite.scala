/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, spy, times, verify}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GeneratedInternalRowToCudfRowIteratorRetrySuite
    extends RmmSparkRetrySuiteBase
        with MockitoSugar {
  private def buildBatch(): ColumnarBatch = {
    val reductionTable = new Table.TestBuilder()
        .column(5L, null.asInstanceOf[java.lang.Long], 3L, 1L)
        .build()
    withResource(reductionTable) { tbl =>
      GpuColumnVector.from(tbl, Seq(LongType).toArray[DataType])
    }
  }

  private def getAndResetNumRetryThrowCurrentTask: Int = {
    // taskId 1 was associated with the current thread in RmmSparkRetrySuiteBase
    RmmSpark.getAndResetNumRetryThrow(/*taskId*/ 1)
  }

  test("a retry when copying to device is handled") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator
    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(Int.MaxValue),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric)
      // this forces a retry on the copy of the host column to a device column
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      withResource(myIter.next()) { devBatch =>
        withResource(buildBatch()) { expected =>
          TestUtils.compareBatches(expected, devBatch)
        }
      }
      assert(!GpuColumnVector.extractBases(batch).exists(_.getRefCount > 0))
      assert(!myIter.hasNext)
      assertResult(0)(RapidsBufferCatalog.getDeviceStorage.currentSize)
    }
  }

  test("a retry when converting to a table is handled") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator
    var rapidsBufferSpy: RapidsBuffer = null
    doAnswer(new Answer[AnyRef]() {
      override def answer(invocation: InvocationOnMock): AnyRef = {
        val res = invocation.callRealMethod()
        // we mock things this way due to code generation issues with mockito.
        // when we add a table we have
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 3,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        rapidsBufferSpy = spy(res.asInstanceOf[RapidsBuffer])
        rapidsBufferSpy
      }
    }).when(deviceStorage)
        .addTable(any(), any(), any(), any())

    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = spy(GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(Int.MaxValue),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric))
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 2,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      assertResult(0)(getAndResetNumRetryThrowCurrentTask)
      withResource(myIter.next()) { devBatch =>
        withResource(buildBatch()) { expected =>
          TestUtils.compareBatches(expected, devBatch)
        }
      }
      assertResult(6)(getAndResetNumRetryThrowCurrentTask)
      assert(!myIter.hasNext)
      assertResult(0)(RapidsBufferCatalog.getDeviceStorage.currentSize)
      // This is my wrap around of checking that we did retry the last part
      // where we are converting the device column of rows into an actual column.
      // Because we asked for 3 retries, we would ask the spill framework 4 times to materialize
      // a batch.
      verify(rapidsBufferSpy, times(4))
          .getColumnarBatch(any())
    }
  }

  test("spilling the device column of rows works") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator
    var rapidsBufferSpy: RapidsBuffer = null
    doAnswer(new Answer[AnyRef]() {
      override def answer(invocation: InvocationOnMock): AnyRef = {
        val res = invocation.callRealMethod()
        // we mock things this way due to code generation issues with mockito.
        // when we add a table we have
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 3,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        rapidsBufferSpy = spy(res.asInstanceOf[RapidsBuffer])
        // at this point we have created a buffer in the Spill Framework
        // lets spill it
        RapidsBufferCatalog.singleton.synchronousSpill(deviceStorage, 0)
        rapidsBufferSpy
      }
    }).when(deviceStorage)
        .addTable(any(), any(), any(), any())

    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = spy(GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(Int.MaxValue),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric))
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 2,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      assertResult(0)(getAndResetNumRetryThrowCurrentTask)
      withResource(myIter.next()) { devBatch =>
        withResource(buildBatch()) { expected =>
          TestUtils.compareBatches(expected, devBatch)
        }
      }
      assertResult(6)(getAndResetNumRetryThrowCurrentTask)
      assert(!myIter.hasNext)
      assertResult(0)(RapidsBufferCatalog.getDeviceStorage.currentSize)
      // This is my wrap around of checking that we did retry the last part
      // where we are converting the device column of rows into an actual column.
      // Because we asked for 3 retries, we would ask the spill framework 4 times to materialize
      // a batch.
      verify(rapidsBufferSpy, times(4))
          .getColumnarBatch(any())
    }
  }

  test("a split and retry when copying to device is not handled, and we throw") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator

    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(1),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric)
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      assertThrows[GpuSplitAndRetryOOM] {
        myIter.next()
      }
      assertResult(0)(RapidsBufferCatalog.getDeviceStorage.currentSize)
    }
  }

  test("a retry when allocating host buffer for data and offsets is handled") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator
    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(Int.MaxValue),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric)
      // Do this so we can avoid forcing failures in any host allocations
      // in ColumnarToRowIterator.hasNext()
      assert(ctriter.hasNext)
      // this forces a retry on the allocation of the combined offsets/data buffer
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.CPU.ordinal, 0)
      withResource(myIter.next()) { devBatch =>
        withResource(buildBatch()) { expected =>
          TestUtils.compareBatches(expected, devBatch)
        }
      }
      assert(!GpuColumnVector.extractBases(batch).exists(_.getRefCount > 0))
      assert(!myIter.hasNext)
      assertResult(0)(RapidsBufferCatalog.getDeviceStorage.currentSize)
    }
  }

  test("a split and retry when allocating host buffer for data and offsets is handled") {
    val batch = buildBatch()
    val batchIter = Seq(batch).iterator
    withResource(new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)) { ctriter =>
      val schema = Array(AttributeReference("longcol", LongType)().toAttribute)
      val myIter = GeneratedInternalRowToCudfRowIterator(
        ctriter, schema, TargetSize(Int.MaxValue),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric)
      // Do this so we can avoid forcing failures in any host allocations
      // in ColumnarToRowIterator.hasNext()
      assert(ctriter.hasNext)
      // this forces a split retry on the allocation of the combined offsets/data buffer
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.CPU.ordinal, 0)
      withResource(myIter.next()) { devBatch =>
        withResource(buildBatch()) { expected =>
          TestUtils.compareBatches(expected, devBatch)
        }
      }
      assert(!GpuColumnVector.extractBases(batch).exists(_.getRefCount > 0))
      assert(!myIter.hasNext)
      assertResult(0)(RapidsBufferCatalog.getDeviceStorage.currentSize)
    }
  }
}