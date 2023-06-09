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

import ai.rapids.cudf.{Rmm, RmmAllocationMode, RmmEventHandler, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitTargetSizeInHalf, withRestoreOnRetry, withRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.jni.{RetryOOM, RmmSpark, SplitAndRetryOOM}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, LongType}

class WithRetrySuite
    extends FunSuite
        with BeforeAndAfterEach with MockitoSugar {

  private def buildBatch: SpillableColumnarBatch = {
    val reductionTable = new Table.TestBuilder()
        .column(5L, null.asInstanceOf[java.lang.Long], 3L, 1L)
        .build()
    withResource(reductionTable) { tbl =>
      val cb = GpuColumnVector.from(tbl, Seq(LongType).toArray[DataType])
      spy(SpillableColumnarBatch(cb, -1))
    }
  }
  
  private var rmmWasInitialized = false

  override def beforeEach(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    if (!Rmm.isInitialized) {
      rmmWasInitialized = true
      Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
    }
    val deviceStorage = new RapidsDeviceMemoryStore()
    val catalog = new RapidsBufferCatalog(deviceStorage)
    RapidsBufferCatalog.setDeviceStorage(deviceStorage)
    RapidsBufferCatalog.setCatalog(catalog)
    val mockEventHandler = new BaseRmmEventHandler()
    RmmSpark.setEventHandler(mockEventHandler)
    RmmSpark.associateThreadWithTask(RmmSpark.getCurrentThreadId, 1)
  }

  override def afterEach(): Unit = {
    RmmSpark.removeThreadAssociation(RmmSpark.getCurrentThreadId)
    RmmSpark.clearEventHandler()
    RapidsBufferCatalog.close()
    if (rmmWasInitialized) {
      Rmm.shutdown()
    }
  }

  test("withRetry closes input on failure") {
    val myItems = Seq(buildBatch, buildBatch)
    assertThrows[IllegalStateException] {
      try {
        withRetry(myItems.iterator, splitPolicy = null) { _ =>
          throw new IllegalStateException("unhandled exception")
        }.toSeq
      } finally {
        // verify that close was called on the first item,
        // which was attempted, but not the second
        verify(myItems.head, times(1)).close()
        verify(myItems.last, times(0)).close()
        myItems(1).close()
      }
    }
  }

  test("withRetryNoSplit closes input on failure") {
    val myItems = Seq(buildBatch, buildBatch)
    assertThrows[IllegalStateException] {
      try {
        withRetryNoSplit(myItems) { _ =>
          throw new IllegalStateException("unhandled exception")
        }
      } finally {
        myItems.foreach { item =>
          // verify that close was called
          verify(item, times(1)).close()
        }
      }
    }
  }

  test("withRetry closes input and attempts on failure") {
    val myItems = Seq(buildBatch, buildBatch)
    val myAttempts = Seq(buildBatch, buildBatch)
    val mockSplitPolicy = (toSplit: SpillableColumnarBatch) => {
      withResource(toSplit) { _ =>
        myAttempts
      }
    }
    assertThrows[IllegalStateException] {
      try {
        var didThrow = false
        withRetry(myItems.iterator, mockSplitPolicy) { _ =>
          if (!didThrow) {
            didThrow = true
            throw new SplitAndRetryOOM("in tests")
          } else {
            throw new IllegalStateException("unhandled exception")
          }
        }.toSeq
      } finally {
        myAttempts.foreach { item =>
          // verify that close was called on all attempts
          verify(item, times(1)).close()
        }
        verify(myItems.head, times(1)).close()
        verify(myItems.last, times(0)).close()
        myItems(1).close()
      }
    }
  }

  test("withRetry closes input on missing split policy") {
    val myItems = Seq(buildBatch, buildBatch)
    assertThrows[SplitAndRetryOOM] {
      try {
        withRetry(myItems.iterator, splitPolicy = null) { _ =>
          throw new SplitAndRetryOOM("unhandled split-and-retry")
        }.toSeq
      } finally {
        verify(myItems.head, times(1)).close()
        verify(myItems.last, times(0)).close()
        myItems(1).close()
      }
    }
  }

  test("withRestoreOnRetry restores state on retry") {
    val initialValue = 5
    val increment = 5
    var didThrow = false
    val myCheckpointable = new SimpleCheckpointRestore(initialValue)
    try {
      myCheckpointable.checkpoint()
      withRetryNoSplit {
        withRestoreOnRetry(myCheckpointable) {
          myCheckpointable.value += increment
          if (!didThrow) {
            didThrow = true
            throw new RetryOOM("in tests")
          }
        }
      }
    } finally {
      assert(myCheckpointable.value == (initialValue + increment))
    }
  }

  test("withRestoreOnRetry restores state on causedBy retry") {
    val initialValue = 5
    val increment = 5
    var didThrow = false
    val myCheckpointable = new SimpleCheckpointRestore(initialValue)
    myCheckpointable.checkpoint()
    try {
      assertThrows[IllegalStateException] {
        withRetryNoSplit {
          withRestoreOnRetry(myCheckpointable) {
            myCheckpointable.value += increment
            if (!didThrow) {
              val ex = new IllegalStateException()
              ex.addSuppressed(new RetryOOM("causedby ex in tests"))
              throw ex
              didThrow = true
            }
          }
        }
      }
    } finally {
      assert(myCheckpointable.value == (initialValue + increment))
    }
  }

  test("withRestoreOnRetry restores state for Seq on retry") {
    val initialValue = 5
    val increment = 3
    var didThrow = false
    val myCheckpointables = Seq(new SimpleCheckpointRestore(initialValue),
      new SimpleCheckpointRestore(initialValue + 10))
    try {
      myCheckpointables.foreach(_.checkpoint())
      withRetryNoSplit {
        withRestoreOnRetry(myCheckpointables) {
          myCheckpointables.foreach(_.value += increment)
          if (!didThrow) {
            didThrow = true
            throw new RetryOOM("in tests")
          }
        }
      }
    } finally {
      assert(myCheckpointables(0).value == (initialValue + increment))
      assert(myCheckpointables(1).value == (initialValue + 10 + increment))
    }
  }

  test("withRestoreOnRetry restores state for Seq on caused by retry") {
    val initialValue = 5
    val increment = 3
    var didThrow = false
    val myCheckpointables = Seq(new SimpleCheckpointRestore(initialValue),
      new SimpleCheckpointRestore(initialValue + 10))
    myCheckpointables.foreach(_.checkpoint())
    try {
      assertThrows[IllegalStateException] {
        withRetryNoSplit {
          withRestoreOnRetry(myCheckpointables) {
            myCheckpointables.foreach(_.value += increment)
            if (!didThrow) {
              didThrow = true
              val ex = new IllegalStateException()
              ex.addSuppressed(new RetryOOM("causedby ex in tests"))
              throw ex
            }
          }
        }
      }
    } finally {
      assert(myCheckpointables(0).value == (initialValue + increment))
      assert(myCheckpointables(1).value == (initialValue + 10 + increment))
    }
  }

  test("splitTargetSizeInHalf splits for AutoCloseableTargetSize") {
    val initialValue = 20L
    val minValue = 5L
    val numSplits = 2
    var doThrow = numSplits
    var lastSplitSize = 0L
    val myTarget = AutoCloseableTargetSize(initialValue, minValue)
    try {
      withRetry(myTarget, splitTargetSizeInHalf) { attempt =>
        lastSplitSize = attempt.targetSize
        if (doThrow > 0) {
          doThrow = doThrow - 1
          throw new SplitAndRetryOOM("in tests")
        }
      }.toSeq
    } finally {
      assert(lastSplitSize >= minValue)
      assert(lastSplitSize == (initialValue / (2 * numSplits)))
    }
  }

  test("splitTargetSizeInHalf on AutoCloseableTargetSize throws if limit reached") {
    val initialValue = 20L
    val minValue = 5L
    val numSplits = 3
    var doThrow = numSplits
    var lastSplitSize = 0L
    val myTarget = AutoCloseableTargetSize(initialValue, minValue)
    try {
      assertThrows[SplitAndRetryOOM] {
        withRetry(myTarget, splitTargetSizeInHalf) { attempt =>
          lastSplitSize = attempt.targetSize
          if (doThrow > 0) {
            doThrow = doThrow - 1
            throw new SplitAndRetryOOM("in tests")
          }
        }.toSeq
      }
    } finally {
      assert(lastSplitSize >= minValue)
      assert(lastSplitSize == (initialValue / (2 * (numSplits - 1))))
    }
  }

  private class BaseRmmEventHandler extends RmmEventHandler {
    override def getAllocThresholds: Array[Long] = null
    override def getDeallocThresholds: Array[Long] = null
    override def onAllocThreshold(totalAllocSize: Long): Unit = {}
    override def onDeallocThreshold(totalAllocSize: Long): Unit = {}
    override def onAllocFailure(sizeRequested: Long, retryCount: Int): Boolean = {
      false
    }
  }
  private class SimpleCheckpointRestore(var value:Int) extends CheckpointRestore {
    private var lastValue:Int = value
    def setValue(newVal: Int) = {
      value = newVal
    }
    override def checkpoint() = {
      lastValue = value
    }
    override def restore() = {
      value = lastValue
    }
  }
}
