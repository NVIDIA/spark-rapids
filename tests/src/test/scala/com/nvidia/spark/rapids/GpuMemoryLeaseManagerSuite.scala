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

import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.concurrent.{TimeLimitedTests, TimeLimits}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.TaskContext

class GpuMemoryLeaseManagerSuite extends FunSuite
    with BeforeAndAfterEach with MockitoSugar with TimeLimits  with TimeLimitedTests with Arm {
  override def timeLimit = Span(10, Seconds)

  override def beforeEach(): Unit = GpuMemoryLeaseManager.shutdown()
  override def afterEach(): Unit = GpuMemoryLeaseManager.shutdown()

  def mockContext(taskAttemptId: Long): TaskContext = {
    val context = mock[TaskContext]
    when(context.taskAttemptId()).thenReturn(taskAttemptId)
    context
  }

  test("test basic non-blocking requests") {
    GpuMemoryLeaseManager.initializeForTest(1024)
    val context = mockContext(1)
    assert(GpuMemoryLeaseManager.getTotalLease(context) == 0)
    withResource(GpuMemoryLeaseManager.requestNonBlockingLease(context, 100)) { lease =>
      assert(lease.leaseAmount == 100)
      assert(GpuMemoryLeaseManager.getTotalLease(context) == 100)
    }
    assert(GpuMemoryLeaseManager.getTotalLease(context) == 0)
    GpuMemoryLeaseManager.releaseAllForTask(context)
    assert(GpuMemoryLeaseManager.getTotalLease(context) == 0)
  }

  test("test shrinking non-blocking request") {
    GpuMemoryLeaseManager.initializeForTest(1024)
    val context = mockContext(1)
    assert(GpuMemoryLeaseManager.getTotalLease(context) == 0)
    withResource(GpuMemoryLeaseManager.requestNonBlockingLease(context, 2048)) { lease =>
      assert(lease.leaseAmount == 1024)
      assert(GpuMemoryLeaseManager.getTotalLease(context) == 1024)
    }
    assert(GpuMemoryLeaseManager.getTotalLease(context) == 0)
    GpuMemoryLeaseManager.releaseAllForTask(context)
  }

  test("test blocking call that does not block") {
    GpuMemoryLeaseManager.initializeForTest(1024)
    val context = mockContext(1)
    assert(GpuMemoryLeaseManager.getTotalLease(context) == 0)
    withResource(GpuMemoryLeaseManager.requestLease(context, 100)) { lease =>
      assert(lease.leaseAmount == 100)
      assert(GpuMemoryLeaseManager.getTotalLease(context) == 100)
    }
    assert(GpuMemoryLeaseManager.getTotalLease(context) == 0)
    GpuMemoryLeaseManager.releaseAllForTask(context)
  }

  test("test blocking too large required throws") {
    GpuMemoryLeaseManager.initializeForTest(1024)
    val context = mockContext(1)
    assert(GpuMemoryLeaseManager.getTotalLease(context) == 0)
    assertThrows[IllegalArgumentException] {
      withResource(GpuMemoryLeaseManager.requestLease(context, 0, 2048)) { _ =>
      }
    }
    assert(GpuMemoryLeaseManager.getTotalLease(context) == 0)
    GpuMemoryLeaseManager.releaseAllForTask(context)
  }

  test("test basic blocking behavior") {
    GpuMemoryLeaseManager.initializeForTest(1024)
    val firstContext = mockContext(1)
    val secondContext = mockContext(2)
    assert(GpuMemoryLeaseManager.getTotalLease(firstContext) == 0)
    assert(GpuMemoryLeaseManager.getTotalLease(secondContext) == 0)
    var childLease: MemoryLease = null
    val child = new Thread() {
      override def run(): Unit = {
        childLease = GpuMemoryLeaseManager.requestLease(secondContext, 900)
      }
    }
    withResource(GpuMemoryLeaseManager.requestLease(firstContext, 900)) { lease =>
      assert(lease.leaseAmount == 900)
      child.start()
      // The child should block so give it some time to come up...
      Thread.sleep(100)
      assert(childLease == null)
    }
    // The blocking lease should be done now and the other thread wakes up
    Thread.sleep(100)
    assert(childLease != null)
    assert(childLease.leaseAmount == 900)
    childLease.close()
  }
}
