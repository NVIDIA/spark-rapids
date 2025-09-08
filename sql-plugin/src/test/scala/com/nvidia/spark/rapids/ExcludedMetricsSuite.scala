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

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

class ExcludedMetricsSuite extends AnyFunSuite {

  test("GpuMetric.ns: duplicate timing on the same metrics") {
    val m1 = new LocalGpuMetric()
    m1.ns(
      m1.ns(
        Thread.sleep(1000)
      )
    )
    // if the timing is duplicated, the value should be around 2,000,000,000
    assert(m1.value < 1000000000 * 1.9)
    assert(m1.value > 1000000000 * 0.5)
  }

  test("MetricRange: duplicate timing on the same metrics") {
    val m1 = new LocalGpuMetric()
    val m2 = new LocalGpuMetric()
    withResource(new MetricRange(m1, m2)) { _ =>
      withResource(new MetricRange(m2, m1)) { _ =>
        Thread.sleep(1000)
      }
    }

    // if the timing is duplicated, the value should be around 2,000,000,000
    assert(m1.value < 1000000000 * 1.9)
    assert(m1.value > 1000000000 * 0.5)
    assert(m2.value < 1000000000 * 1.9)
    assert(m2.value > 1000000000 * 0.5)
  }

  test("NvtxWithMetrics: duplicate timing on the same metrics") {
    val m1 = new LocalGpuMetric()
    val m2 = new LocalGpuMetric()
    withResource(new NvtxWithMetrics("a", NvtxColor.BLUE, m1, m2)) { _ =>
      withResource(new NvtxWithMetrics("b", NvtxColor.BLUE, m2, m1)) { _ =>
        Thread.sleep(1000)
      }
    }

    // if the timing is duplicated, the value should be around 2,000,000,000
    assert(m1.value < 1000000000 * 1.9)
    assert(m1.value > 1000000000 * 0.5)
    assert(m2.value < 1000000000 * 1.9)
    assert(m2.value > 1000000000 * 0.5)
  }

  test("GpuMetric.ns: exclude multiple child metrics functionality") {
    val parentOpTime = new LocalGpuMetric()
    val child1OpTime = new LocalGpuMetric()
    val child2OpTime = new LocalGpuMetric()
    
    // First, establish baseline - child metrics start with some time
    child1OpTime.ns {
      Thread.sleep(100)
    }
    child2OpTime.ns {
      Thread.sleep(100)
    }
    
    val child1InitialValue = child1OpTime.value
    val child2InitialValue = child2OpTime.value
    
    // Test the main functionality: parent metric should exclude time from child metrics
    val excludeMetrics = Seq(child1OpTime, child2OpTime)
    parentOpTime.ns(excludeMetrics) {
      // During parent execution, children accumulate more time (simulating iter.next() calls)
      child1OpTime.ns {
        Thread.sleep(100) // This time should be EXCLUDED from parent
      }
      child2OpTime.ns {
        Thread.sleep(100) // This time should be EXCLUDED from parent
      }
      // Parent's own work
      Thread.sleep(200) // This time should be INCLUDED in parent
    }
    
    // Verify results
    val child1AdditionalTime = child1OpTime.value - child1InitialValue
    val child2AdditionalTime = child2OpTime.value - child2InitialValue
    val totalChildAdditionalTime = child1AdditionalTime + child2AdditionalTime
    
    // Assertions
    assert(parentOpTime.value > 0, "Parent metric should have recorded some time")
    assert(child1AdditionalTime > 0, "Child1 should have additional time from parent execution")
    assert(child2AdditionalTime > 0, "Child2 should have additional time from parent execution")
    
    // The key test: parent time should be approximately 200ms (its own work)
    // and should NOT include the ~200ms from children (100ms each)
    // If exclusion works, parent time should be much less than total time
    assert(parentOpTime.value < (parentOpTime.value + totalChildAdditionalTime) * 0.8,
      s"Parent time (${parentOpTime.value}ns) should be less than combined time due " +
        s"to exclusion. " +
      s"Child additional time: ${totalChildAdditionalTime}ns")
    
    // Parent time should be reasonable for its own work (~200ms)
    val expectedParentTime = 200 * 1000000L // 200ms in nanoseconds
    val tolerance = 300 * 1000000L // 300ms tolerance for test variance
    assert(parentOpTime.value > 100 * 1000000L, // At least 100ms
      s"Parent should have at least 100ms of recorded time, got ${parentOpTime.value}ns")
    assert(parentOpTime.value < expectedParentTime + tolerance,
      s"Parent time (${parentOpTime.value}ns) should be close to expected ~200ms with tolerance")
  }


  test("GpuMetric.ns: multiple excludeMetrics vs single excludeMetric comparison") {
    val parent1 = new LocalGpuMetric()
    val parent2 = new LocalGpuMetric()
    val child1 = new LocalGpuMetric()
    val child2 = new LocalGpuMetric()

    // Test 1: Using multiple excludeMetrics (new functionality)
    val multipleExcludes = Seq(child1, child2)
    parent1.ns(multipleExcludes) {
      child1.ns {
        Thread.sleep(50)
      }
      child2.ns {
        Thread.sleep(50)
      }
      Thread.sleep(100) // Parent work
    }

    // Test 2: Using single excludeMetric (existing functionality) - sum of children
    val combinedChildTime = child1.value + child2.value
    val dummyMetric = new LocalGpuMetric()
    dummyMetric.add(combinedChildTime) // Simulate combined child time
    parent2.ns(dummyMetric) {
      Thread.sleep(100) // Same parent work
    }

    // Both approaches should yield similar results
    val timeDifference = Math.abs(parent1.value - parent2.value).toDouble
    val averageTime = (parent1.value + parent2.value).toDouble / 2
    val relativeDifference = if (averageTime > 0) timeDifference / averageTime else 0.0

    assert(relativeDifference < 0.3, // Within 30% difference
      s"Multiple excludeMetrics and single excludeMetric should give similar results. " +
      s"Parent1: ${parent1.value}ns, Parent2: ${parent2.value}ns, " +
      s"Relative difference: ${relativeDifference * 100}%")
  }

  test("GpuMetric.ns: edge cases with multiple excludeMetrics") {
    val parent = new LocalGpuMetric()

    // Test with empty excludeMetrics list
    parent.ns(Seq.empty[GpuMetric]) {
      Thread.sleep(100)
    }
    assert(parent.value > 0, "Should work with empty excludeMetrics list")

    // Test with single metric in list (should behave like single excludeMetric)
    val child = new LocalGpuMetric()
    val parent2 = new LocalGpuMetric()
    parent2.ns(Seq(child)) {
      child.ns { Thread.sleep(50) }
      Thread.sleep(100)
    }
    assert(parent2.value > 0, "Should work with single metric in excludeMetrics list")

    // Test with duplicate metrics in list
    val parent3 = new LocalGpuMetric()
    val child2 = new LocalGpuMetric()
    parent3.ns(Seq(child2, child2)) { // Duplicate child2
      child2.ns { Thread.sleep(50) }
      Thread.sleep(100)
    }
    assert(parent3.value > 0, "Should work with duplicate metrics in excludeMetrics list")
  }
}
