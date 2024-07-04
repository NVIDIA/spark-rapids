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

class MetricsSuite extends AnyFunSuite {

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
}
