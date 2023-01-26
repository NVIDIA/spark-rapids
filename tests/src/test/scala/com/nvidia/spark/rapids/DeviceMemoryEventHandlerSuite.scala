/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

class DeviceMemoryEventHandlerSuite extends FunSuite with MockitoSugar {

  test("a failed allocation should be retried if we spilled enough") {
    val mockCatalog = mock[RapidsBufferCatalog]
    val mockStore = mock[RapidsDeviceMemoryStore]
    when(mockStore.currentSize).thenReturn(1024)
    when(mockCatalog.synchronousSpill(any(), any())).thenAnswer(_ => Some(1024))
    val handler = new DeviceMemoryEventHandler(
      mockCatalog,
      mockStore,
      None,
      false,
      2)
    assertResult(true)(handler.onAllocFailure(1024, 0))
  }

  test("when we deplete the store, retry up to max failed OOM retries") {
    val mockCatalog = mock[RapidsBufferCatalog]
    val mockStore = mock[RapidsDeviceMemoryStore]
    when(mockStore.currentSize).thenReturn(0)
    when(mockCatalog.synchronousSpill(any(), any())).thenAnswer(_ => Some(0))
    val handler = new DeviceMemoryEventHandler(
      mockCatalog,
      mockStore,
      None,
      false,
      2)
    assertResult(true)(handler.onAllocFailure(1024, 0)) // sync
    assertResult(true)(handler.onAllocFailure(1024, 1)) // sync 2
    assertResult(false)(handler.onAllocFailure(1024, 2)) // cuDF would OOM here
  }

  test("we reset our OOM state after a successful retry") {
    val mockCatalog = mock[RapidsBufferCatalog]
    val mockStore = mock[RapidsDeviceMemoryStore]
    when(mockStore.currentSize).thenReturn(0)
    when(mockCatalog.synchronousSpill(any(), any())).thenAnswer(_ => Some(0))
    val handler = new DeviceMemoryEventHandler(
      mockCatalog,
      mockStore,
      None,
      false,
      2)
    // with this call we sync, and we mark our attempts at 1, we store 0 as the last count
    assertResult(true)(handler.onAllocFailure(1024, 0))
    // this retryCount is still 0, we should be back at 1 for attempts
    assertResult(true)(handler.onAllocFailure(1024, 0))
    assertResult(true)(handler.onAllocFailure(1024, 1))
    assertResult(false)(handler.onAllocFailure(1024, 2)) // cuDF would OOM here
  }

  test("a negative allocation cannot be retried and handler throws") {
    val mockCatalog = mock[RapidsBufferCatalog]
    val mockStore = mock[RapidsDeviceMemoryStore]
    when(mockStore.currentSize).thenReturn(1024)
    when(mockCatalog.synchronousSpill(any(), any())).thenAnswer(_ => Some(1024))
    val handler = new DeviceMemoryEventHandler(
      mockCatalog,
      mockStore,
      None,
      false,
      2)
    assertThrows[IllegalArgumentException](handler.onAllocFailure(-1, 0))
  }

  test("a negative retry count is invalid") {
    val mockCatalog = mock[RapidsBufferCatalog]
    val mockStore = mock[RapidsDeviceMemoryStore]
    when(mockStore.currentSize).thenReturn(1024)
    when(mockCatalog.synchronousSpill(any(), any())).thenAnswer(_ => Some(1024))
    val handler = new DeviceMemoryEventHandler(
      mockCatalog,
      mockStore,
      None,
      false,
      2)
    assertThrows[IllegalArgumentException](handler.onAllocFailure(1024, -1))
  }
}
