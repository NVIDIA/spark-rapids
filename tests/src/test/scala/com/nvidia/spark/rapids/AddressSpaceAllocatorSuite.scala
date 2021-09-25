/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import org.scalatest.FunSuite

class AddressSpaceAllocatorSuite extends FunSuite {
  test("empty allocator") {
    val allocatorSize = 1024
    val allocator = new AddressSpaceAllocator(allocatorSize)
    assertResult(allocatorSize)(allocator.available)
    assertResult(0)(allocator.allocatedSize)
    assertResult(0)(allocator.numAllocatedBlocks)
    assertResult(1)(allocator.numFreeBlocks)
  }

  test("full allocation") {
    val allocatorSize = 1024
    val allocator = new AddressSpaceAllocator(allocatorSize)
    val allocated = allocator.allocate(allocatorSize)
    assertResult(Some(0))(allocated)
    assertResult(0)(allocator.available)
    assertResult(allocatorSize)(allocator.allocatedSize)
    assertResult(1)(allocator.numAllocatedBlocks)
    assertResult(0)(allocator.numFreeBlocks)
    assertResult(None)(allocator.allocate(1))
  }

  test("zero-sized allocation") {
    val allocatorSize = 1024
    val allocator = new AddressSpaceAllocator(allocatorSize)
    assertResult(None)(allocator.allocate(0))
  }

  test("invalid free") {
    val allocatorSize = 1024
    val allocator = new AddressSpaceAllocator(allocatorSize)
    assertThrows[IllegalArgumentException](allocator.free(0))
    assertThrows[IllegalArgumentException](allocator.free(1024))
    assertThrows[IllegalArgumentException](allocator.free(-1))
    assertThrows[IllegalArgumentException](allocator.free(1025))
    assertResult(Some(0))(allocator.allocate(100))
    assertThrows[IllegalArgumentException](allocator.free(50))
    allocator.free(0)
  }

  test("best-fit allocation") {
    val allocatorSize = 1024
    val allocator = new AddressSpaceAllocator(allocatorSize)
    assertResult(Some(0))(allocator.allocate(300))
    assertResult(Some(300))(allocator.allocate(400))
    assertResult(Some(700))(allocator.allocate(100))
    assertResult(Some(800))(allocator.allocate(200))
    assertResult(Some(1000))(allocator.allocate(10))
    assertResult(None)(allocator.allocate(100))
    assertResult(Some(1010))(allocator.allocate(14))
    assertResult(0)(allocator.available)
    assertResult(allocatorSize)(allocator.allocatedSize)
    assertResult(0)(allocator.numFreeBlocks)
    assertResult(6)(allocator.numAllocatedBlocks)
    allocator.free(300)
    allocator.free(1010)
    allocator.free(800)
    assertResult(Some(1010))(allocator.allocate(10))
    assertResult(Some(800))(allocator.allocate(100))
    assertResult(Some(300))(allocator.allocate(400))
    assertResult(920)(allocator.allocatedSize)
    assertResult(104)(allocator.available)
    assertResult(2)(allocator.numFreeBlocks)
    assertResult(6)(allocator.numAllocatedBlocks)
    allocator.free(800)
    allocator.free(300)
    allocator.free(700)
    allocator.free(0)
    allocator.free(1000)
    allocator.free(1010)
    assertResult(0)(allocator.allocatedSize)
    assertResult(allocatorSize)(allocator.available)
    assertResult(1)(allocator.numFreeBlocks)
    assertResult(0)(allocator.numAllocatedBlocks)
  }
}
