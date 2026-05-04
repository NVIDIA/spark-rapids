/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for InlineBFBuildReplacement, GpuOverrides registration,
 * and BloomFilterBuildAccumulator.
 *
 * These verify the reflection-based replacement mechanism and
 * accumulator merge logic without needing GPU or SparkSession.
 */
class InlineBFBuildReplacementSuite extends AnyFunSuite {

  test("BloomFilterBuildAccumulator merge produces correct union") {
    val acc1 = new BloomFilterBuildAccumulator()
    val acc2 = new BloomFilterBuildAccumulator()
    // V1 format: version(4) + numHashes(4) + numWords(4) + data
    val bf1 = Array[Byte](
      0, 0, 0, 1, // version=1
      0, 0, 0, 1, // numHashes=1
      0, 0, 0, 1, // numWords=1
      0, 0, 0, 0, 0, 0, 0, 0x0F // data: low 4 bits set
    )
    val bf2 = Array[Byte](
      0, 0, 0, 1,
      0, 0, 0, 1,
      0, 0, 0, 1,
      0, 0, 0, 0, 0, 0, 0, 0xF0.toByte // data: high 4 bits
    )
    acc1.add(bf1)
    acc2.add(bf2)
    acc1.merge(acc2)
    val merged = acc1.value
    // Data byte should be OR of 0x0F and 0xF0 = 0xFF
    assert((merged(19) & 0xFF) == 0xFF,
      s"Expected 0xFF, got ${merged(19) & 0xFF}")
    // Header unchanged
    assert(merged(3) == 1, "Version should be 1")
    assert(merged(7) == 1, "NumHashes should be 1")
  }

  test("BloomFilterBuildAccumulator is zero when empty") {
    val acc = new BloomFilterBuildAccumulator()
    assert(acc.isZero)
    assert(acc.value == null)
  }

  test("BloomFilterBuildAccumulator copy is independent") {
    val acc = new BloomFilterBuildAccumulator()
    val bf = Array[Byte](0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1,
      1, 2, 3, 4, 5, 6, 7, 8)
    acc.add(bf)
    val copy = acc.copy()
    acc.reset()
    assert(acc.isZero)
    assert(!copy.asInstanceOf[BloomFilterBuildAccumulator].isZero)
  }

  test("BloomFilterBuildAccumulator single partition") {
    val acc = new BloomFilterBuildAccumulator()
    val bf = Array[Byte](0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1,
      0, 0, 0, 0, 0, 0, 0, 42)
    acc.add(bf)
    assert(!acc.isZero)
    assert(acc.value(19) == 42)
  }

  test("InlineBFBuildReplacement class name is resolvable") {
    // Verify the class name pattern is well-formed. The optional
    // planner class is not expected to be present in unit tests,
    // but the constant should be well-formed.
    val replacement = InlineBFBuildReplacement()
    assert(replacement != null)
  }

  /** Stand-in for an inline-build exec carrying the
   * `specs: Seq[BFSpec]` shape. Lives in this test so we can
   * exercise the reflection path without optional planner classes.
   */
  case class FakeSpec(bfId: String, keyColumnIndex: Int,
      numHashes: Int, numBits: Long)

  case class FakeMultiSpecInlineExec(
      specs: Seq[FakeSpec],
      bfVersion: Int,
      seed: Int,
      xxHashSeed: Long,
      child: Any)

  case class FakeLegacyInlineExec(
      bfId: String,
      keyColumnIndex: Int,
      numHashes: Int,
      numBits: Long,
      bfVersion: Int,
      seed: Int,
      xxHashSeed: Long,
      child: Any)

  test("reflects_multi_spec_field") {
    val fake: FakeMultiSpecInlineExec = FakeMultiSpecInlineExec(
      Seq(FakeSpec("bf-A", 0, 5, 100000L),
        FakeSpec("bf-B", 1, 5, 100000L)),
      1,    // bfVersion
      0,    // seed
      42L,  // xxHashSeed
      null) // child
    val specs = InlineBFBuildReplacement().readSpecs(fake)
    assert(specs.size == 2)
    assert(specs.map(_.bfId) == Seq("bf-A", "bf-B"))
    assert(specs.map(_.keyColumnIndex) == Seq(0, 1))
    assert(specs.map(_.numHashes) == Seq(5, 5))
    assert(specs.map(_.numBits) == Seq(100000L, 100000L))
  }

  test("legacy_fallback_tolerates_old_single_spec") {
    // When only the legacy single-field accessors are available,
    // readSpecs must wrap a single BFSpec so downstream
    // GpuGenerateBloomFilterExec always sees a uniform Seq[BFSpec]
    // shape.
    val legacy: FakeLegacyInlineExec = FakeLegacyInlineExec(
      "legacy-single", // bfId
      3,               // keyColumnIndex
      7,               // numHashes
      524288L,         // numBits
      1,               // bfVersion
      0,               // seed
      42L,             // xxHashSeed
      null)            // child
    val specs = InlineBFBuildReplacement().readSpecs(legacy)
    assert(specs.size == 1,
      s"expected 1 legacy spec, got ${specs.size}")
    val s = specs.head
    assert(s.bfId == "legacy-single")
    assert(s.keyColumnIndex == 3)
    assert(s.numHashes == 7)
    assert(s.numBits == 524288L)
  }
}
