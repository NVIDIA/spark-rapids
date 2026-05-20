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

import com.nvidia.spark.rapids.BloomFilterTestHelpers._
import com.nvidia.spark.rapids.FakeInlineExecs._
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for InlineBFBuildReplacement, GpuOverrides registration, and
 * BloomFilterBuildAccumulator. These verify the reflection-based replacement
 * mechanism and accumulator merge logic without needing GPU or SparkSession.
 */
class InlineBFBuildReplacementSuite extends AnyFunSuite {

  Seq(1, 2).foreach { version =>
    test(s"BloomFilterBuildAccumulator merge produces correct union [v$version]") {
      val acc1 = new BloomFilterBuildAccumulator()
      val acc2 = new BloomFilterBuildAccumulator()
      acc1.add(makeBfBytes(version = version, dataLastByte = 0x0F))
      acc2.add(makeBfBytes(version = version, dataLastByte = 0xF0))
      acc1.merge(acc2)
      val merged = acc1.value
      val dataLastIdx = headerSize(version) + 7
      assert((merged(dataLastIdx) & 0xFF) == 0xFF,
        s"Expected 0xFF, got ${merged(dataLastIdx) & 0xFF}")
      assert(merged(3) == version, s"Version byte should be $version")
      assert(merged(7) == 1, "NumHashes should be 1")
    }
  }

  test("BloomFilterBuildAccumulator is zero when empty") {
    val acc = new BloomFilterBuildAccumulator()
    assert(acc.isZero)
    assert(acc.value == null)
  }

  test("BloomFilterBuildAccumulator copy is independent") {
    val acc = new BloomFilterBuildAccumulator()
    acc.add(makeBfBytes(dataLastByte = 1))
    val copy = acc.copy()
    acc.reset()
    assert(acc.isZero)
    assert(!copy.asInstanceOf[BloomFilterBuildAccumulator].isZero)
  }

  Seq(1, 2).foreach { version =>
    test(s"BloomFilterBuildAccumulator single partition [v$version]") {
      val acc = new BloomFilterBuildAccumulator()
      acc.add(makeBfBytes(version = version, dataLastByte = 42))
      assert(!acc.isZero)
      assert(acc.value(headerSize(version) + 7) == 42)
    }
  }

  test("accumulator_merge_v2_preserves_seed_header_field") {
    // The V2 wire format carries a 4-byte seed at offsets 8..11. `mergeBytes`
    // selects dataOffset=16 for V2 and OR-merges only data bytes (offset >= 16);
    // the seed bytes must survive the merge unchanged. Without this guarantee a
    // multi-partition V2 merge would corrupt the seed and break probe-side hashing.
    val left  = makeBfBytes(version = 2, seed = 0xDEADBEEF, dataLastByte = 0x0F)
    val right = makeBfBytes(version = 2, seed = 0xDEADBEEF, dataLastByte = 0xF0)
    val acc = new BloomFilterBuildAccumulator()
    acc.add(left)
    acc.add(right)
    val merged = acc.value
    assert(merged(8)  == 0xDE.toByte, "seed byte 0 preserved")
    assert(merged(9)  == 0xAD.toByte, "seed byte 1 preserved")
    assert(merged(10) == 0xBE.toByte, "seed byte 2 preserved")
    assert(merged(11) == 0xEF.toByte, "seed byte 3 preserved")
    assert((merged(V2_HEADER_SIZE + 7) & 0xFF) == 0xFF,
      "data byte OR-merged across partitions")
    assert(merged(3) == 2, "V2 version header byte preserved")
  }

  test("BloomFilterBuildAccumulator.add(null) is a defensive no-op") {
    val acc = new BloomFilterBuildAccumulator()
    acc.add(null)
    assert(acc.isZero,
      "add(null) must not mutate state; production never calls this path " +
        "but the guard closes the NPE surface for future callers")
  }

  test("BloomFilterBuildAccumulator size mismatch fails closed via skip sentinel") {
    // A length mismatch in mergeBytes indicates a planner-side bug emitting different
    // numBits for the same bfId across partitions. The accumulator must NOT throw — a
    // thrown exception escapes into Spark's DAGScheduler accumulator-merge path, which
    // catches it via NonFatal and continues; the BF would then be shipped to the probe
    // side missing one partition's contribution, producing false negatives in
    // mightContainLong and dropping rows that should match the join. Instead, the
    // accumulator must publish the skip sentinel so the probe side applies no filter —
    // matching the oversize-skip and unsafe-build fail-closed contract elsewhere in
    // GpuGenerateBloomFilterExec.
    val acc = new BloomFilterBuildAccumulator()
    val partitionA = makeBfBytes(version = 1, numWords = 1, dataLastByte = 0x0F)
    val partitionB = makeBfBytes(version = 1, numWords = 2, dataLastByte = 0xF0)
    assert(partitionA.length != partitionB.length,
      "test fixtures must differ in length to exercise the size-mismatch path")
    acc.add(partitionA)
    acc.add(partitionB)
    val observedValue = if (acc.value == null) {
      "null"
    } else {
      s"length ${acc.value.length}"
    }
    assert(acc.value eq BloomFilterBuildAccumulator.SkipSentinel,
      "size-mismatch merge must canonicalize to the skip sentinel, not throw or " +
        s"leave a partial value (got $observedValue)")
  }

  test("InlineBFBuildReplacement class name is resolvable") {
    // The optional planner class is not on the unit-test classpath; only the
    // class-name constant inside the rule needs to be well-formed.
    val replacement = InlineBFBuildReplacement()
    assert(replacement != null)
  }

  test("reflects_multi_spec_field") {
    val fake: FakeMultiSpecInlineExec = FakeMultiSpecInlineExec(
      Seq(FakeSpec("bf-A", 0, 5, 100000L),
        FakeSpec("bf-B", 1, 5, 100000L)),
      bfVersion = 1,
      seed = 0,
      xxHashSeed = 42L,
      child = null)
    val specs = InlineBFBuildReplacement().readSpecs(fake)
    assert(specs.size == 2)
    assert(specs.map(_.bfId) == Seq("bf-A", "bf-B"))
    assert(specs.map(_.keyColumnIndex) == Seq(0, 1))
    assert(specs.map(_.numHashes) == Seq(5, 5))
    assert(specs.map(_.numBits) == Seq(100000L, 100000L))
  }

  test("readSpecs propagates reflective accessor failure (fail-closed contract)") {
    val broken = FakeBrokenInlineExec(null)
    val ex = intercept[Exception] {
      InlineBFBuildReplacement().readSpecs(broken)
    }
    // The exception must surface so the apply-level NonFatal catch can
    // discard the replacement and return the original plan unchanged.
    assert(ex.getCause != null || ex.getMessage != null,
      "reflection failure must surface as an exception, not silently succeed")
  }

  test("legacy_fallback_tolerates_old_single_spec") {
    // When only the legacy single-field accessors are available, readSpecs must
    // wrap a single BFSpec so downstream GpuGenerateBloomFilterExec always sees
    // a uniform Seq[BFSpec] shape.
    val legacy: FakeLegacyInlineExec = FakeLegacyInlineExec(
      bfId = "legacy-single",
      keyColumnIndex = 3,
      numHashes = 7,
      numBits = 524288L,
      bfVersion = 1,
      seed = 0,
      xxHashSeed = 42L,
      child = null)
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
