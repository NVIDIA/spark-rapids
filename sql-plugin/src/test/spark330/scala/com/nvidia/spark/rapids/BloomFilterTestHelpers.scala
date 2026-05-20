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

/**
 * Shared test fixtures for CuBF unit suites.
 *
 * Contents:
 *   - Wire-format constants (`V1_HEADER_SIZE`, `V2_HEADER_SIZE`).
 *   - `makeBfBytes` — synthetic serialized BF payload builder for both V1 and V2 layouts.
 *   - `AbstractCountingSpy` plus the two concrete updater spies used to assert per-batch /
 *     per-build invariants without a GPU dependency.
 *
 * Wire-format layouts (design § 5.5, big-endian):
 *   V1:  version(4) | numHashes(4) | numWords(4)            | data(numWords * 8)
 *   V2:  version(4) | numHashes(4) | seed(4) | numWords(4)  | data(numWords * 8)
 *
 * `BloomFilterBuildAccumulator.mergeBytes` selects the data offset by reading the
 * version field from the merged buffer, so the same fixture builder exercises both
 * V1 and V2 merge paths just by varying the `version` argument.
 */
object BloomFilterTestHelpers {

  /** V1 header size. Used as the data-section base offset for V1 fixtures. */
  val V1_HEADER_SIZE: Int = 12

  /** V2 header size. The extra 4 bytes carry the V2-only seed field at offsets 8..11. */
  val V2_HEADER_SIZE: Int = 16

  /** Header size for a given wire-format version (defaults to V1 for any value other than 2). */
  def headerSize(version: Int): Int =
    if (version == 2) V2_HEADER_SIZE else V1_HEADER_SIZE

  /**
   * Build a synthetic serialized bloom-filter payload. The trailing data byte is
   * controllable so OR-merge assertions can target a single representative bit pattern.
   *
   * @param version       wire-format version (1 or 2)
   * @param numHashes     numHashes header field
   * @param numWords      numWords header field; data section is `numWords * 8` bytes
   * @param dataLastByte  value written into the trailing byte of the data section
   * @param seed          V2-only seed header field; ignored for V1
   */
  def makeBfBytes(
      version: Int = 1,
      numHashes: Int = 1,
      numWords: Int = 1,
      dataLastByte: Int = 0,
      seed: Int = 0): Array[Byte] = {
    val header = headerSize(version)
    val dataLen = numWords * 8
    val out = new Array[Byte](header + dataLen)
    writeIntBE(out, 0, version)
    writeIntBE(out, 4, numHashes)
    if (version == 2) {
      writeIntBE(out, 8, seed)
      writeIntBE(out, 12, numWords)
    } else {
      writeIntBE(out, 8, numWords)
    }
    if (dataLen > 0) {
      out(header + dataLen - 1) = dataLastByte.toByte
    }
    out
  }

  private def writeIntBE(buf: Array[Byte], offset: Int, value: Int): Unit = {
    buf(offset)     = ((value >>> 24) & 0xFF).toByte
    buf(offset + 1) = ((value >>> 16) & 0xFF).toByte
    buf(offset + 2) = ((value >>>  8) & 0xFF).toByte
    buf(offset + 3) = ( value         & 0xFF).toByte
  }

  /** Base for unit-test counting spies. Concrete subclasses extend an updater trait. */
  abstract class AbstractCountingSpy {
    var invocationCount: Int = 0
  }

  /** Counting spy for `BloomFilterPredicateUpdater`. Records the most recent args. */
  final class CountingPredicateUpdater
      extends AbstractCountingSpy with BloomFilterPredicateUpdater {
    var lastRowsIn: Long = -1L
    var lastRowsPassed: Long = -1L
    override def update(rowsIn: Long, rowsPassed: Long): Unit = {
      invocationCount += 1
      lastRowsIn = rowsIn
      lastRowsPassed = rowsPassed
    }
  }

  /** Counting spy for `BloomFilterBuildCostUpdater`. Records the most recent args. */
  final class CountingBuildUpdater
      extends AbstractCountingSpy with BloomFilterBuildCostUpdater {
    var lastBuildWallNanos: Long = -1L
    var lastBfBytes: Long = -1L
    override def update(buildWallNanos: Long, bfBytes: Long): Unit = {
      invocationCount += 1
      lastBuildWallNanos = buildWallNanos
      lastBfBytes = bfBytes
    }
  }
}

/**
 * Reflection-target shapes for `InlineBFBuildReplacement.readSpecs`. Defined as
 * top-level case classes (not nested in a test class) so they carry no implicit
 * outer reference; the public field accessors are the same surface a real
 * planner-emitted exec exposes.
 */
object FakeInlineExecs {

  /** Stand-in for the planner's per-BF build spec. */
  case class FakeSpec(
      bfId: String,
      keyColumnIndex: Int,
      numHashes: Int,
      numBits: Long)

  /** Multi-spec inline-build exec shape (preferred reflective path). */
  case class FakeMultiSpecInlineExec(
      specs: Seq[FakeSpec],
      bfVersion: Int,
      seed: Int,
      xxHashSeed: Long,
      child: Any)

  /** Legacy single-spec inline-build exec shape (fallback reflective path). */
  case class FakeLegacyInlineExec(
      bfId: String,
      keyColumnIndex: Int,
      numHashes: Int,
      numBits: Long,
      bfVersion: Int,
      seed: Int,
      xxHashSeed: Long,
      child: Any)

  /**
   * Shape whose `specs` accessor throws. Drives the fail-closed `NonFatal`
   * branch in `replaceWithGpu` / `readSpecs`: a real planner module returning
   * a broken accessor must surface as an exception, not a silent success.
   */
  case class FakeBrokenInlineExec(child: Any) {
    def specs: Seq[Any] = throw new RuntimeException("synthetic reflection failure")
    def bfVersion: Int = 1
    def seed: Int = 0
    def xxHashSeed: Long = 42L
  }
}
