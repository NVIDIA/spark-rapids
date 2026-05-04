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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{BinaryType, LongType}

/**
 * GPU-free unit tests for `GpuBloomFilterMightContain` observability wiring.
 *
 * Substitutes a counting spy `BloomFilterPredicateUpdater` to
 * assert the per-batch contract structurally without the GPU
 * `mightContainLong` / `sum(DType.INT64)` path. Also verifies that
 * two instances differing only in `bfId` / `probeUpdater`
 * canonicalize equal.
 */
class GpuBloomFilterMightContainSuite extends AnyFunSuite {

  /** Counting spy. Records every `update` call's args + count. */
  private final class CountingUpdater extends BloomFilterPredicateUpdater {
    var invocationCount: Int = 0
    var lastRowsIn: Long = -1L
    var lastRowsPassed: Long = -1L

    override def update(rowsIn: Long, rowsPassed: Long): Unit = {
      invocationCount += 1
      lastRowsIn = rowsIn
      lastRowsPassed = rowsPassed
    }
  }

  private def newExpr(
      bfId: Option[String],
      updater: Option[BloomFilterPredicateUpdater]
  ): GpuBloomFilterMightContain = {
    GpuBloomFilterMightContain(
      bloomFilterExpression = Literal(null, BinaryType),
      valueExpression = Literal(0L, LongType),
      bfId = bfId,
      probeUpdater = updater)
  }

  test("recordBatchUpdate invokes updater exactly once per call") {
    val spy = new CountingUpdater
    val expr = newExpr(bfId = Some("cubf-test-r7"), updater = Some(spy))
    expr.recordBatchUpdate(1000000L, 700000L)
    assert(spy.invocationCount === 1, "update must fire once per batch, never per row")
    assert(spy.lastRowsIn === 1000000L)
    assert(spy.lastRowsPassed === 700000L)
  }

  test("multi-batch invocation count matches batch count") {
    val spy = new CountingUpdater
    val expr = newExpr(bfId = Some("cubf-test-multi"), updater = Some(spy))
    expr.recordBatchUpdate(500000L, 350000L)
    expr.recordBatchUpdate(500000L, 200000L)
    expr.recordBatchUpdate(500000L, 150000L)
    assert(spy.invocationCount === 3, "3 batches produce 3 update calls")
    assert(spy.lastRowsIn === 500000L)
    assert(spy.lastRowsPassed === 150000L)
  }

  test("recordBatchUpdate is a no-op when probeUpdater is None") {
    val expr = newExpr(bfId = Some("cubf-no-updater"), updater = None)
    expr.recordBatchUpdate(1000000L, 700000L)
    // No spy, no assertion to fire; passing through to assert no exception.
    succeed
  }

  test("canonicalized drops bfId so distinct instances compare equal") {
    val spy1 = new CountingUpdater
    val spy2 = new CountingUpdater
    val a = newExpr(bfId = Some("cubf-aaaa"), updater = Some(spy1))
    val b = newExpr(bfId = Some("cubf-bbbb"), updater = Some(spy2))
    val c = newExpr(bfId = None, updater = None)
    assert(a.canonicalized == b.canonicalized,
      "bfId difference must be erased by canonicalize")
    assert(a.canonicalized == c.canonicalized,
      "presence vs absence of bfId/probeUpdater must canonicalize equal")
  }

  test("canonicalized leaves bfId / probeUpdater as None") {
    val spy = new CountingUpdater
    val a = newExpr(bfId = Some("cubf-xyz"), updater = Some(spy))
    val canon = a.canonicalized.asInstanceOf[GpuBloomFilterMightContain]
    assert(canon.bfId.isEmpty)
    assert(canon.probeUpdater.isEmpty)
  }
}
