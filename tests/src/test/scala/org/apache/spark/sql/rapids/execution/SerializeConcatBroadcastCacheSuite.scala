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

package org.apache.spark.sql.rapids.execution

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal}
import org.apache.spark.sql.types.IntegerType

/**
 * Unit tests for the projected-rows memoization on
 * [[SerializeConcatHostBuffersDeserializeBatch]] introduced to avoid re-running the
 * host-side projection of a DPP broadcast across reused probe sites.
 *
 * The tests construct an empty broadcast value (`data = null`, no columns, no rows) so
 * we can exercise the cache helper in isolation without needing RMM, the spill
 * framework, a Spark session, or a GPU. The cache helper itself is purely host-side
 * Java collection logic.
 */
class SerializeConcatBroadcastCacheSuite extends AnyFunSuite {

  private def newEmptyBroadcast(): SerializeConcatHostBuffersDeserializeBatch =
    new SerializeConcatHostBuffersDeserializeBatch(
      data = null,
      output = Seq.empty,
      numRows = 0,
      dataLen = 0L)

  private def row(i: Int): InternalRow = new GenericInternalRow(Array[Any](i))

  /** Build a fresh AttributeReference each call so the underlying ExprId differs. */
  private def attr(name: String): AttributeReference =
    AttributeReference(name, IntegerType, nullable = true)()

  test("ProjectedRowsKey: canonicalized equal expressions yield equal keys") {
    // Same logical expression tree produced by two independently-built
    // AttributeReferences (different ExprIds). Spark's ReusedSubquery rule treats these
    // as equivalent once canonicalized, and our cache key must reflect that so reused
    // subqueries hit the cache.
    val a1 = attr("k").canonicalized
    val a2 = attr("k").canonicalized
    val key1 = ProjectedRowsKey("subquery", Seq(0), Seq(a1), None)
    val key2 = ProjectedRowsKey("subquery", Seq(0), Seq(a2), None)
    assert(key1 == key2, "canonicalized AttributeReference should produce equal keys")
    assert(key1.hashCode == key2.hashCode, "equal keys must share the same hashCode")
  }

  test("ProjectedRowsKey: 'kind' discriminator separates subquery vs broadcastRow keys") {
    // GpuSubqueryBroadcastExec and GpuBroadcastToRowExec can share the same broadcast
    // value but project differently. The discriminator must keep their cache entries
    // separate so a "broadcastRow" projection cannot satisfy a "subquery" lookup.
    val canon = attr("k").canonicalized
    val sub = ProjectedRowsKey("subquery", Seq(0), Seq(canon), None)
    val row = ProjectedRowsKey("broadcastRow", Seq(0), Seq(canon), None)
    assert(sub != row)
  }

  test("ProjectedRowsKey: indices ordering matters") {
    // Selecting columns 0,1 vs 1,0 yields different InternalRow layouts; the cache
    // key must distinguish them.
    val canon = attr("k").canonicalized
    val k01 = ProjectedRowsKey("subquery", Seq(0, 1), Seq(canon), None)
    val k10 = ProjectedRowsKey("subquery", Seq(1, 0), Seq(canon), None)
    assert(k01 != k10)
  }

  test("ProjectedRowsKey: modeKeys differences are reflected") {
    val canon = attr("k").canonicalized
    val withMode = ProjectedRowsKey(
      "subquery", Seq(0), Seq(canon), Some(Seq(Literal(1, IntegerType).canonicalized)))
    val withoutMode = ProjectedRowsKey("subquery", Seq(0), Seq(canon), None)
    assert(withMode != withoutMode)
  }

  test("projectedRowsOrCompute: second call with same key reuses cached array") {
    val bcast = newEmptyBroadcast()
    val key = ProjectedRowsKey("subquery", Seq(0), Seq.empty[Expression], None)
    val computeCount = new AtomicInteger(0)
    val expected = Array(row(1), row(2), row(3))

    val first = bcast.projectedRowsOrCompute(key) {
      computeCount.incrementAndGet()
      expected
    }
    val second = bcast.projectedRowsOrCompute(key) {
      computeCount.incrementAndGet()
      Array(row(99))
    }

    assert(computeCount.get() == 1, "compute thunk must run exactly once for repeated key")
    assert(first eq second, "second call must return the same array instance")
    assert(first eq expected, "cached array must be the one produced by the first compute")
  }

  test("projectedRowsOrCompute: distinct keys compute independently") {
    val bcast = newEmptyBroadcast()
    val keyA = ProjectedRowsKey("subquery", Seq(0), Seq.empty[Expression], None)
    val keyB = ProjectedRowsKey("broadcastRow", Seq(0), Seq.empty[Expression], None)
    val computeCount = new AtomicInteger(0)

    val resultA = bcast.projectedRowsOrCompute(keyA) {
      computeCount.incrementAndGet()
      Array(row(1))
    }
    val resultB = bcast.projectedRowsOrCompute(keyB) {
      computeCount.incrementAndGet()
      Array(row(2))
    }

    assert(computeCount.get() == 2, "each distinct key must run its own compute")
    assert(resultA(0).getInt(0) == 1)
    assert(resultB(0).getInt(0) == 2)

    // Hits on each key after the initial computes do NOT trigger compute again.
    bcast.projectedRowsOrCompute(keyA)(throw new AssertionError("must not run"))
    bcast.projectedRowsOrCompute(keyB)(throw new AssertionError("must not run"))
  }

  test("closeInternal: clears the cache so the next call re-computes") {
    val bcast = newEmptyBroadcast()
    val key = ProjectedRowsKey("subquery", Seq(0), Seq.empty[Expression], None)
    val computeCount = new AtomicInteger(0)

    bcast.projectedRowsOrCompute(key) {
      computeCount.incrementAndGet()
      Array(row(1))
    }
    bcast.projectedRowsOrCompute(key) {
      computeCount.incrementAndGet()
      Array(row(2))
    }
    assert(computeCount.get() == 1, "warm-up: cache should serve the second call")

    bcast.closeInternal()

    val afterClose = bcast.projectedRowsOrCompute(key) {
      computeCount.incrementAndGet()
      Array(row(42))
    }
    assert(computeCount.get() == 2, "closeInternal must invalidate the cache")
    assert(afterClose(0).getInt(0) == 42)
  }

  test("projectedRowsOrCompute: concurrent racers all observe the same cached array") {
    // Stress the putIfAbsent race path: many threads all call projectedRowsOrCompute
    // on the same key at roughly the same moment. The contract is that the cache
    // converges to one shared Array and all callers eventually return the same
    // instance, even though `compute` may run more than once on the loser threads
    // (whose results are discarded).
    val bcast = newEmptyBroadcast()
    val key = ProjectedRowsKey("subquery", Seq(0), Seq.empty[Expression], None)
    val numThreads = 16
    val barrier = new CountDownLatch(1)
    val computeCount = new AtomicInteger(0)
    val executor = Executors.newFixedThreadPool(numThreads)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

    try {
      val futures = (0 until numThreads).map { _ =>
        Future {
          barrier.await()
          bcast.projectedRowsOrCompute(key) {
            computeCount.incrementAndGet()
            Array(row(1))
          }
        }
      }
      barrier.countDown()
      val results = Await.result(Future.sequence(futures), 10.seconds)

      val canonical = results.head
      assert(results.forall(_ eq canonical),
        "all racing threads must return the same cached array instance")
      assert(computeCount.get() >= 1 && computeCount.get() <= numThreads,
        s"compute should run at least once and at most once per thread, " +
          s"got ${computeCount.get()}")
    } finally {
      executor.shutdownNow()
      executor.awaitTermination(5, TimeUnit.SECONDS)
    }
  }

  test("projectedRowsOrCompute: cache is per broadcast-value instance") {
    // The cache is per-broadcast-value: two distinct broadcast instances must have
    // disjoint caches even when probed with the same key.
    val bcastA = newEmptyBroadcast()
    val bcastB = newEmptyBroadcast()
    val key = ProjectedRowsKey("subquery", Seq(0), Seq.empty[Expression], None)
    val resultA = bcastA.projectedRowsOrCompute(key)(Array(row(1)))
    val resultB = bcastB.projectedRowsOrCompute(key)(Array(row(2)))
    assert(resultA(0).getInt(0) == 1)
    assert(resultB(0).getInt(0) == 2)
    assert(resultA ne resultB)
  }

  test("ProjectedRowsKey: structural symmetry sanity check") {
    // Catches accidental field reordering in the case class which would silently
    // change the cache semantics. Using `productArity` instead of explicit field
    // counts in case the case class grows in a follow-up.
    val canon = attr("k").canonicalized
    val k = ProjectedRowsKey("subquery", Seq(0), Seq(canon), None)
    assert(k.productArity == 4)
    assert(k.kind == "subquery")
    assert(k.indices == Seq(0))
    assert(k.canonicalBuildKeys == Seq(canon))
    assert(k.canonicalModeKeys.isEmpty)
  }
}
