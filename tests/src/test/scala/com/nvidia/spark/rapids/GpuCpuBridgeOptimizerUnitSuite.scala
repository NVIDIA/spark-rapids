/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{Add, And, AttributeReference, CaseWhen, GreaterThan, LessThan, Literal, Multiply, XxHash64}
import org.apache.spark.sql.types.LongType

class GpuCpuBridgeOptimizerUnitSuite extends AnyFunSuite {

  private def conf(): RapidsConf = new RapidsConf(Map(
    RapidsConf.ENABLE_CPU_BRIDGE.key -> "true"
  ))

  private def wrap(e: org.apache.spark.sql.catalyst.expressions.Expression): BaseExprMeta[_] = {
    val meta = GpuOverrides.wrapExpr(e, conf(), None)
    meta.tagForGpu()
    meta
  }

  test("(a*2) + (a*3) prefers CPU multiplies under CPU add") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val mul1 = Multiply(a, Literal(2L))
    val mul2 = Multiply(a, Literal(3L))
    val add = Add(mul1, mul2)

    val addMeta = wrap(add)
    // simulate add disabled on GPU
    addMeta.willNotWorkOnGpu("disabled for test")


    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(addMeta)


    assert(addMeta.willUseGpuCpuBridge)
    val addChildren = addMeta.childExprs
    assert(addChildren.size == 2)
    val leftMult: BaseExprMeta[_] = addChildren.head
    val rightMult: BaseExprMeta[_] = addChildren(1)
    assert(leftMult.willUseGpuCpuBridge)
    assert(rightMult.willUseGpuCpuBridge)
    val leftMultChildren = leftMult.childExprs
    assert(leftMultChildren.size == 2)
    // attr a is not marked for CPU or GPU. It will be co-located with the parent expression.
    assert(leftMultChildren(1).willUseGpuCpuBridge) // lit 2
    val rightMultChildren = rightMult.childExprs
    assert(rightMultChildren.size == 2)
    // attr a is not marked for CPU or GPU. It will be co-located with the parent expression.
    assert(rightMultChildren(1).willUseGpuCpuBridge) // lit 3
  }

  test("Under CPU Add parent with shared leaf, xxhash64 (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val hash = XxHash64(Seq(a), 42L)
    val add = Add(a, hash)

    val addMeta = wrap(add)
    // force Add to CPU
    addMeta.willNotWorkOnGpu("disabled for test")

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(addMeta)

    assert(addMeta.willUseGpuCpuBridge)

    // Find child meta for xxhash64
    val maybeHashMeta = addMeta.childExprs.find(m => m.wrapped.isInstanceOf[XxHash64])
    assert(maybeHashMeta.isDefined, "Expected xxhash64 under Add")
    val hashMeta = maybeHashMeta.get
    // Siblings share a leaf (a) so CPU is strictly cheaper than GPU.
    assert(hashMeta.willUseGpuCpuBridge, "xxhash64 should prefer GPU on tie under CPU Add")
  }

  test("No CPU requirements stays on GPU (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val mul = Multiply(Literal(2L), a)
    val mulMeta = wrap(mul)
    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(mulMeta)
    assert(!mulMeta.willUseGpuCpuBridge)
  }

  test("CPU children tie at prefers GPU (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val gt = GreaterThan(a, Literal(5L))
    val lt = LessThan(a, Literal(10L))
    val andExpr = And(gt, lt)

    val andMeta = wrap(andExpr)
    // Force both comparisons to CPU bridge
    def forceCompares(meta: BaseExprMeta[_]): Unit = {
      val w = meta.wrapped
      if (w.isInstanceOf[GreaterThan] || w.isInstanceOf[LessThan]) {
        meta.willNotWorkOnGpu("disabled for test")
      }
      meta.childExprs.foreach(forceCompares)
    }
    forceCompares(andMeta)

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(andMeta)

    // Parent AND should prefer GPU on a true tie
    assert(!andMeta.willUseGpuCpuBridge, "AND should prefer GPU on a true tie")

    // Children should be on CPU bridge
    val childCompares = andMeta.childExprs.filter(m => m.wrapped.isInstanceOf[GreaterThan] ||
      m.wrapped.isInstanceOf[LessThan])
    assert(childCompares.size == 2, 
      s"Expected two comparison children under AND: ${andMeta.childExprs}")
    assert(childCompares.forall(_.willUseGpuCpuBridge), 
      "Comparisons should use CPU bridge under AND")
  }

  test("Nested CaseWhen with CPU Add uses bridges only where needed (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val b = AttributeReference("b", LongType, nullable = false)()
    val c = AttributeReference("c", LongType, nullable = false)()

    val thenExpr = Add(Add(a, b), Literal(5L))
    val elseExpr = Add(Add(a, c), Literal(2L))
    val cw = CaseWhen(Seq((GreaterThan(a, Literal(0L)), thenExpr)), Some(elseExpr))

    val root = wrap(cw)
    // Tag and force all Adds to CPU bridge
    def forceAdds(meta: BaseExprMeta[_]): Unit = {
      if (meta.wrapped.isInstanceOf[Add]) meta.willNotWorkOnGpu("disabled for test")
      meta.childExprs.foreach(forceAdds)
    }
    forceAdds(root)

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(root)

    // Verify that all inner Adds are on CPU bridge
    def collectAdds(meta: BaseExprMeta[_], 
      acc: scala.collection.mutable.ListBuffer[BaseExprMeta[_]]): Unit = {
      if (meta.wrapped.isInstanceOf[Add]) acc += meta
      meta.childExprs.foreach(collectAdds(_, acc))
    }
    val adds = scala.collection.mutable.ListBuffer[BaseExprMeta[_]]()
    collectAdds(root, adds)
    assert(adds.nonEmpty, "Expected Add expressions inside CaseWhen")
    assert(adds.forall(_.willUseGpuCpuBridge), s"Expected all Adds on CPU bridge: $adds")
  }

  test("Heuristic: CPU XxHash64 with 13 inputs favors GPU children (unit)") {
    val inputs = (1 to 13).map(i => AttributeReference(s"a$i", LongType, nullable = false)())
    val hash = XxHash64(inputs, 42L)

    val hashMeta = wrap(hash)
    // Force parent to CPU to trigger subset selection
    hashMeta.willNotWorkOnGpu("disabled for test")

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(hashMeta)

    assert(hashMeta.willUseGpuCpuBridge, "Parent XxHash64 should be on CPU bridge")
    // With independent per-child choice under CPU parent, ties favor GPU → expect 
    // no children on CPU
    val childOnCpu = hashMeta.childExprs.filter(_.willUseGpuCpuBridge)
    assert(childOnCpu.isEmpty, s"Expected all children on GPU under " +
      s"CPU XxHash64: ${hashMeta.childExprs}")
  }

  test("Heuristic boundary: 7 vs 8 XxHash64 inputs yield consistent placement (unit)") {
    def run(n: Int): (Boolean, Seq[Boolean]) = {
      val inputs = (1 to n).map(i => AttributeReference(s"a$i", LongType, nullable = false)())
      val hash = XxHash64(inputs, 42L)
      val meta = wrap(hash)
      meta.willNotWorkOnGpu("disabled for test")
      GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(meta)
      (meta.willUseGpuCpuBridge, meta.childExprs.map(_.willUseGpuCpuBridge))
    }

    val (m7, kids7) = run(7)   // exact enumeration path
    val (m8, kids8) = run(8)   // heuristic path

    assert(m7 && m8, "Parents should be on CPU bridge")
    // Expect all children on GPU in both cases under tie-breaking that prefers GPU
    assert(kids7.forall(_ == false), s"Expected all 7 children on GPU: $kids7")
    assert(kids8.forall(_ == false), s"Expected all 8 children on GPU: $kids8")
  }

  test("Heuristic: 13 mixed Add/Multiply children into XxHash64 under CPU (unit)") {
    // Use distinct leaves per child to avoid shared-leaf advantage for CPU
    val children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] =
      (1 to 13).map { i =>
        val ai = AttributeReference(s"a$i", LongType, nullable = false)()
        if ((i & 1) == 0) Add(ai, Literal(i.toLong)) else Multiply(ai, Literal(i.toLong))
      }
    val hash = XxHash64(children, 42L)
    val meta = wrap(hash)
    meta.willNotWorkOnGpu("disabled for test")

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(meta)

    assert(meta.willUseGpuCpuBridge, "Parent XxHash64 should be on CPU bridge")
    // Under the current heuristic, each child compares 
    // cpuCost (0) vs gpuAlt (gpuCost + moveOut = 1),
    // so it strictly prefers CPU per child. The parent-level import charge is added only once
    // after selection, so the heuristic still yields all CPU children here.
    val onCpu = meta.childExprs.map(_.willUseGpuCpuBridge)
    assert(onCpu.forall(_ == true), s"Expected all children on CPU: $onCpu")
  }

  test("Heuristic: shared leaf under CPU parent → CPU strictly cheaper (unit)") {
    // All children share the same leaf, so importing once for the CPU parent is cheaper
    val a = AttributeReference("a", LongType, nullable = false)()
    val children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] =
      (1 to 13).map { i =>
        if ((i & 1) == 0) Add(a, Literal(i.toLong)) else Multiply(a, Literal(i.toLong))
      }
    val hash = XxHash64(children, 42L)
    val meta = wrap(hash)
    // Force parent to CPU
    meta.willNotWorkOnGpu("disabled for test")

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(meta)

    assert(meta.willUseGpuCpuBridge, "Parent XxHash64 should be on CPU bridge")
    val onCpu = meta.childExprs.map(_.willUseGpuCpuBridge)
    assert(onCpu.forall(_ == true), s"Expected all children on CPU: $onCpu")
  }

  test("Heuristic: distinct leaves under GPU parent → GPU strictly cheaper (unit)") {
    // Parent stays on GPU; moving CPU child outputs would cost 1 per child → choose GPU
    val children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] =
      (1 to 13).map { i =>
        val ai = AttributeReference(s"a$i", LongType, nullable = false)()
        if ((i & 1) == 0) Add(ai, Literal(i.toLong)) else Multiply(ai, Literal(i.toLong))
      }
    val hash = XxHash64(children, 42L)
    val meta = wrap(hash)

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(meta)

    assert(!meta.willUseGpuCpuBridge, "Parent should remain on GPU")
    val onCpu = meta.childExprs.map(_.willUseGpuCpuBridge)
    assert(onCpu.forall(_ == false), s"Expected all children on GPU: $onCpu")
  }
}


