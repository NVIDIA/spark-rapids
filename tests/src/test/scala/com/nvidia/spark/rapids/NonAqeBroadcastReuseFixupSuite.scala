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

package com.nvidia.spark.rapids

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.execution.{FilterExec, RangeExec, SparkPlan}
import org.apache.spark.sql.execution.{InSubqueryExec => SparkInSubqueryExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExec, GpuSubqueryBroadcastExec}

/**
 * Unit coverage for `GpuTransitionOverrides.fixupNonAdaptiveBroadcastReuse`, the non-AQE
 * counterpart of `fixupAdaptiveExchangeReuse` already exercised by `ReusedExchangeFixupSuite`.
 *
 * The non-AQE fixup only fires for a `GpuBroadcastExchangeExec` that lives inside a
 * `GpuSubqueryBroadcastExec` referenced by an `ExecSubqueryExpression` (DPP). A faithful
 * test plan therefore needs to wire up both the main-plan broadcast and a hand-built DPP
 * subquery expression.
 */
class NonAqeBroadcastReuseFixupSuite extends SparkQueryCompareTestSuite {

  private val baseConf: SparkConf = new SparkConf()
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.exchange.reuse", "true")
      .set(RapidsConf.ENABLE_NON_AQE_BROADCAST_REUSE_FIXUP.key, "true")

  private def newRange(): RangeExec = RangeExec(Range(1, 2, 1, Some(1)))

  private def newGpuBroadcast(child: RangeExec): GpuBroadcastExchangeExec = {
    val mode = HashedRelationBroadcastMode(child.output)
    GpuBroadcastExchangeExec(mode, child)(BroadcastExchangeExec(mode, child))
  }

  private def newGpuSubqueryBroadcast(child: GpuBroadcastExchangeExec): GpuSubqueryBroadcastExec = {
    val keys = Seq(child.child.asInstanceOf[RangeExec].output.head)
    GpuSubqueryBroadcastExec("dpp", Seq(0), keys, child)(modeKeys = Some(keys))
  }

  /**
   * Build a synthetic non-AQE DPP plan:
   *
   *   FilterExec(
   *     condition = InSubqueryExec(plan = GpuSubqueryBroadcastExec(child = G2)),
   *     child     = G1   // main-plan GpuBroadcastExchangeExec
   *   )
   *
   * G1 and G2 share `(mode.canonicalized, child.canonicalized)` so the fixup should rewrite
   * the DPP-side broadcast (G2) into `ReusedExchangeExec` pointing at G1.
   */
  private def buildDppPlan(): (SparkPlan, GpuBroadcastExchangeExec, GpuBroadcastExchangeExec) = {
    val range = newRange()
    val mainG = newGpuBroadcast(range)
    val dppG = newGpuBroadcast(range)
    val gsb = newGpuSubqueryBroadcast(dppG)
    val inSub = SparkInSubqueryExec(range.output.head, gsb, NamedExpression.newExprId)
    (FilterExec(inSub, mainG), mainG, dppG)
  }

  private def reusedExchangeChildren(p: SparkPlan): Seq[SparkPlan] = {
    val collected = scala.collection.mutable.ArrayBuffer.empty[SparkPlan]
    p.foreach { node =>
      node.expressions.foreach(_.foreach {
        case sub: SparkInSubqueryExec =>
          sub.plan match {
            case gsb: GpuSubqueryBroadcastExec =>
              gsb.child match {
                case r: ReusedExchangeExec => collected += r.child
                case _ =>
              }
            case _ =>
          }
        case _ =>
      })
    }
    collected.toSeq
  }

  test("fixupNonAdaptiveBroadcastReuse rewrites matching DPP broadcast to ReusedExchangeExec") {
    withGpuSparkSession(_ => {
      val (plan, mainG, _) = buildDppPlan()
      val updated = new GpuTransitionOverrides().fixupNonAdaptiveBroadcastReuse(plan)

      val reusedChildren = reusedExchangeChildren(updated)
      assert(reusedChildren.size == 1,
        s"expected exactly one ReusedExchangeExec under the GpuSubqueryBroadcastExec, got " +
          s"${reusedChildren.size} in:\n${updated.treeString}")
      assert(reusedChildren.head eq mainG,
        s"expected the ReusedExchangeExec to point at the main-plan broadcast G1, got " +
          s"${reusedChildren.head}")
    }, baseConf)
  }

  test("fixupNonAdaptiveBroadcastReuse leaves plans with no main-plan broadcast unchanged") {
    // No GpuBroadcastExchangeExec in the main plan; just a RangeExec wrapped in a Filter whose
    // condition still references a DPP-side GpuSubqueryBroadcastExec. The early-exit at the
    // `if (mainPlanBroadcasts.isEmpty) return p` line in fixupNonAdaptiveBroadcastReuse should
    // return the input plan unmodified.
    withGpuSparkSession(_ => {
      val range = newRange()
      val dppG = newGpuBroadcast(range)
      val gsb = newGpuSubqueryBroadcast(dppG)
      val inSub = SparkInSubqueryExec(range.output.head, gsb, NamedExpression.newExprId)
      val plan = FilterExec(inSub, range)

      val updated = new GpuTransitionOverrides().fixupNonAdaptiveBroadcastReuse(plan)
      assert(updated eq plan,
        s"expected the plan to be returned unchanged when mainPlanBroadcasts.isEmpty, got:\n" +
          s"${updated.treeString}")
    }, baseConf)
  }

  test("ENABLE_NON_AQE_BROADCAST_REUSE_FIXUP kill switch disables the fixup gate") {
    // The non-AQE fixup is invoked from GpuTransitionOverrides.apply only when
    // RapidsConf.isNonAqeBroadcastReuseFixupEnabled is true (the gate). Verify the kill switch
    // flips the gate condition to false.
    val killSwitchConf = baseConf.clone()
        .set(RapidsConf.ENABLE_NON_AQE_BROADCAST_REUSE_FIXUP.key, "false")
    withGpuSparkSession(spark => {
      val rapidsConf = new RapidsConf(spark.sessionState.conf)
      assert(!rapidsConf.isNonAqeBroadcastReuseFixupEnabled,
        "isNonAqeBroadcastReuseFixupEnabled should be false when the kill switch is set")
    }, killSwitchConf)

    withGpuSparkSession(spark => {
      val rapidsConf = new RapidsConf(spark.sessionState.conf)
      assert(rapidsConf.isNonAqeBroadcastReuseFixupEnabled,
        "isNonAqeBroadcastReuseFixupEnabled should be true with the default conf (createWithDefault(true))")
    }, baseConf)
  }
}
