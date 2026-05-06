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

import scala.util.control.NonFatal

import org.apache.spark.sql.internal.SQLConf

/**
 * Single source of truth for the CuBF runtime-feedback feature-flag keys and the gate that both
 * call sites use to decide whether to wire optional observability accumulators.
 *
 * Today the gate is the AND of two flags; flag arity is an implementation detail of `isEnabled`.
 * Every flag defaults to `false`. All must parse to `true` for the gate to open.
 *
 * Consumers:
 *   - `BloomFilterShims.resolveProbeWiring` — probe-side `BloomFilterProbeAccumulator` wiring.
 *   - `InlineBFBuildReplacement.resolveBuildCostUpdaters` — build-side
 *     `BloomFilterBuildCostAccumulator` wiring.
 *
 * When the gate is closed, both call sites return inert state and the GPU operators run without
 * instrumentation, exactly as if the optional planner module were absent.
 */
object CuBFFeedbackFlags {

  private[rapids] val RUNTIME_FEEDBACK_ENABLED_KEY =
    "spark.rapids.sql.cuBloomFilter.runtimeFeedback.enabled"

  private[rapids] val RUNTIME_FEEDBACK_INSTRUMENTATION_ENABLED_KEY =
    "spark.rapids.sql.cuBloomFilter.runtimeFeedback.instrumentation.enabled"

  /**
   * Gate predicate over the runtime-feedback flags.
   *
   * Returns `true` only when every gating flag is present in the active `SQLConf` and parses to
   * `true`. Returns `false` for any value that fails to parse as a boolean (`yes` / `no` / `1` /
   * empty / etc.). Both call sites depend on this fail-closed behavior to keep observability
   * optional: a malformed flag value must never fail the query — it must silently disable
   * instrumentation.
   */
  def isEnabled(conf: SQLConf): Boolean =
    flagEnabled(conf, RUNTIME_FEEDBACK_ENABLED_KEY) &&
      flagEnabled(conf, RUNTIME_FEEDBACK_INSTRUMENTATION_ENABLED_KEY)

  private def flagEnabled(conf: SQLConf, key: String): Boolean = {
    try conf.getConfString(key, "false").toBoolean
    catch { case NonFatal(_) => false }
  }
}
