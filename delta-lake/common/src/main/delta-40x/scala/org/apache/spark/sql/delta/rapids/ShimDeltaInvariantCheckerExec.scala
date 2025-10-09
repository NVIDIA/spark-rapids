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

package org.apache.spark.sql.delta.rapids

import org.apache.spark.sql.delta.constraints.{CheckDeltaInvariant, Constraint, DeltaInvariantCheckerExec}
import org.apache.spark.sql.execution.SparkPlan

/**
 * Per-version shim for constructing the CPU Delta invariant checker exec.
 *
 * Delta 4.0 expects a sequence of CheckDeltaInvariant already built.
 */
object ShimDeltaInvariantCheckerExec {
  def apply(
      plan: SparkPlan,
      constraints: Seq[Constraint],
      invariants: Seq[CheckDeltaInvariant]): SparkPlan = {
    DeltaInvariantCheckerExec(plan, invariants)
  }
}
