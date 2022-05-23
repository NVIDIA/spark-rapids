/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{RowCountPlanVisitor, SparkPlanMeta}

import org.apache.spark.sql.execution.GlobalLimitExec

object GlobalLimitShims {

  /** If the given plan is a GlobalLimitExec */
  def isMe(plan: Any): Boolean = plan match {
    // not use isInstanceOf here, so it can verify the number of parameters.
    case GlobalLimitExec(_, _, _) => true
    case _ => false
  }

  /**
   * Estimate the number of rows for a GlobalLimitExec.
   * It will blow up if the plan is not a GlobalLimitExec.
   */
  def visit(plan: SparkPlanMeta[_]): Option[BigInt] = plan.wrapped match {
    case GlobalLimitExec(limit, _, offset) =>
      // offset is ignored now, but we should support it.
      RowCountPlanVisitor.visit(plan.childPlans.head).map(_.min(limit)).orElse(Some(limit))
    case op =>
      throw new IllegalArgumentException("Not a GlobalLimitExec")
  }
}
