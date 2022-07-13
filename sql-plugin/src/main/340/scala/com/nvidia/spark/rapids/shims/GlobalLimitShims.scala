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

  /**
   * Estimate the number of rows for a GlobalLimitExec.
   */
  def visit(plan: SparkPlanMeta[GlobalLimitExec]): Option[BigInt] = {
    // offset is introduce in spark-3.4.0
    val offset = plan.wrapped.limit
    val limit = plan.wrapped.limit
    val sliced = if (limit > 0) {
      Some(limit - offset)
    } else {
      // limit can be -1, meaning no limit
      None
    }
    RowCountPlanVisitor.visit(plan.childPlans.head)
      .map { rowNum =>
        val remaining = Math.max(rowNum - offset, 0)
        sliced.map(_.min(remaining)).getOrElse(remaining)
      }
      .orElse(sliced)
  }
}
