/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2._

trait Spark31Xuntil33XShims extends SparkShims {

  def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan] = {
    GpuOverrides.neverReplaceExec[ShowCurrentNamespaceExec]("Namespace metadata operation")
  }
}

// First, Last and Collect have mistakenly been marked as non-deterministic until Spark-3.3.
// They are actually deterministic iff their child expression is deterministic.
trait GpuDeterministicFirstLastCollectShim extends Expression {
  override lazy val deterministic = false
}
