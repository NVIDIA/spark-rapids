/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark311db

import com.nvidia.spark.rapids.GpuBuildSide

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.rapids.execution._

/**
 *  Spark 3.1 changed packages of BuildLeft, BuildRight, BuildSide
 */
case class GpuBroadcastNestedLoopJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    join: BroadcastNestedLoopJoinExec,
    joinType: JoinType,
    condition: Option[Expression],
    targetSizeBytes: Long)
  extends GpuBroadcastNestedLoopJoinExecBase(left, right, joinType, condition,
    targetSizeBytes) {

  def getGpuBuildSide: GpuBuildSide = {
    GpuJoinUtils.getGpuBuildSide(join.buildSide)
  }
}
