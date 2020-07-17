/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.GpuMetricNames._
import org.apache.spark.sql.rapids.execution._

import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.internal.Logging



case class GpuBroadcastNestedLoopJoinExec31(
    left: SparkPlan,
    right: SparkPlan,
    join: BroadcastNestedLoopJoinExec,
    joinType: JoinType,
    condition: Option[Expression]) extends GpuBroadcastNestedLoopJoinExecBase(left, right, join, joinType, condition) with Logging {


  def getBuildSide: GpuBuildSide = {
    join.buildSide match {
      case BuildRight => GpuBuildRight
      case BuildLeft => GpuBuildLeft
      case _ => throw new Exception("unknown buildSide Type")
    }
  }
}

object GpuBroadcastNestedLoopJoinExec31 extends Logging {

  def createInstance(
      left: SparkPlan,
      right: SparkPlan,
      join: BroadcastNestedLoopJoinExec,
      joinType: JoinType,
      condition: Option[Expression]): GpuBroadcastNestedLoopJoinExecBase = {
    
    GpuBroadcastNestedLoopJoinExec31(left, right, join, joinType, condition)
  }

}
