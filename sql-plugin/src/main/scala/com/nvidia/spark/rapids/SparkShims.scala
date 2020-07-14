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

package com.nvidia.spark.rapids

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

sealed abstract class GpuBuildSide

case object GpuBuildRight extends GpuBuildSide

case object GpuBuildLeft extends GpuBuildSide


trait SparkShims {

  def isGpuHashJoin(plan: SparkPlan): Boolean
  def getBuildSide(join: ShuffledHashJoinExec): GpuBuildSide
  def getBuildSide(join: BroadcastNestedLoopJoinExec): GpuBuildSide
  def getBuildSide(join: BroadcastHashJoinExec): GpuBuildSide
  def getExprs: Seq[ExprRule[_ <: Expression]]
  def getExecs: Seq[ExecRule[_ <: SparkPlan]]

}


