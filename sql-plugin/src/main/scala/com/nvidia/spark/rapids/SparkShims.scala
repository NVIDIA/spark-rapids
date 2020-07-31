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

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.rapids.execution.GpuBroadcastNestedLoopJoinExecBase
import org.apache.spark.sql.types._
import org.apache.spark.storage.{BlockId, BlockManagerId}

/**
 * Spark BuildSide, BuildRight, BuildLeft moved packages in Spark 3.1
 * so create GPU versions of these that can be agnostic to Spark version.
 */
sealed abstract class GpuBuildSide

case object GpuBuildRight extends GpuBuildSide

case object GpuBuildLeft extends GpuBuildSide

trait SparkShims {
  def isGpuHashJoin(plan: SparkPlan): Boolean
  def isGpuBroadcastHashJoin(plan: SparkPlan): Boolean
  def isGpuShuffledHashJoin(plan: SparkPlan): Boolean
  def getRapidsShuffleManagerClass: String
  def getBuildSide(join: HashJoin): GpuBuildSide
  def getBuildSide(join: BroadcastNestedLoopJoinExec): GpuBuildSide
  def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]]
  def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]]
  def getScalaUDFAsExpression(
    function: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil,
    outputEncoder: Option[ExpressionEncoder[_]] = None,
    udfName: Option[String] = None,
    nullable: Boolean = true,
    udfDeterministic: Boolean = true): Expression

  def getGpuBroadcastNestedLoopJoinShim(
    left: SparkPlan,
    right: SparkPlan,
    join: BroadcastNestedLoopJoinExec,
    joinType: JoinType,
    condition: Option[Expression],
    targetSizeBytes: Long): GpuBroadcastNestedLoopJoinExecBase

  def getMapSizesByExecutorId(
    shuffleId: Int,
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])]

  def injectQueryStagePrepRule(
      extensions: SparkSessionExtensions,
      rule: SparkSession => Rule[SparkPlan])
}
