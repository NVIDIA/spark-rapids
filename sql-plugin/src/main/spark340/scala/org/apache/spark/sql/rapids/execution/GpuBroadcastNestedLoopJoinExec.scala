/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "340"}
{"spark": "341"}
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, InnerLike, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec


class GpuBroadcastNestedLoopJoinMeta(
    join: BroadcastNestedLoopJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBroadcastNestedLoopJoinMetaBase(join, conf, parent, rule) {

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    // The broadcast part of this must be a BroadcastExchangeExec
    val buildSide = gpuBuildSide match {
      case GpuBuildLeft => left
      case GpuBuildRight => right
    }
    verifyBuildSideWasReplaced(buildSide)

    val condition = conditionMeta.map(_.convertToGpu())
    val isAstCondition = conditionMeta.forall(_.canThisBeAst)
    join.joinType match {
      case _: InnerLike =>
      case LeftOuter | LeftSemi | LeftAnti if gpuBuildSide == GpuBuildLeft =>
        throw new IllegalStateException(s"Unsupported build side for join type ${join.joinType}")
      case RightOuter if gpuBuildSide == GpuBuildRight =>
        throw new IllegalStateException(s"Unsupported build side for join type ${join.joinType}")
      case LeftOuter | RightOuter | LeftSemi | LeftAnti | ExistenceJoin(_) =>
        // Cannot post-filter these types of joins
        assert(isAstCondition, s"Non-AST condition in ${join.joinType}")
      case _ => throw new IllegalStateException(s"Unsupported join type ${join.joinType}")
    }

    val joinExec = GpuBroadcastNestedLoopJoinExec(
      left, right,
      join.joinType, gpuBuildSide,
      if (isAstCondition) condition else None,
      conf.gpuTargetBatchSizeBytes)
    if (isAstCondition) {
      joinExec
    } else {
      // condition cannot be implemented via AST so fallback to a post-filter if necessary
      condition.map {
        // TODO: Restore batch coalescing logic here.
        // Avoid requesting a post-filter-coalesce here, as we've seen poor performance with
        // the cross join microbenchmark. This is a short-term hack for the benchmark, and
        // ultimately this should be solved with the resolution of one or more of the following:
        // https://github.com/NVIDIA/spark-rapids/issues/3749
        // https://github.com/NVIDIA/spark-rapids/issues/3750
        c => GpuFilterExec(c, joinExec)(coalesceAfter = false)
      }.getOrElse(joinExec)
    }
  }
}


case class GpuBroadcastNestedLoopJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    joinType: JoinType,
    gpuBuildSide: GpuBuildSide,
    condition: Option[Expression],
    targetSizeBytes: Long) extends GpuBroadcastNestedLoopJoinExecBase(
      left, right, joinType, gpuBuildSide, condition, targetSizeBytes
    )
