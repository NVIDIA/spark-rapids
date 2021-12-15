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

package com.nvidia.spark.rapids.shims.v2

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{FullOuter, InnerLike, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.rapids.execution.{GpuShuffledNestedLoopJoinExec, JoinTypeChecks}
import org.apache.spark.sql.types.StructType

object GpuJoinUtils {
  def getGpuBuildSide(buildSide: BuildSide): GpuBuildSide = {
    buildSide match {
      case BuildRight => GpuBuildRight
      case BuildLeft => GpuBuildLeft
      case _ => throw new Exception("unknown buildSide Type")
    }
  }
}

class GpuShuffledHashJoinMeta(
    join: ShuffledHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[ShuffledHashJoinExec](join, conf, parent, rule) {
  val leftKeys: Seq[BaseExprMeta[_]] =
    join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val rightKeys: Seq[BaseExprMeta[_]] =
    join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val conditionMeta: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val equalityExprs = join.leftKeys.zip(join.rightKeys).map {
    case (l, r) => EqualTo(l, r)
  }
  val nestedConditionExpr: Option[Expression] = join.condition match {
    case Some(joinExpr) => Some(equalityExprs.foldRight(joinExpr) {
      case (equalExpr, rest) => And(equalExpr, rest)
    })
    case None => None
  }
  val nestedCondition: Option[BaseExprMeta[_]] =
    nestedConditionExpr.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  def canReplaceWithNestedLoopJoin(): Boolean = {
    conf.enableReplaceConditionalHashJoin && join.condition.isDefined &&
        nestedCondition.forall(_.canThisBeAst)
  }

  override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ conditionMeta

  override val namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.equiJoinMeta(leftKeys, rightKeys, conditionMeta)

  override def tagPlanForGpu(): Unit = {
    //GpuHashJoin.tagJoin(this, join.joinType, join.leftKeys, join.rightKeys, join.condition)

    val joinType = join.joinType
    val keyDataTypes = (leftKeys ++ rightKeys).map(_.dataType)

    def unSupportNonEqualCondition(): Unit = if (join.condition.isDefined) {
      this.willNotWorkOnGpu(s"$joinType joins currently do not support conditions")
    }
    def unSupportNonEqualNonAst(): Unit = if (join.condition.isDefined) {
      if (conf.enableReplaceConditionalHashJoin) {
        nestedCondition.foreach(requireAstForGpuOn)
      } else {
        this.willNotWorkOnGpu(s"$joinType joins currently do not support conditions")
      }
    }
    def unSupportStructKeys(): Unit = if (keyDataTypes.exists(_.isInstanceOf[StructType])) {
      this.willNotWorkOnGpu(s"$joinType joins currently do not support with struct keys")
    }

    JoinTypeChecks.tagForGpu(joinType, this)
    joinType match {
      case _: InnerLike =>
      case RightOuter | LeftOuter | LeftSemi | LeftAnti =>
        unSupportNonEqualNonAst()
      case FullOuter =>
        unSupportNonEqualCondition()
        // FullOuter join cannot support with struct keys as two issues below
        //  * https://github.com/NVIDIA/spark-rapids/issues/2126
        //  * https://github.com/rapidsai/cudf/issues/7947
        unSupportStructKeys()
      case _ =>
        this.willNotWorkOnGpu(s"$joinType currently is not supported")
    }
  }

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    val substituteShuffledNestedLoopJoin: Boolean = join.joinType match {
      case _: InnerLike => canReplaceWithNestedLoopJoin()
      case RightOuter | LeftOuter | LeftSemi | LeftAnti  => canReplaceWithNestedLoopJoin()
      case _ => false
    }

    if (substituteShuffledNestedLoopJoin) {
      GpuShuffledNestedLoopJoinExec(
        left,
        right,
        join.joinType,
        GpuJoinUtils.getGpuBuildSide(join.buildSide),
        nestedCondition.map(_.convertToGpu()),
        false,
        join.leftKeys,
        join.rightKeys)
    } else {
      val joinExec = GpuShuffledHashJoinExec(
        leftKeys.map(_.convertToGpu()),
        rightKeys.map(_.convertToGpu()),
        join.joinType,
        GpuJoinUtils.getGpuBuildSide(join.buildSide),
        None,
        left,
        right,
        isSkewJoin = false)(
        join.leftKeys,
        join.rightKeys)
      // The GPU does not yet support conditional joins, so conditions are implemented
      // as a filter after the join when possible.
      conditionMeta.map(c => GpuFilterExec(c.convertToGpu(), joinExec)).getOrElse(joinExec)
    }
  }
}

case class GpuShuffledHashJoinExec(
    override val leftKeys: Seq[Expression],
    override val rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    override val condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    override val isSkewJoin: Boolean)(
    cpuLeftKeys: Seq[Expression],
    cpuRightKeys: Seq[Expression])
  extends GpuShuffledHashJoinBase(
    buildSide,
    condition,
    isSkewJoin = isSkewJoin,
    cpuLeftKeys,
    cpuRightKeys) {

  override def otherCopyArgs: Seq[AnyRef] = cpuLeftKeys :: cpuRightKeys :: Nil
}
