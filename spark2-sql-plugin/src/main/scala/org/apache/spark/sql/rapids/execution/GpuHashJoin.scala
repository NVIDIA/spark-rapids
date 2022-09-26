/*
 * Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
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
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims._

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, FullOuter, Inner, InnerLike, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._

object JoinTypeChecks {
  def tagForGpu(joinType: JoinType, meta: RapidsMeta[_, _]): Unit = {
    val conf = meta.conf
    joinType match {
      case Inner if !conf.areInnerJoinsEnabled =>
        meta.willNotWorkOnGpu("inner joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_INNER_JOIN.key} to true")
      case Cross if !conf.areCrossJoinsEnabled =>
        meta.willNotWorkOnGpu("cross joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_CROSS_JOIN.key} to true")
      case LeftOuter if !conf.areLeftOuterJoinsEnabled =>
        meta.willNotWorkOnGpu("left outer joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_LEFT_OUTER_JOIN.key} to true")
      case RightOuter if !conf.areRightOuterJoinsEnabled =>
        meta.willNotWorkOnGpu("right outer joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_RIGHT_OUTER_JOIN.key} to true")
      case FullOuter if !conf.areFullOuterJoinsEnabled =>
        meta.willNotWorkOnGpu("full outer joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_FULL_OUTER_JOIN.key} to true")
      case LeftSemi if !conf.areLeftSemiJoinsEnabled =>
        meta.willNotWorkOnGpu("left semi joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_LEFT_SEMI_JOIN.key} to true")
      case LeftAnti if !conf.areLeftAntiJoinsEnabled =>
        meta.willNotWorkOnGpu("left anti joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_LEFT_ANTI_JOIN.key} to true")
      case ExistenceJoin(_) if !conf.areExistenceJoinsEnabled =>
        meta.willNotWorkOnGpu("existence joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_EXISTENCE_JOIN.key} to true")
      case _ => // not disabled
    }
  }

  val LEFT_KEYS = "leftKeys"
  val RIGHT_KEYS = "rightKeys"
  val CONDITION = "condition"

  private[this] val cudfSupportedKeyTypes =
    (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.STRUCT).nested()
  private[this] val sparkSupportedJoinKeyTypes = TypeSig.all - TypeSig.MAP.nested()

  private[this] val joinRideAlongTypes =
    (cudfSupportedKeyTypes + TypeSig.DECIMAL_128 + TypeSig.BINARY +
        TypeSig.ARRAY + TypeSig.MAP).nested()

  val equiJoinExecChecks: ExecChecks = ExecChecks(
    joinRideAlongTypes,
    TypeSig.all,
    Map(
      LEFT_KEYS -> InputCheck(cudfSupportedKeyTypes, sparkSupportedJoinKeyTypes),
      RIGHT_KEYS -> InputCheck(cudfSupportedKeyTypes, sparkSupportedJoinKeyTypes),
      CONDITION -> InputCheck(TypeSig.BOOLEAN, TypeSig.BOOLEAN)))

  def equiJoinMeta(leftKeys: Seq[BaseExprMeta[_]],
      rightKeys: Seq[BaseExprMeta[_]],
      condition: Option[BaseExprMeta[_]]): Map[String, Seq[BaseExprMeta[_]]] = {
    Map(
      LEFT_KEYS -> leftKeys,
      RIGHT_KEYS -> rightKeys,
      CONDITION -> condition.toSeq)
  }

  val nonEquiJoinChecks: ExecChecks = ExecChecks(
    joinRideAlongTypes,
    TypeSig.all,
    Map(CONDITION -> InputCheck(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
      notes = List("A non-inner join only is supported if the condition expression can be " +
          "converted to a GPU AST expression"))))

  def nonEquiJoinMeta(condition: Option[BaseExprMeta[_]]): Map[String, Seq[BaseExprMeta[_]]] =
    Map(CONDITION -> condition.toSeq)
}

object GpuHashJoin {

  def tagJoin(
      meta: SparkPlanMeta[_],
      joinType: JoinType,
      buildSide: GpuBuildSide,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      conditionMeta: Option[BaseExprMeta[_]]): Unit = {
    val keyDataTypes = (leftKeys ++ rightKeys).map(_.dataType)

    JoinTypeChecks.tagForGpu(joinType, meta)
    joinType match {
      case _: InnerLike =>
      case RightOuter | LeftOuter | LeftSemi | LeftAnti | ExistenceJoin(_) =>
        conditionMeta.foreach(meta.requireAstForGpuOn)
      case FullOuter =>
        conditionMeta.foreach(meta.requireAstForGpuOn)
        // FullOuter join cannot support with struct keys as two issues below
        //  * https://github.com/NVIDIA/spark-rapids/issues/2126
        //  * https://github.com/rapidsai/cudf/issues/7947
        if (keyDataTypes.exists(_.isInstanceOf[StructType])) {
          meta.willNotWorkOnGpu(s"$joinType joins currently do not support with struct keys")
        }
      case _ =>
        meta.willNotWorkOnGpu(s"$joinType currently is not supported")
    }
    buildSide match {
      case GpuBuildLeft if !canBuildLeft(joinType) =>
        meta.willNotWorkOnGpu(s"$joinType does not support left-side build")
      case GpuBuildRight if !canBuildRight(joinType) =>
        meta.willNotWorkOnGpu(s"$joinType does not support right-side build")
      case _ =>
    }
  }

  /** Determine if this type of join supports using the right side of the join as the build side. */
  def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
    case _ => false
  }

  /** Determine if this type of join supports using the left side of the join as the build side. */
  def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | RightOuter | FullOuter => true
    case _ => false
  }
}
