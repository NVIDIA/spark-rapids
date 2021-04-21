/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark301

import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.spark300.{GpuShuffledHashJoinMeta, GpuSortMergeJoinMeta, Spark300Shims}
import com.nvidia.spark.rapids.spark301.RapidsShuffleManager

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{First, Last}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExecBase, GpuShuffleExchangeExecBase}
import org.apache.spark.sql.types.DataType
import org.apache.spark.storage.{BlockId, BlockManagerId}

class Spark301Shims extends Spark300Shims {

  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    super.getExecs ++ Seq(
      GpuOverrides.exec[SortMergeJoinExec](
        "Sort merge join, replacing with shuffled hash join",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.ARRAY +
          TypeSig.STRUCT).nested(TypeSig.commonCudfTypes + TypeSig.NULL), TypeSig.all),
        (join, conf, p, r) => new GpuSortMergeJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[BroadcastHashJoinExec](
        "Implementation of join using broadcast data",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.ARRAY +
          TypeSig.STRUCT).nested(TypeSig.commonCudfTypes + TypeSig.NULL), TypeSig.all),
        (join, conf, p, r) => new GpuBroadcastHashJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[ShuffledHashJoinExec](
        "Implementation of join using hashed shuffled data",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.ARRAY +
          TypeSig.STRUCT).nested(TypeSig.commonCudfTypes + TypeSig.NULL), TypeSig.all),
        (join, conf, p, r) => new GpuShuffledHashJoinMeta(join, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r))
  }

  def exprs301: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[First](
      "first aggregate operator",
      ExprChecks.aggNotWindow(TypeSig.commonCudfTypes + TypeSig.NULL, TypeSig.all,
        Seq(ParamCheck("input", TypeSig.commonCudfTypes + TypeSig.NULL, TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[First](a, conf, p, r) {
        override def convertToGpu(): GpuExpression =
          GpuFirst(childExprs.head.convertToGpu(), a.ignoreNulls)
      }),
    GpuOverrides.expr[Last](
      "last aggregate operator",
      ExprChecks.aggNotWindow(TypeSig.commonCudfTypes + TypeSig.NULL, TypeSig.all,
        Seq(ParamCheck("input", TypeSig.commonCudfTypes + TypeSig.NULL, TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[Last](a, conf, p, r) {
        override def convertToGpu(): GpuExpression =
          GpuLast(childExprs.head.convertToGpu(), a.ignoreNulls)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    super.getExprs ++ exprs301
  }

  override def getRapidsShuffleManagerClass: String = {
    classOf[RapidsShuffleManager].getCanonicalName
  }

  override def getMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    SparkEnv.get.mapOutputTracker.getMapSizesByRange(shuffleId,
      startMapIndex, endMapIndex, startPartition, endPartition)
  }

  override def getGpuBroadcastExchangeExec(
      mode: BroadcastMode,
      child: SparkPlan): GpuBroadcastExchangeExecBase = {
    GpuBroadcastExchangeExec(mode, child)
  }

  override def getGpuShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan,
      cpuShuffle: Option[ShuffleExchangeExec]): GpuShuffleExchangeExecBase = {
    val canChangeNumPartitions = cpuShuffle.forall(_.canChangeNumPartitions)
    GpuShuffleExchangeExec(outputPartitioning, child, canChangeNumPartitions)
  }

  override def getGpuShuffleExchangeExec(
      queryStage: ShuffleQueryStageExec): GpuShuffleExchangeExecBase = {
    queryStage.shuffle.asInstanceOf[GpuShuffleExchangeExecBase]
  }

  override def isGpuBroadcastHashJoin(plan: SparkPlan): Boolean = {
    plan match {
      case _: GpuBroadcastHashJoinExec => true
      case _ => false
    }
  }

  override def isBroadcastExchangeLike(plan: SparkPlan): Boolean =
    plan.isInstanceOf[BroadcastExchangeLike]

  override def isShuffleExchangeLike(plan: SparkPlan): Boolean =
    plan.isInstanceOf[ShuffleExchangeLike]

  override def getQueryStageRuntimeStatistics(qs: QueryStageExec): Statistics =
    qs.getRuntimeStatistics

  override def injectQueryStagePrepRule(
      extensions: SparkSessionExtensions,
      ruleBuilder: SparkSession => Rule[SparkPlan]): Unit = {
    extensions.injectQueryStagePrepRule(ruleBuilder)
  }

  /**
   * Return list of matching predicates present in the plan
   * This is in shim due to changes in ShuffleQueryStageExec between Spark versions.
   */
  override def findOperators(plan: SparkPlan, predicate: SparkPlan => Boolean): Seq[SparkPlan] = {
    def recurse(
        plan: SparkPlan,
        predicate: SparkPlan => Boolean,
        accum: ListBuffer[SparkPlan]): Seq[SparkPlan] = {
      plan match {
        case _ if predicate(plan) =>
          accum += plan
          plan.children.flatMap(p => recurse(p, predicate, accum)).headOption
        case a: AdaptiveSparkPlanExec => recurse(a.executedPlan, predicate, accum)
        case qs: BroadcastQueryStageExec => recurse(qs.broadcast, predicate, accum)
        case qs: ShuffleQueryStageExec => recurse(qs.shuffle, predicate, accum)
        case other => other.children.flatMap(p => recurse(p, predicate, accum)).headOption
      }
      accum
    }
    recurse(plan, predicate, new ListBuffer[SparkPlan]())
  }
}
