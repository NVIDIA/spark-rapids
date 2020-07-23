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

package com.nvidia.spark.rapids.shims.spark300

import java.time.ZoneId

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.spark300.RapidsShuffleManager

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.aggregate.{First, Last}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, HashJoin, SortMergeJoinExec}
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.rapids.GpuTimeSub
import org.apache.spark.sql.rapids.execution.GpuBroadcastNestedLoopJoinExecBase
import org.apache.spark.sql.rapids.shims.spark300._
import org.apache.spark.sql.types._
import org.apache.spark.storage.{BlockId, BlockManagerId}
import org.apache.spark.unsafe.types.CalendarInterval

class Spark300Shims extends SparkShims {

  override def getScalaUDFAsExpression(
      function: AnyRef,
      dataType: DataType,
      children: Seq[Expression],
      inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil,
      outputEncoder: Option[ExpressionEncoder[_]] = None,
      udfName: Option[String] = None,
      nullable: Boolean = true,
      udfDeterministic: Boolean = true): Expression = {
    // outputEncoder is only used in Spark 3.1+
    ScalaUDF(function, dataType, children, inputEncoders, udfName, nullable, udfDeterministic)
  }

  override def getMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    // startMapIndex and endMapIndex ignored as we don't support those for gpu shuffle.
    SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(shuffleId, startPartition, endPartition)
  }

  override def getGpuBroadcastNestedLoopJoinShim(
      left: SparkPlan,
      right: SparkPlan,
      join: BroadcastNestedLoopJoinExec,
      joinType: JoinType,
      condition: Option[Expression],
      targetSizeBytes: Long): GpuBroadcastNestedLoopJoinExecBase = {
    GpuBroadcastNestedLoopJoinExec(left, right, join, joinType, condition, targetSizeBytes)
  }

  override def isGpuHashJoin(plan: SparkPlan): Boolean = {
    plan match {
      case _: GpuHashJoin => true
      case p => false
    }
  }

  override def isGpuBroadcastHashJoin(plan: SparkPlan): Boolean = {
    plan match {
      case _: GpuBroadcastHashJoinExec => true
      case p => false
    }
  }

  override def isGpuShuffledHashJoin(plan: SparkPlan): Boolean = {
    plan match {
      case _: GpuShuffledHashJoinExec => true
      case p => false
    }
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    Seq(
      GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {
          // partition filters and data filters are not run on the GPU
          override val childExprs: Seq[ExprMeta[_]] = Seq.empty

          override def tagPlanForGpu(): Unit = GpuFileSourceScanExec.tagSupport(this)

          override def convertToGpu(): GpuExec = {
            val newRelation = HadoopFsRelation(
              wrapped.relation.location,
              wrapped.relation.partitionSchema,
              wrapped.relation.dataSchema,
              wrapped.relation.bucketSpec,
              GpuFileSourceScanExec.convertFileFormat(wrapped.relation.fileFormat),
              wrapped.relation.options)(wrapped.relation.sparkSession)
            GpuFileSourceScanExec(
              newRelation,
              wrapped.output,
              wrapped.requiredSchema,
              wrapped.partitionFilters,
              wrapped.optionalBucketSet,
              wrapped.dataFilters,
              wrapped.tableIdentifier)
          }
        }),
      GpuOverrides.exec[SortMergeJoinExec](
        "Sort merge join, replacing with shuffled hash join",
        (join, conf, p, r) => new GpuSortMergeJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[BroadcastHashJoinExec](
        "Implementation of join using broadcast data",
        (join, conf, p, r) => new GpuBroadcastHashJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[ShuffledHashJoinExec](
        "Implementation of join using hashed shuffled data",
        (join, conf, p, r) => new GpuShuffledHashJoinMeta(join, conf, p, r)),
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
  }

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    Seq(
      GpuOverrides.expr[TimeSub](
        "Subtracts interval from timestamp",
        (a, conf, p, r) => new BinaryExprMeta[TimeSub](a, conf, p, r) {
          override def tagExprForGpu(): Unit = {
            a.interval match {
              case Literal(intvl: CalendarInterval, DataTypes.CalendarIntervalType) =>
                if (intvl.months != 0) {
                  willNotWorkOnGpu("interval months isn't supported")
                }
              case _ =>
                willNotWorkOnGpu("only literals are supported for intervals")
            }
            if (ZoneId.of(a.timeZoneId.get).normalized() != GpuOverrides.UTC_TIMEZONE_ID) {
              willNotWorkOnGpu("Only UTC zone id is supported")
            }
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
            GpuTimeSub(lhs, rhs)
          }
        }
      ),
      GpuOverrides.expr[First](
        "first aggregate operator",
        (a, conf, p, r) => new ExprMeta[First](a, conf, p, r) {
          val child: BaseExprMeta[_] = GpuOverrides.wrapExpr(a.child, conf, Some(this))
          val ignoreNulls: BaseExprMeta[_] =
            GpuOverrides.wrapExpr(a.ignoreNullsExpr, conf, Some(this))
          override val childExprs: Seq[BaseExprMeta[_]] = Seq(child, ignoreNulls)

          override def convertToGpu(): GpuExpression =
            GpuFirst(child.convertToGpu(), ignoreNulls.convertToGpu())
        }),
      GpuOverrides.expr[Last](
        "last aggregate operator",
        (a, conf, p, r) => new ExprMeta[Last](a, conf, p, r) {
          val child: BaseExprMeta[_] = GpuOverrides.wrapExpr(a.child, conf, Some(this))
          val ignoreNulls: BaseExprMeta[_] =
            GpuOverrides.wrapExpr(a.ignoreNullsExpr, conf, Some(this))
          override val childExprs: Seq[BaseExprMeta[_]] = Seq(child, ignoreNulls)

          override def convertToGpu(): GpuExpression =
            GpuLast(child.convertToGpu(), ignoreNulls.convertToGpu())
        }),
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }

  override def getBuildSide(join: HashJoin): GpuBuildSide = {
    GpuJoinUtils.getGpuBuildSide(join.buildSide)
  }

  override def getBuildSide(join: BroadcastNestedLoopJoinExec): GpuBuildSide = {
    GpuJoinUtils.getGpuBuildSide(join.buildSide)
  }

  override def getRapidsShuffleManagerClass: String = {
    classOf[RapidsShuffleManager].getCanonicalName
  }
}
