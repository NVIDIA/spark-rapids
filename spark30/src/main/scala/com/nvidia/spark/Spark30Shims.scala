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

import java.time.ZoneId

import com.nvidia.spark.rapids._
import org.apache.spark.sql.rapids._

//import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins.{BroadcastNestedLoopJoinExec, BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}



class Spark30Shims extends SparkShims with Logging {

  def isGpuHashJoin(plan: SparkPlan): Boolean = {
    plan match {
      case _: GpuHashJoin30 => true
      case p => false
    }
  }

  def getExecs: Seq[ExecRule[_ <: SparkPlan]] = {
    Seq(

    GpuOverrides.exec[FileSourceScanExec](
      "Reading data from files, often from Hive tables",
      (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {
        // partition filters and data filters are not run on the GPU
        override val childExprs: Seq[ExprMeta[_]] = Seq.empty

        override def tagPlanForGpu(): Unit = GpuFileSourceScanExec30.tagSupport(this)

        override def convertToGpu(): GpuExec = {
          val newRelation = HadoopFsRelation(
            wrapped.relation.location,
            wrapped.relation.partitionSchema,
            wrapped.relation.dataSchema,
            wrapped.relation.bucketSpec,
            GpuFileSourceScanExec30.convertFileFormat(wrapped.relation.fileFormat),
            wrapped.relation.options)(wrapped.relation.sparkSession)
          GpuFileSourceScanExec30(
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
      (join, conf, p, r) => new GpuSortMergeJoinMeta30(join, conf, p, r)),
    GpuOverrides.exec[BroadcastHashJoinExec](
      "Implementation of join using broadcast data",
      (join, conf, p, r) => new GpuBroadcastHashJoinMeta30(join, conf, p, r)),
    GpuOverrides.exec[ShuffledHashJoinExec](
      "Implementation of join using hashed shuffled data",
      (join, conf, p, r) => new GpuShuffledHashJoinMeta30(join, conf, p, r)),
    )
  }

  def getExprs: Seq[ExprRule[_ <: Expression]] = {
    Seq(
    GpuOverrides.expr[TimeSub](
      "Subtracts interval from timestamp",
      (a, conf, p, r) => new BinaryExprMeta[TimeSub](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          logWarning("in TimeSub")
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
          logWarning("in TimeSub convert")
          GpuTimeSub(lhs, rhs)
        }
      }
    ),
    )
  }


  def getBuildSide(join: ShuffledHashJoinExec): GpuBuildSide = {
    val buildSide = join.buildSide
    buildSide match {
      case e: buildSide.type if e.toString.contains("BuildRight") => {
        logInfo("Tom buildright " + e)
        GpuBuildRight
      }
      case l: buildSide.type if l.toString.contains("BuildLeft") => {
        logInfo("Tom buildleft "+ l)
        GpuBuildLeft
      }
      case _ => throw new Exception("unknown buildSide Type")
    }
  }

  def getBuildSide(join: BroadcastNestedLoopJoinExec): GpuBuildSide = {
    val buildSide = join.buildSide
    buildSide match {
      case e: buildSide.type if e.toString.contains("BuildRight") => {
        logInfo("bnlje Tom buildright " + e)
        GpuBuildRight
      }
      case l: buildSide.type if l.toString.contains("BuildLeft") => {
        logInfo("bnlje Tom buildleft "+ l)
        GpuBuildLeft
      }
      case _ => throw new Exception("unknown buildSide Type")
    }
  }
  def getBuildSide(join: BroadcastHashJoinExec): GpuBuildSide = {
    val buildSide = join.buildSide
    buildSide match {
      case e: buildSide.type if e.toString.contains("BuildRight") => {
        logInfo("bnlje Tom buildright " + e)
        GpuBuildRight
      }
      case l: buildSide.type if l.toString.contains("BuildLeft") => {
        logInfo("bnlje Tom buildleft "+ l)
        GpuBuildLeft
      }
      case _ => throw new Exception("unknown buildSide Type")
    }
  }
}

