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

package com.nvidia.spark.rapids.shims.spark301

import java.time.ZoneId

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.spark30.Spark30Shims

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
import org.apache.spark.sql.types._
import org.apache.spark.storage.{BlockId, BlockManagerId}
import org.apache.spark.unsafe.types.CalendarInterval

class Spark301Shims extends Spark30Shims {

    override def getExprs: Seq[ExprRule[_ <: Expression]] = {
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
          override def convertToGpu(): GpuExpression =
            GpuFirst(childExprs(0).convertToGpu(), a.ignoreNulls)
        }),
      GpuOverrides.expr[Last](
        "last aggregate operator",
        (a, conf, p, r) => new ExprMeta[Last](a, conf, p, r) {
          override def convertToGpu(): GpuExpression =
            GpuLast(childExprs(0).convertToGpu(), a.ignoreNulls)
        }),
    )
  }
}
