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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "341db"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.exchange.{EXECUTOR_BROADCAST, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.rapids.{GpuCheckOverflowInTableInsert, GpuElementAtMeta}
import org.apache.spark.sql.rapids.execution.{GpuBroadcastHashJoinExec, GpuBroadcastNestedLoopJoinExec}

trait Spark330PlusDBShims extends Spark321PlusDBShims with Logging {
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[CheckOverflowInTableInsert](
        "Casting a numeric value as another numeric type in store assignment",
        ExprChecks.unaryProjectInputMatchesOutput(
          TypeSig.all,
          TypeSig.all),
        (t, conf, p, r) => new UnaryExprMeta[CheckOverflowInTableInsert](t, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = {
            child match {
              case c: GpuCast => GpuCheckOverflowInTableInsert(c, t.columnName)
              case _ =>
                throw new IllegalStateException("Expression child is not of Type GpuCast")
            }
          }
        }),
      GpuElementAtMeta.elementAtRule(true)
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ shimExprs ++ DayTimeIntervalShims.exprs ++ RoundingShims.exprs
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    super.getExecs ++ PythonMapInArrowExecShims.execs

  override def reproduceEmptyStringBug: Boolean = false

  override def isExecutorBroadcastShuffle(shuffle: ShuffleExchangeLike): Boolean = {
    shuffle.shuffleOrigin.equals(EXECUTOR_BROADCAST)
  }

  override def shuffleParentReadsShuffleData(shuffle: ShuffleExchangeLike,
      parent: SparkPlan): Boolean = {
    parent match {
      case _: GpuBroadcastHashJoinExec =>
        shuffle.shuffleOrigin.equals(EXECUTOR_BROADCAST)
      case _: GpuBroadcastNestedLoopJoinExec =>
        shuffle.shuffleOrigin.equals(EXECUTOR_BROADCAST)
      case _ => false
    }
  }

  override def checkColumnarToRowWithExecBroadcast(p: SparkPlan, parent: Option[SparkPlan]): Boolean = {
    p match {
      case ColumnarToRowExec(AQEShuffleReadExec(_: ShuffleQueryStageExec, _, _)) =>
        parent match {
          case Some(bhje: BroadcastHashJoinExec) if bhje.isExecutorBroadcast =>
            true
          case _ =>
            false
        }
      case _ =>
        false
    }
  }

  override def convertColumnarToRowWithExecBroadcast(p: SparkPlan, parent: Option[SparkPlan]): SparkPlan = {
    p match {
      case c2re@ColumnarToRowExec(aqesr@AQEShuffleReadExec(s: ShuffleQueryStageExec, _, _)) =>
        parent match {
          case Some(bhje: BroadcastHashJoinExec) if bhje.isExecutorBroadcast =>
            logWarning("tgraves aqe read is: " + aqesr + " coalesced: " + aqesr.isCoalescedRead +
              " spec: " + aqesr.partitionSpecs.mkString(",") + " parent is: " + parent)
            logWarning("columnar to row with AQEShuffleReadExec " + c2re + " entire plan is: " + p)
            val planopt = GpuTransitionOverrides.optimizeAdaptiveTransitions(s, Some(p))
            val c2r = GpuColumnarToRowExec(planopt)
            logWarning("tom planopt is " + planopt + " ctor: " + c2r)
            SparkShimImpl.addRowShuffleToQueryStageTransitionIfNeeded(c2r, s, fromBHJExecutorBroadcast = true)

          case _ =>
            logWarning("not bhj")
            c2re
        }
      case _ =>
        p
    }
  }

  override def addRowShuffleToQueryStageTransitionIfNeeded(c2r: ColumnarToRowTransition,
      sqse: ShuffleQueryStageExec, fromBHJExecutorBroadcast: Boolean = false): SparkPlan = {
    val plan = GpuTransitionOverrides.getNonQueryStagePlan(sqse)
    plan match {
      case shuffle: ShuffleExchangeLike if shuffle.shuffleOrigin.equals(EXECUTOR_BROADCAST) || fromBHJExecutorBroadcast =>
        logInfo("in executor broadcast handling, creating new shuffle exchange")
        ShuffleExchangeExec(SinglePartition, c2r, EXECUTOR_BROADCAST)
      case _ =>
        c2r
    }
  }
}
