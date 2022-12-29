/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import com.databricks.sql.execution.window.RunningWindowFunctionExec
import com.databricks.sql.optimizer.PlanDynamicPruningFilters
import com.nvidia.spark.rapids._
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.execution.window._
import org.apache.spark.sql.rapids.execution._
import org.apache.spark.sql.rapids.execution.shims.{GpuSubqueryBroadcastMeta,ReuseGpuBroadcastExchangeAndSubquery}
import org.apache.spark.sql.rapids.shims._
import org.apache.spark.sql.types._


trait Spark321PlusDBShims extends SparkShims
  with Spark321PlusShims {
  override def isCastingStringToNegDecimalScaleSupported: Boolean = true

  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference]): RDD[InternalRow] = {
    new GpuFileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def broadcastModeTransform(mode: BroadcastMode, rows: Array[InternalRow]): Any = {
    // In some cases we can be asked to transform when there's no task context, which appears to
    // be new behavior since Databricks 10.4. A task memory manager must be passed, so if one is
    // not available we construct one from the main memory manager using a task attempt ID of 0.
    val memoryManager = Option(TaskContext.get).map(_.taskMemoryManager()).getOrElse {
      new TaskMemoryManager(SparkEnv.get.memoryManager, 0)
    }
    mode.transform(rows, memoryManager)
  }

  override def newBroadcastQueryStageExec(
      old: BroadcastQueryStageExec,
      newPlan: SparkPlan): BroadcastQueryStageExec =
    BroadcastQueryStageExec(old.id, newPlan, old.originalPlan, old.isSparkExchange)

  override def filesFromFileIndex(fileCatalog: PartitioningAwareFileIndex): Seq[FileStatus] = {
    fileCatalog.allFiles().map(_.toFileStatus)
  }

  override def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan] = null

  override def getWindowExpressions(winPy: WindowInPandasExec): Seq[NamedExpression] =
    winPy.projectList

  override def isWindowFunctionExec(plan: SparkPlan): Boolean =
    plan.isInstanceOf[WindowExecBase] || plan.isInstanceOf[RunningWindowFunctionExec]

  override def applyShimPlanRules(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    if (plan.conf.adaptiveExecutionEnabled) {
      plan // AQE+DPP cooperation ensures the optimization runs early
    } else {
      val sparkSession = plan.session
      val rules = Seq(
        PlanDynamicPruningFilters(sparkSession)
      )
      rules.foldLeft(plan) { case (sp, rule) =>
        rule.apply(sp)
      }
    }
  }

  override def applyPostShimPlanRules(plan: SparkPlan): SparkPlan = {
    val rules = Seq(
      ReuseGpuBroadcastExchangeAndSubquery
    )
    rules.foldLeft(plan) { case (sp, rule) =>
      rule.apply(sp)
    }
  }

  private val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    Seq(
      GpuOverrides.exec[SubqueryBroadcastExec](
        "Plan to collect and transform the broadcast key values",
        ExecChecks(TypeSig.all, TypeSig.all),
        (s, conf, p, r) => new GpuSubqueryBroadcastMeta(s, conf, p, r)),
      GpuOverrides.exec[RunningWindowFunctionExec](
        "Databricks-specific window function exec, for \"running\" windows, " +
            "i.e. (UNBOUNDED PRECEDING TO CURRENT ROW)",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
              TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
          TypeSig.all,
          Map("partitionSpec" ->
              InputCheck(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
                TypeSig.all))),
        (runningWindowFunctionExec, conf, p, r) =>
          new GpuRunningWindowExecMeta(runningWindowFunctionExec, conf, p, r)
      )
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    super.getExecs ++ shimExecs

  /**
   * Case class ShuffleQueryStageExec holds an additional field shuffleOrigin
   * affecting the unapply method signature
   */
  override def reusedExchangeExecPfn: PartialFunction[SparkPlan, ReusedExchangeExec] = {
    case ShuffleQueryStageExec(_, e: ReusedExchangeExec, _, _) => e
    case BroadcastQueryStageExec(_, e: ReusedExchangeExec, _, _) => e
  }

  override def reproduceEmptyStringBug: Boolean = true
}