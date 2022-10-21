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

import com.databricks.sql.execution.ReuseExchangeAndSubquery
// import org.apache.spark.sql.execution.reuse.ReuseExchangeAndSubquery
import com.databricks.sql.execution.window.RunningWindowFunctionExec
import com.databricks.sql.optimizer.PlanDynamicPruningFilters
import com.nvidia.spark.rapids._
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, IdentityBroadcastMode}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.execution.python.WindowInPandasExec
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.execution.{GpuBroadcastMeta, GpuBroadcastExchangeExec, GpuSubqueryBroadcastExec}
import org.apache.spark.sql.rapids.shims.GpuFileScanRDD
import org.apache.spark.sql.types._

object SparkShimImpl extends Spark321PlusShims with Spark320until340Shims {

  override def getSparkShimVersion: ShimVersion = ShimLoader.getShimVersion

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

  override def applyInitialShimPlanRules(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    if (plan.conf.adaptiveExecutionEnabled) {
      plan // AQE+DPP cooperation ensures the optimization runs early
    } else {
      val sparkSession = plan.session
      val rules = Seq(
        PlanDynamicPruningFilters(sparkSession),
      )
      rules.foldLeft(plan) { case (sp, rule) =>
        rule.apply(sp)
      }
    }
  }

  override def applyFinalShimPlanRules(plan: SparkPlan): SparkPlan = {
    if (plan.conf.adaptiveExecutionEnabled) {
      plan // AQE+DPP cooperation ensures the optimization runs early
    } else {
      val rules = Seq(
        ReuseExchangeAndSubquery
      )
      rules.foldLeft(plan) { case (sp, rule) =>
        rule.apply(sp)
      }
    }

  }

  private val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    Seq(
      GpuOverrides.exec[SubqueryBroadcastExec](
        "Plan to collect and transform the broadcast key values",
        ExecChecks(TypeSig.all, TypeSig.all),
        (s, conf, p, r) => new SparkPlanMeta[SubqueryBroadcastExec](s, conf, p, r) {
          private var broadcastBuilder: () => SparkPlan = _

          override val childExprs: Seq[BaseExprMeta[_]] = Nil

          override val childPlans: Seq[SparkPlanMeta[SparkPlan]] = Nil

          override def tagPlanForGpu(): Unit = s.child match {
            case ex @ BroadcastExchangeExec(_, child) =>
              logWarning("DPP+DB")
              val exMeta = new GpuBroadcastMeta(ex.copy(child = child), conf, p, r)
              exMeta.tagForGpu()
              if (exMeta.canThisBeReplaced) {
                broadcastBuilder = () => exMeta.convertToGpu()
              } else {
                willNotWorkOnGpu("underlying BroadcastExchange can not run in the GPU.")
              }
            case bqse: BroadcastQueryStageExec =>
              logWarning("DPP+DB+AQE")
              logWarning(s"plan: ${bqse.plan}")
              bqse.plan match {
                case reuse: ReusedExchangeExec =>
                  reuse.child match {
                    case gpuExchange: GpuBroadcastExchangeExec =>
                      // A BroadcastExchange has already been replaced, so it can run on the GPU
                      broadcastBuilder = () => reuse
                    case _ =>
                      willNotWorkOnGpu("underlying BroadcastExchange can not run in the GPU.")
                  }
              }
            case _ =>
              logWarning(s"what is child: ${s.child.getClass}")
              willNotWorkOnGpu("the subquery to broadcast can not entirely run in the GPU.")
          }
          /**
          * Simply returns the original plan. Because its only child, BroadcastExchange, doesn't
          * need to change if SubqueryBroadcastExec falls back to the CPU.
          */
          override def convertToCpu(): SparkPlan = s

          override def convertToGpu(): GpuExec = {
            GpuSubqueryBroadcastExec(s.name, s.index, s.buildKeys, broadcastBuilder())(
              getBroadcastModeKeyExprs)
          }

          /** Extract the broadcast mode key expressions if there are any. */
          private def getBroadcastModeKeyExprs: Option[Seq[Expression]] = {
            val broadcastMode = s.child match {
              case b: BroadcastExchangeExec =>
                b.mode
              case bqse: BroadcastQueryStageExec =>
                bqse.plan match {
                  case reuse: ReusedExchangeExec =>
                    reuse.child match {
                      case g: GpuBroadcastExchangeExec =>
                        g.mode
                    }
                  case _ =>
                    throw new AssertionError("should not reach here")
                }
            }

            broadcastMode match {
              case HashedRelationBroadcastMode(keys, _) => Some(keys)
              case IdentityBroadcastMode => None
              case m => throw new UnsupportedOperationException(s"Unknown broadcast mode $m")
            }
          }
        }),
      GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.ARRAY + TypeSig.BINARY + TypeSig.DECIMAL_128).nested(), TypeSig.all),
        (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {

          // Replaces SubqueryBroadcastExec inside dynamic pruning filters with GPU counterpart
          // if possible. Instead regarding filters as childExprs of current Meta, we create
          // a new meta for SubqueryBroadcastExec. The reason is that the GPU replacement of
          // FileSourceScan is independent from the replacement of the partitionFilters. It is
          // possible that the FileSourceScan is on the CPU, while the dynamic partitionFilters
          // are on the GPU. And vice versa.
          private lazy val partitionFilters = {
            val convertBroadcast = (bc: SubqueryBroadcastExec) => {
              val meta = GpuOverrides.wrapAndTagPlan(bc, conf)
              meta.tagForExplain()
              val converted = meta.convertIfNeeded()
              logWarning(s"DPP: converted to $converted")
              converted.asInstanceOf[BaseSubqueryExec]
            }

            wrapped.partitionFilters.map { filter =>
              filter.transformDown {
                case dpe @ DynamicPruningExpression(inSub: InSubqueryExec) =>
                  logWarning(s"DPE: inSub plan: ${inSub.plan}")
                  inSub.plan match {
                    case bc: SubqueryBroadcastExec =>
                      dpe.copy(inSub.copy(plan = convertBroadcast(bc)))
                    case reuse @ ReusedSubqueryExec(bc: SubqueryBroadcastExec) =>
                      dpe.copy(inSub.copy(plan = reuse.copy(convertBroadcast(bc))))
                    case _ =>
                      dpe
                  }
              }
            }
          }

          // partition filters and data filters are not run on the GPU
          override val childExprs: Seq[ExprMeta[_]] = Seq.empty

          override def tagPlanForGpu(): Unit = {
            // this is very specific check to have any of the Delta log metadata queries
            // fallback and run on the CPU since there is some incompatibilities in
            // Databricks Spark and Apache Spark.
            if (wrapped.relation.fileFormat.isInstanceOf[JsonFileFormat] &&
                wrapped.relation.location.getClass.getCanonicalName() ==
                    "com.databricks.sql.transaction.tahoe.DeltaLogFileIndex") {
              this.entirePlanWillNotWork("Plans that read Delta Index JSON files can not run " +
                  "any part of the plan on the GPU!")
            }
            GpuFileSourceScanExec.tagSupport(this)
          }

          override def convertToCpu(): SparkPlan = {
            wrapped.copy(partitionFilters = partitionFilters)
          }

          override def convertToGpu(): GpuExec = {
            val sparkSession = wrapped.relation.sparkSession
            val options = wrapped.relation.options

            val (location, alluxioPathsToReplaceMap) =
              if (conf.isAlluxioReplacementAlgoConvertTime) {
                AlluxioUtils.replacePathIfNeeded(
                  conf,
                  wrapped.relation,
                  partitionFilters,
                  wrapped.dataFilters)
              } else {
                (wrapped.relation.location, None)
              }

            val newRelation = HadoopFsRelation(
              location,
              wrapped.relation.partitionSchema,
              wrapped.relation.dataSchema,
              wrapped.relation.bucketSpec,
              GpuFileSourceScanExec.convertFileFormat(wrapped.relation.fileFormat),
              options)(sparkSession)

            GpuFileSourceScanExec(
              newRelation,
              wrapped.output,
              wrapped.requiredSchema,
              partitionFilters,
              wrapped.optionalBucketSet,
              // TODO: Does Databricks have coalesced bucketing implemented?
              None,
              wrapped.dataFilters,
              wrapped.tableIdentifier,
              wrapped.disableBucketedScan,
              queryUsesInputFile = false,
              alluxioPathsToReplaceMap)(conf)
          }
        }),
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
}

// Fallback to the default definition of `deterministic`
trait GpuDeterministicFirstLastCollectShim extends Expression
