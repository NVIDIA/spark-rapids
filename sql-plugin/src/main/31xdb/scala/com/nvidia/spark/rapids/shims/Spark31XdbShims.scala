/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.command.{AlterTableRecoverPartitionsCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.v2.ShowCurrentNamespaceExec
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.execution._
import org.apache.spark.sql.rapids.execution.python._
import org.apache.spark.sql.rapids.execution.shims.{GpuSubqueryBroadcastMeta, ReuseGpuBroadcastExchangeAndSubquery}
import org.apache.spark.sql.rapids.shims._
import org.apache.spark.sql.types._

abstract class Spark31XdbShims extends Spark31XdbShimsBase with Logging {

  override def v1RepairTableCommand(tableName: TableIdentifier): RunnableCommand =
    AlterTableRecoverPartitionsCommand(tableName)

  override def isWindowFunctionExec(plan: SparkPlan): Boolean =
    plan.isInstanceOf[WindowExecBase] || plan.isInstanceOf[RunningWindowFunctionExec]

  override def applyShimPlanRules(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    if (plan.conf.adaptiveExecutionEnabled) {
      plan // AQE+DPP cooperation ensures the optimization runs early
    } else {
      val sparkSession = SparkSession.getActiveSession.orNull
      val rules = Seq(
        PlanDynamicPruningFilters(sparkSession)
      )
      rules.foldLeft(plan) { case (sp, rule) =>
        rule.apply(sp)
      }
    }
  }

  override def applyPostShimPlanRules(plan: SparkPlan): SparkPlan = {
    if (plan.conf.adaptiveExecutionEnabled) {
      plan // AQE+DPP cooperation ensures the optimization runs early
    } else {
      val rules = Seq(
        ReuseGpuBroadcastExchangeAndSubquery
      )
      rules.foldLeft(plan) { case (sp, rule) =>
        rule.apply(sp)
      }
    }
  }

  override def ansiCastRule: ExprRule[_ <: Expression] = {
    GpuOverrides.expr[AnsiCast](
      "Convert a column of one type of data into another type",
      new CastChecks {
        import TypeSig._
        // nullChecks are the same

        override val booleanChecks: TypeSig = integral + fp + BOOLEAN + STRING + DECIMAL_128
        override val sparkBooleanSig: TypeSig = cpuNumeric + BOOLEAN + STRING

        override val integralChecks: TypeSig = gpuNumeric + BOOLEAN + STRING
        override val sparkIntegralSig: TypeSig = cpuNumeric + BOOLEAN + STRING

        override val fpChecks: TypeSig = (gpuNumeric + BOOLEAN + STRING)
            .withPsNote(TypeEnum.STRING, fpToStringPsNote)
        override val sparkFpSig: TypeSig = cpuNumeric + BOOLEAN + STRING

        override val dateChecks: TypeSig = TIMESTAMP + DATE + STRING
        override val sparkDateSig: TypeSig = TIMESTAMP + DATE + STRING

        override val timestampChecks: TypeSig = TIMESTAMP + DATE + STRING
        override val sparkTimestampSig: TypeSig = TIMESTAMP + DATE + STRING

        // stringChecks are the same
        // binaryChecks are the same
        override val decimalChecks: TypeSig = gpuNumeric + STRING
        override val sparkDecimalSig: TypeSig = cpuNumeric + BOOLEAN + STRING

        // calendarChecks are the same

        override val arrayChecks: TypeSig =
          ARRAY.nested(commonCudfTypes + DECIMAL_128 + NULL + ARRAY + BINARY + STRUCT) +
              psNote(TypeEnum.ARRAY, "The array's child type must also support being cast to " +
                  "the desired child type")
        override val sparkArraySig: TypeSig = ARRAY.nested(all)

        override val mapChecks: TypeSig =
          MAP.nested(commonCudfTypes + DECIMAL_128 + NULL + ARRAY + BINARY + STRUCT + MAP) +
              psNote(TypeEnum.MAP, "the map's key and value must also support being cast to the " +
                  "desired child types")
        override val sparkMapSig: TypeSig = MAP.nested(all)

        override val structChecks: TypeSig =
          STRUCT.nested(commonCudfTypes + DECIMAL_128 + NULL + ARRAY + BINARY + STRUCT) +
            psNote(TypeEnum.STRUCT, "the struct's children must also support being cast to the " +
                "desired child type(s)")
        override val sparkStructSig: TypeSig = STRUCT.nested(all)

        override val udtChecks: TypeSig = none
        override val sparkUdtSig: TypeSig = UDT
      },
      // 312db supports Ansi mode when casting string to date, this means that an exception
      // will be thrown when casting an invalid value to date in Ansi mode.
      // Set `stringToAnsiDate` = true
      (cast, conf, p, r) => new CastExprMeta[AnsiCast](cast, ansiEnabled = true, conf = conf,
        parent = p, rule = r, doFloatToIntCheck = true, stringToAnsiDate = true))
  }

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[Cast](
        "Convert a column of one type of data into another type",
        new CastChecks(),
        // 312db supports Ansi mode when casting string to date, this means that an exception
        // will be thrown when casting an invalid value to date in Ansi mode.
        // Set `stringToAnsiDate` = true
        (cast, conf, p, r) => new CastExprMeta[Cast](cast,
          SparkSession.active.sessionState.conf.ansiEnabled, conf, p, r,
          doFloatToIntCheck = true, stringToAnsiDate = true)),
    GpuOverrides.expr[Average](
      "Average aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        Seq(ParamCheck("input",
          TypeSig.integral + TypeSig.fp + TypeSig.DECIMAL_128,
          TypeSig.cpuNumeric))),
      (a, conf, p, r) => new AggExprMeta[Average](a, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          GpuOverrides.checkAndTagFloatAgg(a.child.dataType, conf, this)
        }

        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
          GpuAverage(childExprs.head)

        // Average is not supported in ANSI mode right now, no matter the type
        override val ansiTypeToCheck: Option[DataType] = None
      }),
    GpuOverrides.expr[Abs](
      "Absolute value",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.implicitCastsAstTypes, TypeSig.gpuNumeric,
        TypeSig.cpuNumeric),
      (a, conf, p, r) => new UnaryAstExprMeta[Abs](a, conf, p, r) {
        // ANSI support for ABS was added in 3.2.0 SPARK-33275
        override def convertToGpu(child: Expression): GpuExpression = GpuAbs(child, false)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    Seq(
      GpuOverrides.exec[SubqueryBroadcastExec](
        "Plan to collect and transform the broadcast key values",
        ExecChecks(TypeSig.all, TypeSig.all),
        (s, conf, p, r) => new GpuSubqueryBroadcastMeta(s, conf, p, r)
      ),
      GpuOverrides.exec[WindowInPandasExec](
        "The backend for Window Aggregation Pandas UDF, Accelerates the data transfer between" +
          " the Java process and the Python process. It also supports scheduling GPU resources" +
          " for the Python process when enabled. For now it only supports row based window frame.",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested(TypeSig.commonCudfTypes),
          TypeSig.all),
        (winPy, conf, p, r) => new GpuWindowInPandasExecMetaBase(winPy, conf, p, r) {
          override val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
            winPy.projectList.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

          override def convertToGpu(): GpuExec = {
            GpuWindowInPandasExec(
              windowExpressions.map(_.convertToGpu()),
              partitionSpec.map(_.convertToGpu()),
              // leave ordering expression on the CPU, it's not used for GPU computation
              winPy.orderSpec,
              childPlans.head.convertIfNeeded()
            )(winPy.partitionSpec)
          }
        }).disabledByDefault("it only supports row based frame for now"),
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
      ),
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
              // Because the PlanSubqueries rule is not called (and does not work as expected),
              // we might actually have to fully convert the subquery plan as the plugin would 
              // intend (in this case calling GpuTransitionOverrides to insert GpuCoalesceBatches, 
              // etc.) to match the other side of the join to reuse the BroadcastExchange.
              // This happens when SubqueryBroadcast has the original (Gpu)BroadcastExchangeExec
              converted match {
                case e: GpuSubqueryBroadcastExec => e.child match {
                  // If the GpuBroadcastExchange is here, then we will need to run the transition 
                  // overrides here
                  case _: GpuBroadcastExchangeExec =>
                    var updated = ApplyColumnarRulesAndInsertTransitions(Seq())
                        .apply(converted)
                    updated = (new GpuTransitionOverrides()).apply(updated)
                    updated match {
                      case h: GpuBringBackToHost =>
                        h.child.asInstanceOf[BaseSubqueryExec]
                      case c2r: GpuColumnarToRowExec =>
                        c2r.child.asInstanceOf[BaseSubqueryExec]
                      case _: GpuSubqueryBroadcastExec =>
                        updated.asInstanceOf[BaseSubqueryExec]
                    }
                  // Otherwise, if this SubqueryBroadcast is using a ReusedExchange, then we don't
                  // do anything further
                  case _: ReusedExchangeExec => 
                    converted.asInstanceOf[BaseSubqueryExec]
                }
                case _ =>
                  converted.asInstanceOf[BaseSubqueryExec]
              }
            }
            wrapped.partitionFilters.map { filter =>
              filter.transformDown {
                case dpe @ DynamicPruningExpression(inSub: InSubqueryExec) =>
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
              if (AlluxioCfgUtils.enabledAlluxioReplacementAlgoConvertTime(conf)) {
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
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
  }

  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Seq(
    GpuOverrides.scan[ParquetScan](
      "Parquet parsing",
      (a, conf, p, r) => new RapidsParquetScanMeta(a, conf, p, r)),
    GpuOverrides.scan[OrcScan](
      "ORC parsing",
      (a, conf, p, r) => new RapidsOrcScanMeta(a, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap

  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference]): RDD[InternalRow] = {
    new GpuFileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def reusedExchangeExecPfn: PartialFunction[SparkPlan, ReusedExchangeExec] = {
    case ShuffleQueryStageExec(_, e: ReusedExchangeExec, _) => e
    case BroadcastQueryStageExec(_, e: ReusedExchangeExec, _) => e
  }

  /** dropped by SPARK-34234 */
  override def attachTreeIfSupported[TreeType <: TreeNode[_], A](
      tree: TreeType,
      msg: String)(
      f: => A
  ): A = {
    attachTree(tree, msg)(f)
  }

  override def hasAliasQuoteFix: Boolean = false

  override def hasCastFloatTimestampUpcast: Boolean = false

  override def filesFromFileIndex(fileCatalog: PartitioningAwareFileIndex): Seq[FileStatus] = {
    fileCatalog.allFiles().map(_.toFileStatus)
  }

  override def isEmptyRelation(relation: Any): Boolean = false
  override def tryTransformIfEmptyRelation(mode: BroadcastMode): Option[Any] = None

  override def broadcastModeTransform(mode: BroadcastMode, rows: Array[InternalRow]): Any =
    mode.transform(rows, TaskContext.get.taskMemoryManager())

  override def getAdaptiveInputPlan(adaptivePlan: AdaptiveSparkPlanExec): SparkPlan = {
    adaptivePlan.inputPlan
  }

  override def supportsColumnarAdaptivePlans: Boolean = false

  override def columnarAdaptivePlan(a: AdaptiveSparkPlanExec, goal: CoalesceSizeGoal): SparkPlan = {
    // When the input is an adaptive plan we do not get to see the GPU version until
    // the plan is executed and sometimes the plan will have a GpuColumnarToRowExec as the
    // final operator and we can bypass this to keep the data columnar by inserting
    // the [[AvoidAdaptiveTransitionToRow]] operator here
    AvoidAdaptiveTransitionToRow(GpuRowToColumnarExec(a, goal))
  }

  def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan] = {
    GpuOverrides.neverReplaceExec[ShowCurrentNamespaceExec]("Namespace metadata operation")
  }
}

// First, Last and Collect have mistakenly been marked as non-deterministic until Spark-3.3.
// They are actually deterministic iff their child expression is deterministic.
trait GpuDeterministicFirstLastCollectShim extends Expression {
  override lazy val deterministic = false
}
