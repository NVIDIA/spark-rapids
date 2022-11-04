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

import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuOverrides.exec

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.execution._
import org.apache.spark.sql.rapids.execution.python._
import org.apache.spark.sql.rapids.shims._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Shim base class that can be compiled with every supported 3.2.0+
 */
trait Spark320PlusShims extends SparkShims with RebaseShims with Logging {

  def getWindowExpressions(winPy: WindowInPandasExec): Seq[NamedExpression]

  override final def aqeShuffleReaderExec: ExecRule[_ <: SparkPlan] = exec[AQEShuffleReadExec](
    "A wrapper of shuffle query stage",
    ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.ARRAY +
      TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY).nested(), TypeSig.all),
    (exec, conf, p, r) => new GpuCustomShuffleReaderMeta(exec, conf, p, r))

  override final def sessionFromPlan(plan: SparkPlan): SparkSession = {
    plan.session
  }

  override def isEmptyRelation(relation: Any): Boolean = relation match {
    case EmptyHashedRelation => true
    case arr: Array[InternalRow] if arr.isEmpty => true
    case _ => false
  }

  override def tryTransformIfEmptyRelation(mode: BroadcastMode): Option[Any] = {
    Some(broadcastModeTransform(mode, Array.empty)).filter(isEmptyRelation)
  }

  override final def isExchangeOp(plan: SparkPlanMeta[_]): Boolean = {
    // if the child query stage already executed on GPU then we need to keep the
    // next operator on GPU in these cases
    SQLConf.get.adaptiveExecutionEnabled && (plan.wrapped match {
      case _: AQEShuffleReadExec
           | _: ShuffledHashJoinExec
           | _: BroadcastHashJoinExec
           | _: BroadcastExchangeExec
           | _: BroadcastNestedLoopJoinExec => true
      case _ => false
    })
  }

  override final def isAqePlan(p: SparkPlan): Boolean = p match {
    case _: AdaptiveSparkPlanExec |
         _: QueryStageExec |
         _: AQEShuffleReadExec => true
    case _ => false
  }

  override def getDateFormatter(): DateFormatter = {
    // TODO verify
    DateFormatter()
  }

  override def isCustomReaderExec(x: SparkPlan): Boolean = x match {
    case _: GpuCustomShuffleReaderExec | _: AQEShuffleReadExec => true
    case _ => false
  }

  override def v1RepairTableCommand(tableName: TableIdentifier): RunnableCommand =
    RepairTableCommand(tableName,
      // These match the one place that this is called, if we start to call this in more places
      // we will need to change the API to pass these values in.
      enableAddPartitions = true,
      enableDropPartitions = false)

  override def shouldFailDivOverflow(): Boolean = SQLConf.get.ansiEnabled

  override def leafNodeDefaultParallelism(ss: SparkSession): Int = {
    Spark32XShimsUtils.leafNodeDefaultParallelism(ss)
  }

  override def isWindowFunctionExec(plan: SparkPlan): Boolean = plan.isInstanceOf[WindowExecBase]

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[Cast](
      "Convert a column of one type of data into another type",
      new CastChecks(),
      (cast, conf, p, r) => new CastExprMeta[Cast](cast,
        SparkSession.active.sessionState.conf.ansiEnabled, conf, p, r,
        doFloatToIntCheck = true, stringToAnsiDate = true)),
    GpuOverrides.expr[Average](
      "Average aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        // NullType is not technically allowed by Spark, but in practice in 3.2.0
        // it can show up
        Seq(ParamCheck("input",
          TypeSig.integral + TypeSig.fp + TypeSig.DECIMAL_128 + TypeSig.NULL,
          TypeSig.numericAndInterval + TypeSig.NULL))),
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
        val ansiEnabled = SQLConf.get.ansiEnabled

        override def tagSelfForAst(): Unit = {
          if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
            willNotWorkInAst("AST unary minus does not support ANSI mode.")
          }
        }

        // ANSI support for ABS was added in 3.2.0 SPARK-33275
        override def convertToGpu(child: Expression): GpuExpression = GpuAbs(child, ansiEnabled)
      }),
    GpuOverrides.expr[Literal](
      "Holds a static value from the query",
      ExprChecks.projectAndAst(
        TypeSig.astTypes,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.CALENDAR
            + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT
            + TypeSig.ansiIntervals)
            .nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
                TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT),
        TypeSig.all),
      (lit, conf, p, r) => new LiteralExprMeta(lit, conf, p, r)),
    GpuOverrides.expr[TimeAdd](
      "Adds interval to timestamp",
      ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
        ("start", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        ("interval", TypeSig.lit(TypeEnum.DAYTIME) + TypeSig.lit(TypeEnum.CALENDAR),
          TypeSig.DAYTIME + TypeSig.CALENDAR)),
      (timeAdd, conf, p, r) => new BinaryExprMeta[TimeAdd](timeAdd, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          GpuOverrides.extractLit(timeAdd.interval).foreach { lit =>
            lit.dataType match {
              case CalendarIntervalType =>
                val intvl = lit.value.asInstanceOf[CalendarInterval]
                if (intvl.months != 0) {
                  willNotWorkOnGpu("interval months isn't supported")
                }
              case _: DayTimeIntervalType => // Supported
            }
          }
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuTimeAdd(lhs, rhs)
      }),
    GpuOverrides.expr[SpecifiedWindowFrame](
      "Specification of the width of the group (or \"frame\") of input rows " +
        "around which a window function is evaluated",
      ExprChecks.projectOnly(
        TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DAYTIME,
        TypeSig.numericAndInterval,
        Seq(
          ParamCheck("lower",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DAYTIME
              + TypeSig.DECIMAL_128,
            TypeSig.numericAndInterval),
          ParamCheck("upper",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DAYTIME
              + TypeSig.DECIMAL_128,
            TypeSig.numericAndInterval))),
      (windowFrame, conf, p, r) => new GpuSpecifiedWindowFrameMeta(windowFrame, conf, p, r)),
    GpuOverrides.expr[WindowExpression](
      "Calculates a return value for every input row of a table based on a group (or " +
        "\"window\") of rows",
      ExprChecks.windowOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all,
        Seq(ParamCheck("windowFunction",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
          TypeSig.all),
          ParamCheck("windowSpec",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DECIMAL_64 +
              TypeSig.DAYTIME, TypeSig.numericAndInterval))),
      (windowExpression, conf, p, r) => new GpuWindowExpressionMeta(windowExpression, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    Seq(
      GpuOverrides.exec[WindowInPandasExec](
        "The backend for Window Aggregation Pandas UDF, Accelerates the data transfer between" +
          " the Java process and the Python process. It also supports scheduling GPU resources" +
          " for the Python process when enabled. For now it only supports row based window frame.",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested(TypeSig.commonCudfTypes),
          TypeSig.all),
        (winPy, conf, p, r) => new GpuWindowInPandasExecMetaBase(winPy, conf, p, r) {
          override val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
            getWindowExpressions(winPy).map(GpuOverrides.wrapExpr(_, conf, Some(this)))

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
      GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
          TypeSig.ARRAY + TypeSig.BINARY + TypeSig.DECIMAL_128).nested(), TypeSig.all),
        (fsse, conf, p, r) => new FileSourceScanExecMeta(fsse, conf, p, r)),
      GpuOverrides.exec[BatchScanExec](
        "The backend for most file input",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.DECIMAL_128 + TypeSig.BINARY).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new BatchScanExecMeta(p, conf, parent, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
  }

  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Seq(
    GpuOverrides.scan[ParquetScan](
      "Parquet parsing",
      (a, conf, p, r) => new RapidsParquetScanMeta(a, conf, p, r)),
    GpuOverrides.scan[OrcScan](
      "ORC parsing",
      (a, conf, p, r) => new RapidsOrcScanMeta(a, conf, p, r)),
    GpuOverrides.scan[CSVScan](
      "CSV parsing",
      (a, conf, p, r) => new RapidsCsvScanMeta(a, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap

  /** dropped by SPARK-34234 */
  override def attachTreeIfSupported[TreeType <: TreeNode[_], A](
      tree: TreeType,
      msg: String)(
      f: => A
  ): A = {
    identity(f)
  }

  override def hasAliasQuoteFix: Boolean = true

  override def hasCastFloatTimestampUpcast: Boolean = true

  override def findOperators(plan: SparkPlan, predicate: SparkPlan => Boolean): Seq[SparkPlan] = {
    def recurse(
        plan: SparkPlan,
        predicate: SparkPlan => Boolean,
        accum: ListBuffer[SparkPlan]): Seq[SparkPlan] = {
      if (predicate(plan)) {
        accum += plan
      }
      plan match {
        case a: AdaptiveSparkPlanExec => recurse(a.executedPlan, predicate, accum)
        case qs: BroadcastQueryStageExec => recurse(qs.broadcast, predicate, accum)
        case qs: ShuffleQueryStageExec => recurse(qs.shuffle, predicate, accum)
        case c: CommandResultExec => recurse(c.commandPhysicalPlan, predicate, accum)
        case other => other.children.flatMap(p => recurse(p, predicate, accum)).headOption
      }
      accum
    }

    recurse(plan, predicate, new ListBuffer[SparkPlan]())
  }

  override def skipAssertIsOnTheGpu(plan: SparkPlan): Boolean = plan match {
    case _: CommandResultExec => true
    case _ => false
  }

  override def getAdaptiveInputPlan(adaptivePlan: AdaptiveSparkPlanExec): SparkPlan = {
    adaptivePlan.initialPlan
  }

  override def columnarAdaptivePlan(a: AdaptiveSparkPlanExec,
      goal: CoalesceSizeGoal): SparkPlan = {
    a.copy(supportsColumnar = true)
  }

  override def supportsColumnarAdaptivePlans: Boolean = true

  def tagFileSourceScanExec(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    GpuFileSourceScanExec.tagSupport(meta)
  }

  class FileSourceScanExecMeta(plan: FileSourceScanExec,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]],
      rule: DataFromReplacementRule)
      extends SparkPlanMeta[FileSourceScanExec](plan, conf, parent, rule) {

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
        meta.convertIfNeeded().asInstanceOf[BaseSubqueryExec]
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

    override def tagPlanForGpu(): Unit = tagFileSourceScanExec(this)

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
        wrapped.optionalNumCoalescedBuckets,
        wrapped.dataFilters,
        wrapped.tableIdentifier,
        wrapped.disableBucketedScan,
        queryUsesInputFile = false,
        alluxioPathsToReplaceMap)(conf)
    }
  }

  class BatchScanExecMeta(p: BatchScanExec,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]],
      rule: DataFromReplacementRule)
      extends SparkPlanMeta[BatchScanExec](p, conf, parent, rule) {
    // Replaces SubqueryBroadcastExec inside dynamic pruning filters with GPU counterpart
    // if possible. Instead regarding filters as childExprs of current Meta, we create
    // a new meta for SubqueryBroadcastExec. The reason is that the GPU replacement of
    // BatchScanExec is independent from the replacement of the runtime filters. It is
    // possible that the BatchScanExec is on the CPU, while the dynamic runtime filters
    // are on the GPU. And vice versa.
    private lazy val runtimeFilters = {
      val convertBroadcast = (bc: SubqueryBroadcastExec) => {
        val meta = GpuOverrides.wrapAndTagPlan(bc, conf)
        meta.tagForExplain()
        meta.convertIfNeeded().asInstanceOf[BaseSubqueryExec]
      }
      wrapped.runtimeFilters.map { filter =>
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

    override val childExprs: Seq[BaseExprMeta[_]] = {
      // We want to leave the runtime filters as CPU expressions
      p.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
    }

    override val childScans: scala.Seq[ScanMeta[_]] =
      Seq(GpuOverrides.wrapScan(p.scan, conf, Some(this)))

    override def tagPlanForGpu(): Unit = {
      if (!p.runtimeFilters.isEmpty && !childScans.head.supportsRuntimeFilters) {
        willNotWorkOnGpu("runtime filtering (DPP) is not supported for this scan")
      }
    }

    override def convertToCpu(): SparkPlan = {
      wrapped.copy(runtimeFilters = runtimeFilters)
    }

    override def convertToGpu(): GpuExec =
      GpuBatchScanExec(p.output, childScans.head.convertToGpu(), runtimeFilters)
  }
}
