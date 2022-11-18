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

import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids._
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils}
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.command.{AlterTableRecoverPartitionsCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.execution.GpuCustomShuffleReaderExec
import org.apache.spark.sql.rapids.execution.python._
import org.apache.spark.sql.types._

// 31x nondb shims, used by 311cdh and 31x
abstract class Spark31XShims extends Spark31Xuntil33XShims with Logging {
  override def parquetRebaseReadKey: String =
    SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ.key
  override def parquetRebaseWriteKey: String =
    SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE.key
  override def avroRebaseReadKey: String =
    SQLConf.LEGACY_AVRO_REBASE_MODE_IN_READ.key
  override def avroRebaseWriteKey: String =
    SQLConf.LEGACY_AVRO_REBASE_MODE_IN_WRITE.key
  override def parquetRebaseRead(conf: SQLConf): String =
    conf.getConf(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ)
  override def parquetRebaseWrite(conf: SQLConf): String =
    conf.getConf(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE)

  override def sessionFromPlan(plan: SparkPlan): SparkSession = {
    plan.sqlContext.sparkSession
  }

  override def filesFromFileIndex(fileIndex: PartitioningAwareFileIndex): Seq[FileStatus] = {
    fileIndex.allFiles()
  }

  def broadcastModeTransform(mode: BroadcastMode, rows: Array[InternalRow]): Any =
    mode.transform(rows)

  override def newBroadcastQueryStageExec(
      old: BroadcastQueryStageExec,
      newPlan: SparkPlan): BroadcastQueryStageExec = BroadcastQueryStageExec(old.id, newPlan)

  override def getDateFormatter(): DateFormatter = {
    DateFormatter(DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone))
  }

  override def isExchangeOp(plan: SparkPlanMeta[_]): Boolean = {
    // if the child query stage already executed on GPU then we need to keep the
    // next operator on GPU in these cases
    SQLConf.get.adaptiveExecutionEnabled && (plan.wrapped match {
      case _: CustomShuffleReaderExec
           | _: ShuffledHashJoinExec
           | _: BroadcastHashJoinExec
           | _: BroadcastExchangeExec
           | _: BroadcastNestedLoopJoinExec => true
      case _ => false
    })
  }

  override def isAqePlan(p: SparkPlan): Boolean = p match {
    case _: AdaptiveSparkPlanExec |
         _: QueryStageExec |
         _: CustomShuffleReaderExec => true
    case _ => false
  }

  override def isCustomReaderExec(x: SparkPlan): Boolean = x match {
    case _: GpuCustomShuffleReaderExec | _: CustomShuffleReaderExec => true
    case _ => false
  }

  override def aqeShuffleReaderExec: ExecRule[_ <: SparkPlan] =
    GpuOverrides.exec[CustomShuffleReaderExec](
      "A wrapper of shuffle query stage",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.ARRAY +
          TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY).nested(), TypeSig.all),
      (exec, conf, p, r) => new GpuCustomShuffleReaderMeta(exec, conf, p, r))

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
        case other => other.children.flatMap(p => recurse(p, predicate, accum)).headOption
      }
      accum
    }
    recurse(plan, predicate, new ListBuffer[SparkPlan]())
  }

  override def skipAssertIsOnTheGpu(plan: SparkPlan): Boolean = false

  override def shouldFailDivOverflow: Boolean = false

  override def leafNodeDefaultParallelism(ss: SparkSession): Int = {
    ss.sparkContext.defaultParallelism
  }

  override def v1RepairTableCommand(tableName: TableIdentifier): RunnableCommand =
    AlterTableRecoverPartitionsCommand(tableName)

  override def isWindowFunctionExec(plan: SparkPlan): Boolean = plan.isInstanceOf[WindowExecBase]

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
    new FileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def hasAliasQuoteFix: Boolean = false

  override def reusedExchangeExecPfn: PartialFunction[SparkPlan, ReusedExchangeExec] = {
    case ShuffleQueryStageExec(_, e: ReusedExchangeExec) => e
    case BroadcastQueryStageExec(_, e: ReusedExchangeExec) => e
  }

  override def int96ParquetRebaseRead(conf: SQLConf): String =
    conf.getConf(SQLConf.LEGACY_PARQUET_INT96_REBASE_MODE_IN_READ)
  override def int96ParquetRebaseWrite(conf: SQLConf): String =
    conf.getConf(SQLConf.LEGACY_PARQUET_INT96_REBASE_MODE_IN_WRITE)
  override def int96ParquetRebaseReadKey: String =
    SQLConf.LEGACY_PARQUET_INT96_REBASE_MODE_IN_READ.key
  override def int96ParquetRebaseWriteKey: String =
    SQLConf.LEGACY_PARQUET_INT96_REBASE_MODE_IN_WRITE.key

  override def tryTransformIfEmptyRelation(mode: BroadcastMode): Option[Any] = {
    Some(broadcastModeTransform(mode, Array.empty)).filter(isEmptyRelation)
  }

  override def isEmptyRelation(relation: Any): Boolean = relation match {
    case EmptyHashedRelation => true
    case arr: Array[InternalRow] if arr.isEmpty => true
    case _ => false
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
      (cast, conf, p, r) => new CastExprMeta[AnsiCast](cast, ansiEnabled = true, conf = conf,
        parent = p, rule = r, doFloatToIntCheck = true, stringToAnsiDate = false))
  }

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[Cast](
        "Convert a column of one type of data into another type",
        new CastChecks(),
        (cast, conf, p, r) => new CastExprMeta[Cast](cast,
          SparkSession.active.sessionState.conf.ansiEnabled, conf, p, r,
          doFloatToIntCheck = true, stringToAnsiDate = false)),
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
      GpuOverrides.exec[WindowInPandasExec](
        "The backend for Window Aggregation Pandas UDF, Accelerates the data transfer between" +
          " the Java process and the Python process. It also supports scheduling GPU resources" +
          " for the Python process when enabled. For now it only supports row based window frame.",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested(TypeSig.commonCudfTypes),
          TypeSig.all),
        (winPy, conf, p, r) => new GpuWindowInPandasExecMetaBase(winPy, conf, p, r) {
          override val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
            winPy.windowExpression.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

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

          override def tagPlanForGpu(): Unit = GpuFileSourceScanExec.tagSupport(this)

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
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
  }

  /** dropped by SPARK-34234 */
  override def attachTreeIfSupported[TreeType <: TreeNode[_], A](
      tree: TreeType,
      msg: String)(
      f: => A
  ): A = {
    attachTree(tree, msg)(f)
  }

  override def getAdaptiveInputPlan(adaptivePlan: AdaptiveSparkPlanExec): SparkPlan = {
    adaptivePlan.inputPlan
  }

  override def hasCastFloatTimestampUpcast: Boolean = false

  override def isCastingStringToNegDecimalScaleSupported: Boolean = false

  override def supportsColumnarAdaptivePlans: Boolean = false

  override def columnarAdaptivePlan(a: AdaptiveSparkPlanExec, goal: CoalesceSizeGoal): SparkPlan = {
    // When the input is an adaptive plan we do not get to see the GPU version until
    // the plan is executed and sometimes the plan will have a GpuColumnarToRowExec as the
    // final operator and we can bypass this to keep the data columnar by inserting
    // the [[AvoidAdaptiveTransitionToRow]] operator here
    AvoidAdaptiveTransitionToRow(GpuRowToColumnarExec(a, goal))
  }
}
