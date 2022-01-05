/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.v2

import java.nio.ByteBuffer

import com.nvidia.spark.rapids._
import org.apache.arrow.memory.ReferenceManager
import org.apache.arrow.vector.ValueVector

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rapids.shims.v2.GpuShuffleExchangeExec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.python.{AggregateInPandasExec, ArrowEvalPythonExec, FlatMapGroupsInPandasExec, MapInPandasExec, WindowInPandasExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuAbs, GpuAverage, GpuFileSourceScanExec, GpuTimeSub}
import org.apache.spark.sql.rapids.execution.{GpuShuffleExchangeExecBase, JoinTypeChecks}
import org.apache.spark.sql.rapids.execution.python._
import org.apache.spark.sql.rapids.execution.python.shims.v2._
import org.apache.spark.sql.types._
import org.apache.spark.storage.{BlockId, BlockManagerId}
import org.apache.spark.unsafe.types.CalendarInterval

abstract class Spark30XShims extends Spark301until320Shims with Logging {
  override def int96ParquetRebaseRead(conf: SQLConf): String =
    parquetRebaseRead(conf)
  override def int96ParquetRebaseWrite(conf: SQLConf): String =
    parquetRebaseWrite(conf)
  override def int96ParquetRebaseReadKey: String =
    parquetRebaseReadKey
  override def int96ParquetRebaseWriteKey: String =
    parquetRebaseWriteKey
  override def hasSeparateINT96RebaseConf: Boolean = false

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
    SparkEnv.get.mapOutputTracker.getMapSizesByRange(shuffleId,
      startMapIndex, endMapIndex, startPartition, endPartition)
  }

  override def getGpuShuffleExchangeExec(
      gpuOutputPartitioning: GpuPartitioning,
      child: SparkPlan,
      cpuOutputPartitioning: Partitioning,
      cpuShuffle: Option[ShuffleExchangeExec]): GpuShuffleExchangeExecBase = {
    val canChangeNumPartitions = cpuShuffle.forall(_.canChangeNumPartitions)
    GpuShuffleExchangeExec(gpuOutputPartitioning, child, canChangeNumPartitions)(
      cpuOutputPartitioning)
  }

  override def getGpuShuffleExchangeExec(
      queryStage: ShuffleQueryStageExec): GpuShuffleExchangeExecBase = {
    queryStage.shuffle.asInstanceOf[GpuShuffleExchangeExecBase]
  }

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
          TypeSig.ARRAY + TypeSig.DECIMAL_128_FULL).nested(), TypeSig.all),
        (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {

          // Replaces SubqueryBroadcastExec inside dynamic pruning filters with GPU counterpart
          // if possible. Instead regarding filters as childExprs of current Meta, we create
          // a new meta for SubqueryBroadcastExec. The reason is that the GPU replacement of
          // FileSourceScan is independent from the replacement of the partitionFilters. It is
          // possible that the FileSourceScan is on the CPU, while the dynamic partitionFilters
          // are on the GPU. And vice versa.
          private lazy val partitionFilters = wrapped.partitionFilters.map { filter =>
            filter.transformDown {
              case dpe @ DynamicPruningExpression(inSub: InSubqueryExec)
                if inSub.plan.isInstanceOf[SubqueryBroadcastExec] =>

                val subBcMeta = GpuOverrides.wrapAndTagPlan(inSub.plan, conf)
                subBcMeta.tagForExplain()
                val gpuSubBroadcast = subBcMeta.convertIfNeeded().asInstanceOf[BaseSubqueryExec]
                dpe.copy(inSub.copy(plan = gpuSubBroadcast))
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

            val location = replaceWithAlluxioPathIfNeeded(
              conf,
              wrapped.relation,
              partitionFilters,
              wrapped.dataFilters)

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
              None,
              wrapped.dataFilters,
              wrapped.tableIdentifier)(conf)
          }
        }),
      GpuOverrides.exec[SortMergeJoinExec](
        "Sort merge join, replacing with shuffled hash join",
        JoinTypeChecks.equiJoinExecChecks,
        (join, conf, p, r) => new GpuSortMergeJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[BroadcastHashJoinExec](
        "Implementation of join using broadcast data",
        JoinTypeChecks.equiJoinExecChecks,
        (join, conf, p, r) => new GpuBroadcastHashJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[ShuffledHashJoinExec](
        "Implementation of join using hashed shuffled data",
        JoinTypeChecks.equiJoinExecChecks,
        (join, conf, p, r) => new GpuShuffledHashJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[ArrowEvalPythonExec](
        "The backend of the Scalar Pandas UDFs. Accelerates the data transfer between the" +
          " Java process and the Python process. It also supports scheduling GPU resources" +
          " for the Python process when enabled",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all),
        (e, conf, p, r) =>
          new SparkPlanMeta[ArrowEvalPythonExec](e, conf, p, r) {
            val udfs: Seq[BaseExprMeta[PythonUDF]] =
              e.udfs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
            val resultAttrs: Seq[BaseExprMeta[Attribute]] =
              e.resultAttrs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
            override val childExprs: Seq[BaseExprMeta[_]] = udfs ++ resultAttrs

            override def replaceMessage: String = "partially run on GPU"
            override def noReplacementPossibleMessage(reasons: String): String =
              s"cannot run even partially on the GPU because $reasons"

            override def convertToGpu(): GpuExec =
              GpuArrowEvalPythonExec(udfs.map(_.convertToGpu()).asInstanceOf[Seq[GpuPythonUDF]],
                resultAttrs.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
                childPlans.head.convertIfNeeded(),
                e.evalType)
          }),
      GpuOverrides.exec[MapInPandasExec](
        "The backend for Map Pandas Iterator UDF. Accelerates the data transfer between the" +
          " Java process and the Python process. It also supports scheduling GPU resources" +
          " for the Python process when enabled.",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all),
        (mapPy, conf, p, r) => new GpuMapInPandasExecMeta(mapPy, conf, p, r)),
      GpuOverrides.exec[FlatMapGroupsInPandasExec](
        "The backend for Flat Map Groups Pandas UDF, Accelerates the data transfer between the" +
          " Java process and the Python process. It also supports scheduling GPU resources" +
          " for the Python process when enabled.",
        ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
        (flatPy, conf, p, r) => new GpuFlatMapGroupsInPandasExecMeta(flatPy, conf, p, r)),
      GpuOverrides.exec[AggregateInPandasExec](
        "The backend for an Aggregation Pandas UDF, this accelerates the data transfer between" +
          " the Java process and the Python process. It also supports scheduling GPU resources" +
          " for the Python process when enabled.",
        ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
        (aggPy, conf, p, r) => new GpuAggregateInPandasExecMeta(aggPy, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
  }

  protected def getExprsSansTimeSub: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    Seq(
      GpuOverrides.expr[Cast](
        "Convert a column of one type of data into another type",
        new CastChecks(),
        (cast, conf, p, r) => new CastExprMeta[Cast](cast,
          SparkSession.active.sessionState.conf.ansiEnabled, conf, p, r,
          doFloatToIntCheck = false, stringToAnsiDate = false)),
      GpuOverrides.expr[AnsiCast](
        "Convert a column of one type of data into another type",
        new CastChecks(),
        (cast, conf, p, r) => new CastExprMeta[AnsiCast](cast, ansiEnabled = true, conf = conf,
          parent = p, rule = r, doFloatToIntCheck = false, stringToAnsiDate = false)),
      GpuOverrides.expr[Average](
        "Average aggregate operator",
        ExprChecks.fullAgg(
          TypeSig.DOUBLE + TypeSig.DECIMAL_128_FULL,
          TypeSig.DOUBLE + TypeSig.DECIMAL_128_FULL,
          Seq(ParamCheck("input",
            TypeSig.integral + TypeSig.fp + TypeSig.DECIMAL_128_FULL,
            TypeSig.numeric))),
        (a, conf, p, r) => new AggExprMeta[Average](a, conf, p, r) {
          override def tagAggForGpu(): Unit = {
            // For Decimal Average the SUM adds a precision of 10 to avoid overflowing
            // then it divides by the count with an output scale that is 4 more than the input
            // scale. With how our divide works to match Spark, this means that we will need a
            // precision of 5 more. So 38 - 10 - 5 = 23
            val dataType = a.child.dataType
            dataType match {
              case dt: DecimalType =>
                if (dt.precision > 23) {
                  if (conf.needDecimalGuarantees) {
                    willNotWorkOnGpu("GpuAverage cannot guarantee proper overflow checks for " +
                      s"a precision large than 23. The current precision is ${dt.precision}")
                  } else {
                    logWarning("Decimal overflow guarantees disabled for " +
                      s"Average(${a.child.dataType}) produces $dt with an " +
                      s"intermediate precision of ${dt.precision + 15}")
                  }
                }
              case _ => // NOOP
            }
            GpuOverrides.checkAndTagFloatAgg(dataType, conf, this)
          }

          override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
            GpuAverage(childExprs.head)

          // Average is not supported in ANSI mode right now, no matter the type
          override val ansiTypeToCheck: Option[DataType] = None
        }),
      GpuOverrides.expr[Abs](
        "Absolute value",
        ExprChecks.unaryProjectAndAstInputMatchesOutput(
          TypeSig.implicitCastsAstTypes, TypeSig.gpuNumeric + TypeSig.DECIMAL_128_FULL,
          TypeSig.numeric),
        (a, conf, p, r) => new UnaryAstExprMeta[Abs](a, conf, p, r) {
          // ANSI support for ABS was added in 3.2.0 SPARK-33275
          override def convertToGpu(child: Expression): GpuExpression = GpuAbs(child, false)
        }),
      GpuOverrides.expr[RegExpReplace](
        "RegExpReplace support for string literal input patterns",
        ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
          Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
            ParamCheck("regex", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
            ParamCheck("rep", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
        (a, conf, p, r) => new GpuRegExpReplaceMeta(a, conf, p, r)).disabledByDefault(
        "the implementation is not 100% compatible. " +
          "See the compatibility guide for more information."),
      GpuScalaUDFMeta.exprMeta
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    getExprsSansTimeSub + (classOf[TimeSub] -> GpuOverrides.expr[TimeSub](
      "Subtracts interval from timestamp",
      ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
        ("start", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        ("interval", TypeSig.lit(TypeEnum.CALENDAR)
          .withPsNote(TypeEnum.CALENDAR, "months not supported"), TypeSig.CALENDAR)),
      (timeSub, conf, p, r) => new BinaryExprMeta[TimeSub](timeSub, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          timeSub.interval match {
            case Literal(intvl: CalendarInterval, DataTypes.CalendarIntervalType) =>
              if (intvl.months != 0) {
                willNotWorkOnGpu("interval months isn't supported")
              }
            case _ =>
          }
          checkTimeZoneId(timeSub.timeZoneId)
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuTimeSub(lhs, rhs)
      }))
  }

  // Hardcoded for Spark-3.0.*
  override def getFileSourceMaxMetadataValueLength(sqlConf: SQLConf): Int = 100

  override def getGpuColumnarToRowTransition(plan: SparkPlan,
      exportColumnRdd: Boolean): GpuColumnarToRowExecParent = {
    GpuColumnarToRowExec(plan, exportColumnRdd)
  }

  override def sortOrder(
      child: Expression,
      direction: SortDirection,
      nullOrdering: NullOrdering): SortOrder = SortOrder(child, direction, nullOrdering, Set.empty)

  override def copySortOrderWithNewChild(s: SortOrder, child: Expression): SortOrder = {
    s.copy(child = child)
  }

  override def shouldIgnorePath(path: String): Boolean = {
    InMemoryFileIndex.shouldFilterOut(path)
  }

  override def getLegacyComplexTypeToString(): Boolean = true

  // Arrow version changed between Spark versions
  override def getArrowDataBuf(vec: ValueVector): (ByteBuffer, ReferenceManager) = {
    val arrowBuf = vec.getDataBuffer
    (arrowBuf.nioBuffer(), arrowBuf.getReferenceManager)
  }

  override def shouldFailDivByZero(): Boolean = false

  /** dropped by SPARK-34234 */
  override def attachTreeIfSupported[TreeType <: TreeNode[_], A](
      tree: TreeType,
      msg: String)(
      f: => A
  ): A = {
    attachTree(tree, msg)(f)
  }

  override def hasCastFloatTimestampUpcast: Boolean = false

  override def getAdaptiveInputPlan(adaptivePlan: AdaptiveSparkPlanExec): SparkPlan = {
    adaptivePlan.initialPlan
  }

  override def isCastingStringToNegDecimalScaleSupported: Boolean = true

  // this is to help with an optimization in Spark 3.1, so we disable it by default in Spark 3.0.x
  override def isEmptyRelation(relation: Any): Boolean = false
  override def tryTransformIfEmptyRelation(mode: BroadcastMode): Option[Any] = None

  override def supportsColumnarAdaptivePlans: Boolean = false

  override def columnarAdaptivePlan(a: AdaptiveSparkPlanExec, goal: CoalesceSizeGoal): SparkPlan = {
    // When the input is an adaptive plan we do not get to see the GPU version until
    // the plan is executed and sometimes the plan will have a GpuColumnarToRowExec as the
    // final operator and we can bypass this to keep the data columnar by inserting
    // the [[AvoidAdaptiveTransitionToRow]] operator here
    AvoidAdaptiveTransitionToRow(GpuRowToColumnarExec(a, goal))
  }
}
