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

import ai.rapids.cudf.DType
import com.nvidia.spark.InMemoryTableScanMeta
import com.nvidia.spark.rapids._
import org.apache.parquet.schema.MessageType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BaseSubqueryExec, CoalesceExec, FileSourceScanExec, FilterExec, InSubqueryExec, ProjectExec, ReusedSubqueryExec, SparkPlan, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, FilePartition, FileScanRDD, HadoopFsRelation, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.execution.GpuShuffleMeta
import org.apache.spark.sql.rapids.shims.GpuTimeAdd
import org.apache.spark.sql.types.{CalendarIntervalType, DayTimeIntervalType, DecimalType, StructType}
import org.apache.spark.unsafe.types.CalendarInterval

trait Spark33XShims extends Spark321PlusShims with Spark320PlusNonDBShims {

  /**
   * For spark3.3+ optionally return null if element not exists.
   */
  override def shouldFailOnElementNotExists(): Boolean = SQLConf.get.strictIndexOperator

  override def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan] = null

  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference]): RDD[InternalRow] = {
    new FileScanRDD(sparkSession, readFunction, filePartitions, readDataSchema, metadataColumns)
  }

  override def getParquetFilters(
      schema: MessageType,
      pushDownDate: Boolean,
      pushDownTimestamp: Boolean,
      pushDownDecimal: Boolean,
      pushDownStartWith: Boolean,
      pushDownInFilterThreshold: Int,
      caseSensitive: Boolean,
      lookupFileMeta: String => String,
      dateTimeRebaseModeFromConf: String): ParquetFilters = {
    val datetimeRebaseMode = DataSourceUtils
      .datetimeRebaseSpec(lookupFileMeta, dateTimeRebaseModeFromConf)
    new ParquetFilters(schema, pushDownDate, pushDownTimestamp, pushDownDecimal, pushDownStartWith,
      pushDownInFilterThreshold, caseSensitive, datetimeRebaseMode)
  }

  override def tagFileSourceScanExec(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    if (meta.wrapped.expressions.exists {
      case FileSourceMetadataAttribute(_) => true
      case _ => false
    }) {
      meta.willNotWorkOnGpu("hidden metadata columns are not supported on GPU")
    }
    super.tagFileSourceScanExec(meta)
  }

  // 330+ supports DAYTIME interval types
  override def getFileFormats: Map[FileFormatType, Map[FileFormatOp, FileFormatChecks]] = {
    Map(
      (CsvFormatType, FileFormatChecks(
        cudfRead = TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.DAYTIME,
        cudfWrite = TypeSig.none,
        sparkSig = TypeSig.cpuAtomics)),
      (ParquetFormatType, FileFormatChecks(
        cudfRead = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
            TypeSig.ARRAY + TypeSig.MAP + TypeSig.DAYTIME).nested(),
        cudfWrite = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
            TypeSig.ARRAY + TypeSig.MAP + TypeSig.DAYTIME).nested(),
        sparkSig = (TypeSig.cpuAtomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
            TypeSig.UDT + TypeSig.DAYTIME).nested())))
  }

  // 330+ supports DAYTIME interval types
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val _gpuCommonTypes = TypeSig.commonCudfTypes + TypeSig.NULL
    val map: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[Coalesce](
        "Returns the first non-null argument if exists. Otherwise, null",
        ExprChecks.projectOnly(
          (_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT +
              TypeSig.ansiInterval).nested(),
          TypeSig.all,
          repeatingParamCheck = Some(RepeatingParamCheck("param",
            (_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT +
                TypeSig.ansiInterval).nested(),
            TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[Coalesce](a, conf, p, r) {
          override def convertToGpu():
          GpuExpression = GpuCoalesce(childExprs.map(_.convertToGpu()))
        }),
      GpuOverrides.expr[AttributeReference](
        "References an input column",
        ExprChecks.projectAndAst(
          TypeSig.astTypes,
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
              TypeSig.STRUCT + TypeSig.DECIMAL_128 + TypeSig.ansiInterval).nested(),
          TypeSig.all),
        (att, conf, p, r) => new BaseExprMeta[AttributeReference](att, conf, p, r) {
          // This is the only NOOP operator.  It goes away when things are bound
          override def convertToGpu(): Expression = att

          // There are so many of these that we don't need to print them out, unless it
          // will not work on the GPU
          override def print(append: StringBuilder, depth: Int, all: Boolean): Unit = {
            if (!this.canThisBeReplaced || cannotRunOnGpuBecauseOfSparkPlan) {
              super.print(append, depth, all)
            }
          }
        }),
      GpuOverrides.expr[RoundCeil](
        "Computes the ceiling of the given expression to d decimal places",
        ExprChecks.binaryProject(
          TypeSig.gpuNumeric, TypeSig.cpuNumeric,
          ("value", TypeSig.gpuNumeric +
              TypeSig.psNote(TypeEnum.FLOAT, "result may round slightly differently") +
              TypeSig.psNote(TypeEnum.DOUBLE, "result may round slightly differently"),
              TypeSig.cpuNumeric),
          ("scale", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT))),
        (ceil, conf, p, r) => new BinaryExprMeta[RoundCeil](ceil, conf, p, r) {
          override def tagExprForGpu(): Unit = {
            ceil.child.dataType match {
              case dt: DecimalType =>
                val precision = GpuFloorCeil.unboundedOutputPrecision(dt)
                if (precision > DType.DECIMAL128_MAX_PRECISION) {
                  willNotWorkOnGpu(s"output precision $precision would require overflow " +
                      s"checks, which are not supported yet")
                }
              case _ => // NOOP
            }
            GpuOverrides.extractLit(ceil.scale).foreach { scale =>
              if (scale.value != null &&
                  scale.value.asInstanceOf[Integer] != 0) {
                willNotWorkOnGpu("Scale other than 0 is not supported")
              }
            }
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuCeil(lhs)
        }),
      GpuOverrides.expr[RoundFloor](
        "Computes the floor of the given expression to d decimal places",
        ExprChecks.binaryProject(
          TypeSig.gpuNumeric, TypeSig.cpuNumeric,
          ("value", TypeSig.gpuNumeric +
              TypeSig.psNote(TypeEnum.FLOAT, "result may round slightly differently") +
              TypeSig.psNote(TypeEnum.DOUBLE, "result may round slightly differently"),
              TypeSig.cpuNumeric),
          ("scale", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT))),
        (floor, conf, p, r) => new BinaryExprMeta[RoundFloor](floor, conf, p, r) {
          override def tagExprForGpu(): Unit = {
            floor.child.dataType match {
              case dt: DecimalType =>
                val precision = GpuFloorCeil.unboundedOutputPrecision(dt)
                if (precision > DType.DECIMAL128_MAX_PRECISION) {
                  willNotWorkOnGpu(s"output precision $precision would require overflow " +
                      s"checks, which are not supported yet")
                }
              case _ => // NOOP
            }
            GpuOverrides.extractLit(floor.scale).foreach { scale =>
              if (scale.value != null &&
                  scale.value.asInstanceOf[Integer] != 0) {
                willNotWorkOnGpu("Scale other than 0 is not supported")
              }
            }
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuFloor(lhs)
        }),
      GpuOverrides.expr[TimeAdd](
        "Adds interval to timestamp",
        ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
          ("start", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
          // interval support DAYTIME column or CALENDAR literal
          ("interval", TypeSig.DAYTIME + TypeSig.lit(TypeEnum.CALENDAR)
              .withPsNote(TypeEnum.CALENDAR, "month intervals are not supported"),
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
            checkTimeZoneId(timeAdd.timeZoneId)
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuTimeAdd(lhs, rhs)
        }),
      GpuOverrides.expr[IsNull](
        "Checks if a value is null",
        ExprChecks.unaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
              TypeSig.STRUCT + TypeSig.DECIMAL_128 + TypeSig.DAYTIME).nested(),
          TypeSig.all),
        (a, conf, p, r) => new UnaryExprMeta[IsNull](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = GpuIsNull(child)
        }),
      GpuOverrides.expr[IsNotNull](
        "Checks if a value is not null",
        ExprChecks.unaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
              TypeSig.STRUCT + TypeSig.DECIMAL_128 + TypeSig.DAYTIME).nested(),
          TypeSig.all),
        (a, conf, p, r) => new UnaryExprMeta[IsNotNull](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = GpuIsNotNull(child)
        }),
      GpuOverrides.expr[EqualNullSafe](
        "Check if the values are equal including nulls <=>",
        ExprChecks.binaryProject(
          TypeSig.BOOLEAN, TypeSig.BOOLEAN,
          ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.DAYTIME,
              TypeSig.comparable),
          ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.DAYTIME,
              TypeSig.comparable)),
        (a, conf, p, r) => new BinaryExprMeta[EqualNullSafe](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuEqualNullSafe(lhs, rhs)
        }),
      GpuOverrides.expr[EqualTo](
        "Check if the values are equal",
        ExprChecks.binaryProjectAndAst(
          TypeSig.comparisonAstTypes,
          TypeSig.BOOLEAN, TypeSig.BOOLEAN,
          ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.DAYTIME,
              TypeSig.comparable),
          ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.DAYTIME,
              TypeSig.comparable)),
        (a, conf, p, r) => new BinaryAstExprMeta[EqualTo](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuEqualTo(lhs, rhs)
        }),
      GpuOverrides.expr[GreaterThan](
        "> operator",
        ExprChecks.binaryProjectAndAst(
          TypeSig.comparisonAstTypes,
          TypeSig.BOOLEAN, TypeSig.BOOLEAN,
          ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.DAYTIME,
              TypeSig.orderable),
          ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.DAYTIME,
              TypeSig.orderable)),
        (a, conf, p, r) => new BinaryAstExprMeta[GreaterThan](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuGreaterThan(lhs, rhs)
        }),
      GpuOverrides.expr[GreaterThanOrEqual](
        ">= operator",
        ExprChecks.binaryProjectAndAst(
          TypeSig.comparisonAstTypes,
          TypeSig.BOOLEAN, TypeSig.BOOLEAN,
          ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.DAYTIME,
              TypeSig.orderable),
          ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.DAYTIME,
              TypeSig.orderable)),
        (a, conf, p, r) => new BinaryAstExprMeta[GreaterThanOrEqual](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuGreaterThanOrEqual(lhs, rhs)
        }),
      GpuOverrides.expr[LessThan](
        "< operator",
        ExprChecks.binaryProjectAndAst(
          TypeSig.comparisonAstTypes,
          TypeSig.BOOLEAN, TypeSig.BOOLEAN,
          ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.DAYTIME,
              TypeSig.orderable),
          ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.DAYTIME,
              TypeSig.orderable)),
        (a, conf, p, r) => new BinaryAstExprMeta[LessThan](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuLessThan(lhs, rhs)
        }),
      GpuOverrides.expr[LessThanOrEqual](
        "<= operator",
        ExprChecks.binaryProjectAndAst(
          TypeSig.comparisonAstTypes,
          TypeSig.BOOLEAN, TypeSig.BOOLEAN,
          ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.DAYTIME,
              TypeSig.orderable),
          ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.DAYTIME,
              TypeSig.orderable)),
        (a, conf, p, r) => new BinaryAstExprMeta[LessThanOrEqual](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuLessThanOrEqual(lhs, rhs)
        }),
        GpuOverrides.expr[Abs](
          "Absolute value",
          ExprChecks.unaryProjectAndAstInputMatchesOutput(
            TypeSig.implicitCastsAstTypes, TypeSig.gpuNumeric + TypeSig.ansiInterval,
            TypeSig.cpuNumeric + TypeSig.ansiInterval),
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
      GpuOverrides.expr[UnaryMinus](
        "Negate a numeric value",
        ExprChecks.unaryProjectAndAstInputMatchesOutput(
          TypeSig.implicitCastsAstTypes,
          TypeSig.gpuNumeric + TypeSig.ansiInterval,
          TypeSig.numericAndInterval),
        (a, conf, p, r) => new UnaryAstExprMeta[UnaryMinus](a, conf, p, r) {
          val ansiEnabled = SQLConf.get.ansiEnabled

          override def tagSelfForAst(): Unit = {
            if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
              willNotWorkInAst("AST unary minus does not support ANSI mode.")
            }
          }

          override def convertToGpu(child: Expression): GpuExpression =
            GpuUnaryMinus(child, ansiEnabled)
        }),
      GpuOverrides.expr[UnaryPositive](
        "A numeric value with a + in front of it",
        ExprChecks.unaryProjectAndAstInputMatchesOutput(
          TypeSig.astTypes,
          TypeSig.gpuNumeric + TypeSig.ansiInterval,
          TypeSig.numericAndInterval),
        (a, conf, p, r) => new UnaryAstExprMeta[UnaryPositive](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = GpuUnaryPositive(child)
        }),
      GpuOverrides.expr[Add](
        "Addition",
        ExprChecks.binaryProjectAndAst(
          TypeSig.implicitCastsAstTypes,
          TypeSig.gpuNumeric + TypeSig.ansiInterval, TypeSig.numericAndInterval,
          ("lhs", TypeSig.gpuNumeric + TypeSig.ansiInterval,
              TypeSig.numericAndInterval),
          ("rhs", TypeSig.gpuNumeric + TypeSig.ansiInterval,
              TypeSig.numericAndInterval)),
        (a, conf, p, r) => new BinaryAstExprMeta[Add](a, conf, p, r) {
          private val ansiEnabled = SQLConf.get.ansiEnabled

          override def tagSelfForAst(): Unit = {
            if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
              willNotWorkInAst("AST Addition does not support ANSI mode.")
            }
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuAdd(lhs, rhs, failOnError = ansiEnabled)
        }),
      GpuOverrides.expr[Subtract](
        "Subtraction",
        ExprChecks.binaryProjectAndAst(
          TypeSig.implicitCastsAstTypes,
          TypeSig.gpuNumeric + TypeSig.ansiInterval, TypeSig.numericAndInterval,
          ("lhs", TypeSig.gpuNumeric + TypeSig.ansiInterval,
              TypeSig.numericAndInterval),
          ("rhs", TypeSig.gpuNumeric + TypeSig.ansiInterval,
              TypeSig.numericAndInterval)),
        (a, conf, p, r) => new BinaryAstExprMeta[Subtract](a, conf, p, r) {
          private val ansiEnabled = SQLConf.get.ansiEnabled

          override def tagSelfForAst(): Unit = {
            if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
              willNotWorkInAst("AST Subtraction does not support ANSI mode.")
            }
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuSubtract(lhs, rhs, ansiEnabled)
        }),
      GpuOverrides.expr[Alias](
        "Gives a column a name",
        ExprChecks.unaryProjectAndAstInputMatchesOutput(
          TypeSig.astTypes,
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT
              + TypeSig.DECIMAL_128 + TypeSig.ansiInterval).nested(),
          TypeSig.all),
        (a, conf, p, r) => new UnaryAstExprMeta[Alias](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression =
            GpuAlias(child, a.name)(a.exprId, a.qualifier, a.explicitMetadata)
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ map
  }

  // 330+ supports DAYTIME interval types
  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    val _gpuCommonTypes = TypeSig.commonCudfTypes + TypeSig.NULL
    val map: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
      GpuOverrides.exec[ShuffleExchangeExec](
        "The backend for most data being exchanged between processes",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ansiInterval).nested()
            .withPsNote(TypeEnum.STRUCT, "Round-robin partitioning is not supported for nested " +
                s"structs if ${SQLConf.SORT_BEFORE_REPARTITION.key} is true")
            .withPsNote(TypeEnum.ARRAY, "Round-robin partitioning is not supported if " +
                s"${SQLConf.SORT_BEFORE_REPARTITION.key} is true")
            .withPsNote(TypeEnum.MAP, "Round-robin partitioning is not supported if " +
                s"${SQLConf.SORT_BEFORE_REPARTITION.key} is true"),
          TypeSig.all),
        (shuffle, conf, p, r) => new GpuShuffleMeta(shuffle, conf, p, r)),
      GpuOverrides.exec[BatchScanExec](
        "The backend for most file input",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY +
              TypeSig.DECIMAL_128 + TypeSig.DAYTIME).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new SparkPlanMeta[BatchScanExec](p, conf, parent, r) {
          override val childScans: scala.Seq[ScanMeta[_]] =
            Seq(GpuOverrides.wrapScan(p.scan, conf, Some(this)))

          override def convertToGpu(): GpuExec =
            GpuBatchScanExec(p.output, childScans.head.convertToGpu())
        }),
      GpuOverrides.exec[CoalesceExec](
        "The backend for the dataframe coalesce method",
        ExecChecks((_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT + TypeSig.ARRAY +
            TypeSig.MAP + TypeSig.ansiInterval).nested(),
          TypeSig.all),
        (coalesce, conf, parent, r) => new SparkPlanMeta[CoalesceExec](coalesce, conf, parent, r) {
          override def convertToGpu(): GpuExec =
            GpuCoalesceExec(coalesce.numPartitions, childPlans.head.convertIfNeeded())
        }),
      GpuOverrides.exec[DataWritingCommandExec](
        "Writing data",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128.withPsNote(
          TypeEnum.DECIMAL, "128bit decimal only supported for Orc and Parquet") +
            TypeSig.STRUCT.withPsNote(TypeEnum.STRUCT, "Only supported for Parquet") +
            TypeSig.MAP.withPsNote(TypeEnum.MAP, "Only supported for Parquet") +
            TypeSig.ARRAY.withPsNote(TypeEnum.ARRAY, "Only supported for Parquet") +
            TypeSig.DAYTIME).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new SparkPlanMeta[DataWritingCommandExec](p, conf, parent, r) {
          override val childDataWriteCmds: scala.Seq[DataWritingCommandMeta[_]] =
            Seq(GpuOverrides.wrapDataWriteCmds(p.cmd, conf, Some(this)))

          override def convertToGpu(): GpuExec =
            GpuDataWritingCommandExec(childDataWriteCmds.head.convertToGpu(),
              childPlans.head.convertIfNeeded())
        }),
      // this is copied, only added TypeSig.DAYTIME check
      GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.ARRAY + TypeSig.DECIMAL_128 + TypeSig.DAYTIME).nested(),
          TypeSig.all),
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
                case dpe@DynamicPruningExpression(inSub: InSubqueryExec) =>
                  inSub.plan match {
                    case bc: SubqueryBroadcastExec =>
                      dpe.copy(inSub.copy(plan = convertBroadcast(bc)))
                    case reuse@ReusedSubqueryExec(bc: SubqueryBroadcastExec) =>
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
              wrapped.optionalNumCoalescedBuckets,
              wrapped.dataFilters,
              wrapped.tableIdentifier,
              wrapped.disableBucketedScan)(conf)
          }
        }),
      GpuOverrides.exec[InMemoryTableScanExec](
        "Implementation of InMemoryTableScanExec to use GPU accelerated Caching",
        // NullType is actually supported
        ExecChecks(TypeSig.commonCudfTypesWithNested + TypeSig.DAYTIME, TypeSig.all),
        (scan, conf, p, r) => new InMemoryTableScanMeta(scan, conf, p, r)),
      GpuOverrides.exec[ProjectExec](
        "The backend for most select, withColumn and dropColumn statements",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
              TypeSig.ARRAY + TypeSig.DECIMAL_128 + TypeSig.ansiInterval).nested(),
          TypeSig.all),
        (proj, conf, p, r) => new GpuProjectExecMeta(proj, conf, p, r)),
      GpuOverrides.exec[FilterExec](
        "The backend for most filter statements",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.ARRAY + TypeSig.DECIMAL_128 + TypeSig.DAYTIME).nested(), TypeSig.all),
        (filter, conf, p, r) => new SparkPlanMeta[FilterExec](filter, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuFilterExec(childExprs.head.convertToGpu(), childPlans.head.convertIfNeeded())
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
    super.getExecs ++ map
  }

}

// Fallback to the default definition of `deterministic`
trait GpuDeterministicFirstLastCollectShim extends Expression
