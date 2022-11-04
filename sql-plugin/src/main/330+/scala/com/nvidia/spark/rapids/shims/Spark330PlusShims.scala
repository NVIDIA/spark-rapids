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
import com.nvidia.spark.rapids._
import org.apache.parquet.schema.MessageType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.python.PythonMapInArrowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.execution.python.GpuPythonMapInArrowExecMeta
import org.apache.spark.sql.rapids.shims.{GpuDivideDTInterval, GpuDivideYMInterval, GpuMultiplyDTInterval, GpuMultiplyYMInterval, GpuTimeAdd}
import org.apache.spark.sql.types.{CalendarIntervalType, DayTimeIntervalType, DecimalType, StructType}
import org.apache.spark.unsafe.types.CalendarInterval

trait Spark330PlusShims extends Spark321PlusShims with Spark320PlusNonDBShims {

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

  // GPU support ANSI interval types from 330
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val map: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
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

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
            // use Spark `RoundCeil.dataType` to keep consistent between Spark versions.
            GpuCeil(lhs, ceil.dataType)
          }
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

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
            // use Spark `RoundFloor.dataType` to keep consistent between Spark versions.
            GpuFloor(lhs, floor.dataType)
          }
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
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuTimeAdd(lhs, rhs)
        }),
        GpuOverrides.expr[Abs](
          "Absolute value",
          ExprChecks.unaryProjectAndAstInputMatchesOutput(
            TypeSig.implicitCastsAstTypes,
            TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
            TypeSig.cpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes),
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
      GpuOverrides.expr[MultiplyYMInterval](
        "Year-month interval * number",
        ExprChecks.binaryProject(
          TypeSig.YEARMONTH,
          TypeSig.YEARMONTH,
          ("lhs", TypeSig.YEARMONTH, TypeSig.YEARMONTH),
          ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
        (a, conf, p, r) => new BinaryExprMeta[MultiplyYMInterval](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuMultiplyYMInterval(lhs, rhs)
        }),
      GpuOverrides.expr[MultiplyDTInterval](
        "Day-time interval * number",
        ExprChecks.binaryProject(
          TypeSig.DAYTIME,
          TypeSig.DAYTIME,
          ("lhs", TypeSig.DAYTIME, TypeSig.DAYTIME),
          ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
        (a, conf, p, r) => new BinaryExprMeta[MultiplyDTInterval](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuMultiplyDTInterval(lhs, rhs)
        }),
      GpuOverrides.expr[DivideYMInterval](
        "Year-month interval * operator",
        ExprChecks.binaryProject(
          TypeSig.YEARMONTH,
          TypeSig.YEARMONTH,
          ("lhs", TypeSig.YEARMONTH, TypeSig.YEARMONTH),
          ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
        (a, conf, p, r) => new BinaryExprMeta[DivideYMInterval](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuDivideYMInterval(lhs, rhs)
        }),
      GpuOverrides.expr[DivideDTInterval](
        "Day-time interval * operator",
        ExprChecks.binaryProject(
          TypeSig.DAYTIME,
          TypeSig.DAYTIME,
          ("lhs", TypeSig.DAYTIME, TypeSig.DAYTIME),
          ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
        (a, conf, p, r) => new BinaryExprMeta[DivideDTInterval](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuDivideDTInterval(lhs, rhs)
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ map
  }

  // GPU support ANSI interval types from 330
  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    val map: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
      GpuOverrides.exec[BatchScanExec](
        "The backend for most file input",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY +
              TypeSig.DECIMAL_128 + TypeSig.BINARY +
              GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new BatchScanExecMeta(p, conf, parent, r)),
      GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.ARRAY + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (fsse, conf, p, r) => new FileSourceScanExecMeta(fsse, conf, p, r)),
      GpuOverrides.exec[PythonMapInArrowExec](
        "The backend for Map Arrow Iterator UDF. Accelerates the data transfer between the" +
          " Java process and the Python process. It also supports scheduling GPU resources" +
          " for the Python process when enabled.",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all),
        (mapPy, conf, p, r) => new GpuPythonMapInArrowExecMeta(mapPy, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
    super.getExecs ++ map
  }

}

// Fallback to the default definition of `deterministic`
trait GpuDeterministicFirstLastCollectShim extends Expression
