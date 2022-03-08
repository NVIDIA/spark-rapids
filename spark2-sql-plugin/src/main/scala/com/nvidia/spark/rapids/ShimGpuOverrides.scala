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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.shims._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.{AggregateInPandasExec, ArrowEvalPythonExec, FlatMapGroupsInPandasExec, WindowInPandasExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.execution._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

// Overrides that are in the shim in normal sql-plugin, moved here for easier diffing
object ShimGpuOverrides extends Logging {

  val shimExpressions: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] =
    Seq(
      GpuOverrides.expr[Cast](
        "Convert a column of one type of data into another type",
        new CastChecks(),
        (cast, conf, p, r) => new CastExprMeta[Cast](cast, false, conf, p, r,
          doFloatToIntCheck = false, stringToAnsiDate = false)),
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
  
        }),
      GpuOverrides.expr[Abs](
        "Absolute value",
        ExprChecks.unaryProjectAndAstInputMatchesOutput(
          TypeSig.implicitCastsAstTypes, TypeSig.gpuNumeric,
          TypeSig.cpuNumeric),
        (a, conf, p, r) => new UnaryAstExprMeta[Abs](a, conf, p, r) {
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
      GpuOverrides.expr[TimeSub](
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
        }),
      GpuOverrides.expr[ScalaUDF](
        "User Defined Function, the UDF can choose to implement a RAPIDS accelerated interface " +
          "to get better performance.",
        ExprChecks.projectOnly(
          GpuUserDefinedFunction.udfTypeSig,
          TypeSig.all,
          repeatingParamCheck =
            Some(RepeatingParamCheck("param", GpuUserDefinedFunction.udfTypeSig, TypeSig.all))),
        (expr, conf, p, r) => new ScalaUDFMetaBase(expr, conf, p, r) {
        })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
     GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
          TypeSig.ARRAY + TypeSig.DECIMAL_128).nested(), TypeSig.all),
        (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {

          // partition filters and data filters are not run on the GPU
          override val childExprs: Seq[ExprMeta[_]] = Seq.empty

          override def tagPlanForGpu(): Unit = GpuFileSourceScanExec.tagSupport(this)
        }),
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
            e.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
          override val childExprs: Seq[BaseExprMeta[_]] = udfs ++ resultAttrs
          override def replaceMessage: String = "partially run on GPU"
          override def noReplacementPossibleMessage(reasons: String): String =
            s"cannot run even partially on the GPU because $reasons"
      }),
    GpuOverrides.exec[FlatMapGroupsInPandasExec](
      "The backend for Flat Map Groups Pandas UDF, Accelerates the data transfer between the" +
        " Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled.",
      ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
      (flatPy, conf, p, r) => new GpuFlatMapGroupsInPandasExecMeta(flatPy, conf, p, r)),
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
      }).disabledByDefault("it only supports row based frame for now"),
    GpuOverrides.exec[AggregateInPandasExec](
      "The backend for an Aggregation Pandas UDF, this accelerates the data transfer between" +
        " the Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled.",
      ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
      (aggPy, conf, p, r) => new GpuAggregateInPandasExecMeta(aggPy, conf, p, r))
  ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[SparkPlan]), r) }.toMap
}

