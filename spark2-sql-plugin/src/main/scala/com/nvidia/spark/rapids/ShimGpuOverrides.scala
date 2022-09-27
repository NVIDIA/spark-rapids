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
import org.apache.spark.sql.execution.python.{AggregateInPandasExec, FlatMapGroupsInPandasExec, WindowInPandasExec}
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
  
        }),
      GpuOverrides.expr[Abs](
        "Absolute value",
        ExprChecks.unaryProjectAndAstInputMatchesOutput(
          TypeSig.implicitCastsAstTypes, TypeSig.gpuNumeric,
          TypeSig.cpuNumeric),
        (a, conf, p, r) => new UnaryAstExprMeta[Abs](a, conf, p, r) {
        })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[FileSourceScanExec](
      "Reading data from files, often from Hive tables",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
        TypeSig.ARRAY + TypeSig.BINARY + TypeSig.DECIMAL_128).nested(), TypeSig.all),
        (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {

          // partition filters and data filters are not run on the GPU
          override val childExprs: Seq[ExprMeta[_]] = Seq.empty

          override def tagPlanForGpu(): Unit = GpuFileSourceScanExec.tagSupport(this)
        }),
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
      }).disabledByDefault("it only supports row based frame for now")
  ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[SparkPlan]), r) }.toMap
}

