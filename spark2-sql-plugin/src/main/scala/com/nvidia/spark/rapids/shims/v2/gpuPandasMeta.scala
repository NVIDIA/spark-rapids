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

package com.nvidia.spark.rapids.shims.v2

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.python.{AggregateInPandasExec, ArrowEvalPythonExec, FlatMapGroupsInPandasExec, WindowInPandasExec}
import org.apache.spark.sql.types._

abstract class GpuWindowInPandasExecMetaBase(
    winPandas: WindowInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[WindowInPandasExec](winPandas, conf, parent, rule) {

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  val windowExpressions: Seq[BaseExprMeta[NamedExpression]]

  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    winPandas.partitionSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    winPandas.orderSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  // Same check with that in GpuWindowExecMeta
  override def tagPlanForGpu(): Unit = {
    // Implementation depends on receiving a `NamedExpression` wrapped WindowExpression.
    windowExpressions.map(meta => meta.wrapped)
      .filter(expr => !expr.isInstanceOf[NamedExpression])
      .foreach(_ => willNotWorkOnGpu(because = "Unexpected query plan with Windowing" +
        " Pandas UDF; cannot convert for GPU execution. " +
        "(Detail: WindowExpression not wrapped in `NamedExpression`.)"))

    // Early check for the frame type, only supporting RowFrame for now, which is different from
    // the node GpuWindowExec.
    windowExpressions
      .flatMap(meta => meta.wrapped.collect { case e: SpecifiedWindowFrame => e })
      .filter(swf => swf.frameType.equals(RangeFrame))
      .foreach(rf => willNotWorkOnGpu(because = s"Only support RowFrame for now," +
        s" but found ${rf.frameType}"))
  }
}

class GpuAggregateInPandasExecMeta(
    aggPandas: AggregateInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[AggregateInPandasExec](aggPandas, conf, parent, rule) {

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  private val groupingNamedExprs: Seq[BaseExprMeta[NamedExpression]] =
    aggPandas.groupingExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val udfs: Seq[BaseExprMeta[PythonUDF]] =
    aggPandas.udfExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val resultNamedExprs: Seq[BaseExprMeta[NamedExpression]] =
    aggPandas.resultExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] = groupingNamedExprs ++ udfs ++ resultNamedExprs
}

class GpuFlatMapGroupsInPandasExecMeta(
    flatPandas: FlatMapGroupsInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[FlatMapGroupsInPandasExec](flatPandas, conf, parent, rule) {

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  private val groupingAttrs: Seq[BaseExprMeta[Attribute]] =
    flatPandas.groupingAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val udf: BaseExprMeta[PythonUDF] = GpuOverrides.wrapExpr(
    flatPandas.func.asInstanceOf[PythonUDF], conf, Some(this))

  private val resultAttrs: Seq[BaseExprMeta[Attribute]] =
    flatPandas.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] = groupingAttrs ++ resultAttrs :+ udf
}
