/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "311"}
{"spark": "312"}
{"spark": "313"}
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "321db"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "340"}
{"spark": "341"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.python.AggregateInPandasExec
import org.apache.spark.sql.rapids.execution.python.{GpuAggregateInPandasExec, GpuPythonUDF}

class GpuAggregateInPandasExecMeta(
    aggPandas: AggregateInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
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

  override def convertToGpu(): GpuExec =
    GpuAggregateInPandasExec(
      groupingNamedExprs.map(_.convertToGpu()).asInstanceOf[Seq[NamedExpression]],
      udfs.map(_.convertToGpu()).asInstanceOf[Seq[GpuPythonUDF]],
      resultNamedExprs.map(_.convertToGpu()).asInstanceOf[Seq[NamedExpression]],
      childPlans.head.convertIfNeeded()
    )(aggPandas.groupingExpressions)
}