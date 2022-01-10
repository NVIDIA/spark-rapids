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
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, FlatMapGroupsInPandasExec}
import org.apache.spark.sql.execution.python.PythonUDF

object ShimOverrides {

  val exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[Concat](
      "List/String concatenate",
      ExprChecks.projectOnly((TypeSig.STRING + TypeSig.ARRAY).nested(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
        (TypeSig.STRING + TypeSig.BINARY + TypeSig.ARRAY).nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.STRING + TypeSig.ARRAY).nested(
            TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
          (TypeSig.STRING + TypeSig.BINARY + TypeSig.ARRAY).nested(TypeSig.all)))),
      (a, conf, p, r) => new ExprMeta[Concat](a, conf, p, r) {
      }),
    GpuOverrides.expr[PythonUDF](
      "UDF run in an external python process. Does not actually run on the GPU, but " +
          "the transfer of data to/from it can be accelerated",
      ExprChecks.fullAggAndProject(
        // Different types of Pandas UDF support different sets of output type. Please refer to
        //   https://github.com/apache/spark/blob/master/python/pyspark/sql/udf.py#L98
        // for more details.
        // It is impossible to specify the exact type signature for each Pandas UDF type in a single
        // expression 'PythonUDF'.
        // So use the 'unionOfPandasUdfOut' to cover all types for Spark. The type signature of
        // plugin is also an union of all the types of Pandas UDF.
        (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested() + TypeSig.STRUCT,
        TypeSig.unionOfPandasUdfOut,
        repeatingParamCheck = Some(RepeatingParamCheck(
          "param",
          (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[PythonUDF](a, conf, p, r) {
        override def replaceMessage: String = "not block GPU acceleration"
        override def noReplacementPossibleMessage(reasons: String): String =
          s"blocks running on GPU because $reasons"

        })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  val execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
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
      (flatPy, conf, p, r) => new SparkPlanMeta[FlatMapGroupsInPandasExec](flatPy, conf, p, r) {
        override def replaceMessage: String = "partially run on GPU"
        override def noReplacementPossibleMessage(reasons: String): String =
          s"cannot run even partially on the GPU because $reasons"

        private val groupingAttrs: Seq[BaseExprMeta[Attribute]] =
          flatPy.groupingAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        private val udf: BaseExprMeta[PythonUDF] = GpuOverrides.wrapExpr(
          flatPy.func.asInstanceOf[PythonUDF], conf, Some(this))

        private val resultAttrs: Seq[BaseExprMeta[Attribute]] =
          flatPy.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        override val childExprs: Seq[BaseExprMeta[_]] = groupingAttrs ++ resultAttrs :+ udf
      })
  ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[SparkPlan]), r) }.toMap

}
