/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
{"spark": "350"}
{"spark": "351"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{Expression, PythonUDAF, ToPrettyString}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.WindowGroupLimitExec
import org.apache.spark.sql.rapids.execution.python.GpuPythonUDAF
import org.apache.spark.sql.types.StringType

object SparkShimImpl extends Spark340PlusNonDBShims {

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[ToPrettyString]("An internal expressions which is used to " +
        "generate pretty string for all kinds of values",
        new ToPrettyStringChecks(),
        (toPrettyString, conf, p, r) => {
          new CastExprMetaBase[ToPrettyString](toPrettyString, conf, p, r) {

            override val toType: StringType.type = StringType

            override def convertToGpu(child: Expression): GpuExpression = {
              GpuToPrettyString(child)
            }
          }
      }), 
      GpuOverrides.expr[PythonUDAF](
        "UDF run in an external python process. Does not actually run on the GPU, but " +
          "the transfer of data to/from it can be accelerated",
        ExprChecks.fullAggAndProject(
          // Different types of Pandas UDF support different sets of output type. Please refer to
          //   https://github.com/apache/spark/blob/master/python/pyspark/sql/udf.py#L98
          // for more details.
          // It is impossible to specify the exact type signature for each Pandas UDF type in a
          // single expression 'PythonUDF'.
          // So use the 'unionOfPandasUdfOut' to cover all types for Spark. The type signature of
          // plugin is also an union of all the types of Pandas UDF.
          (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested() + TypeSig.STRUCT,
          TypeSig.unionOfPandasUdfOut,
          repeatingParamCheck = Some(RepeatingParamCheck(
            "param",
            (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[PythonUDAF](a, conf, p, r) {
          override def replaceMessage: String = "not block GPU acceleration"

          override def noReplacementPossibleMessage(reasons: String): String =
            s"blocks running on GPU because $reasons"

          override def convertToGpu(): GpuExpression =
            GpuPythonUDAF(a.name, a.func, a.dataType,
              childExprs.map(_.convertToGpu()),
              a.evalType, a.udfDeterministic, a.resultId)
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ shimExprs
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]]= Seq(
      GpuOverrides.exec[WindowGroupLimitExec](
        "Apply group-limits for row groups destined for rank-based window functions like " +
          "row_number(), rank(), and dense_rank()",
        ExecChecks( // Similar to WindowExec.
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
           TypeSig.STRUCT +  TypeSig.ARRAY + TypeSig.MAP).nested(),
          TypeSig.all),
        (limit, conf, p, r) => new GpuWindowGroupLimitExecMeta(limit, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
    super.getExecs ++ shimExecs
  }
}
