/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
{"spark": "400"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.shims

import com.nvidia.spark.rapids.{DataFromReplacementRule, ExprMeta, GpuExpression, GpuOverrides, RapidsConf, RapidsMeta}

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, StructsToJson}
import org.apache.spark.sql.catalyst.expressions.json.StructsToJsonEvaluator
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.rapids.GpuStructsToJson
import org.apache.spark.sql.types._

class InvokeExprMeta(
    invoke: Invoke,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends ExprMeta[Invoke](invoke, conf, p, r) {

  /**
   * Invoke is a dynamic expression, it can have different children.
   */
  override def isChildExprsCountDynamic: Boolean = true

  override final def tagExprForGpu(): Unit = {
    invoke match {
      case Invoke(
      Literal(eval: StructsToJsonEvaluator, _: ObjectType),
      functionName: String,
      _: StringType,
      arguments: Seq[Expression],
      methodInputTypes: Seq[AbstractDataType],
      _: Boolean,
      _: Boolean,
      _: Boolean) if (functionName == "evaluate"
        && arguments.size == 1
        && methodInputTypes.size == 1) =>
      // StructsToJson is equivalent to
      // invoke(literal(StructsToJsonEvaluator), "evaluate", string_type, arguments, ...)
      // here forward the checking to `StructsToJson`, in this way we can avoid duplicated checks
      // definition
      val child = invoke.arguments(0)
      val stj = StructsToJson(eval.options, child, eval.timeZoneId)
      val stjMeta = GpuOverrides.wrapExpr(stj, conf, None)
        stjMeta.initReasons()
        stjMeta.tagForGpu()
      if(!stjMeta.canThisBeReplaced) {
        val sb = new StringBuilder()
        stjMeta.print(sb, depth = 0, all = false)
        willNotWorkOnGpu(sb.toString())
      }
      case _ =>
        // Unsupported invoke expr
        willNotWorkOnGpu(s"Unsupported invoke expr: $invoke")
    }
  }

  override def convertToGpu(): GpuExpression = {
    invoke match {
      case Invoke(
      Literal(evaluator: StructsToJsonEvaluator, _: ObjectType),
      functionName: String,
      _: StringType,
      arguments: Seq[Expression],
      methodInputTypes: Seq[AbstractDataType],
      _: Boolean,
      _: Boolean,
      _: Boolean) if (functionName == "evaluate"
        && arguments.size == 1
        && methodInputTypes.size == 1) =>
        // Supported invoke expr which wraps an StructsToJsonEvaluator
        val child = childExprs(1).convertToGpu().asInstanceOf[Expression]
        GpuStructsToJson(evaluator.options, child, evaluator.timeZoneId)
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported invoke expr: $invoke")
    }
  }
}
