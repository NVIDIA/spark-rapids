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

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, StructsToJson}
import org.apache.spark.sql.catalyst.expressions.json.StructsToJsonEvaluator
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.INVOKE
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{AbstractDataType, ObjectType, StringType}

/**
 *
 * From Spark 400, it transforms the following expr to `Invoke` with `Literal` and `XxEvaluator`
 *   StructsToJson => Invoke(Literal(StructsToJsonEvaluator), "evaluate", type, arguments)
 *   ...
 * This rule is used to do the revert of the above transforms.
 * After the revert, it can still leverage the existing GPU overriding strategy.
 */
object RevertInvokeRule extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan =
    plan.transformExpressionsWithPruning(_.containsPattern(INVOKE)) {
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
          val child = arguments.head
          StructsToJson(evaluator.options, child, evaluator.timeZoneId)
    }
}

object PreRuleShims {
  def getPreRules: Seq[Rule[SparkPlan]] = Seq(RevertInvokeRule)
}
