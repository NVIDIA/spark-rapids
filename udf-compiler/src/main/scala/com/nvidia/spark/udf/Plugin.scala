/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.udf

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

class Plugin extends Function1[SparkSessionExtensions, Unit] with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logWarning("Installing rapids UDF compiler extensions to Spark. The compiler is disabled" +
        s" by default. To enable it, set `${RapidsConf.UDF_COMPILER_ENABLED}` to true")
    extensions.injectResolutionRule(_ => LogicalPlanRules())
  }
}

case class LogicalPlanRules() extends Rule[LogicalPlan] with Logging {
  def replacePartialFunc(plan: LogicalPlan): PartialFunction[Expression, Expression] = {
    case d: Expression => {
      val nvtx = new NvtxRange("replace UDF", NvtxColor.BLUE)
      try {
        attemptToReplaceExpression(plan, d)
      } finally {
        nvtx.close()
      }
    }
  }

  def attemptToReplaceExpression(plan: LogicalPlan, exp: Expression): Expression = {
    val conf = new RapidsConf(plan.conf)
    // iterating over NamedExpression
    exp match {
      case f: ScalaUDF => // found a ScalaUDF
        GpuScalaUDFLogical(f).compile(conf.isTestEnabled)
      case _ =>
        if (exp == null) {
          exp
        } else {
          try {
            if (exp.children != null && !exp.children.exists(x => x == null)) {
              exp.withNewChildren(exp.children.map(c => {
                if (c != null && c.isInstanceOf[Expression]) {
                  attemptToReplaceExpression(plan, c)
                } else {
                  c
                }
              }))
            } else {
              exp
            }
          } catch {
            case _: NullPointerException => {
              exp
            }
          }
        }
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = new RapidsConf(plan.conf)
    if (conf.isUdfCompilerEnabled) {
      plan match {
        case project: Project =>
          Project(project.projectList.map(e => attemptToReplaceExpression(plan, e))
              .asInstanceOf[Seq[NamedExpression]], apply(project.child))
        case x => {
          x.transformExpressions(replacePartialFunc(plan))
        }
      }
    } else {
      plan
    }
  }
}
