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

//import ai.rapids.cudf.{NvtxColor, NvtxRange}
//import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
//import org.apache.spark.sql.rapids.GpuScalaUDF.getRapidsUDFInstance

class Plugin extends Function1[SparkSessionExtensions, Unit] with Logging {
  //Console.println("udf compiler is enabled")
  override def apply(extensions: SparkSessionExtensions): Unit = {
    //Console.println("udf compiler is enabled")
    //logWarning("Installing rapids UDF compiler extensions to Spark. The compiler is disabled" +
      //  s" by default. To enable it, set `${RapidsConf.UDF_COMPILER_ENABLED}` to true")
    extensions.injectResolutionRule(_ => LogicalPlanRules())
  }
}

case class LogicalPlanRules() extends Rule[LogicalPlan] with Logging {
  //Console.println("udf compiler logical plan inserted")
  def replacePartialFunc(plan: LogicalPlan): PartialFunction[Expression, Expression] = {
    case d: Expression => {
      //val nvtx = new NvtxRange("replace UDF", NvtxColor.BLUE)
      //Console.println("udf compiler expression is enabled")
      try {
        attemptToReplaceExpression(plan, d)
      } finally {
        //nvtx.close()
      }
    }
  }

  def attemptToReplaceExpression(plan: LogicalPlan, exp: Expression): Expression = {
    //val conf = new RapidsConf(plan.conf)
    // iterating over NamedExpression
    //Console.println("udf compiler attempt to replace is enabled")
    exp match {
      // Check if this UDF implements RapidsUDF interface. If so, the UDF has already provided a
      // columnar execution that could run on GPU, then no need to translate it to Catalyst
      // expressions. If not, compile it.
      //case f: ScalaUDF if getRapidsUDFInstance(f.function).isEmpty =>
      case f: ScalaUDF =>
        GpuScalaUDFLogical(f).compile(true)
      case _ =>
        if (exp == null) {
          exp
        } else {
          try {
            if (exp.children != null && !exp.children.contains(null)) {
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
    //val conf = new RapidsConf(plan.conf)
    //Console.println("udf compiler attempt to replace is enabled: apply")
    //if (conf.isUdfCompilerEnabled) {
    if (true) {
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
