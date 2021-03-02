/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

package org.apache.spark.sql.hive.rapids

import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids.{ExprChecks, ExprMeta, ExprRule, GpuExpression, GpuOverrides, RepeatingParamCheck, TypeSig}
import com.nvidia.spark.rapids.GpuUserDefinedFunction.udfTypeSig

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.hive.{HiveGenericUDF, HiveSimpleUDF}

object GpuHiveOverrides {
  def isSparkHiveAvailable: Boolean = {
    // Using the same approach as SparkSession.hiveClassesArePresent
    val loader = Thread.currentThread().getContextClassLoader
    try {
      Class.forName("org.apache.spark.sql.hive.HiveSessionStateBuilder", true, loader)
      Class.forName("org.apache.hadoop.hive.conf.HiveConf", true, loader)
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

  /**
   * Builds the rules that are specific to spark-hive Catalyst nodes. This will return an empty
   * mapping if spark-hive is unavailable.
   */
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    if (!isSparkHiveAvailable) {
      return Map.empty
    }

    Seq(
      GpuOverrides.expr[HiveSimpleUDF](
        "Hive UDF, support requires the UDF to implement a RAPIDS accelerated interface",
        ExprChecks.projectNotLambda(
          udfTypeSig,
          TypeSig.all,
          repeatingParamCheck = Some(RepeatingParamCheck("param", udfTypeSig, TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[HiveSimpleUDF](a, conf, p, r) {
          override def tagExprForGpu(): Unit = {
            a.function match {
              case _: RapidsUDF =>
              case _ =>
                willNotWorkOnGpu(s"Hive UDF ${a.name} implemented by " +
                    s"${a.funcWrapper.functionClassName} does not provide a GPU implementation")
            }
          }

          override def convertToGpu(): GpuExpression = {
            // To avoid adding a Hive dependency just to check if the UDF function is deterministic,
            // we use the original HiveSimpleUDF `deterministic` method as a proxy.
            GpuHiveSimpleUDF(
              a.name,
              a.funcWrapper,
              childExprs.map(_.convertToGpu()),
              a.dataType,
              a.deterministic)
          }
        }),
      GpuOverrides.expr[HiveGenericUDF](
        "Hive Generic UDF, support requires the UDF to implement a " +
            "RAPIDS accelerated interface",
        ExprChecks.projectNotLambda(
          udfTypeSig,
          TypeSig.all,
          repeatingParamCheck = Some(RepeatingParamCheck("param", udfTypeSig, TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[HiveGenericUDF](a, conf, p, r) {
          override def tagExprForGpu(): Unit = {
            a.function match {
              case _: RapidsUDF =>
              case _ =>
                willNotWorkOnGpu(s"Hive GenericUDF ${a.name} implemented by " +
                    s"${a.funcWrapper.functionClassName} does not provide a GPU implementation")
            }
          }

          override def convertToGpu(): GpuExpression = {
            // To avoid adding a Hive dependency just to check if the UDF function is deterministic,
            // we use the original HiveGenericUDF `deterministic` method as a proxy.
            GpuHiveGenericUDF(
              a.name,
              a.funcWrapper,
              childExprs.map(_.convertToGpu()),
              a.dataType,
              a.deterministic,
              a.foldable)
          }
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }
}
