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

package org.apache.spark.sql.hive.rapids

import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids.{ExprChecks, ExprMeta, ExprRule, GpuExpression, GpuOverrides, RapidsConf, RepeatingParamCheck, TypeSig}
import com.nvidia.spark.rapids.GpuUserDefinedFunction.udfTypeSig

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.hive.{HiveGenericUDF, HiveSimpleUDF}

class HiveProviderImpl extends HiveProvider {

  /**
   * Builds the rules that are specific to spark-hive Catalyst nodes. This will return an empty
   * mapping if spark-hive is unavailable.
   */
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    Seq(
      GpuOverrides.expr[HiveSimpleUDF](
        "Hive UDF, the UDF can choose to implement a RAPIDS accelerated interface to" +
            " get better performance",
        ExprChecks.projectOnly(
          udfTypeSig,
          TypeSig.all,
          repeatingParamCheck = Some(RepeatingParamCheck("param", udfTypeSig, TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[HiveSimpleUDF](a, conf, p, r) {
          private val opRapidsFunc = a.function match {
            case rapidsUDF: RapidsUDF => Some(rapidsUDF)
            case _ => None
          }

          override def tagExprForGpu(): Unit = {
            if (opRapidsFunc.isEmpty && !conf.isCpuBasedUDFEnabled) {
              willNotWorkOnGpu(s"Hive SimpleUDF ${a.name} implemented by " +
                  s"${a.funcWrapper.functionClassName} does not provide a GPU implementation " +
                  s"and CPU-based UDFs are not enabled by `${RapidsConf.ENABLE_CPU_BASED_UDF.key}`")
            }
          }

          override def convertToGpu(): GpuExpression = {
            opRapidsFunc.map { _ =>
              // We use the original HiveGenericUDF `deterministic` method as a proxy
              // for simplicity.
              GpuHiveSimpleUDF(
                a.name,
                a.funcWrapper,
                childExprs.map(_.convertToGpu()),
                a.dataType,
                a.deterministic)
            }.getOrElse {
              // This `require` is just for double check.
              require(conf.isCpuBasedUDFEnabled)
              GpuRowBasedHiveSimpleUDF(
                a.name,
                a.funcWrapper,
                childExprs.map(_.convertToGpu()))
            }
          }
        }),
      GpuOverrides.expr[HiveGenericUDF](
        "Hive Generic UDF, the UDF can choose to implement a RAPIDS accelerated interface to" +
            " get better performance",
        ExprChecks.projectOnly(
          udfTypeSig,
          TypeSig.all,
          repeatingParamCheck = Some(RepeatingParamCheck("param", udfTypeSig, TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[HiveGenericUDF](a, conf, p, r) {
          private val opRapidsFunc = a.function match {
            case rapidsUDF: RapidsUDF => Some(rapidsUDF)
            case _ => None
          }

          override def tagExprForGpu(): Unit = {
            if (opRapidsFunc.isEmpty && !conf.isCpuBasedUDFEnabled) {
              willNotWorkOnGpu(s"Hive GenericUDF ${a.name} implemented by " +
                  s"${a.funcWrapper.functionClassName} does not provide a GPU implementation " +
                  s"and CPU-based UDFs are not enabled by `${RapidsConf.ENABLE_CPU_BASED_UDF.key}`")
            }
          }

          override def convertToGpu(): GpuExpression = {
            opRapidsFunc.map { _ =>
              // We use the original HiveGenericUDF `deterministic` method as a proxy
              // for simplicity.
              GpuHiveGenericUDF(
                a.name,
                a.funcWrapper,
                childExprs.map(_.convertToGpu()),
                a.dataType,
                a.deterministic,
                a.foldable)
            }.getOrElse {
              // This `require` is just for double check.
              require(conf.isCpuBasedUDFEnabled)
              GpuRowBasedHiveGenericUDF(
                a.name,
                a.funcWrapper,
                childExprs.map(_.convertToGpu()))
            }
          }
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }
}
