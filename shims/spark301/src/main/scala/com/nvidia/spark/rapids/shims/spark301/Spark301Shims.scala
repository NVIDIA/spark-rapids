/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark301

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.spark300.Spark300Shims
import com.nvidia.spark.rapids.spark301.RapidsShuffleManager

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{First, Last}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

class Spark301Shims extends Spark300Shims {

  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  def exprs301: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[First](
      "first aggregate operator",
      (a, conf, p, r) => new ExprMeta[First](a, conf, p, r) {
        override def convertToGpu(): GpuExpression =
          GpuFirst(childExprs(0).convertToGpu(), a.ignoreNulls)
      }),
    GpuOverrides.expr[Last](
      "last aggregate operator",
      (a, conf, p, r) => new ExprMeta[Last](a, conf, p, r) {
        override def convertToGpu(): GpuExpression =
          GpuLast(childExprs(0).convertToGpu(), a.ignoreNulls)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    super.getExprs ++ exprs301
  }

  override def getRapidsShuffleManagerClass: String = {
    classOf[RapidsShuffleManager].getCanonicalName
  }

  override def injectQueryStagePrepRule(
      extensions: SparkSessionExtensions,
      ruleBuilder: SparkSession => Rule[SparkPlan]): Unit = {
    extensions.injectQueryStagePrepRule(ruleBuilder)
  }
}
