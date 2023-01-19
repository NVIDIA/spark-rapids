/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import scala.util.Try

import com.nvidia.spark.rapids.{CreatableRelationProviderRule, ExecRule, ShimLoader}
import com.nvidia.spark.rapids.delta.shims.DeltaProviderShims

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.UnshimmedTrampolineUtil
import org.apache.spark.sql.sources.CreatableRelationProvider

/** Interfaces to avoid accessing the optional Delta Lake jars directly in common code. */
trait DeltaProvider {
  def getCreatableRelationRules: Map[Class[_ <: CreatableRelationProvider],
      CreatableRelationProviderRule[_ <: CreatableRelationProvider]]

  def getExecRules: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]]

  def getStrategyRules: Seq[Strategy]
}

class NoDeltaProvider extends DeltaProvider {
  override def getCreatableRelationRules: Map[Class[_ <: CreatableRelationProvider],
      CreatableRelationProviderRule[_ <: CreatableRelationProvider]] = Map.empty

  override def getExecRules: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Map.empty

  override def getStrategyRules: Seq[Strategy] = Nil
}

object DeltaProvider {
  private lazy val provider = {
    val hasDeltaJar = DeltaProviderShims.cpuDataSourceClassName.exists { name =>
      UnshimmedTrampolineUtil.classIsLoadable(name) && Try(ShimLoader.loadClass(name)).isSuccess
    }
    if (hasDeltaJar) {
      ShimLoader.newDeltaProvider()
    } else {
      new NoDeltaProvider()
    }
  }

  def apply(): DeltaProvider = provider
}
