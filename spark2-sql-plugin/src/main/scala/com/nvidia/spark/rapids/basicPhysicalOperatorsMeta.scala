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

package com.nvidia.spark.rapids

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ProjectExec

class GpuProjectExecMeta(
    proj: ProjectExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _]],
    r: DataFromReplacementRule) extends SparkPlanMeta[ProjectExec](proj, conf, p, r)
    with Logging {
      /*
  override def convertToGpu(): GpuExec = {
    // Force list to avoid recursive Java serialization of lazy list Seq implementation
    val gpuExprs = childExprs.map(_.convertToGpu().asInstanceOf[NamedExpression]).toList
    val gpuChild = childPlans.head.convertIfNeeded()
    if (conf.isProjectAstEnabled) {
      if (childExprs.forall(_.canThisBeAst)) {
        return GpuProjectAstExec(gpuExprs, gpuChild)
      }
      // explain AST because this is optional and it is sometimes hard to debug
      if (conf.shouldExplain) {
        val explain = childExprs.map(_.explainAst(conf.shouldExplainAll))
            .filter(_.nonEmpty)
        if (explain.nonEmpty) {
          logWarning(s"AST PROJECT\n$explain")
        }
      }
    }
    GpuProjectExec(gpuExprs, gpuChild)
  }
  */
}

