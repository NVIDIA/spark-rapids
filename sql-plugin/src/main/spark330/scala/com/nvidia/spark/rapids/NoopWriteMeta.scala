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
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, OverwriteByExpressionExec}

trait NoopWriteHelper {
  // NoopTable is a private class, so we have to use reflection
  private val noopClassNames = Seq("org.apache.spark.sql.execution.datasources.noop.NoopWrite$")

  def isNoopWrite(write: Write): Boolean = {
    noopClassNames.contains(write.getClass.getName)
  }
}

class OverwriteByExpressionExecMeta(
    wrapped: OverwriteByExpressionExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[OverwriteByExpressionExec](wrapped, conf, parent, rule)
  with HasCustomTaggingData with NoopWriteHelper {

  override def tagPlanForGpu(): Unit = {
    if (!isNoopWrite(wrapped.write)) {
      willNotWorkOnGpu(s"Only NoopWrite is currently supported " +
        s"found ${wrapped.write.getClass.getName}")
    }
  }

  override def convertToGpu(): GpuExec = {
    GpuOverwriteByExpressionExec(childPlans.head.convertIfNeeded())
  }
}

class AppendDataExecMeta(
    wrapped: AppendDataExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[AppendDataExec](wrapped, conf, parent, rule)
  with HasCustomTaggingData with NoopWriteHelper {

  override def tagPlanForGpu(): Unit = {
    if (!isNoopWrite(wrapped.write)) {
      willNotWorkOnGpu(s"Only NoopWrite is currently supported " +
        s"found ${wrapped.write.getClass.getName}")
    }
  }

  override def convertToGpu(): GpuExec = {
    GpuAppendDataExec(childPlans.head.convertIfNeeded())
  }
}
