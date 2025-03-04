/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "341db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids.{DataFromReplacementRule, GpuExec, RapidsConf, RapidsMeta}

import org.apache.spark.sql.execution.SubqueryBroadcastExec

class GpuSubqueryBroadcastMeta(s: SubqueryBroadcastExec,
                               conf: RapidsConf,
                               p: Option[RapidsMeta[_, _, _]],
                               r: DataFromReplacementRule)
  extends GpuSubqueryBroadcastMeta330DBBase(s, conf, p, r) {

  override def convertToGpu(): GpuExec = {
    GpuSubqueryBroadcastExec(s.name, Seq(s.index), s.buildKeys, broadcastBuilder())(
      getBroadcastModeKeyExprs)
  }

}