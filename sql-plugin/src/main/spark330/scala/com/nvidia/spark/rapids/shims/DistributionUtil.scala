/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, BroadcastDistribution, ClusteredDistribution, Distribution, OrderedDistribution, StatefulOpClusteredDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.rapids.execution.TrampolineUtil

object DistributionUtil {

  def isSupported(distribution: Distribution): Boolean = {
    distribution match {
      case UnspecifiedDistribution => true
      case AllTuples => true
      case b: BroadcastDistribution => TrampolineUtil.isSupportedRelation(b.mode)
      case _: ClusteredDistribution => true
      case _: OrderedDistribution => true
      case _: StatefulOpClusteredDistribution => true
      case _ => false
    }
  }
}
