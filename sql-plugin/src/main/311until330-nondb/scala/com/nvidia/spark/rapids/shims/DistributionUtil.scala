/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, BroadcastDistribution, ClusteredDistribution, Distribution, HashClusteredDistribution, OrderedDistribution, UnspecifiedDistribution}

object DistributionUtil {

  def isSupported(distributions: Seq[Distribution]): Boolean = {
    val finalRes = distributions.map { dist =>
      dist match {
        case _: UnspecifiedDistribution.type => true // UnspecifiedDistribution is case object
        case AllTuples.type => true
        case b: BroadcastDistribution => TrampolineUtil.isSupportedRelation(b.mode)
        case _: ClusteredDistribution => true
        case _: OrderedDistribution => true
        case _: HashClusteredDistribution => true
        case _ => false
      }
    }
    finalRes.forall(_ == true)
  }
}
