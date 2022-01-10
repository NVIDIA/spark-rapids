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

import org.apache.spark.sql.execution.SortExec

sealed trait SortExecType extends Serializable

object OutOfCoreSort extends SortExecType
object FullSortSingleBatch extends SortExecType
object SortEachBatch extends SortExecType

class GpuSortMeta(
    sort: SortExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[SortExec](sort, conf, parent, rule) {

  // Uses output attributes of child plan because SortExec will not change the attributes,
  // and we need to propagate possible type conversions on the output attributes of
  // GpuSortAggregateExec.
  override protected val useOutputAttributesOfChild: Boolean = true

  // For transparent plan like ShuffleExchange, the accessibility of runtime data transition is
  // depended on the next non-transparent plan. So, we need to trace back.
  override val availableRuntimeDataTransition: Boolean =
    childPlans.head.availableRuntimeDataTransition

    /*
  override def convertToGpu(): GpuExec = {
    GpuSortExec(childExprs.map(_.convertToGpu()).asInstanceOf[Seq[SortOrder]],
      sort.global,
      childPlans.head.convertIfNeeded(),
      if (conf.stableSort) FullSortSingleBatch else OutOfCoreSort
    )(sort.sortOrder)
  }
  */
}

