/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution.TrampolineUtil
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}

class GpuBroadcastMeta(
    exchange: BroadcastExchangeExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule) extends
  SparkPlanMeta[BroadcastExchangeExec](exchange, conf, parent, rule) {

  override def tagPlanForGpu(): Unit = {
    if (!TrampolineUtil.isSupportedRelation(exchange.mode)) {
      willNotWorkOnGpu(
        "Broadcast exchange is only supported for HashedJoin or BroadcastNestedLoopJoin")
    }
    def isSupported(rm: RapidsMeta[_, _]): Boolean = rm.wrapped match {
      case _: BroadcastHashJoinExec => true
      case _: BroadcastNestedLoopJoinExec => true
      case _ => false
    }
    if (parent.isDefined) {
      if (!parent.exists(isSupported)) {
        willNotWorkOnGpu("BroadcastExchange only works on the GPU if being used " +
            "with a GPU version of BroadcastHashJoinExec or BroadcastNestedLoopJoinExec")
      }
    }
  }

  /*
  override def convertToGpu(): GpuExec = {
    GpuBroadcastExchangeExec(exchange.mode, childPlans.head.convertIfNeeded())
  }
  */
}
