/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.lore

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.concurrent.atomic.AtomicInteger

import com.nvidia.spark.rapids.GpuExec

import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}

object IdGen {
  val LORE_ID_TAG: TreeNodeTag[String] = new TreeNodeTag[String]("rapids.gpu.lore.id")

  /**
   * LORE id generator. Key is [[SQLExecution.EXECUTION_ID_KEY]].
   */
  private val idGen: ConcurrentMap[String, AtomicInteger] =
    new ConcurrentHashMap[String, AtomicInteger]()

  private def nextLoreIdOfSparkPlan(plan: SparkPlan): Option[Int] = {
    // When the execution id is not set, it means there is no actual execution happening, in this
    // case we don't need to generate lore id.
    Option(plan.session.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map { executionId =>
        idGen.computeIfAbsent(executionId, _ => new AtomicInteger(0)).getAndIncrement()
      }
  }

  def tagLoreId(sparkPlan: SparkPlan): SparkPlan = {
    sparkPlan.foreachUp {
      case g: GpuExec => {
        nextLoreIdOfSparkPlan(g).foreach { id =>
          g.setTagValue(LORE_ID_TAG, id.toString)
        }
      }
      case _ =>
    }

    sparkPlan
  }

  def loreIdOf(node: SparkPlan): Option[String] = {
    node.getTagValue(LORE_ID_TAG)
  }
}
