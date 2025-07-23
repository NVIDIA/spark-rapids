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

package com.nvidia.spark.rapids.lore

import scala.collection.mutable

import java.util
import org.apache.commons.lang3.reflect.FieldUtils

import org.apache.spark.sql.execution.SparkPlan

trait GpuLoreSupport {
  self: SparkPlan =>

  /**
   * This is quite similar to calculating the hash code of a spark plan.
   * The difference is that we want lore id to be the same even for hash joins where their
   * children are swapped, that's why we added a sort for product elements' hash codes.
   *
   * We rule out several solutions:
   * 1. Logical plan, which is not available after gpu conversion.
   * 2. Text representation of plan, since minor changes to text plan representation may break it.
   */
  private lazy val _gpuLoreId: Int = {
    val arr = this.productArity
    // Case objects have the hashCode inlined directly into the
    // synthetic hashCode method, but this method should still give
    // a correct result if passed a case object.
    if (arr == 0) {
      this.productPrefix.hashCode
    } else {
      var h = scala.util.hashing.MurmurHash3.productSeed
      h = scala.util.hashing.MurmurHash3.mix(h, this.productPrefix.hashCode)
      val hashCodes = new Array[Int](arr)
      for (i <- 0 until arr) {
        val element = this.productElement(i)
        hashCodes(i) = element match {
          case g: GpuLoreSupport => g.loreId()
          case _ => element.##
        }
      }
      // Since murmur hash 3 is order-sensitive, we add this sort here to avoid hash code changes
      // like a hash join where the left and right children are swapped.
      util.Arrays.sort(hashCodes)
      for (hashCode <- hashCodes) {
        h = scala.util.hashing.MurmurHash3.mix(h, hashCode)
      }
      scala.util.hashing.MurmurHash3.finalizeHash(h, arr)
    }
  }

  def loreId(): Int = {
    _gpuLoreId
  }

  def prepareForLoreSerialization(): SparkPlan = {
    val newPlan = this.clone()

    doCleanUpTagsForLoreDump(newPlan)
    doReplaceInputForLoreDump(newPlan)
  }

  protected def doCleanUpTagsForLoreDump(p: SparkPlan): Unit = {
    FieldUtils.getField(classOf[SparkPlan], "tags", true)
      .get(p)
      .asInstanceOf[mutable.Map[_, _]]
      .clear()
  }

  protected def doReplaceInputForLoreDump(p: SparkPlan): SparkPlan = {
    val childrenCount = p.children.length

    if (childrenCount > 0) {
      p.withNewChildren((0 until childrenCount).map(_ => GpuNoopExec()))
    } else {
      p
    }
  }
}
