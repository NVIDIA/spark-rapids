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

package com.nvidia.spark.rapids

import org.apache.spark.sql.catalyst.expressions.aggregate._

object GpuBaseAggregateHelper {
  /*
   * The Scala 2.13 compiler's type inferencing is different from 2.12. The normal inferred return
   * type of this is something like `Array[Set[Product with Serializable with AggregateMode]`, 
   * since these are algebraic data types (ADTs) in Scala and the compiler wants the most 
   * precise type possible. The subtype objects of AggregateMode do extend Product and 
   * Serializable, but AggregateMode itself does not. So we need force this return type so that
   * we can use these Sets as expected. Forcing this return type does not work in 2.12, so hence 
   * the split here.
   */
  def getAggPatternsCanReplace(strPatternToReplace: String): Array[Set[AggregateMode]] = {
    strPatternToReplace.split("\\|").map { subPattern =>
        subPattern.split("&").map {
          case "partial" => Partial
          case "partialmerge" => PartialMerge
          case "final" => Final
          case "complete" => Complete
          case s => throw new IllegalArgumentException(s"Invalid Aggregate Mode $s")
        }.toSet
      }
  }
}
