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
{"spark": "320"}
{"spark": "321"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import java.util.Objects

import org.apache.orc._

object OrcStatisticShim {
  def supports(left: ColumnStatistics, right: ColumnStatistics): Boolean = (left, right) match {
    case (_: DateColumnStatistics, _: DateColumnStatistics) => true
    case (_: StringColumnStatistics, _: StringColumnStatistics) => true
    case (_: CollectionColumnStatistics, _: CollectionColumnStatistics) => true
    case _ => false
  }

  def equals(left: ColumnStatistics, right: ColumnStatistics): Boolean = (left, right) match {
    case (dateStat: DateColumnStatistics, otherDateStat: DateColumnStatistics) =>
      Objects.equals(dateStat.getMinimumLocalDate, otherDateStat.getMinimumLocalDate) &&
          Objects.equals(dateStat.getMaximumLocalDate, otherDateStat.getMaximumLocalDate)
    case (strStat: StringColumnStatistics, otherStrStat: StringColumnStatistics) =>
      Objects.equals(strStat.getLowerBound, otherStrStat.getLowerBound) &&
          Objects.equals(strStat.getUpperBound, otherStrStat.getUpperBound) &&
          Objects.equals(strStat.getMinimum, otherStrStat.getMinimum) &&
          Objects.equals(strStat.getMaximum, otherStrStat.getMaximum) &&
          Objects.equals(strStat.getSum, otherStrStat.getSum)
    case (cStat: CollectionColumnStatistics, otherCStat: CollectionColumnStatistics) =>
      Objects.equals(cStat.getMinimumChildren, otherCStat.getMinimumChildren) &&
          Objects.equals(cStat.getMaximumChildren, otherCStat.getMaximumChildren) &&
          Objects.equals(cStat.getTotalChildren, otherCStat.getTotalChildren)
  }
}
