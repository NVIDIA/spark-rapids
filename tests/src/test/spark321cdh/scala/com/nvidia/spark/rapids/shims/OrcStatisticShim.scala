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

/*** spark-rapids-shim-json-lines
{"spark": "321cdh"}
{"spark": "330cdh"}
{"spark": "332cdh"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import java.util.Objects

import org.apache.orc._

object OrcStatisticShim {
  def supports(left: ColumnStatistics, right: ColumnStatistics): Boolean = (left, right) match {
    case (_: DateColumnStatistics, _: DateColumnStatistics) => true
    case (_: StringColumnStatistics, _: StringColumnStatistics) => true
    case _ => false
  }

  def equals(left: ColumnStatistics, right: ColumnStatistics): Boolean = (left, right) match {
    // have no CollectionColumnStatistics for this shim
    case (dateStat: DateColumnStatistics, otherDateStat: DateColumnStatistics) =>
      Objects.equals(dateStat.getMinimum, otherDateStat.getMinimum) &&
          Objects.equals(dateStat.getMaximum, otherDateStat.getMaximum)
    case (strStat: StringColumnStatistics, otherStrStat: StringColumnStatistics) =>
      Objects.equals(strStat.getMinimum, otherStrStat.getMinimum) &&
          Objects.equals(strStat.getMaximum, otherStrStat.getMaximum) &&
          Objects.equals(strStat.getSum, otherStrStat.getSum)
  }
}
