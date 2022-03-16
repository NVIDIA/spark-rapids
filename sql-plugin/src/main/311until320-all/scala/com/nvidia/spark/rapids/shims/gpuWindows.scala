/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.{DataFromReplacementRule, GpuSpecifiedWindowFrameMetaBase, GpuWindowExpressionMetaBase, ParsedBoundary, RapidsConf, RapidsMeta}

import org.apache.spark.sql.catalyst.expressions.{Expression, SpecifiedWindowFrame, WindowExpression}
import org.apache.spark.sql.types.{CalendarIntervalType, DataType, DateType, IntegerType, TimestampType}

class GpuSpecifiedWindowFrameMeta(
    windowFrame: SpecifiedWindowFrame,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: DataFromReplacementRule)
  extends GpuSpecifiedWindowFrameMetaBase(windowFrame, conf, parent, rule) {}

class GpuWindowExpressionMeta(
    windowExpression: WindowExpression,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: DataFromReplacementRule)
  extends GpuWindowExpressionMetaBase(windowExpression, conf, parent, rule) {}

object GpuWindowUtil {

  /**
   * Check if the type of RangeFrame is valid in GpuWindowSpecDefinition
   * @param orderSpecType the first order by data type
   * @param ft the first frame boundary data type
   * @return true to valid, false to invalid
   */
  def isValidRangeFrameType(orderSpecType: DataType, ft: DataType): Boolean = {
    (orderSpecType, ft) match {
      case (DateType, IntegerType) => true
      case (TimestampType, CalendarIntervalType) => true
      case (a, b) => a == b
    }
  }

  def getRangeBoundaryValue(boundary: Expression): ParsedBoundary = boundary match {
    case anything => throw new UnsupportedOperationException("Unsupported window frame" +
      s" expression $anything")
  }
}

